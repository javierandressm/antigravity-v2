"""Read-only AG exposure observer; errors cannot disarm or mutate the trading state."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from antigravity_config import load_bot_config
from antigravity_runtime import atomic_write_json, read_json
from binance_futures_client import BinanceFuturesClient
from portfolio_exposure import PortfolioExposureEngine

ROOT = Path(__file__).resolve().parent
MARKET_PATH = ROOT / "ag_portfolio_market.json"
SNAPSHOT_PATH = ROOT / "ag_portfolio_exposure.json"
EVENTS_PATH = ROOT / "ag_portfolio_observations.jsonl"
ENTRY_PATH = ROOT / "ag_portfolio_entry_assessments.jsonl"
_JOURNAL_LOCK = threading.Lock()


def append_event(path, value):
    with _JOURNAL_LOCK:
        with path.open("a", encoding="utf-8") as target:
            target.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")


def live_legs(bridge, positions, algos, fee_rates):
    """Normalize real remaining quantities; only strictly owned active Algo stops count."""
    legs = []
    config = load_bot_config()
    for p in positions:
        amount = float(p.get("positionAmt", 0) or 0)
        if not amount:
            continue
        symbol = p["symbol"]
        meta = bridge.state.get("positions", {}).get(symbol, {})
        side = "LONG" if amount > 0 else "SHORT"
        sl_target = meta.get("current_sl_price") or meta.get("initial_sl_price")
        owned = bool(meta and meta.get("side") == side)
        matched = next((a for a in algos if owned and sl_target is not None and bridge._owned_algo_order_matches(
            a, meta, symbol=symbol, order_type="STOP_MARKET", side="SELL" if amount > 0 else "BUY",
            position_side=p.get("positionSide", "BOTH"), trigger_price=sl_target, close_position=True)), None)
        legs.append({"symbol": symbol, "side": side, "qty": abs(amount),
                     "entry": float(p["entryPrice"]), "mark": float(p["markPrice"]),
                     "sl": float(matched["triggerPrice"]) if matched else None,
                     "sl_confirmed": matched is not None, "sl_algo_id": matched.get("algoId") if matched else None,
                     "owned": owned, "leverage": float(p.get("leverage", 0) or 0),
                     "fee_rate": fee_rates.get(symbol, meta.get("portfolio_fee_rate")),
                     "slippage_pct": config["risk"]["slippage_reserve_pct"]})
    return legs


def assess_entry(bridge, positions, algos, candidate, *, equity, available, signal_metadata=None,
                 preview_only=False, reserved_entries=0):
    """Measure the exact sized entry against a fresh Binance account snapshot."""
    config = load_bot_config(); settings = config["portfolio_exposure"]
    cache = read_json(MARKET_PATH); now = time.time()
    legs = live_legs(bridge, positions, algos, cache.get("fee_rates", {}))
    engine = PortfolioExposureEngine(settings)
    account = bridge.client.get_account_info()
    valid_account = isinstance(account, dict) and all(k in account for k in ("totalMarginBalance", "totalInitialMargin", "availableBalance"))
    measured_equity = float(account["totalMarginBalance"]) if valid_account else equity
    used = float(account["totalInitialMargin"]) if valid_account else None
    free = float(account["availableBalance"]) if valid_account else None
    args = dict(equity=measured_equity, series=cache.get("series", {}), asof=now, account_asof=now,
                margin_source="BINANCE_ACCOUNT_INITIAL_MARGIN")
    before = engine.measure(legs, margin_used=used, available=free, **args)
    candidate = dict(candidate, planned=True, sl_confirmed=False,
                     slippage_pct=config["risk"]["slippage_reserve_pct"])
    required = candidate["qty"] * candidate["mark"] / candidate["leverage"]
    after = engine.measure(legs + [candidate], margin_used=used+required if used is not None else None,
                           available=free-required if free is not None else None, **args)
    signal_metadata = signal_metadata or {}
    normal_passed = signal_metadata.get("organic_ag_filters_passed") is True
    decision = engine.capacity_decision(before, after, normal_filters_passed=normal_passed,
                                        reserved_entries=reserved_entries)
    decision["fresh_equity_usdt"] = measured_equity if valid_account else None
    decision["fresh_available_margin_usdt"] = free
    event = {"recorded_at": now, "source": "AG_PRE_SUBMIT_EXACT_SIZED_PLAN", "network": "BINANCE_TESTNET" if bridge.client.testnet else "BINANCE_MAINNET",
             "normal_filters_passed": normal_passed, "signal": signal_metadata, "candidate": candidate,
             "read_only_capacity_preview": preview_only,
             "sizing_equity_usdt": equity, "sizing_available_usdt": available,
             "reserved_entries": reserved_entries,
             "venue_margin_leverage_confirmation": "REQUIRED_AT_REAL_SUBMIT" if preview_only else "CONFIRMED",
             "before": before, "after": after, "capacity_decision": decision,
             "production_limit": config["operations"]["max_concurrent_positions"], "execution_effect": "NONE"}
    append_event(ENTRY_PATH, event)
    return decision


class PortfolioObserver:
    def __init__(self):
        self.config = load_bot_config(); self.settings = self.config["portfolio_exposure"]
        self.client = BinanceFuturesClient()
        self.fee_rates = {}; self.fees_at = {}; self.last_markets = {}

    def read(self, endpoint, params=None, signed=True):
                                                                          
        return self.client._request("GET", endpoint, params or {}, signed=signed)

    def capture(self):
        from binance_execution_bridge import BinanceExecutionBridge
        bridge = BinanceExecutionBridge(self.client)
        account = self.read("/fapi/v2/account")
        positions = self.read("/fapi/v2/positionRisk")
        algos = self.read("/fapi/v1/openAlgoOrders")
        if (not isinstance(account, dict) or "totalMarginBalance" not in account
                or not isinstance(positions, list) or not isinstance(algos, list)):
            raise RuntimeError("Read-only exposure snapshot unavailable")
        asof = time.time()
        active = [p for p in positions if float(p.get("positionAmt", 0) or 0)]
        feed = read_json(ROOT / "live_whale_consensus.json")
        candidates = {str(s.get("raw_symbol") or s.get("symbol", "").replace("/", ""))
                      for s in feed.get("signals", [])
                      if s.get("status") in {"CONFIRMED_ONCHAIN", "CONFIRMED_HYBRID"}}
        symbols = sorted({p["symbol"] for p in active} | candidates | {"BTCUSDT"})
        cache = read_json(MARKET_PATH); series = cache.get("series", {})
        for symbol in symbols:
            if asof - self.last_markets.get(symbol, 0) >= float(self.settings["market_refresh_seconds"]):
                rows = self.read("/fapi/v1/klines", {"symbol": symbol, "interval": self.settings["bar_interval"],
                                "limit": int(self.settings["lookback_bars"])+2}, signed=False)
                if isinstance(rows, list) and rows and isinstance(rows[0], list):
                    series[symbol] = [{"t": int(r[6]), "close": float(r[4])} for r in rows if int(r[6]) < asof*1000]
                    self.last_markets[symbol] = asof
            if symbol != "BTCUSDT" or any(p["symbol"] == symbol for p in active):
                if asof-self.fees_at.get(symbol, 0) >= float(self.settings["market_refresh_seconds"]):
                    result = self.read("/fapi/v1/commissionRate", {"symbol": symbol})
                    if isinstance(result, dict) and result.get("takerCommissionRate") is not None:
                        self.fee_rates[symbol] = float(result["takerCommissionRate"]); self.fees_at[symbol] = asof
        atomic_write_json(MARKET_PATH, {"asof": asof, "interval": self.settings["bar_interval"],
                          "series": series, "fee_rates": self.fee_rates, "source": "BINANCE_FAPI_CLOSED_KLINES"})
        metrics = PortfolioExposureEngine(self.settings).measure(
            live_legs(bridge, active, algos, self.fee_rates), equity=float(account["totalMarginBalance"]),
            margin_used=float(account["totalInitialMargin"]), available=float(account["availableBalance"]),
            series=series, asof=asof, account_asof=asof)
        feed = read_json(ROOT / "live_whale_consensus.json")
        feed_age = asof-float(feed.get("timestamp", 0) or 0)
        confirmed = [{k: signal.get(k) for k in ("symbol", "raw_symbol", "side", "fingerprint", "status", "reference_price", "total_votes", "hl_votes", "supporters")}
                     for signal in feed.get("signals", [])
                     if signal.get("status") in {"CONFIRMED_ONCHAIN", "CONFIRMED_HYBRID"}]
        base_capacity = int(self.settings["base_capacity"])
        maximum_capacity = int(self.config["operations"]["max_concurrent_positions"])
        position_count = len(active)
        if position_count >= maximum_capacity:
            capacity_status = "MAXIMUM_CAPACITY_REACHED"
        elif position_count >= base_capacity:
            capacity_status = "CONDITIONAL_SLOT_REQUIRES_VALID_CANDIDATE"
        else:
            capacity_status = "BASE_SLOT_AVAILABLE"
        snapshot = {"status": "ok", "asof": asof, "observer_pid": __import__("os").getpid(),
                    "network": "BINANCE_TESTNET" if self.client.testnet else "BINANCE_MAINNET",
                    "production_limit": maximum_capacity,
                    "base_capacity": base_capacity,
                    "maximum_capacity": maximum_capacity,
                    "current_positions": position_count,
                    "additional_slots_available": max(0, maximum_capacity - position_count),
                    "base_capacity_reached": position_count >= base_capacity,
                    "maximum_capacity_reached": position_count >= maximum_capacity,
                    "capacity_status": capacity_status,
                    "delta_aware_eligibility": "NOT_EVALUATED_NO_CANDIDATE",
                    "conditional_slots_enabled": self.settings["conditional_slots_enabled"] is True, "metrics": metrics,
                    "feed_age_seconds": feed_age, "feed_status": feed.get("status"),
                    "organic_consensus_observations": confirmed,
                    "candidate_filter_status": "NOT_EVALUATED_NO_CANDIDATE",
                                                                                  
                                                            
                    "current_capacity_full": position_count >= maximum_capacity}
        atomic_write_json(SNAPSHOT_PATH, snapshot)
        append_event(EVENTS_PATH, snapshot)
        return snapshot

    def run(self):
        while True:
            try:
                self.capture()
            except Exception as exc:
                atomic_write_json(SNAPSHOT_PATH, {"status": "unavailable", "asof": time.time(),
                    "error": str(exc)[:400], "conditional_slots_enabled": self.settings["conditional_slots_enabled"] is True,
                    "production_limit": self.config["operations"]["max_concurrent_positions"]})
            time.sleep(float(self.settings["observer_interval_seconds"]))


def start_observer():
    observer = PortfolioObserver()
    thread = threading.Thread(target=observer.run, name="AGPortfolioObserver", daemon=True)
    thread.start()
    return thread
