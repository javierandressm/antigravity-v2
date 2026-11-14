"""Pure helpers for presenting Binance ledger entries and reconciled AG closes."""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable


BINANCE_CLOSURE_SOURCE = "BINANCE_FUTURES_USER_TRADES_AND_ALGO_HISTORY"
BINANCE_PNL_ATTRIBUTION = "BINANCE_FUTURES_USER_TRADES"


def reconciliation_start_ms(symbol: str, metadata: dict, entry_order: dict) -> int:
    """Use the owned Binance entry timestamp, including fills before the local clock."""
    if (not isinstance(entry_order, dict) or entry_order.get("error")
            or entry_order.get("symbol") != symbol
            or str(entry_order.get("orderId")) != str(metadata.get("entry_order_id"))
            or entry_order.get("status") != "FILLED"):
        raise ValueError("Binance entry identity/fill not confirmed for reconciliation")
    venue_time = int(entry_order.get("time", 0) or 0)
    if venue_time <= 0:
        raise ValueError("Binance entry timestamp missing")
    return min(int(float(metadata["opened_at"]) * 1000), venue_time)


def confirmed_tp1_order_id(symbol: str, metadata: dict, algo: dict) -> str | None:
    """Attribute a native TP1 only to the exact persisted owned, finished Algo."""
    if not isinstance(algo, dict):
        return None
    algo_id = str(metadata.get("tp_algo_id") or "")
    client_id = metadata.get("algo_client_ids", {}).get(algo_id)
    if (not algo_id or not client_id or str(algo.get("algoId")) != algo_id
            or algo.get("clientAlgoId") != client_id or algo.get("symbol") != symbol
            or algo.get("algoStatus") != "FINISHED"
            or algo.get("orderType") != "TAKE_PROFIT_MARKET"
            or algo.get("side") != ("SELL" if metadata.get("side") == "LONG" else "BUY")
            or algo.get("positionSide", "BOTH") != metadata.get("position_side", "BOTH")):
        return None
    actual = str(algo.get("actualOrderId") or "")
    return actual if actual and actual != "0" else None


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _time_text(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")


def infer_accounting_reset_ms(state: dict, fallback_ms: int = 0) -> int:
    """Choose the ledger window from persisted AG history when no reset is saved.

    A flat account can still have completed trades. Never move its history start
    to the current request time just because there are no open positions.
    """
    if not isinstance(state, dict):
        return max(0, int(fallback_ms or 0))
    try:
        explicit = int(state.get("reset_timestamp_ms") or 0)
    except (TypeError, ValueError, OverflowError):
        explicit = 0
    if explicit > 0:
        return explicit

    candidates: list[int] = []

    def add_seconds(value: Any) -> None:
        number = _finite(value)
        if number is not None and number > 0:
            candidates.append(int(number * 1000))

    def add_milliseconds(value: Any) -> None:
        number = _finite(value)
        if number is not None and number > 0:
            candidates.append(int(number))

    positions = state.get("positions", {})
    if isinstance(positions, dict):
        for metadata in positions.values():
            if isinstance(metadata, dict):
                add_seconds(metadata.get("opened_at"))
    for event in (state.get("closed_positions", {}) or {}).values() if isinstance(state.get("closed_positions"), dict) else []:
        metadata = event.get("metadata", {}) if isinstance(event, dict) else {}
        if isinstance(metadata, dict):
            add_seconds(metadata.get("opened_at"))
    for closure in state.get("reconciled_closures", []) if isinstance(state.get("reconciled_closures"), list) else []:
        if not isinstance(closure, dict):
            continue
        add_seconds(closure.get("opened_at"))
        fills = closure.get("matched_trades", [])
        if isinstance(fills, list):
            for fill in fills:
                if isinstance(fill, dict):
                    add_milliseconds(fill.get("time"))
    for failure in state.get("protection_failures", []) if isinstance(state.get("protection_failures"), list) else []:
        if not isinstance(failure, dict):
            continue
        for key in ("entry_fill", "safety_close_fill"):
            fill = failure.get(key)
            if isinstance(fill, dict):
                add_milliseconds(fill.get("time"))
        add_seconds(failure.get("detected_at"))

    return min(candidates) if candidates else max(0, int(fallback_ms or 0))


def verified_reconciled_closures(state: dict, reset_ts_ms: int = 0) -> list[dict]:
    """Return only closures whose Binance fill evidence and exact PnL are persisted."""
    closures = state.get("reconciled_closures", []) if isinstance(state, dict) else []
    if not isinstance(closures, list):
        return []
    unique: dict[str, dict] = {}
    for item in closures:
        if not isinstance(item, dict):
            continue
        key = str(item.get("closure_key") or "")
        pnl = _finite(item.get("net_pnl_usdt"))
        fills = item.get("matched_trades")
        if (not key or str(item.get("status", "")).upper() != "RECONCILED"
                or item.get("source") != BINANCE_CLOSURE_SOURCE
                or item.get("pnl_attribution") != BINANCE_PNL_ATTRIBUTION
                or pnl is None or not isinstance(fills, list) or not fills):
            continue
        tracked_orders = {str(value) for value in item.get("tracked_order_ids", []) if value is not None}
        matched_orders = {str(value) for value in item.get("matched_order_ids", []) if value is not None}
        trade_ids = {str(value) for value in item.get("matched_trade_ids", []) if value is not None}
        valid_fills = [
            fill for fill in fills if isinstance(fill, dict)
            and str(fill.get("orderId", "")) in tracked_orders
            and str(fill.get("orderId", "")) in matched_orders
            and str(fill.get("id", "")) in trade_ids
            and str(fill.get("symbol", item.get("symbol", ""))) == str(item.get("symbol", ""))
        ]
                                                                               
        if not valid_fills or {str(fill.get("id")) for fill in valid_fills} != trade_ids:
            continue
                                                                             
                                                                                
        valid_order_ids = {str(fill.get("orderId")) for fill in valid_fills}
        if matched_orders != valid_order_ids:
            continue
        buy_qty = sum(_finite(fill.get("qty")) or 0.0 for fill in valid_fills
                      if str(fill.get("side", "")).upper() == "BUY")
        sell_qty = sum(_finite(fill.get("qty")) or 0.0 for fill in valid_fills
                       if str(fill.get("side", "")).upper() == "SELL")
        if buy_qty <= 0 or sell_qty <= 0 or not math.isclose(buy_qty, sell_qty, rel_tol=1e-9, abs_tol=1e-10):
            continue
        try:
            closed_ms = int(float(item.get("closed_at", 0)) * 1000)
        except (TypeError, ValueError, OverflowError):
            continue
        if closed_ms <= 0 or closed_ms < int(reset_ts_ms or 0):
            continue
        row = dict(item)
        row["_closed_ms"] = closed_ms
        row["_verified_net_pnl"] = pnl
        row["_valid_fills"] = valid_fills
        unique[key] = row
    return sorted(unique.values(), key=lambda item: (item["_closed_ms"], item["closure_key"]))


def build_dashboard_ledger(
    income_items: Iterable[dict],
    owned_tp1_trade_keys: set[tuple[str, str]],
    state: dict,
    *,
    reset_ts_ms: int,
    base_equity: float,
    tp1_commissions: dict[tuple[str, str], float] | None = None,
) -> dict:
    """Merge owned Binance TP1 income and exact reconciled position-close rows.

    ``tp1_commissions`` maps (symbol, tradeId) to the commission Binance charged
    on that TP1 fill; when present the TP1 row is shown net, like the closures.

    Reconciled closure fills supersede any individual TP1 income rows belonging
    to the same close, preventing a partial fill from being counted twice.
    """
    closures = verified_reconciled_closures(state, reset_ts_ms)
    closure_fill_keys = {
        (str(closure.get("symbol", "")), str(fill.get("id", "")))
        for closure in closures for fill in closure["_valid_fills"]
    }

    events: list[dict] = []
    for item in income_items if isinstance(income_items, Iterable) else []:
        if not isinstance(item, dict):
            continue
        try:
            timestamp_ms = int(item.get("time", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            continue
        symbol = str(item.get("symbol", ""))
        trade_id = str(item.get("tradeId", ""))
        key = (symbol, trade_id)
        amount = _finite(item.get("income"))
        if (timestamp_ms < int(reset_ts_ms or 0) or amount is None
                or key not in owned_tp1_trade_keys or key in closure_fill_keys):
            continue
        commission = _finite((tp1_commissions or {}).get(key))
        if commission:
            amount = round(amount - commission, 8)
            reason = "TP1 parcial · neto tras comisión Binance"
        else:
            reason = "TP1 parcial · asiento REALIZED_PNL Binance"
        events.append({
            "time": _time_text(timestamp_ms), "timestamp": timestamp_ms,
            "symbol": symbol, "coin": symbol.replace("USDT", ""),
            "realized_pnl": amount, "reason": reason,
            "trade_id": trade_id, "tran_id": str(item.get("tranId", "")),
            "record_type": "BINANCE_TP1_INCOME", "is_position_closure": False,
        })

    closure_trade_keys = set()
    completed_pnls: list[float] = []
    for closure in closures:
        symbol = str(closure.get("symbol", ""))
        pnl = closure["_verified_net_pnl"]
        closed_ms = closure["_closed_ms"]
        matched_ids = [str(value) for value in closure.get("matched_trade_ids", [])]
        closure_trade_keys.update((symbol, trade_id) for trade_id in matched_ids)
        classification = str(closure.get("classification", "VERIFIED_CLOSE")).upper()
        reason_by_classification = {
            "STOP_MARKET_TRIGGERED": "Cierre conciliado · Stop ejecutado en Binance",
            "TAKE_PROFIT_MARKET_TRIGGERED": "Cierre conciliado · Take Profit ejecutado en Binance",
            "PROTECTION_FAILURE_SAFETY_CLOSE": "Cierre de seguridad conciliado en Binance",
        }
        details = []
        for fill in closure["_valid_fills"]:
            fill_time = int(fill.get("time", 0) or 0)
            realized = _finite(fill.get("realizedPnl")) or 0.0
            commission = _finite(fill.get("commission")) or 0.0
            details.append({
                "trade_id": str(fill.get("id", "")),
                "order_id": str(fill.get("orderId", "")),
                "side": str(fill.get("side", "")),
                "quantity": str(fill.get("qty", "")),
                "price": str(fill.get("price", "")),
                "time": _time_text(fill_time) if fill_time else "",
                "net_fill_pnl": realized - commission,
            })
        events.append({
            "time": _time_text(closed_ms), "timestamp": closed_ms,
            "symbol": symbol, "coin": symbol.replace("USDT", ""),
            "realized_pnl": pnl, "reason": reason_by_classification.get(
                classification, "Cierre de posición conciliado con Binance"),
            "trade_id": str(closure.get("closure_key")),
            "tran_id": ",".join(matched_ids),
            "record_type": "RECONCILED_CLOSURE", "is_position_closure": True,
            "closure_key": str(closure.get("closure_key")),
            "side": closure.get("side"), "classification": classification,
            "binance_order_ids": [str(value) for value in closure.get("matched_order_ids", [])],
            "matched_trade_ids": matched_ids, "matched_fills": details,
            "pnl_attribution": closure.get("pnl_attribution"),
            "source": closure.get("source"),
                                                                                                      
            "supporters": [str(w) for w in (closure.get("supporters") or []) if w],
            "opened_ts": int(_finite(closure.get("opened_at")) * 1000) if _finite(closure.get("opened_at")) else None,
        })
        completed_pnls.append(pnl)

    events.sort(key=lambda item: (int(item["timestamp"]), item["symbol"], item["record_type"]))
    running_equity = float(base_equity)
    equity_history = []
    reset_ms = int(reset_ts_ms or 0)
    if reset_ms <= 0:
        reset_ms = min((int(item["timestamp"]) for item in events), default=0)
    if reset_ms > 0:
        equity_history.append({
            "time": _time_text(reset_ms), "timestamp": reset_ms,
            "equity": round(running_equity, 8), "pnl": 0.0,
            "roi_pct": 0.0, "event": "Calibración Inicial", "symbol": "BASE",
        })
    for item in events:
        amount = float(item["realized_pnl"])
        running_equity += amount
        item["running_equity"] = round(running_equity, 8)
        item["roi_pct"] = round((running_equity - float(base_equity)) / float(base_equity) * 100.0, 8) if base_equity else 0.0
        equity_history.append({
            "time": item["time"], "timestamp": item["timestamp"],
            "equity": round(running_equity, 8), "pnl": amount,
            "roi_pct": item["roi_pct"], "event": item["reason"], "symbol": item["symbol"],
        })

    gross_profit = sum(pnl for pnl in completed_pnls if pnl > 0)
    gross_loss = sum(-pnl for pnl in completed_pnls if pnl < 0)
    nonzero = sum(1 for pnl in completed_pnls if pnl != 0)
    stats = {
        "ledger_entries": len(events),
        "closed_operations": len(completed_pnls),
        "wins_count": sum(1 for pnl in completed_pnls if pnl > 0),
        "losses_count": sum(1 for pnl in completed_pnls if pnl < 0),
        "win_rate": round(sum(1 for pnl in completed_pnls if pnl > 0) / nonzero * 100.0, 2) if nonzero else None,
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "total_profit": round(gross_profit, 8), "total_loss": round(gross_loss, 8),
    }
    return {
        "events_chronological": events,
        "closed_trades": list(reversed(events)),
        "equity_history": equity_history,
        "realized_pnl": round(sum(float(item["realized_pnl"]) for item in events), 8),
        "stats": stats,
        "verified_closure_count": len(closures),
        "closure_fill_trade_keys": closure_trade_keys,
    }
