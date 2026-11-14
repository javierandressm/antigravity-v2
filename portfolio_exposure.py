"""AG portfolio measurements and advisory capacity decisions. No exchange writes."""
from __future__ import annotations

import math
import hashlib
from itertools import combinations
from statistics import mean


def finite(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def covariance(x, y):
    mx, my = mean(x), mean(y)
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (len(x) - 1)


class PortfolioExposureEngine:
    """Uses aligned, closed returns only. Missing evidence never becomes neutral exposure."""

    def __init__(self, settings):
        self.settings = dict(settings)

    def returns(self, rows, asof):
        rows = sorted((r for r in rows if finite(r.get("t")) is not None
                       and float(r["t"]) <= asof * 1000
                       and (finite(r.get("close")) or 0) > 0), key=lambda r: r["t"])
        rows = rows[-int(self.settings["lookback_bars"]) - 1:]
        interval = int(self.settings["bar_seconds"]) * 1000
        return {int(b["t"]): float(b["close"]) / float(a["close"]) - 1
                for a, b in zip(rows, rows[1:]) if int(b["t"]) - int(a["t"]) == interval}

    def measure(self, legs, *, equity, margin_used, available, series, asof,
                account_asof=None, margin_source="BINANCE_ACCOUNT"):
        equity = finite(equity)
        if equity is None or equity <= 0:
            raise ValueError("Equity must be positive and finite")
        minimum = int(self.settings["min_return_observations"])
        problems = []
        if account_asof is None or asof - account_asof > float(self.settings["snapshot_max_age_seconds"]):
            problems.append("ACCOUNT_SNAPSHOT_STALE_OR_UNTIMED")
        normalized = []
        for raw in legs:
            qty, entry, mark = (finite(raw.get(k)) for k in ("qty", "entry", "mark"))
            if (qty is None or qty <= 0 or entry is None or entry <= 0 or mark is None
                    or mark <= 0 or raw.get("side") not in {"LONG", "SHORT"}):
                problems.append("INVALID_POSITION_DATA")
                continue
            sign = 1 if raw["side"] == "LONG" else -1
            stop = finite(raw.get("sl"))
            usable_stop = bool(raw.get("sl_confirmed") or raw.get("planned")) and stop is not None and stop > 0
            fee = finite(raw.get("fee_rate"))
            slip = finite(raw.get("slippage_pct"))
            known_cost = fee is not None and fee >= 0 and slip is not None and slip >= 0
                                                                                              
            entry_loss = qty * max(0, sign * (entry - stop)) if usable_stop else None
            mark_loss = qty * max(0, sign * (mark - stop)) if usable_stop else None
            entry_fee = qty * entry * fee if known_cost else None
            exit_cost = qty * (stop * fee + mark * slip / 100) if usable_stop and known_cost else None
            risk = entry_loss + entry_fee + exit_cost if entry_loss is not None and exit_cost is not None else None
            if not usable_stop:
                problems.append("STOP_UNCONFIRMED:" + raw["symbol"])
            if not known_cost:
                problems.append("EXIT_COST_UNKNOWN:" + raw["symbol"])
            normalized.append({**raw, "qty": qty, "entry": entry, "mark": mark,
                               "notional_usdt": qty * mark, "signed_notional_usdt": sign * qty * mark,
                               "entry_price_loss_to_sl_usdt": entry_loss,
                               "mark_price_giveback_to_sl_usdt": mark_loss,
                               "entry_fee_reserve_usdt": entry_fee,
                               "exit_cost_reserve_usdt": exit_cost, "risk_to_sl_usdt": risk})
        symbols = sorted({r["symbol"] for r in normalized})
        returns = {s: self.returns(series.get(s, []), asof) for s in set(symbols + ["BTCUSDT"])}
        stale = {s for s, values in returns.items() if not values or
                 asof - max(values) / 1000 > float(self.settings["market_max_age_seconds"])}
        beta, correlations, sample_counts = {}, {}, {}
        for symbol in symbols:
            times = sorted(set(returns[symbol]) & set(returns["BTCUSDT"]))
            sample_counts[symbol] = len(times)
            if len(times) < minimum or symbol in stale or "BTCUSDT" in stale:
                beta[symbol] = None
                continue
            x = [returns["BTCUSDT"][t] for t in times]
            y = [returns[symbol][t] for t in times]
            var = covariance(x, x)
            beta[symbol] = covariance(x, y) / var if var > 0 else None
        for a, b in combinations(symbols, 2):
            times = sorted(set(returns[a]) & set(returns[b]))
            rho = None
            if len(times) >= minimum and a not in stale and b not in stale:
                x, y = [returns[a][t] for t in times], [returns[b][t] for t in times]
                vx, vy = covariance(x, x), covariance(y, y)
                if vx > 0 and vy > 0:
                    rho = max(-1, min(1, covariance(x, y) / math.sqrt(vx * vy)))
            correlations[a + "|" + b] = {"correlation": rho, "observations": len(times)}
        gross = sum(r["notional_usdt"] for r in normalized)
        long = sum(r["notional_usdt"] for r in normalized if r["side"] == "LONG")
        short = gross - long
        beta_known = all(beta.get(s) is not None for s in symbols)
        correlation_known = all(r["correlation"] is not None for r in correlations.values())
        beta_net = sum(r["signed_notional_usdt"] * beta[r["symbol"]] for r in normalized) if beta_known else None
        if not beta_known:
            problems.append("BTC_BETA_INSUFFICIENT")
        if not correlation_known:
            problems.append("PAIR_CORRELATION_INSUFFICIENT")
                                                                                                  
        groups = [{s} for s in symbols]
        for key, pair in correlations.items():
            if pair["correlation"] is None or pair["correlation"] < float(self.settings["correlation_threshold"]):
                continue
            a, b = key.split("|")
            merged = set().union(*(g for g in groups if a in g or b in g))
            groups = [g for g in groups if a not in g and b not in g] + [merged]
        clusters = []
        for group in groups:
            cluster_legs = [r for r in normalized if r["symbol"] in group]
            amount = sum(r["notional_usdt"] for r in cluster_legs)
            sensitivity = sum(r["signed_notional_usdt"] * beta[r["symbol"]] for r in cluster_legs) if beta_known else None
            clusters.append({"symbols": sorted(group), "gross_usdt": amount,
                             "gross_share_pct": amount / gross * 100 if gross else 0,
                             "net_btc_beta_usdt": sensitivity})
        common = sorted(set.intersection(*(set(returns[s]) for s in symbols))) if symbols else []
        volatility = None
        if not symbols:
            volatility = 0.0
        elif len(common) >= minimum and not (set(symbols) & stale):
            portfolio_returns = [sum(r["signed_notional_usdt"] / equity * returns[r["symbol"]][t]
                                     for r in normalized) for t in common]
            volatility = math.sqrt(max(0, covariance(portfolio_returns, portfolio_returns))) * 100
        if volatility is None:
            problems.append("ALIGNED_PORTFOLIO_RETURNS_INSUFFICIENT")
        risk_known = len(normalized) == len(legs) and all(r["risk_to_sl_usdt"] is not None for r in normalized)
        risk = sum(r["risk_to_sl_usdt"] for r in normalized) if risk_known else None
        asset_amounts = {s: sum(r["notional_usdt"] for r in normalized if r["symbol"] == s) for s in symbols}
        margin_used, available = finite(margin_used), finite(available)
        if margin_used is None or available is None:
            problems.append("MARGIN_UNKNOWN")
        return {"asof": asof, "equity_usdt": equity, "positions": normalized, "position_count": len(legs),
                "long_notional_usdt": long, "short_notional_usdt": short, "gross_notional_usdt": gross,
                "net_notional_usdt": long - short, "gross_notional_pct_equity": gross / equity * 100,
                "direction_concentration_pct": max(long, short) / gross * 100 if gross else 0,
                "asset_hhi": sum((amount / gross) ** 2 for amount in asset_amounts.values()) if gross else 0,
                "risk_to_sl_usdt": risk, "risk_to_sl_pct_equity": risk / equity * 100 if risk is not None else None,
                "net_btc_beta_usdt": beta_net, "net_btc_beta_pct_equity": beta_net / equity * 100 if beta_net is not None else None,
                "btc_beta": beta, "beta_observations": sample_counts, "pair_correlations": correlations,
                "correlated_clusters": clusters if correlation_known else None,
                "largest_cluster_share_pct": max((c["gross_share_pct"] for c in clusters), default=0) if correlation_known else None,
                "largest_cluster_net_beta_pct_equity": max((abs(c["net_btc_beta_usdt"]) / equity * 100 for c in clusters), default=0) if correlation_known and beta_known else None,
                "correlation_adjusted_volatility_pct_equity_per_bar": volatility,
                "aligned_return_window": {"observations": len(common),
                    "start_ms": common[0] if common else None, "end_ms": common[-1] if common else None,
                    "signature": hashlib.sha256(str(common).encode()).hexdigest()},
                "margin_used_usdt": margin_used, "margin_used_pct_equity": margin_used / equity * 100 if margin_used is not None else None,
                "available_margin_usdt": available, "margin_source": margin_source,
                "complete_for_conditional_capacity": not problems, "data_gaps": sorted(set(problems)),
                "risk_note": "Entry-to-SL loss plus entry/exit fee and slippage reserves; funding/gaps are not guaranteed by a stop. Mark-to-SL giveback is separate."}

    def capacity_decision(self, before, after, *, normal_filters_passed, reserved_entries=0):
        slot = before["position_count"] + int(reserved_entries) + 1
        base, maximum = int(self.settings["base_capacity"]), int(self.settings["research_capacity"])
        reasons, soft_warnings = [], []
        if slot > maximum:
            reasons.append("MAXIMUM_CAPACITY_EXCEEDED")
        if after["position_count"] != before["position_count"] + 1:
            reasons.append("CANDIDATE_COUNT_MISMATCH")
        if normal_filters_passed is not True:
            reasons.append("NORMAL_AG_FILTERS_NOT_PROVEN")
        extra = slot > base
        max_scale = 1.0
        marginal_risk_budget = None
        hard_headroom = {}
        if extra:
            if reserved_entries:
                reasons.append("UNMEASURED_ENTRY_RESERVATIONS")
            if not before["complete_for_conditional_capacity"] or not after["complete_for_conditional_capacity"]:
                reasons.append("EXPOSURE_EVIDENCE_INCOMPLETE")
            else:
                eps = 1e-9
                if before["aligned_return_window"]["signature"] != after["aligned_return_window"]["signature"]:
                    reasons.append("RETURN_WINDOWS_NOT_COMPARABLE")
                equity = finite(after.get("equity_usdt"))
                before_risk = finite(before.get("risk_to_sl_usdt"))
                after_risk = finite(after.get("risk_to_sl_usdt"))
                before_gross = finite(before.get("gross_notional_usdt"))
                after_gross = finite(after.get("gross_notional_usdt"))
                before_margin = finite(before.get("margin_used_usdt"))
                after_margin = finite(after.get("margin_used_usdt"))
                before_free = finite(before.get("available_margin_usdt"))
                after_free = finite(after.get("available_margin_usdt"))
                if any(value is None for value in (equity, before_risk, after_risk, before_gross,
                                                    after_gross, before_margin, after_margin,
                                                    before_free, after_free)) or equity <= 0:
                    reasons.append("HARD_LIMIT_INPUTS_UNAVAILABLE")
                else:
                    risk_cap = equity * float(self.settings["aggregate_sl_risk_cap_pct"]) / 100
                    gross_cap = equity * float(self.settings["gross_notional_cap_pct"]) / 100
                    margin_cap = equity * float(self.settings["margin_cap_pct"]) / 100
                    free_reserve = equity * float(self.settings["free_margin_min_pct"]) / 100
                    risk_delta = after_risk - before_risk
                    gross_delta = after_gross - before_gross
                    margin_delta = after_margin - before_margin
                    free_margin_consumed = before_free - after_free
                    hard_headroom = {
                        "aggregate_sl_risk_usdt": max(0.0, risk_cap - before_risk),
                        "gross_notional_usdt": max(0.0, gross_cap - before_gross),
                        "margin_usdt": max(0.0, margin_cap - before_margin),
                        "free_margin_above_reserve_usdt": max(0.0, before_free - free_reserve),
                    }
                    if risk_delta <= eps or gross_delta <= eps or margin_delta <= eps or free_margin_consumed <= eps:
                        reasons.append("INVALID_MARGINAL_CAPACITY_MEASUREMENT")
                    else:
                                                                                      
                                                                                    
                                                              
                        marginal_risk_budget = hard_headroom["aggregate_sl_risk_usdt"] / max(1, slot)
                        factors = {
                            "marginal_sl_risk_allocation": marginal_risk_budget / risk_delta,
                            "aggregate_sl_risk_cap": hard_headroom["aggregate_sl_risk_usdt"] / risk_delta,
                            "gross_notional_cap": hard_headroom["gross_notional_usdt"] / gross_delta,
                            "margin_cap": hard_headroom["margin_usdt"] / margin_delta,
                            "free_margin_reserve": hard_headroom["free_margin_above_reserve_usdt"] / free_margin_consumed,
                        }
                        max_scale = max(0.0, min(1.0, *(max(0.0, value) for value in factors.values())))
                        if max_scale <= eps:
                            reasons.append("NO_MARGINAL_HARD_LIMIT_BUDGET")
                        elif max_scale < 1.0 - eps:
                            reasons.append("SIZE_REDUCTION_REQUIRED")

                                                                            
                                                                              
                                                                          
                for metric, code in (
                    ("net_btc_beta_usdt", "BTC_SENSITIVITY_INCREASES"),
                    ("direction_concentration_pct", "DIRECTION_CONCENTRATION_INCREASES"),
                    ("largest_cluster_share_pct", "LARGEST_CLUSTER_SHARE_INCREASES"),
                    ("largest_cluster_net_beta_pct_equity", "LARGEST_CLUSTER_BETA_INCREASES"),
                    ("asset_hhi", "ASSET_CONCENTRATION_INCREASES"),
                    ("correlation_adjusted_volatility_pct_equity_per_bar", "CORRELATED_VOLATILITY_INCREASES"),
                ):
                    old, new = finite(before.get(metric)), finite(after.get(metric))
                    if old is not None and new is not None and abs(new) > abs(old) + eps:
                        soft_warnings.append(code)
        allowed = not reasons
        enabled = self.settings.get("conditional_slots_enabled") is True
                                                                                
                                                                                 
        return {"slot": slot, "conditional_slot": extra, "research_acceptable": allowed,
                "production_authorized": allowed and max_scale >= 1.0 - 1e-9 and (not extra or enabled),
                "conditional_slots_enabled": enabled, "reasons": reasons,
                "soft_warnings": soft_warnings,
                "recommended_quantity_scale": max_scale if extra else 1.0,
                "marginal_risk_budget_usdt": marginal_risk_budget,
                "hard_limit_headroom": hard_headroom,
                "policy_version": self.settings["policy_version"],
                "note": "Conditional slots keep the existing portfolio hard caps; soft diversification changes are reported, not treated as hard blocks. Slot risk budget is remaining aggregate SL-risk headroom divided by resulting occupied slots."}
