"""Risk and leverage policy shared by the daemon and local command center."""

from antigravity_config import load_bot_config

CONFIG = load_bot_config()

def evaluate_signal_policy(symbol, confirmations, equity):
    clean_symbol = symbol.upper().replace("USDT", "").replace("/", "")
    assets = CONFIG["assets"]
    if clean_symbol in assets["major"]:
        category = "major"
        label = "MAJOR"
    elif clean_symbol in assets["mid_cap"]:
        category = "mid_cap"
        label = "MID-CAP"
    else:
        category = "high_beta"
        label = "HIGH-BETA ALT"

    min_confirmations = int(CONFIG["consensus"]["minimum_confirmations"][category])
                                                                                             
    symbol_minimum = int((CONFIG["consensus"].get("minimum_confirmations_by_symbol") or {}).get(clean_symbol, 0))
    category_minimum = min_confirmations
    min_confirmations = max(min_confirmations, symbol_minimum)
    high_confidence_votes = int(CONFIG["consensus"]["high_confidence_confirmations"])
    if int(confirmations) < min_confirmations:
        reason = f"Consenso insuficiente: {confirmations} < {min_confirmations}"
        if symbol_minimum > category_minimum:
            reason += f" (mínimo propio de {clean_symbol})"
        return {"eligible": False, "category": label, "reason": reason}

    confidence = "high_confidence" if int(confirmations) >= high_confidence_votes else "standard"
    leverage = int(CONFIG["leverage"][category][confidence])
    risk_pct = float(CONFIG["risk"]["high_confidence_trade_risk_pct"] if confidence == "high_confidence" else CONFIG["risk"]["standard_trade_risk_pct"])
                                                                                                                    
    symbol_risk_cap = (CONFIG["risk"].get("max_trade_risk_pct_by_symbol") or {}).get(clean_symbol)
    if symbol_risk_cap is not None:
        risk_pct = min(risk_pct, float(symbol_risk_cap))
    risk_budget = float(equity) * risk_pct / 100.0
    return {
        "eligible": True,
        "category": label,
        "leverage": leverage,
        "risk_pct": risk_pct,
        "dollar_risk": risk_budget,
        "risk_budget": risk_budget,
        "leverage_policy": CONFIG["leverage"]
    }
