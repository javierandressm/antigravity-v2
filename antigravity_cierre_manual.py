"""03-11-2026 (usuario): espera tras un cierre manual. Si una posición se cerró sin que saltara su stop ni su TP (cierre a mano,
"Cerrar todo" del panel o cualquier cierre fuera del bot: classification VERIFIED_CLOSE en reconciled_closures), el bot no vuelve a
entrar en esa misma moneda y ese mismo lado durante ESPERA_HORAS. Con dinero real (06-10 -> 03-11), 12 h habrían evitado 6
re-entradas que sumaron -24,96 $ (HYPE 29-10 11:35 -9,37 $ y 16:48 -8,79 $, entre otras). La simulación no tiene cierres manuales,
así que esta regla no la cambia. Solo lee el estado del bot: no toca órdenes ni posiciones."""

ESPERA_HORAS = 12.0


def espera_config(config):
    """Horas de espera; antigravity_config.json puede cambiarlas con {"cierre_manual": {"espera_horas": N}} (0 = apagado)."""
    try:
        return float((config or {}).get("cierre_manual", {}).get("espera_horas", ESPERA_HORAS))
    except (TypeError, ValueError, AttributeError):
        return ESPERA_HORAS


def bloqueo(state, symbol, side, now, horas=ESPERA_HORAS):
    """Horas que faltan si la última posición de `symbol` y `side` cerrada a mano lo hizo hace menos de `horas`; si no, None."""
    if not horas or horas <= 0:
        return None
    ultimo = None
    for r in (state or {}).get("reconciled_closures", []) or []:
        if not isinstance(r, dict) or r.get("symbol") != symbol or str(r.get("side", "")).upper() != str(side).upper():
            continue
        if r.get("classification") != "VERIFIED_CLOSE" or r.get("superseded_record"):
            continue
        try:
            t = float(r.get("closed_at") or 0)
        except (TypeError, ValueError):
            continue
        if t > 0 and (ultimo is None or t > ultimo):
            ultimo = t
    if ultimo is None:
        return None
    falta = horas * 3600.0 - (float(now) - ultimo)
    return round(falta / 3600.0, 2) if falta > 0 else None
