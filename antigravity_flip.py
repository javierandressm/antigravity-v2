"""Flip con 4+ ballenas (26-10-2026, decisión del usuario; research/wallet_discovery_20261021 48_flip_3 y 49_flip_4_reglas).

Si el agente tiene una posición abierta y protegida y llega una señal CONFIRMADA del lado contrario con al menos `min_votes` ballenas,
el daemon cierra la posición a mercado (close_position, reduce-only, cancela sus algos) y deja anotado un "flip pendiente". Cuando
Binance confirma y se reconcilia el cierre, la señal contraria entra por el camino normal (mismos filtros, política, sizing, SL y TP)
sin esperar el cooldown de la moneda, solo dentro de la ventana `reopen_window_minutes`.

Configuración (antigravity_config.json): "flip": {"enabled": true|false, "min_votes": 4, "reopen_window_minutes": 30}.
Sin la sección o con enabled=false, el bot se comporta exactamente como antes.
"""


def flip_config(config):
    f = config.get("flip") if isinstance(config, dict) else None
    if not isinstance(f, dict) or f.get("enabled") is not True:
        return None
    min_votes = int(f.get("min_votes", 4))
    if min_votes < 3:
        raise ValueError("flip.min_votes debe ser >= 3")
    return {"min_votes": min_votes, "window_s": float(f.get("reopen_window_minutes", 30)) * 60.0}


def votos(sig):
    try:
        return int(sig.get("hl_votes", sig.get("total_votes", 0)) or 0)
    except (TypeError, ValueError, AttributeError):
        return 0


def debe_voltear(cfg, sig, pos_meta, executed_fingerprints):
    """True si la señal confirmada pide voltear la posición abierta de esa moneda."""
    if not cfg or not isinstance(sig, dict) or not isinstance(pos_meta, dict):
        return False
    if sig.get("status") not in ("CONFIRMED_ONCHAIN", "CONFIRMED_HYBRID"):
        return False
    nuevo = str(sig.get("side", "")).upper()
    actual = str(pos_meta.get("side", "")).upper()
    if nuevo not in ("LONG", "SHORT") or actual not in ("LONG", "SHORT") or nuevo == actual:
        return False
    if votos(sig) < cfg["min_votes"]:
        return False
    fp = sig.get("fingerprint")
    if fp and fp in (executed_fingerprints or []):
        return False
    if str(pos_meta.get("status", "")).upper() in {"CLOSE_PENDING", "RECONCILING", "ENTRY_PENDING", "ENTRY_UNKNOWN"}:
        return False
    return True


def anotar_flip(state, symbol, sig, now):
    state.setdefault("flip_pendiente", {})[symbol] = {"side": str(sig.get("side", "")).upper(), "fingerprint": sig.get("fingerprint"),
                                                      "votos": votos(sig), "at": now}


def cooldown_exento(state, cfg, symbol, side, now):
    """La reentrada del flip no espera el cooldown de la moneda (solo el mismo lado y dentro de la ventana)."""
    if not cfg:
        return False
    p = (state.get("flip_pendiente") or {}).get(symbol)
    if not isinstance(p, dict) or p.get("side") != str(side).upper():
        return False
    return 0 <= now - float(p.get("at", 0)) <= cfg["window_s"]


def limpiar_vencidos(state, cfg, now):
    pend = state.get("flip_pendiente") or {}
    window = cfg["window_s"] if cfg else 0.0
    vencidos = [s for s, p in pend.items() if not isinstance(p, dict) or now - float(p.get("at", 0)) > window]
    for s in vencidos:
        pend.pop(s, None)
    return vencidos
