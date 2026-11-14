"""Anti-rebote tras desplome (29-10-2026, decisión del usuario tras el short de BNB; research/persecucion_bnb_20261029).

Antes de abrir una posición del lado configurado (por defecto solo SHORT), mira las velas de mark price de 5 min de la ventana
(por defecto 2 h) y el mark actual:
  - short: busca el máximo y, desde ahí, el mínimo. Movimiento = máximo − mínimo; devuelto = mark − mínimo.
  - long (solo si se configura): mínimo y, desde ahí, el máximo. Movimiento = máximo − mínimo; devuelto = máximo − mark.
Si el movimiento es >= `min_movimiento_atr` · ATR(4h) y el precio ya devolvió >= `rebote_min_pct` % de él, la entrada es un
rebote en marcha: no se abre. Si no se pueden leer los datos, la entrada se bloquea (como el resto de filtros del bot).

Configuración (antigravity_config.json):
  "rebote": {"enabled": true, "lados": ["SHORT"], "ventana_h": 2, "min_movimiento_atr": 1.5, "rebote_min_pct": 25}
Sin la sección o con enabled=false, el bot se comporta exactamente como antes.
"""
import time


def rebote_config(config):
    r = config.get("rebote") if isinstance(config, dict) else None
    if not isinstance(r, dict) or r.get("enabled") is not True:
        return None
    lados = tuple(str(x).upper() for x in r.get("lados", ["SHORT"]))
    if not lados or any(x not in ("LONG", "SHORT") for x in lados):
        raise ValueError("rebote.lados debe ser una lista con LONG y/o SHORT")
    cfg = {"lados": lados, "ventana_s": float(r.get("ventana_h", 2)) * 3600.0,
           "min_mov_atr": float(r.get("min_movimiento_atr", 1.5)), "rebote_min": float(r.get("rebote_min_pct", 25)) / 100.0}
    if cfg["ventana_s"] <= 0 or cfg["min_mov_atr"] <= 0 or not 0 < cfg["rebote_min"] < 1:
        raise ValueError("rebote: ventana_h y min_movimiento_atr deben ser > 0 y rebote_min_pct estar entre 0 y 100")
    return cfg


def evaluar(velas, mark, atr4h, lado, cfg):
    """velas: [(high, low), ...] en orden de tiempo. Devuelve {"bloquear", "movimiento_atr", "devuelto_pct"}."""
    if not velas or mark <= 0 or atr4h <= 0:
        raise ValueError("datos insuficientes para medir el rebote")
    highs = [float(h) for h, _ in velas]
    lows = [float(l) for _, l in velas]
    if lado == "SHORT":
        j = max(range(len(highs)), key=lambda k: highs[k])
        fondo = min(lows[j:] + [mark])
        mov, dev = highs[j] - fondo, mark - fondo
    else:
        j = min(range(len(lows)), key=lambda k: lows[k])
        techo = max(highs[j:] + [mark])
        mov, dev = techo - lows[j], techo - mark
    mov_atr = mov / atr4h
    devuelto = dev / mov if mov > 0 else 0.0
    return {"bloquear": mov_atr >= cfg["min_mov_atr"] and devuelto >= cfg["rebote_min"],
            "movimiento_atr": round(mov_atr, 2), "devuelto_pct": round(devuelto * 100, 1)}


def revisar(bridge, symbol, lado, cfg, ahora=None):
    """Lee velas de mark de 5 min (endpoint público), el mark y el ATR(4h) del bot, y evalúa. Lanza si los datos fallan."""
    ahora = time.time() if ahora is None else ahora
    desde_ms = int((ahora - cfg["ventana_s"]) * 1000)
    limite = int(cfg["ventana_s"] // 300) + 2
    k = bridge.client._request("GET", "/fapi/v1/markPriceKlines", {"symbol": symbol, "interval": "5m", "limit": limite})
    if not isinstance(k, list) or not k:
        raise RuntimeError(f"Binance no devolvió velas de mark para {symbol}")
    velas = [(float(x[2]), float(x[3])) for x in k if int(x[0]) + 300_000 > desde_ms]
    if len(velas) < cfg["ventana_s"] // 300 * 0.8:
        raise RuntimeError(f"velas de mark incompletas para {symbol} ({len(velas)})")
    mark = float(bridge.client.get_mark_price(symbol) or 0)
    if mark <= 0:
        raise RuntimeError(f"mark price inválido para {symbol}")
    atr4h = float(bridge.calculate_atr_4h(symbol, mark)[2])
    return evaluar(velas, mark, atr4h, lado, cfg)
