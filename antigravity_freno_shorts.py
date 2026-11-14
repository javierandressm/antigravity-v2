"""05-11-2026 (usuario: "un marica debería decir: esto va a bajar"). FRENO DE SHORTS.
No abrir un LONG cuando muchas ballenas acaban de abrir short en esa moneda.
Investigación: research/wallet_discovery_20261021/scripts/81-85 (simulación de 13 versiones del bot, 8 arranques):
  LONG con pico de shorts nuevos: -0,30 R antes del 06-10 y -0,39 R después (normal: +0,24 y +0,05);
  dinero real: 7 LONG en pico, -34,01 $ (acierto 29 %); negativo en las 13 versiones y en 12 combinaciones de umbral/ventana;
  placebo (pico movido ±3 días) sin efecto.
Definición: "short nuevo" = una de las ~730 wallets vigiladas tiene short en la moneda, abierto hace entre 30 min y 12 h.
Pico = conteo >= umbral de la moneda (p95 histórico, mínimo 5).
Este módulo: el registro de cuándo se vio por primera vez cada short (lo usa el vigilante) y la regla que lee el bot.
Solo frena entradas LONG nuevas. No toca posiciones abiertas, órdenes, SL/TP ni tamaños."""
import json
import os
import time

VENTANA_S = 12 * 3600
EDAD_MIN_S = 30 * 60


class Registro:
    """first_seen[(wallet, coin)] = momento en que se vio el short por primera vez (None = ya estaba al empezar: no cuenta)."""

    def __init__(self, datos=None):
        datos = datos or {}
        self.first = {tuple(k.split("|", 1)): v for k, v in (datos.get("first_seen") or {}).items()}
        self.vistas = set(datos.get("wallets_vistas") or [])

    def actualizar(self, wallet, cortos, ahora):
        """cortos = set de monedas en las que la wallet está en short ahora (lectura válida)."""
        primera = wallet not in self.vistas
        for (w, c) in [k for k in self.first if k[0] == wallet]:
            if c not in cortos:
                del self.first[(w, c)]
        for c in cortos:
            if (wallet, c) not in self.first:
                self.first[(wallet, c)] = None if primera else ahora
        self.vistas.add(wallet)

    def conteo(self, ahora, ventana_s=VENTANA_S, edad_min_s=EDAD_MIN_S):
        out = {}
        for (_, c), t in self.first.items():
            if t is not None and ahora - ventana_s <= t <= ahora - edad_min_s:
                out[c] = out.get(c, 0) + 1
        return out

    def a_json(self):
        return {"first_seen": {f"{w}|{c}": t for (w, c), t in self.first.items()}, "wallets_vistas": sorted(self.vistas)}


def moneda(raw_sym):
    c = str(raw_sym).upper().replace("/", "")
    c = c[:-4] if c.endswith("USDT") else c
    return c[4:] if c.startswith("1000") else c


def config(cfg):
    f = (cfg or {}).get("freno_shorts") or {}
    return {"enabled": bool(f.get("enabled", True)), "max_edad_min": float(f.get("max_edad_min", 40)),
            "archivo": str(f.get("archivo", "freno_shorts_estado.json"))}


def leer_estado(ruta, ahora=None, max_edad_min=40):
    """Devuelve el estado del vigilante o None si no existe, está roto o es viejo (entonces el freno no actúa)."""
    try:
        with open(ruta, encoding="utf-8") as fh:
            e = json.load(fh)
        ahora = time.time() if ahora is None else ahora
        if ahora - float(e["actualizado"]) > max_edad_min * 60:
            return None
        return e
    except (OSError, ValueError, KeyError, TypeError):
        return None


def bloquea(estado, raw_sym, side):
    """(True, n, umbral) si es un LONG y la moneda está en pico de shorts nuevos."""
    if not estado or str(side).upper() not in ("LONG", "BUY"):
        return False, 0, None
    c = moneda(raw_sym)
    n = int((estado.get("conteo") or {}).get(c, 0))
    u = (estado.get("umbral") or {}).get(c, estado.get("umbral_por_defecto", 5))
    try:
        u = int(u)
    except (TypeError, ValueError):
        return False, n, None
    return n >= u, n, u


def guardar_json(ruta, datos):
    tmp = f"{ruta}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(datos, fh)
    os.replace(tmp, ruta)
