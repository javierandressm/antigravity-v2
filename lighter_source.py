"""
ANTIGRAVITY · Lector de wallets de LIGHTER (zkLighter, API pública, sin claves)
==============================================================================
Añadido el 14-10-2026 (investigación research/wallet_discovery_deep_audit_20261012, 11_ y 12_).

Convierte cada cuenta de Lighter al MISMO formato que el escáner usa para Hyperliquid:
  - positions: [{"coin", "szi", "entryPx"}]           (szi con signo, como clearinghouseState)
  - fills:     [{"coin", "time", "px", "sz", "side", "dir"}]  (side B/A y dir "Open Long"/"Close Long"/...,
               como userFills; el 'dir' se reconstruye con la posición antes/después que da la API)
Así la regla de voto de AG (posición abierta + último fill <= 4 h en su dirección) es idéntica para las dos fuentes.

Configuración: TRADERS_SELECCION_LIGHTER.json
  {"modo": "apagado" | "sombra" | "produccion", "lighter_whales": [{"account_index": 123, "l1_address": "0x…", "name": "DEMO_LIGHTER"}]}
  - apagado:    no se hace ninguna petición (comportamiento idéntico al de antes).
  - sombra:     se leen las cuentas y se publican sus votos en live_whale_consensus.json["lighter"], SIN tocar las señales.
  - produccion: sus votos cuentan en el consenso igual que los de Hyperliquid.
Ritmo: 1 petición por segundo como máximo (Lighter no comparte límite con Hyperliquid ni con Binance).
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

ROOT = Path(__file__).resolve().parent
LIGHTER_FILE = ROOT / "TRADERS_SELECCION_LIGHTER.json"
BASE_URL = "https://mainnet.zklighter.elliot.ai/api/v1"
REQUEST_INTERVAL_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 6
MAX_TRADE_PAGES = 10
VALID_MODES = ("apagado", "sombra", "produccion")
EPS = 1e-12

_req_lock = threading.Lock()
_next_req_at = 0.0


def load_config() -> Dict[str, Any]:
    """Lee la configuración. Ante cualquier problema devuelve 'apagado' (nunca rompe el escáner)."""
    default = {"modo": "apagado", "lighter_whales": [], "error": None}
    if not LIGHTER_FILE.exists():
        return default
    try:
        data = json.loads(LIGHTER_FILE.read_text(encoding="utf-8-sig"))
        mode = str(data.get("modo", "apagado")).strip().lower()
        if mode not in VALID_MODES:
            return {**default, "error": f"modo inválido: {mode}"}
        whales = []
        for w in data.get("lighter_whales", []):
            idx = int(w["account_index"])
            if idx <= 0:
                raise ValueError("account_index inválido")
            whales.append({"account_index": idx, "l1_address": str(w.get("l1_address", "")),
                           "name": str(w.get("name") or f"L:{idx}")})
        if len({w["account_index"] for w in whales}) != len(whales):
            return {**default, "error": "account_index duplicado"}
        return {"modo": mode, "lighter_whales": whales, "error": None}
    except Exception as exc:                                         
        return {**default, "error": f"configuración ilegible: {exc}"}


def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET público con ritmo global de 1 petición/s. Lanza excepción si la respuesta no es válida."""
    global _next_req_at
    with _req_lock:
        wait = _next_req_at - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _next_req_at = time.monotonic() + REQUEST_INTERVAL_SECONDS
        r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    payload = r.json()
    if not isinstance(payload, dict) or payload.get("code") not in (200, None):
        raise ValueError(f"respuesta inválida: {str(payload)[:120]}")
    return payload


def fill_dir(before: float, after: float) -> str:
    """Mismo 'dir' que Hyperliquid a partir de la posición antes y después del fill."""
    if abs(before) < EPS:
        return "Open Long" if after > 0 else "Open Short"
    if before > 0:
        if after > before + EPS:
            return "Open Long"
        if after >= -EPS:
            return "Close Long"
        return "Long > Short"
    if after < before - EPS:
        return "Open Short"
    if after <= EPS:
        return "Close Short"
    return "Short > Long"


def positions_from_account(payload: Dict[str, Any], idx: int) -> List[Dict[str, Any]]:
    accounts = payload.get("accounts")
    if not isinstance(accounts, list) or len(accounts) != 1:
        raise ValueError("cuenta no encontrada")
    acc = accounts[0]
    if int(acc.get("index", acc.get("account_index", -1))) != idx:
        raise ValueError("la API devolvió otra cuenta")
    out = []
    for p in acc.get("positions") or []:
        size = float(p.get("position") or 0)
        if size == 0:
            continue
        sign = -1 if int(p.get("sign", 1)) == -1 else 1
        out.append({"coin": str(p.get("symbol")), "szi": str(sign * size), "entryPx": str(p.get("avg_entry_price") or "0")})
    return out


def fills_from_trades(trades: List[Dict[str, Any]], idx: int) -> List[Dict[str, Any]]:
    out = []
    for t in trades:
        if t.get("type", "trade") != "trade" or t.get("market_kind", "perps") != "perps":
            continue
        ask, bid = int(t.get("ask_account_id", -1)), int(t.get("bid_account_id", -1))
        if ask == bid or idx not in (ask, bid):
            continue                              
        sell = ask == idx
        maker_ask = bool(t.get("is_maker_ask"))
        taker = (sell and not maker_ask) or ((not sell) and maker_ask)
        raw_before = t.get("taker_position_size_before") if taker else t.get("maker_position_size_before")
        before = float(raw_before or 0)
        size = float(t.get("size") or 0)
        after = before - size if sell else before + size
        out.append({"coin": str(t.get("market_symbol")), "time": int(t["timestamp"]), "px": str(t.get("price")),
                    "sz": str(size), "side": "A" if sell else "B", "dir": fill_dir(before, after), "fuente": "lighter",
                    "startPosition": str(before)})
    return out


def fetch_lighter_whale(w: Dict[str, Any], fresh_hours: float = 4.0, history_hours: float = None) -> Dict[str, Any]:
    """Posiciones y operaciones recientes. 'cubre_desde_ms': desde cuándo `fills` contiene TODAS las operaciones
    de la cuenta (para el filtro de voto volátil). history_hours > fresh_hours pide más historial (hasta 10 páginas)."""
    idx = int(w["account_index"])
    res = {"addr": f"lighter:{idx}", "name": w.get("name") or f"L:{idx}", "positions": [], "fills": [], "ok": True,
           "errors": [], "fuente": "lighter"}
    try:
        res["positions"] = positions_from_account(_get("account", {"by": "index", "value": idx}), idx)
    except Exception as exc:
        res["ok"] = False
        res["errors"].append(f"lighter account: {exc}")
        return res
    try:
        hours = max(fresh_hours, history_hours or 0)
        limit_ms = int(time.time() * 1000) - int(hours * 3600 * 1000)
        cursor, trades = None, []
        reached_end = False
        for _ in range(MAX_TRADE_PAGES):
            params = {"account_index": idx, "sort_by": "timestamp", "sort_dir": "desc", "limit": 100}
            if cursor:
                params["cursor"] = cursor
            page = _get("trades", params)
            batch = page.get("trades")
            if not isinstance(batch, list):
                raise ValueError("respuesta trades inválida")
            trades.extend(batch)
            cursor = page.get("next_cursor")
            if not batch or not cursor or len(batch) < 100:
                reached_end = True                                   
                break
            if int(batch[-1]["timestamp"]) < limit_ms:
                break
        res["fills"] = fills_from_trades(trades, idx)
        oldest = min((int(t["timestamp"]) for t in trades), default=None)
        res["cubre_desde_ms"] = 0 if reached_end else oldest
    except Exception as exc:
        res["ok"] = False
        res["errors"].append(f"lighter trades: {exc}")
    return res


def fetch_all(whales: List[Dict[str, Any]], fresh_hours: float = 4.0, budget_seconds: float = 60.0,
              history_hours: Dict[int, float] = None) -> List[Dict[str, Any]]:
    """Lee todas las cuentas con un presupuesto de tiempo total: nunca retrasa el escaneo más de ~budget_seconds."""
    out, t0 = [], time.monotonic()
    for w in whales:
        if time.monotonic() - t0 > budget_seconds:
            idx = int(w["account_index"])
            out.append({"addr": f"lighter:{idx}", "name": w.get("name") or f"L:{idx}", "positions": [], "fills": [],
                        "ok": False, "errors": ["presupuesto de tiempo de Lighter agotado"], "fuente": "lighter"})
            continue
        out.append(fetch_lighter_whale(w, fresh_hours, (history_hours or {}).get(int(w["account_index"]))))
    return out


def fresh_votes(data: Dict[str, Any], now_ms: int, tracked: List[str], fresh_hours: float = 4.0) -> List[Dict[str, Any]]:
    """Votos frescos de UNA wallet con la regla exacta del escáner (para el modo sombra)."""
    votes = []
    by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for f in data["fills"]:
        by_coin.setdefault(f.get("coin"), []).append(f)
    for pos in data["positions"]:
        coin = pos.get("coin")
        if coin not in tracked:
            continue
        side = "LONG" if float(pos.get("szi", 0)) > 0 else "SHORT"
        valid = [(int(f.get("time", 0)), f) for f in by_coin.get(coin, []) if 0 < int(f.get("time", 0)) <= now_ms]
        if not valid:
            continue
        latest = max(valid, key=lambda x: x[0])[1]
        age_h = (now_ms - int(latest["time"])) / 3_600_000
        if not 0 <= age_h <= fresh_hours:
            continue
        d = latest.get("dir", "")
        if (side == "LONG" and ("Long" in d or latest.get("side") == "B")) or (side == "SHORT" and ("Short" in d or latest.get("side") == "A")):
            votes.append({"wallet": data["addr"], "name": data["name"], "coin": coin, "side": side,
                          "age_h": round(age_h, 3), "px": float(latest.get("px") or 0)})
    return votes
