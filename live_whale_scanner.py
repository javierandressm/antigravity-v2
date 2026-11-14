"""
ANTIGRAVITY V2.1 · LIVE ON-CHAIN WHALE SCANNER (HYPERLIQUID L1)
=============================================================
Motor de detección de IMPULSOS FRESCOS en tiempo real de 40 ballenas en Hyperliquid L1:
- Detección de órdenes y ejecuciones recientes (userFillsByTime, solo la ventana útil) con ventana <= 4.0 horas.
- Descarta automáticamente posiciones macro pasivas (holdings viejos de semanas).
- Calcula el reference_price real basado en la mediana de los fills recientes (impulsos).
- Blindaje contra persecución de precios: solo trades sincronizados al inicio del movimiento.
"""
import os
import sys
import time
import json
import threading
import statistics
import requests
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:                                                                                                             
    import lighter_source
except Exception:                                                                        
    lighter_source = None

try:                                                                                     
    import vote_volatility
except Exception:                    
    vote_volatility = None

ROOT = Path(__file__).resolve().parent
WHALES_FILE = ROOT / "TRADERS_SELECCION_HL_BINANCE.json"
CACHE_FILE = ROOT / "live_whale_consensus.json"

TRACKED_UNIVERSE = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "SUI", "AVAX", "NEAR",
    "LINK", "ADA", "SEI", "WLD", "ZEC", "LIT", "HYPE",
                                                                                  
    "UNI", "PUMP", "ENA", "SAND", "TAO", "AAVE", "FIL", "ONDO",
                                                                              
                                                                       
    "ARB", "INJ", "LTC", "ZRO", "BCH", "FET", "DOT", "XLM", "APT",
    "HBAR", "TIA", "RENDER", "ETHFI", "TRX", "OP", "XMR", "ICP",
]

MAX_FRESH_HOURS = 4.0                                                                   
VV_LEDGER = vote_volatility.Ledger(TRACKED_UNIVERSE) if vote_volatility is not None else None
SCAN_INTERVAL_SECONDS = 300
CONSENSUS_CACHE_TTL_SECONDS = 600
INFO_REQUEST_INTERVAL_SECONDS = 4.0

_lock = threading.Lock()
_scan_lock = threading.Lock()
_info_request_lock = threading.Lock()
_next_info_request_at = 0.0
_latest_data: Dict[str, Any] = {}


FILLS_WINDOW_MARGIN_MS = 15 * 60 * 1000                                                           
FILLS_BY_TIME_CAP = 2000                                                             
FILLS_CACHE_MAX = 2000                                                                                             
FILLS_OVERLAP_MS = 5 * 60 * 1000                                                                               
FILLS_FALLBACK_TIMEOUT = 15                                                                                     
_fills_cache: Dict[str, List[Dict[str, Any]]] = {}                                                              
                                                                                                                       
                                                                                                                       
                                                                                                         
                                                          
LAST_GOOD_MAX_WALLETS = 3
LAST_GOOD_MAX_AGE_SECONDS = 12 * 60
_last_good: Dict[str, Dict[str, Any]] = {}                                                        
VV_BACKFILL_SLEEP_SECONDS = 12.0                                                                                   


def _post_info(payload: Dict[str, Any], timeout: float = 4):
    """Serialize public HL reads and pace starts across all scanner workers."""
    global _next_info_request_at
    with _info_request_lock:
        now = time.monotonic()
        wait = _next_info_request_at - now
        if wait > 0:
            time.sleep(wait)
        started = time.monotonic()
        _next_info_request_at = started + INFO_REQUEST_INTERVAL_SECONDS
        return requests.post(
            "https://api.hyperliquid.xyz/info",
            json=payload,
            timeout=timeout
        )


def _post_info_retry(payload: Dict[str, Any], timeout: float = 4):
    """Un reintento solo ante errores de red (timeout/conexión), nunca ante HTTP 429 u otros códigos."""
    try:
        return _post_info(payload, timeout)
    except (requests.Timeout, requests.ConnectionError):
        return _post_info(payload, timeout)


def _fill_time(fill: Dict[str, Any]) -> int:
    try:
        return int(fill.get("time", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _fill_key(fill: Dict[str, Any]):
    return (fill.get("tid"), fill.get("oid"), fill.get("time"), fill.get("coin"), fill.get("px"), fill.get("sz"), fill.get("side"))


def _request_fills(payload: Dict[str, Any], timeout: float) -> List[Dict[str, Any]]:
    r = _post_info_retry(payload, timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    fills = r.json()
    if not isinstance(fills, list) or any(not isinstance(fill, dict) for fill in fills):
        raise ValueError(f"respuesta {payload['type']} inválida")
    return fills


def _vv_ingest(key: str, fills: List[Dict[str, Any]], from_ms: int, to_ms: int) -> None:
    """Alimenta el registro del filtro de voto volátil. Nunca rompe la lectura."""
    if VV_LEDGER is None:
        return
    try:
        VV_LEDGER.ingest(key, fills, from_ms, to_ms)
    except Exception:
        pass


def _fetch_recent_fills(addr: str) -> List[Dict[str, Any]]:
    """Los 2.000 fills más recientes de la wallet (lo mismo que userFills), descargando solo lo nuevo.

    14-10-2026: antes cada escaneo descargaba userFills completo de las 36 wallets (~700 KB por wallet, ~25 MB por
    escaneo y mucho peso de API), lo que fallaba con conexiones lentas. Ahora:
      - 1.er escaneo: userFillsByTime solo de la ventana útil (4 h + margen).
      - Siguientes: userFillsByTime desde el último fill conocido (con 5 min de solape), unido a lo ya guardado.
      - Si una respuesta llega al tope (2.000) no se sabe si faltan fills: se usa userFills completo, como antes.
    El voto solo usa el último fill de cada moneda con posición abierta, así que el resultado es el mismo.
    Si la descarga falla, la caché no cambia y la wallet cuenta como fallida (igual que antes).
    """
    key = addr.lower()
    now_ms = int(time.time() * 1000)
    window_start = now_ms - int(MAX_FRESH_HOURS * 3600 * 1000) - FILLS_WINDOW_MARGIN_MS
    cached = _fills_cache.get(key)
    start_ms = window_start
    if cached:
        start_ms = max(window_start, max(_fill_time(f) for f in cached) - FILLS_OVERLAP_MS)
                                                                                        
    new = _request_fills({"type": "userFillsByTime", "user": addr, "startTime": start_ms},
                         4 if cached else FILLS_FALLBACK_TIMEOUT)
    if len(new) < FILLS_BY_TIME_CAP:
        _vv_ingest(key, new, start_ms, now_ms)                                        
        merged: Dict[Any, Dict[str, Any]] = {}
        for fill in (cached or []) + new:
            merged[_fill_key(fill)] = fill
        fills = list(merged.values())
    else:
                                                                                                         
        fills = _request_fills({"type": "userFills", "user": addr}, FILLS_FALLBACK_TIMEOUT)
        oldest = min((_fill_time(f) for f in fills), default=0)
        _vv_ingest(key, fills, oldest + 1 if fills else 0, now_ms)                                                         
    fills = sorted(fills, key=_fill_time, reverse=True)[:FILLS_CACHE_MAX]
    _fills_cache[key] = fills
    return fills



def get_whales() -> List[Dict[str, Any]]:
    if WHALES_FILE.exists():
        try:
            data = json.loads(WHALES_FILE.read_text(encoding="utf-8"))
            return data.get("hyperliquid_whales", [])
        except Exception:
            pass
    return []

def _fetch_single_whale(w: Dict[str, Any]) -> Dict[str, Any]:
    addr = str(w.get("address", "")).strip()
    name = w.get("name") or (addr[:6] + "…" + addr[-4:])
    res = {"addr": addr, "name": name, "positions": [], "fills": [], "ok": True, "errors": []}
                                                                                                                   
    lado = str(w.get("solo_lado") or "").upper()
    if lado in ("LONG", "SHORT"):
        res["solo_lado"] = lado
    if not addr:
        res["ok"] = False
        res["errors"].append("wallet sin address configurada")
        return res

                                                           
    try:
        r1 = _post_info_retry({"type": "clearinghouseState", "user": addr})
        if r1.status_code != 200:
            raise RuntimeError(f"HTTP {r1.status_code}")
        payload = r1.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("assetPositions"), list):
            raise ValueError("respuesta clearinghouseState inválida")
        for p in payload["assetPositions"]:
            pos = p.get("position", {}) if isinstance(p, dict) else None
            if not isinstance(pos, dict):
                raise ValueError("posición con esquema inválido")
            if float(pos.get("szi", 0)) != 0:
                res["positions"].append(pos)
    except Exception as exc:
        res["ok"] = False
        res["errors"].append(f"clearinghouseState: {exc}")

                                                                                 
    try:
        res["fills"] = _fetch_recent_fills(addr)
    except Exception as exc:
        res["ok"] = False
        res["errors"].append(f"userFills: {exc}")

    return res

def _reuse_recent_reads(results: List[Dict[str, Any]], now_s: float, max_wallets: int = None):
    """Sustituye las lecturas fallidas por la última buena si son pocas y recientes. Devuelve (results, reutilizadas)."""
    for data in results:
        if data.get("ok"):
            _last_good[str(data.get("addr", "")).lower()] = {"data": data, "at": now_s}
    failed_idx = [i for i, data in enumerate(results) if not data.get("ok")]
    limit = LAST_GOOD_MAX_WALLETS if max_wallets is None else max_wallets
    if not failed_idx or len(failed_idx) > limit:
        return results, []
    prev = {i: _last_good.get(str(results[i].get("addr", "")).lower()) for i in failed_idx}
    if any(p is None or now_s - p["at"] > LAST_GOOD_MAX_AGE_SECONDS for p in prev.values()):
        return results, []
    out, reused = list(results), []
    for i, p in prev.items():
        out[i] = p["data"]
        reused.append({"name": results[i].get("name"), "age_sec": round(now_s - p["at"], 1), "errors": results[i].get("errors", [])})
    return out, reused


def scan_whales_onchain() -> Dict[str, Any]:
    global _latest_data
    whales = get_whales()
    if not whales:
        result = {"status": "degraded", "message": "No hay wallets válidas configuradas.", "timestamp": time.time(), "signals": []}
        with _lock:
            _latest_data = result
        return result

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=1) as ex:
        results = list(ex.map(_fetch_single_whale, whales))
    scan_duration = round(time.time() - t0, 2)

    results, reused = _reuse_recent_reads(results, time.time())
    failed = [data for data in results if not data.get("ok")]
    if failed:
                                                                                     
        result = {
            "status": "degraded",
            "mode": "ON_CHAIN_FRESH_IMPULSE_CONSENSUS",
            "message": "Lectura incompleta de wallets; consenso no accionable.",
            "total_whales": len(whales),
            "successful_whales": len(results) - len(failed),
            "failed_whales": [{"name": item["name"], "errors": item["errors"]} for item in failed],
            "scan_duration_sec": scan_duration,
            "timestamp": time.time(),
            "signals": []
        }
        with _lock:
            _latest_data = result
        return result

                                                                                                               
    lighter_info = {"modo": "apagado"}
    l_cfg = {"modo": "apagado", "lighter_whales": []}
    if lighter_source is not None:
        try:
            l_cfg = lighter_source.load_config()
        except Exception as exc:                                         
            l_cfg = {"modo": "apagado", "lighter_whales": [], "error": str(exc)}
        lighter_info = {"modo": l_cfg["modo"], "wallets": len(l_cfg["lighter_whales"]), "config_error": l_cfg.get("error")}
        if l_cfg["modo"] in ("sombra", "produccion") and l_cfg["lighter_whales"]:
            lt0 = time.time()
            hist = _vv_lighter_history_hours(l_cfg["lighter_whales"])
            try:
                l_results = lighter_source.fetch_all(l_cfg["lighter_whales"], MAX_FRESH_HOURS,
                                                     budget_seconds=100.0 if hist else 60.0, history_hours=hist)
            except Exception as exc:                                                                
                l_results = [{"addr": f"lighter:{w['account_index']}", "name": w.get("name", ""), "positions": [], "fills": [],
                              "ok": False, "errors": [f"lighter: {exc}"]} for w in l_cfg["lighter_whales"]]
            now_li = int(time.time() * 1000)
            for item in l_results:
                if item.get("ok") and item.get("cubre_desde_ms") is not None:
                    _vv_ingest(item["addr"], item.get("fills", []), int(item["cubre_desde_ms"]), now_li)
            l_failed = [item for item in l_results if not item.get("ok")]
            lighter_info.update({"leidas_ok": len(l_results) - len(l_failed), "duracion_sec": round(time.time() - lt0, 2),
                                 "fallidas": [{"name": item["name"], "errors": item["errors"]} for item in l_failed]})
            l_results, l_reused = _reuse_recent_reads(l_results, time.time(), LAST_GOOD_MAX_WALLETS - len(reused))
            if l_reused:
                reused = reused + l_reused
                lighter_info["reutilizadas"] = l_reused
                l_failed = [item for item in l_results if not item.get("ok")]
            if l_cfg["modo"] == "produccion":
                if l_failed:
                                                                                                       
                    result = {
                        "status": "degraded",
                        "mode": "ON_CHAIN_FRESH_IMPULSE_CONSENSUS",
                        "message": "Lectura incompleta de wallets de Lighter; consenso no accionable.",
                        "total_whales": len(whales) + len(l_results),
                        "successful_whales": len(results) + len(l_results) - len(l_failed),
                        "failed_whales": lighter_info["fallidas"],
                        "scan_duration_sec": round(time.time() - t0, 2),
                        "timestamp": time.time(),
                        "lighter": lighter_info,
                        "signals": []
                    }
                    with _lock:
                        _latest_data = result
                    return result
                results = results + l_results
            else:
                try:                                                                       
                    now_l = int(time.time() * 1000)
                    lighter_info["votos_sombra"] = [v for item in l_results if item.get("ok")
                                                    for v in lighter_source.fresh_votes(item, now_l, TRACKED_UNIVERSE, MAX_FRESH_HOURS)]
                except Exception as exc:
                    lighter_info["votos_sombra"] = []
                    lighter_info["error_sombra"] = str(exc)

    now_ms = int(time.time() * 1000)
    vv_cfg = {"modo": "apagado", "ventana_horas": 24.0}
    if vote_volatility is not None and VV_LEDGER is not None:
        try:
            vv_cfg = vote_volatility.load_config()
        except Exception as exc:
            vv_cfg = {"modo": "apagado", "ventana_horas": 24.0, "error": str(exc)}
    vv_window_ms = int(float(vv_cfg.get("ventana_horas", 24.0)) * 3_600_000)
    vv_info = {"modo": vv_cfg["modo"], "ventana_horas": vv_cfg.get("ventana_horas"), "config_error": vv_cfg.get("error"),
               "votos_filtrados": [], "votos_sin_historial": 0, "votos_revisados": 0}

    votes = defaultdict(lambda: {
        "fresh_long": 0, "fresh_short": 0,
        "stale_long": 0, "stale_short": 0,
        "whales_fresh_long": [], "whales_fresh_short": [],
        "whales_fresh_long_addrs": [], "whales_fresh_short_addrs": [],
        "prices_fresh_long": [], "prices_fresh_short": []
    })

    active_whales_count = 0

    for data in results:
        addr = data["addr"]
        name = data["name"]
        active_pos_by_coin = {p.get("coin"): p for p in data["positions"]}
        if active_pos_by_coin:
            active_whales_count += 1

        fills_by_coin = defaultdict(list)
        for f in data["fills"]:
            c = f.get("coin")
            if c:
                fills_by_coin[c].append(f)

        for coin, pos in active_pos_by_coin.items():
            if coin not in TRACKED_UNIVERSE:
                continue
            szi = float(pos.get("szi", 0))
            pos_side = "LONG" if szi > 0 else "SHORT"
            solo_lado = data.get("solo_lado")
            if solo_lado and pos_side != solo_lado:
                continue                                                                                                    
            entry_px_macro = float(pos.get("entryPx", 0) or 0)

                                                              
            coin_fills = fills_by_coin.get(coin, [])
            recent_fill = None
            if coin_fills:
                valid_fills = []
                for fill in coin_fills:
                    try:
                        fill_time = int(fill.get("time", 0))
                    except (TypeError, ValueError):
                        continue
                    if 0 < fill_time <= now_ms:
                        valid_fills.append((fill_time, fill))
                latest_f = max(valid_fills, key=lambda item: item[0])[1] if valid_fills else None
                fill_time = int(latest_f.get("time", 0)) if latest_f else 0
                age_hours = (now_ms - fill_time) / (1000 * 3600)
                if latest_f and 0 <= age_hours <= MAX_FRESH_HOURS:
                    dir_str = latest_f.get("dir", "")
                    if (pos_side == "LONG" and ("Long" in dir_str or latest_f.get("side") == "B")) or\
                       (pos_side == "SHORT" and ("Short" in dir_str or latest_f.get("side") == "A")):
                        recent_fill = latest_f

                                                                                                                                 
                                                                                                 
            if recent_fill and not solo_lado and vv_cfg["modo"] in ("sombra", "produccion"):
                fill_ms = int(recent_fill.get("time", 0))
                verdict = VV_LEDGER.check(addr, coin, pos_side, fill_ms, vv_window_ms)
                vv_info["votos_revisados"] += 1
                if verdict is None:
                    vv_info["votos_sin_historial"] += 1                                               
                elif verdict:
                    last_opp = VV_LEDGER.last_opposite_end(addr, coin, pos_side)
                    vv_info["votos_filtrados"].append({
                        "wallet": addr, "name": name, "coin": coin, "side": pos_side,
                        "contraria_hace_h": round((fill_ms - last_opp) / 3_600_000, 2)})
                    if vv_cfg["modo"] == "produccion":
                        continue                                                                       

            if recent_fill:
                fresh_px = float(recent_fill.get("px", entry_px_macro) or entry_px_macro)
                age_h = (now_ms - int(recent_fill.get("time", 0))) / (1000 * 3600)
                whale_label = f"{name} ({age_h:.1f}h)"
                if pos_side == "LONG":
                    votes[coin]["fresh_long"] += 1
                    votes[coin]["whales_fresh_long"].append(whale_label)
                    votes[coin]["whales_fresh_long_addrs"].append(addr)
                    if fresh_px > 0:
                        votes[coin]["prices_fresh_long"].append(fresh_px)
                else:
                    votes[coin]["fresh_short"] += 1
                    votes[coin]["whales_fresh_short"].append(whale_label)
                    votes[coin]["whales_fresh_short_addrs"].append(addr)
                    if fresh_px > 0:
                        votes[coin]["prices_fresh_short"].append(fresh_px)
            else:
                if pos_side == "LONG":
                    votes[coin]["stale_long"] += 1
                else:
                    votes[coin]["stale_short"] += 1

    signals = []
    for coin in TRACKED_UNIVERSE:
        raw_sym = coin + "USDT"
        display_sym = coin + "/USDT"
        d = votes[coin]
        fl = d["fresh_long"]
        fs = d["fresh_short"]
        sl = d["stale_long"]
        ss = d["stale_short"]
        tot_fresh = fl + fs

        status = "WAITING"
        side = "NEUTRO"
        ref_price = 0.0
        notes = ""
        supporters = []
        supporter_names = []
        support_pct = 100.0

        if fl >= 2 and fl > fs:
            support_pct = (fl / tot_fresh * 100.0) if tot_fresh > 0 else 100.0
            if fs > 0 and support_pct < 75.0:
                side = "NEUTRO"
                status = "CONFLICT"
                notes = f"Veto de Conflicto fresco: {fl} LONG vs {fs} SHORT (< 75%)"
            else:
                side = "LONG"
                status = "CONFIRMED_ONCHAIN"
                supporters = d["whales_fresh_long_addrs"]
                supporter_names = d["whales_fresh_long"]
                ref_price = statistics.median(d["prices_fresh_long"]) if d["prices_fresh_long"] else 0.0
                notes = f"🚀 Impulso Fresco ({fl} Ballenas < {MAX_FRESH_HOURS}h | Entrada Ref: ${ref_price:.4f})"
        elif fs >= 2 and fs > fl:
            support_pct = (fs / tot_fresh * 100.0) if tot_fresh > 0 else 100.0
            if fl > 0 and support_pct < 75.0:
                side = "NEUTRO"
                status = "CONFLICT"
                notes = f"Veto de Conflicto fresco: {fs} SHORT vs {fl} LONG (< 75%)"
            else:
                side = "SHORT"
                status = "CONFIRMED_ONCHAIN"
                supporters = d["whales_fresh_short_addrs"]
                supporter_names = d["whales_fresh_short"]
                ref_price = statistics.median(d["prices_fresh_short"]) if d["prices_fresh_short"] else 0.0
                notes = f"🔻 Impulso Fresco Bajista ({fs} Ballenas < {MAX_FRESH_HOURS}h | Entrada Ref: ${ref_price:.4f})"
        elif fl == 1 or fs == 1:
            side = "LONG" if fl == 1 else "SHORT"
            status = "WAITING"
            supporters = d["whales_fresh_long_addrs"] if fl == 1 else d["whales_fresh_short_addrs"]
            supporter_names = d["whales_fresh_long"] if fl == 1 else d["whales_fresh_short"]
            ref_price = d["prices_fresh_long"][0] if fl == 1 and d["prices_fresh_long"] else (d["prices_fresh_short"][0] if d["prices_fresh_short"] else 0.0)
            notes = f"1 Ballena con impulso fresco ({supporter_names[0] if supporter_names else ''}). Requiere >= 2 ballenas"
        elif sl > 0 or ss > 0:
            status = "MACRO_HOLDING_STALE"
            side = "LONG" if sl > ss else ("SHORT" if ss > sl else "NEUTRO")
            notes = f"💤 Posición Macro Pasiva ({sl}L / {ss}S sin actividad reciente en > {MAX_FRESH_HOURS}h). Descartada para scalping."
        else:
            status = "WAITING"
            side = "NEUTRO"
            notes = "Sin posiciones en la cohorte"

        tot_votes = len(supporters)
        lev = 7 if tot_votes >= 4 else (5 if tot_votes == 3 else 3)
        risk = 0.020 if tot_votes >= 4 else (0.015 if tot_votes == 3 else 0.010)
        fp = f"{coin}_{side}_{len(supporters)}_{'-'.join(sorted(supporters)[:3])}" if supporters else f"{coin}_{side}_none"

        signals.append({
            "symbol": display_sym,
            "raw_symbol": raw_sym,
            "coin": coin,
            "side": side,
            "hl_votes": tot_votes,
            "total_votes": tot_votes,
            "fresh_votes": tot_votes,
            "long_votes": fl,
            "short_votes": fs,
            "support_pct": round(support_pct, 1),
            "reference_price": round(ref_price, 4),
            "supporters": supporters,
            "supporter_names": supporter_names,
            "fingerprint": fp,
            "leverage": lev,
            "risk_pct": risk,
            "status": status,
            "notes": notes,
            "stale_summary": f"{sl}L / {ss}S",
            "whales_long": d["whales_fresh_long"],
            "whales_short": d["whales_fresh_short"]
        })

    result = {
        "status": "ok",
        "mode": "ON_CHAIN_FRESH_IMPULSE_CONSENSUS",
        "cohort": f"Hyperliquid L1 {len(whales)} + Lighter {len(results) - len(whales)} Elite Whales (Fresh Orders <= 4h)",
        "total_whales": len(results),
        "hyperliquid_whales": len(whales),
        "lighter": lighter_info,
        "active_whales_with_positions": active_whales_count,
        "scan_duration_sec": scan_duration,
        "reused_whales": reused,
        "filtro_volatil": vv_info,
        "max_fresh_hours": MAX_FRESH_HOURS,
        "timestamp": time.time(),
        "signals": signals
    }

    try:
        CACHE_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

    with _lock:
        _latest_data = result

    return result

def get_latest_consensus() -> Dict[str, Any]:
    global _latest_data
    with _lock:
        if _latest_data:
            if _latest_data.get("status") != "ok":
                return _latest_data
            if 0 <= time.time() - _latest_data.get("timestamp", 0) < CONSENSUS_CACHE_TTL_SECONDS:
                return _latest_data

    if CACHE_FILE.exists():
        try:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if cached.get("status") == "ok" and 0 <= time.time() - cached.get("timestamp", 0) < CONSENSUS_CACHE_TTL_SECONDS:
                with _lock:
                    _latest_data = cached
                return cached
        except Exception:
            pass

                                                                       
    return {
        "status": "scanning" if _worker_started else "unavailable",
        "message": "Aún no hay un consenso completo y reciente.",
        "timestamp": time.time(),
        "signals": []
    }

def _vv_lighter_history_hours(lighter_whales: List[Dict[str, Any]]) -> Dict[int, float]:
    """Horas de historial a pedir por cuenta de Lighter: ventana del filtro + 4 h si aún no hay cobertura suficiente."""
    if vote_volatility is None or VV_LEDGER is None:
        return {}
    try:
        cfg = vote_volatility.load_config()
    except Exception:
        return {}
    if cfg["modo"] == "apagado":
        return {}
    need_h = cfg["ventana_horas"] + MAX_FRESH_HOURS + 0.25
    target = int(time.time() * 1000) - int(need_h * 3_600_000)
    out = {}
    for w in lighter_whales:
        cf = VV_LEDGER.covered_from(f"lighter:{int(w['account_index'])}")
        if cf is None or cf > target:
            out[int(w["account_index"])] = need_h
    return out


def _vv_save() -> None:
    if VV_LEDGER is None:
        return
    try:
        VV_LEDGER.save(vote_volatility.LEDGER_FILE)
    except Exception as exc:
        print(f"Filtro voto volátil: no se pudo guardar el registro: {exc}")


def _vv_backfill_worker():
    """Completa hacia atrás (ventana + 4 h) el historial de fills de cada wallet de Hyperliquid, despacio."""
    while True:
        try:
            cfg = vote_volatility.load_config()
            if cfg["modo"] == "apagado":
                time.sleep(120)
                continue
            target = int(time.time() * 1000) - int((cfg["ventana_horas"] + MAX_FRESH_HOURS + 0.25) * 3_600_000)
            for w in get_whales():
                addr = str(w.get("address", "")).strip()
                cov = VV_LEDGER.coverage(addr) if addr else None
                if not addr or cov is None or cov[0] <= target:
                    continue
                try:
                    status = vote_volatility.backfill_wallet(
                        VV_LEDGER, addr, lambda payload: _request_fills(payload, FILLS_FALLBACK_TIMEOUT), target,
                        max_pages=25, sleep_s=VV_BACKFILL_SLEEP_SECONDS)
                    print(f"Filtro voto volátil: historial {addr[:10]} {status}")
                    _vv_save()
                except Exception as exc:
                    print(f"Filtro voto volátil: historial {addr[:10]} falló: {exc}")
                time.sleep(VV_BACKFILL_SLEEP_SECONDS)
        except Exception as exc:
            print(f"Filtro voto volátil: error en relleno histórico: {exc}")
        time.sleep(120)


def _daemon_worker():
    global _latest_data
    if VV_LEDGER is not None:
        VV_LEDGER.load(vote_volatility.LEDGER_FILE)
        threading.Thread(target=_vv_backfill_worker, daemon=True, name="VoteVolatilityBackfill").start()
    while True:
        started = time.monotonic()
        try:
            with _scan_lock:
                result = scan_whales_onchain()
            if result.get("status") != "ok":
                print(f"Consenso no accionable: {result.get('message', result.get('status'))}")
            _vv_save()
        except Exception as e:
            failure = {
                "status": "degraded",
                "message": f"Falló el escaneo de la cohorte; consenso no accionable: {e}",
                "timestamp": time.time(),
                "signals": []
            }
            with _lock:
                _latest_data = failure
            print(f"Error en worker de ballenas frescas: {e}")
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, SCAN_INTERVAL_SECONDS - elapsed))

_worker_started = False
def start_scanner_daemon():
    global _worker_started
    with _lock:
        if _worker_started:
            return
        _worker_started = True
    t = threading.Thread(target=_daemon_worker, daemon=True, name="LiveFreshWhaleScannerThread")
    t.start()

if __name__ == "__main__":
    print("Ejecutando escaneo manual on-chain de impulsos frescos...")
    res = scan_whales_onchain()
    print(f"Listo en {res['scan_duration_sec']}s. Ballenas activas: {res['active_whales_with_positions']}/{res['total_whales']}")
    for s in res["signals"]:
        print(f"{s['symbol']:10} | {s['side']:5} | Status: {s['status']:22} | Ref: {s['reference_price']:10} | {s['notes']}")
