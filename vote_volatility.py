"""
ANTIGRAVITY · Filtro de VOTO VOLÁTIL (14-10-2026)
=================================================
Regla (la misma del replay research/.../scripts/44_replay_sin_ptas2_y_volatiles.py):
  el voto fresco de una wallet en una moneda NO cuenta si esa wallet tuvo la posición CONTRARIA en esa moneda
  en las `ventana_horas` (24 h) anteriores al fill que genera el voto.
  Replay de 92 días (8 fases), 36 + Lighter B: +53,4 % y DD 6,2 % con el filtro frente a +32,6 % y DD 13,8 % sin él;
  mejor en 8/8 fases en la prueba. Motivo: wallets que cambian de lado varias veces al día (p. ej. ptas2 en HYPE el 14-10)
  aportan votos que no son una tesis.

Cómo se sabe si tuvo la posición contraria: cada fill trae la posición ANTES del fill (HL: startPosition; Lighter: se
reconstruye). Si un fill deja una posición LONG en 0 o en SHORT, ahí "terminó un LONG" (y al revés). El registro
guarda, por wallet y moneda, la última vez que terminó un LONG y un SHORT, y el tramo de tiempo en el que se han
procesado TODOS los fills de la wallet (cobertura). Sin cobertura suficiente la regla no se aplica a ese voto: cuenta
como hasta ahora (nunca es más restrictivo por falta de datos).

Configuración: FILTRO_VOTO_VOLATIL.json  {"modo": "apagado" | "sombra" | "produccion", "ventana_horas": 24}
  - apagado:    no hace nada (comportamiento anterior) y no hace peticiones extra.
  - sombra:     calcula y publica qué votos filtraría, sin cambiar las señales.
  - produccion: los votos volátiles no cuentan (ni a favor ni en contra), como en el replay.
Registro persistente: vote_volatility_ledger.json (se carga al arrancar y se guarda tras cada escaneo).
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parent
CONFIG_FILE = ROOT / "FILTRO_VOTO_VOLATIL.json"
LEDGER_FILE = ROOT / "vote_volatility_ledger.json"
VALID_MODES = ("apagado", "sombra", "produccion")
EPS = 1e-12
H_MS = 3_600_000


def load_config() -> Dict[str, Any]:
    """Ante cualquier problema devuelve 'apagado' (nunca rompe el escáner)."""
    default = {"modo": "apagado", "ventana_horas": 24.0, "error": None}
    if not CONFIG_FILE.exists():
        return default
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))
        mode = str(data.get("modo", "apagado")).strip().lower()
        hours = float(data.get("ventana_horas", 24))
        if mode not in VALID_MODES or not 1 <= hours <= 72:
            return {**default, "error": f"configuración inválida: modo={mode} ventana_horas={hours}"}
        return {"modo": mode, "ventana_horas": hours, "error": None}
    except Exception as exc:
        return {**default, "error": f"configuración ilegible: {exc}"}


def _transition(fill: Dict[str, Any]):
    """(posición antes, posición después) del fill, o None si el fill no trae startPosition."""
    try:
        before = float(fill["startPosition"])
        sz = float(fill.get("sz") or 0)
    except (KeyError, TypeError, ValueError):
        return None
    side = fill.get("side")
    if side not in ("A", "B"):
        return None
    return before, (before + sz if side == "B" else before - sz)


class Ledger:
    """Por wallet: cobertura [desde, hasta] (ms) en la que se han visto todos sus fills, y por moneda la última vez
    que terminó una posición LONG y una SHORT."""

    def __init__(self, universe: Optional[Iterable[str]] = None):
        self._lock = threading.Lock()
        self._w: Dict[str, Dict[str, Any]] = {}
        self.universe = set(universe) if universe else None

                                                                                
    def ingest(self, key: str, fills: List[Dict[str, Any]], from_ms: Optional[int], to_ms: Optional[int]) -> None:
        """Procesa fills. Si from_ms/to_ms se dan, son el tramo en el que `fills` son TODOS los fills de la wallet."""
        key = key.lower()
        complete = True
        with self._lock:
            w = self._w.setdefault(key, {"cov": None, "ends": {}})
            for f in fills:
                coin = f.get("coin")
                tr = _transition(f)
                if tr is None:
                    complete = False
                    continue
                if self.universe is not None and coin not in self.universe:
                    continue
                before, after = tr
                try:
                    t = int(f.get("time") or 0)
                except (TypeError, ValueError):
                    complete = False
                    continue
                ends = w["ends"].setdefault(coin, {"LONG": 0, "SHORT": 0})
                if before > EPS and after <= EPS:
                    ends["LONG"] = max(ends["LONG"], t)
                if before < -EPS and after >= -EPS:
                    ends["SHORT"] = max(ends["SHORT"], t)
            if from_ms is None or to_ms is None:
                return
            if not complete:
                w["cov"] = None                                                               
                return
            cov = w["cov"]
            if cov is None:
                w["cov"] = [int(from_ms), int(to_ms)]
            elif from_ms <= cov[1] and to_ms >= cov[0]:
                w["cov"] = [min(cov[0], int(from_ms)), max(cov[1], int(to_ms))]
            elif to_ms > cov[1]:
                w["cov"] = [int(from_ms), int(to_ms)]                                                
                                                                           

                                                                              
    def covered_from(self, key: str) -> Optional[int]:
        with self._lock:
            w = self._w.get(key.lower())
            return w["cov"][0] if w and w["cov"] else None

    def coverage(self, key: str) -> Optional[List[int]]:
        with self._lock:
            w = self._w.get(key.lower())
            return list(w["cov"]) if w and w["cov"] else None

    def check(self, key: str, coin: str, side: str, fill_ms: int, window_ms: int):
        """True = voto volátil; False = voto válido; None = sin historial suficiente (no se aplica la regla)."""
        with self._lock:
            w = self._w.get(key.lower())
            if not w or not w["cov"]:
                return None
            lo, hi = w["cov"]
            if lo > fill_ms - window_ms or hi < fill_ms:
                return None
            opp = "SHORT" if side == "LONG" else "LONG"
            last_opp_end = w["ends"].get(coin, {}).get(opp, 0)
            return last_opp_end > fill_ms - window_ms

    def last_opposite_end(self, key: str, coin: str, side: str) -> int:
        with self._lock:
            w = self._w.get(key.lower())
            opp = "SHORT" if side == "LONG" else "LONG"
            return int(w["ends"].get(coin, {}).get(opp, 0)) if w else 0

                                                                                   
    def to_json(self) -> Dict[str, Any]:
        with self._lock:
            return {"version": 1, "saved_at_ms": int(time.time() * 1000), "wallets": json.loads(json.dumps(self._w))}

    def load_json(self, data: Dict[str, Any]) -> None:
        wallets = data.get("wallets") if isinstance(data, dict) else None
        if not isinstance(wallets, dict):
            return
        clean = {}
        for key, w in wallets.items():
            cov = w.get("cov")
            if cov is not None and not (isinstance(cov, list) and len(cov) == 2 and cov[0] <= cov[1]):
                cov = None
            ends = {c: {"LONG": int(v.get("LONG", 0)), "SHORT": int(v.get("SHORT", 0))}
                    for c, v in (w.get("ends") or {}).items() if isinstance(v, dict)}
            clean[str(key).lower()] = {"cov": [int(cov[0]), int(cov[1])] if cov else None, "ends": ends}
        with self._lock:
            self._w = clean

    def save(self, path: Path = LEDGER_FILE) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.to_json(), separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)

    def load(self, path: Path = LEDGER_FILE) -> bool:
        if not path.exists():
            return False
        try:
            self.load_json(json.loads(path.read_text(encoding="utf-8")))
            return True
        except Exception:
            return False


                                                                                                                    
def backfill_wallet(ledger: Ledger, addr: str, request: Callable[[Dict[str, Any]], List[Dict[str, Any]]],
                    target_start_ms: int, max_pages: int = 25, sleep_s: float = 0.0) -> str:
    """Completa hacia atrás la cobertura de una wallet de Hyperliquid hasta target_start_ms.

    Pide userFillsByTime [inicio, inicio de la cobertura actual] en páginas ascendentes de hasta 2.000 fills.
    Solo extiende la cobertura si llega al final sin huecos. Devuelve un texto con el resultado.
    """
    cov = ledger.coverage(addr)
    if cov is None:
        return "sin_cobertura_inicial"                                                         
    end_ms = cov[0]
    if end_ms <= target_start_ms:
        return "cubierta"
    st = target_start_ms
    for page in range(max_pages):
        fills = request({"type": "userFillsByTime", "user": addr, "startTime": st, "endTime": end_ms})
        if any(_transition(f) is None for f in fills):
            return "fills sin startPosition: no se puede completar"
        ledger.ingest(addr, fills, None, None)                                                  
        if len(fills) < 2000:
            ledger.ingest(addr, [], target_start_ms, end_ms)
            return f"completada en {page + 1} páginas"
        last = max(int(f.get("time") or 0) for f in fills)
        st = last if last > st else st + 1
        if st >= end_ms:
            ledger.ingest(addr, [], target_start_ms, end_ms)
            return f"completada en {page + 1} páginas"
        if sleep_s:
            time.sleep(sleep_s)
    return f"incompleta tras {max_pages} páginas"
