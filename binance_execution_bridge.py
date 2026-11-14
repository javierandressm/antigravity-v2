import gc
import json
import os
import sys
import time
import threading
import math
import uuid
from contextlib import contextmanager
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

from binance_futures_client import BinanceFuturesClient
from antigravity_config import load_bot_config
from antigravity_runtime import atomic_write_json

def print(*args, **kwargs):
    sep = kwargs.get('sep', ' ')
    end = kwargs.get('end', '\n')
    text = sep.join(str(a) for a in args)
    enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
    try:
        sys.stdout.write(text + end)
        sys.stdout.flush()
    except (UnicodeEncodeError, Exception):
        try:
            safe = text.encode(enc, errors='replace').decode(enc)
            sys.stdout.write(safe + end)
            sys.stdout.flush()
        except Exception:
            pass

AGENT_ID = "DEMO_ID_REMOVED"
ROOT = Path(__file__).resolve().parent
CONFIG = load_bot_config()
_EXECUTION_LOCKS = {}
_EXECUTION_LOCKS_GUARD = threading.Lock()
_EXECUTION_LOCK_LOCAL = threading.local()

class BinanceExecutionBridge:
    @staticmethod
    def unit_risk_to_stop(entry, stop, fee_rate, slippage_pct):
        """One risk definition for pre-submit sizing and the actual filled entry."""
        return (abs(entry - stop) + (entry + stop) * fee_rate
                + (entry + stop) * slippage_pct / 100.0)

    @classmethod
    def planned_unit_risk(cls, entry, stop_distance, is_long, fee_rate, slippage_pct):
        """Bound fill costs within the configured reserve without spending tick savings."""
        risks = []
        for fill in (entry * (1 - slippage_pct / 100.0), entry * (1 + slippage_pct / 100.0)):
            stop = fill - stop_distance if is_long else fill + stop_distance
            if fill <= 0 or stop <= 0:
                raise ValueError("Invalid planned fill/stop")
            risks.append(cls.unit_risk_to_stop(fill, stop, fee_rate, slippage_pct))
        return max(risks)

    def __init__(self, client: BinanceFuturesClient):
        self.client = client
        self.agent_id = AGENT_ID
        self.state_file = ROOT / ("antigravity_agent_state.json" if client.testnet else "antigravity_agent_state_mainnet.json")
        lock_key = str(self.state_file.with_suffix(".execution.lock")).lower()
        with _EXECUTION_LOCKS_GUARD:
            self._thread_execution_lock = _EXECUTION_LOCKS.setdefault(lock_key, threading.RLock())
        self._execution_lock_key = lock_key
        self._execution_lock_file = self.state_file.with_suffix(".execution.lock")
        self._state_file_signature = None
        self.state = self.load_state()

    @contextmanager
    def execution_lock(self):
        """Serialize state changes and account mutations across daemon/panel processes."""
        with self._thread_execution_lock:
            held = getattr(_EXECUTION_LOCK_LOCAL, "held", set())
            if self._execution_lock_key in held:
                yield
                return
            lock_path = self._execution_lock_file
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+b")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            timeout = float(CONFIG["operations"]["execution_lock_timeout_seconds"])
            interval = float(CONFIG["operations"]["execution_lock_poll_seconds"])
            deadline = time.monotonic() + timeout
            locked = False
            try:
                while not locked:
                    handle.seek(0)
                    try:
                        if os.name == "nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        locked = True
                    except (OSError, BlockingIOError):
                        if time.monotonic() >= deadline:
                            raise TimeoutError("No se pudo adquirir el bloqueo compartido de ejecución dentro del plazo.")
                        time.sleep(interval)
                held = set(held)
                held.add(self._execution_lock_key)
                _EXECUTION_LOCK_LOCAL.held = held
                yield
            finally:
                current = set(getattr(_EXECUTION_LOCK_LOCAL, "held", set()))
                current.discard(self._execution_lock_key)
                _EXECUTION_LOCK_LOCAL.held = current
                if locked:
                    try:
                        handle.seek(0)
                        if os.name == "nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        handle.close()
                else:
                    handle.close()

    def load_state(self):
        with self.execution_lock():
            self._state_file_signature = self._get_state_file_signature()
            return self._load_state_unlocked()

    def _get_state_file_signature(self):
        try:
            stat = self.state_file.stat()
            return (stat.st_mtime_ns, stat.st_size, getattr(stat, "st_ino", None))
        except FileNotFoundError:
            return None

                                                                                                                       
                                                                                                                       
    STATE_READ_MEMORY_BACKOFF_SECONDS = (0.25, 0.5, 1.0)

    def _load_state_unlocked(self):
        if self.state_file.exists():
            retries = len(self.STATE_READ_MEMORY_BACKOFF_SECONDS)
            for attempt in range(retries + 1):
                try:
                    with self.state_file.open("r", encoding="utf-8") as state_file:
                        state = json.load(state_file)
                    if not isinstance(state, dict):
                        raise ValueError("estado raíz inválido")
                    return state
                except MemoryError as exc:
                    if attempt < retries:
                        gc.collect()
                        time.sleep(self.STATE_READ_MEMORY_BACKOFF_SECONDS[attempt])
                        continue
                    raise RuntimeError(f"No se pudo leer estado {self.state_file.name}: falta de memoria del sistema "
                                       f"(MemoryError) tras {attempt + 1} intentos") from exc
                except Exception as exc:
                    raise RuntimeError(f"No se pudo leer estado {self.state_file.name}: {str(exc) or type(exc).__name__}") from exc
        return {
            "agent_id": self.agent_id, "active_symbols": [], "positions": {},
            "closed_positions": {}, "stopped_theses": {}, "symbol_cooldowns": {},
            "execution_control": {"armed": False, "disarmed_reason": "initially_disarmed"},
            "protection_circuit_breaker": {"active": False},
            "protection_failures": []
        }

    def save_state(self):
        with self.execution_lock():
            current_signature = self._get_state_file_signature()
            if current_signature != self._state_file_signature:
                raise RuntimeError("El estado AG cambió desde la última lectura; no se sobrescribió para evitar perder actualizaciones concurrentes. Recargue el estado y reintente.")
            atomic_write_json(self.state_file, self.state)
            self._state_file_signature = self._get_state_file_signature()

    @staticmethod
    def _exchange_error_code(result):
        if not isinstance(result, dict):
            return None
        try:
            return int(result.get("code"))
        except (TypeError, ValueError):
            return None

    def _forget_executed_fingerprint(self, metadata):
        fingerprint = metadata.get("fingerprint") if isinstance(metadata, dict) else None
        if not fingerprint:
            return False
        fingerprints = self.state.setdefault("executed_fingerprints", [])
        remaining = [value for value in fingerprints if value != fingerprint]
        if len(remaining) == len(fingerprints):
            return False
        self.state["executed_fingerprints"] = remaining
        return True

    def _register_cooling_off_rejection(self, symbol):
        operations = CONFIG["operations"]
        initial = float(operations["cooling_off_retry_initial_seconds"])
        multiplier = float(operations["cooling_off_retry_multiplier"])
        maximum = float(operations["cooling_off_retry_max_seconds"])
        previous = self.state.get("binance_cooling_off", {})
        rejection_count = int(previous.get("rejection_count", 0) or 0) if isinstance(previous, dict) else 0
        delay = float(previous.get("next_retry_delay_seconds", initial) or initial) if rejection_count else initial
        delay = min(maximum, max(initial, delay))
        next_delay = min(maximum, delay * multiplier)
        rejected_at = time.time()
        cooling = {
            "code": -4192,
            "last_rejection_at": rejected_at,
            "retry_after": rejected_at + delay,
            "retry_delay_seconds": delay,
            "next_retry_delay_seconds": next_delay,
            "rejection_count": rejection_count + 1,
            "last_symbol": symbol,
        }
        self.state["binance_cooling_off"] = cooling
        return cooling

    def get_cooling_off_state(self):
        self.state = self.load_state()
        cooling = self.state.get("binance_cooling_off", {})
        return dict(cooling) if isinstance(cooling, dict) else {}

    def entries_armed(self):
        """Mainnet entries require explicit persistent arming and a clear safety breaker."""
        self.state = self.load_state()
        if self.client.testnet:
            return True
        control = self.state.get("execution_control", {})
        breaker = self.state.get("protection_circuit_breaker", {})
        return bool(isinstance(control, dict) and control.get("armed") is True
                    and not (isinstance(breaker, dict) and breaker.get("active") is True))

    def disarm_for_runtime_failure(self, reason, details=None):
        """Persist a global entry disarm after supervisor exhaustion or ambiguity."""
        with self.execution_lock():
            self.state = self.load_state()
            now = time.time()
            control = self.state.setdefault("execution_control", {})
            if not isinstance(control, dict):
                control = {}
                self.state["execution_control"] = control
            if "auto_resume_armed" not in control or control.get("armed") is True:
                control["auto_resume_armed"] = control.get("armed") is True
            prior_reason = str(control.get("disarmed_reason", ""))
            preserve_critical_reason = (control.get("armed") is not True and prior_reason
                                        and prior_reason not in {
                                            "SUPERVISOR_HEARTBEAT_STALE",
                                            "SUPERVISOR_RESTART_BUDGET_EXHAUSTED",
                                            "STARTUP_RECONCILIATION_AMBIGUOUS",
                                        })
            control.update({
                "armed": False,
                "disarmed_at": now,
                "disarmed_reason": prior_reason if preserve_critical_reason else str(reason),
                "runtime_failure_details": details if isinstance(details, dict) else {},
            })
            self.save_state()
            return dict(control)

    def audit_runtime_recovery(self):
        """Read-only exchange audit; never repairs, cancels or submits an order."""
        self.state = self.load_state()
        blockers = []
        positions = self._read_positions()
        standard_orders = self.client.get_open_orders()
        algos = self._read_open_algo_orders()
        if not isinstance(standard_orders, list):
            raise RuntimeError("Binance no confirmó órdenes estándar durante recuperación.")
        if standard_orders:
            blockers.append("UNRECONCILED_STANDARD_ORDERS")
        untracked = self.get_untracked_positions(positions)
        if untracked is None:
            raise RuntimeError("Binance no confirmó modo de posición durante recuperación.")
        if untracked:
            blockers.append("UNTRACKED_LIVE_POSITIONS")
        live = [p for p in positions if self._is_open_position(p)]
        live_symbols = {p.get("symbol") for p in live}
        local_positions = self.state.get("positions", {})
        if any(symbol not in live_symbols for symbol in local_positions):
            blockers.append("LOCAL_POSITION_NOT_RECONCILED")
        if any(not self._algo_is_owned(a, local_positions.get(a.get("symbol"), {}))
               or a.get("symbol") not in live_symbols for a in algos
               if str(a.get("algoStatus", "")).upper() in {"NEW", "TRIGGERING"}):
            blockers.append("UNOWNED_OR_RESIDUAL_ALGO_ORDERS")
        checks = []
        for position in live:
            symbol = position["symbol"]
            verified = self._revalidate_position_protection(symbol, local_positions.get(symbol, {}))
            checks.append({"symbol": symbol, "protected": bool(verified.get("is_protected"))})
        if not all(check["protected"] for check in checks):
            blockers.append("POSITION_PROTECTION_NOT_CONFIRMED")
        if self.has_unresolved_reconciliation():
            blockers.append("UNRESOLVED_POSITION_OR_ORDER_STATE")
        return {"verified_at": time.time(), "blockers": blockers,
                "all_positions_protected": not blockers,
                "pending_reconciliations": list(self.state.get("closed_positions", {})),
                "verified_positions": checks}

    def restore_supervisor_false_disarm(self, evidence):
        """Restore a recoverable runtime disarm after fresh exchange reconciliation."""
        evidence = evidence if isinstance(evidence, dict) else {}
        required = (evidence.get("startup_reconciliation_complete") is True
                    and evidence.get("all_positions_protected") is True
                    and evidence.get("pending_reconciliations") == []
                    and int(evidence.get("supervisor_pid", 0) or 0) > 0)
        if not required:
            return {"restored": False, "reason": "RECOVERY_EVIDENCE_INCOMPLETE"}
        with self.execution_lock():
            self.state = self.load_state()
            if self.state.get("closed_positions"):
                return {"restored": False, "reason": "RECONCILIATION_PENDING"}
            for metadata in self.state.get("positions", {}).values():
                if not isinstance(metadata, dict):
                    continue
                if (metadata.get("entry_submit_uncertain") is True
                        or str(metadata.get("status", "")).upper() in {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}
                        or metadata.get("pending_algo_orders")
                        or metadata.get("close_client_order_id")):
                    return {"restored": False, "reason": "RECONCILIATION_PENDING"}
            control = self.state.get("execution_control", {})
            breaker = self.state.get("protection_circuit_breaker", {})
            recoverable_disarm_reasons = {
                "SUPERVISOR_HEARTBEAT_STALE",
                "SUPERVISOR_RESTART_BUDGET_EXHAUSTED",
                "STARTUP_RECONCILIATION_AMBIGUOUS",
            }
            if (not isinstance(control, dict) or control.get("armed") is True
                    or control.get("disarmed_reason") not in recoverable_disarm_reasons):
                return {"restored": False, "reason": "DISARM_REASON_NOT_RECOVERABLE"}
            if control.get("auto_resume_armed") is False:
                return {"restored": False, "reason": "NO_PRIOR_ARMING_TO_RESTORE"}
            if isinstance(breaker, dict) and breaker.get("active") is True:
                return {"restored": False, "reason": "PROTECTION_BREAKER_ACTIVE"}
            now = time.time()
            recoveries = self.state.setdefault("runtime_disarm_recoveries", [])
            if not isinstance(recoveries, list):
                recoveries = []
                self.state["runtime_disarm_recoveries"] = recoveries
            for previous in recoveries:
                if (isinstance(previous, dict)
                        and previous.get("reason") in recoverable_disarm_reasons
                        and "persistence_result" not in previous):
                    previous["persistence_result"] = "NOT_APPLIED_STATE_REFERENCE_REPLACED"
            disarm_reason = control.get("disarmed_reason")
            recoveries.append({
                "reason": disarm_reason,
                "resolved_as": ("FRESH_BINANCE_RECONCILIATION"
                                if disarm_reason == "STARTUP_RECONCILIATION_AMBIGUOUS"
                                else "PID_SCOPED_DAEMON_HEARTBEAT_STARTUP_FIX"
                                if disarm_reason == "SUPERVISOR_RESTART_BUDGET_EXHAUSTED"
                                else "WINDOWS_PID_LIVENESS_CHECK_FALSE_NEGATIVE"),
                "recovered_at": now, "persistence_result": "APPLIED",
                "evidence": evidence,
            })
            control.update({
                "armed": True, "armed_at": now,
                "armed_reason": "runtime_recovered_after_fresh_binance_reconciliation",
            })
            control.pop("disarmed_reason", None)
            control.pop("runtime_failure_details", None)
            self.save_state()
            return {"restored": True, "recovered_at": now}

    def resolve_protection_breaker_after_reconciliation(self, evidence):
        """Resolve a latched breaker only after its position and remaining portfolio are verified safe."""
        evidence = evidence if isinstance(evidence, dict) else {}
        required = (
            evidence.get("binance_read_verified") is True
            and evidence.get("triggering_position_flat") is True
            and evidence.get("triggering_symbol_orders_empty") is True
            and evidence.get("all_live_positions_protected") is True
            and evidence.get("pending_reconciliations") == []
            and evidence.get("root_cause_fix") == "PRE_SUBMIT_EXCHANGE_FILTER_VALIDATION_V1"
        )
        if not required:
            return {"resolved": False, "reason": "BREAKER_RESOLUTION_EVIDENCE_INCOMPLETE"}
        with self.execution_lock():
            self.state = self.load_state()
            breaker = self.state.get("protection_circuit_breaker", {})
            if not isinstance(breaker, dict) or breaker.get("active") is not True:
                return {"resolved": False, "reason": "NO_ACTIVE_PROTECTION_BREAKER"}
            symbol = str(breaker.get("symbol", "")).upper()
            if not symbol or symbol in self.state.get("positions", {}):
                return {"resolved": False, "reason": "TRIGGERING_POSITION_STILL_TRACKED"}
            if self.state.get("closed_positions"):
                return {"resolved": False, "reason": "RECONCILIATION_PENDING"}
            closure = next((item for item in reversed(self.state.get("reconciled_closures", []))
                            if isinstance(item, dict) and item.get("symbol") == symbol
                            and item.get("status") == "RECONCILED"
                            and item.get("classification") == "PROTECTION_FAILURE_SAFETY_CLOSE"), None)
            if not closure:
                return {"resolved": False, "reason": "TRIGGERING_CLOSURE_NOT_RECONCILED"}
            now = time.time()
            resolution = {
                "symbol": symbol,
                "reason": breaker.get("reason"),
                "resolved_at": now,
                "resolution": "CLOSED_POSITION_RECONCILED_AND_PRE_SUBMIT_FILTER_GUARD_ADDED",
                "closure_key": closure.get("closure_key"),
                "net_pnl_usdt": closure.get("net_pnl_usdt"),
                "evidence": evidence,
            }
            history = self.state.setdefault("protection_breaker_resolutions", [])
            if not isinstance(history, list):
                history = []
                self.state["protection_breaker_resolutions"] = history
            history.append(resolution)
            breaker.update({"active": False, "resolved_at": now,
                            "resolution": resolution["resolution"],
                            "resolution_closure_key": closure.get("closure_key")})
            self.save_state()
            return {"resolved": True, "symbol": symbol, "resolved_at": now,
                    "closure_key": closure.get("closure_key")}

    def restore_protection_failure_disarm(self, evidence):
        """Re-arm only after the protection failure is reconciled and its preflight fix is live."""
        evidence = evidence if isinstance(evidence, dict) else {}
        required = (
            evidence.get("startup_reconciliation_complete") is True
            and evidence.get("all_positions_protected") is True
            and evidence.get("pending_reconciliations") == []
            and evidence.get("root_cause_fix") == "PRE_SUBMIT_EXCHANGE_FILTER_VALIDATION_V1"
            and evidence.get("root_cause_loaded") is True
        )
        if not required:
            return {"restored": False, "reason": "PROTECTION_RECOVERY_EVIDENCE_INCOMPLETE"}
        with self.execution_lock():
            self.state = self.load_state()
            control = self.state.get("execution_control", {})
            breaker = self.state.get("protection_circuit_breaker", {})
            if not isinstance(control, dict) or control.get("disarmed_reason") != "PROTECTION_FAILURE":
                return {"restored": False, "reason": "DISARM_REASON_NOT_PROTECTION_FAILURE"}
            if isinstance(breaker, dict) and breaker.get("active") is True:
                return {"restored": False, "reason": "PROTECTION_BREAKER_ACTIVE"}
            if self.state.get("closed_positions") or self.state.get("pending_reconciliations"):
                return {"restored": False, "reason": "RECONCILIATION_PENDING"}
            now = time.time()
            history = self.state.setdefault("runtime_disarm_recoveries", [])
            if not isinstance(history, list):
                history = []
                self.state["runtime_disarm_recoveries"] = history
            history.append({
                "reason": "PROTECTION_FAILURE",
                "resolved_as": "RECONCILED_CLOSURE_AND_PRE_SUBMIT_PROTECTION_PREFLIGHT",
                "recovered_at": now,
                "persistence_result": "APPLIED",
                "evidence": evidence,
            })
            control.update({"armed": True, "armed_at": now,
                            "armed_reason": "protection_failure_resolved_after_fresh_reconciliation"})
            control.pop("disarmed_reason", None)
            control.pop("runtime_failure_details", None)
            self.save_state()
            return {"restored": True, "recovered_at": now}

    def _trip_protection_circuit_breaker(self, symbol, reason, metadata=None, details=None):
        """Latch a global disarm before any protection-failure safety close is attempted."""
        now = time.time()
        meta = metadata if isinstance(metadata, dict) else self.state.get("positions", {}).get(symbol, {})
        if not isinstance(meta, dict):
            meta = self.state.setdefault("positions", {}).setdefault(symbol, {})
        meta["close_classification"] = "PROTECTION_FAILURE"
        meta["protection_failure_reason"] = str(reason)
        meta["status"] = "PROTECTION_FAILURE_CLOSE_PENDING"
        breaker = self.state.get("protection_circuit_breaker", {})
        if not isinstance(breaker, dict) or not breaker.get("active"):
            self.state["protection_circuit_breaker"] = {
                "active": True,
                "tripped_at": now,
                "symbol": symbol,
                "reason": str(reason),
                "details": details if isinstance(details, dict) else {},
            }
        control = self.state.setdefault("execution_control", {})
        if not isinstance(control, dict):
            control = {}
            self.state["execution_control"] = control
        control.update({"armed": False, "disarmed_at": now,
                        "disarmed_reason": "PROTECTION_FAILURE"})
        failures = self.state.setdefault("protection_failures", [])
        prior = next((item for item in reversed(failures)
                      if isinstance(item, dict)
                      and item.get("entry_client_order_id") == meta.get("entry_client_order_id")
                      and item.get("symbol") == symbol), None)
        if prior is None:
            failures.append({
                "symbol": symbol,
                "side": meta.get("side"),
                "detected_at": now,
                "reason": str(reason),
                "supporters": list(meta.get("supporters", [])),
                "supporter_names": list(meta.get("supporter_names", [])),
                "entry_order_id": meta.get("entry_order_id"),
                "entry_client_order_id": meta.get("entry_client_order_id"),
                "details": details if isinstance(details, dict) else {},
                "status": "CLOSE_PENDING",
            })
        else:
            prior["reason"] = str(reason)
            if isinstance(details, dict):
                prior["details"] = details
        self._forget_executed_fingerprint(meta)
        self.save_state()

    @staticmethod
    def is_operational_close(metadata):
        return isinstance(metadata, dict) and str(metadata.get("close_classification", "")).upper() in {
            "PROTECTION_FAILURE", "SAFETY_CLOSE"
        }

    def has_unresolved_reconciliation(self):
        """True while an entry or position-order state needs Binance reconciliation."""
        self.state = self.load_state()
        if self.state.get("closed_positions"):
            return True
        unresolved_states = {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}
        for metadata in self.state.get("positions", {}).values():
            if not isinstance(metadata, dict):
                continue
            if metadata.get("entry_submit_uncertain") is True:
                return True
            if str(metadata.get("status", "")).upper() in unresolved_states:
                return True
            if metadata.get("pending_algo_orders") or metadata.get("close_client_order_id"):
                return True
        return False

    def archive_reconciled_closure(self, *, symbol, closed_event, matched_trades,
                                   algo_rows, tracked_order_ids, tracked_algo_ids,
                                   net_pnl_usdt, pnl_attribution, classification=None):
        """Persist exchange evidence before a resolved close leaves the pending queue.

        The close queue is operational state, not a history ledger. Keep a
        separate idempotent record so a daemon restart cannot erase the local
        evidence that justified resolving that queue item.
        """
        metadata = closed_event.get("metadata", {}) if isinstance(closed_event, dict) else {}
        if not isinstance(metadata, dict) or not symbol:
            raise ValueError("closure archive requires a symbol and position metadata")
        entry_client_id = str(metadata.get("entry_client_order_id") or "")
        opened_at = metadata.get("opened_at", closed_event.get("detected_at"))
        closure_key = f"{symbol}:{entry_client_id or opened_at}"
        order_ids = {str(value) for value in (tracked_order_ids or []) if value}
        algo_ids = {str(value) for value in (tracked_algo_ids or []) if value}
        trades = [dict(row) for row in (matched_trades or [])
                  if isinstance(row, dict) and str(row.get("orderId", "")) in order_ids]
        exchange_order_ids = sorted({str(row.get("orderId")) for row in trades if row.get("orderId") is not None})
        exchange_trade_ids = sorted({str(row.get("id")) for row in trades if row.get("id") is not None})
        algo_history = [dict(row) for row in (algo_rows or [])
                        if isinstance(row, dict) and str(row.get("algoId", "")) in algo_ids]
        exit_order_ids = set(exchange_order_ids) - {
            str(value) for value in (metadata.get("entry_order_id"), metadata.get("tp1_order_id")) if value
        }
        triggered = [row for row in algo_history
                     if str(row.get("algoStatus", "")).upper() == "FINISHED"
                     and str(row.get("actualOrderId", "")) in exit_order_ids]
        if classification is None:
            if any("STOP" in str(row.get("orderType", "")).upper() for row in triggered):
                classification = "STOP_MARKET_TRIGGERED"
            elif any("TAKE_PROFIT" in str(row.get("orderType", "")).upper() for row in triggered):
                classification = "TAKE_PROFIT_MARKET_TRIGGERED"
            else:
                classification = "VERIFIED_CLOSE"
        trade_times = [int(row.get("time", 0) or 0) for row in trades if int(row.get("time", 0) or 0) > 0]
        closed_at = max(trade_times) / 1000.0 if trade_times else float(closed_event.get("detected_at", 0) or 0)
        record = {
            "closure_key": closure_key,
            "status": "RECONCILED",
            "symbol": symbol,
            "side": metadata.get("side"),
            "position_side": metadata.get("position_side", "BOTH"),
            "opened_at": opened_at,
            "closed_at": closed_at,
            "reconciliation_detected_at": closed_event.get("detected_at"),
            "entry_client_order_id": entry_client_id or None,
            "entry_order_id": metadata.get("entry_order_id"),
            "tp1_order_id": metadata.get("tp1_order_id"),
            "supporters": list(metadata.get("supporters", [])),
            "tracked_order_ids": sorted(order_ids),
            "matched_order_ids": exchange_order_ids,
            "matched_trade_ids": exchange_trade_ids,
            "tracked_algo_ids": sorted(algo_ids),
            "triggered_algo_ids": sorted({str(row.get("algoId")) for row in triggered
                                           if row.get("algoId") is not None}),
            "matched_trades": trades,
            "algo_history": algo_history,
            "classification": classification,
            "net_pnl_usdt": float(net_pnl_usdt) if net_pnl_usdt is not None else None,
            "pnl_attribution": str(pnl_attribution or "unknown"),
            "source": "BINANCE_FUTURES_USER_TRADES_AND_ALGO_HISTORY",
        }
        history = self.state.setdefault("reconciled_closures", [])
        if not isinstance(history, list):
            raise RuntimeError("reconciled_closures is not a list; evidence was not overwritten")
        for index, previous in enumerate(history):
            if isinstance(previous, dict) and previous.get("closure_key") == closure_key:
                merged = dict(previous)
                merged.update(record)
                history[index] = merged
                return merged
        history.append(record)
        return record

    def get_stopped_thesis(self, symbol, side):
        """Read the side-specific lock while accepting legacy flat records."""
        value = self.state.get("stopped_theses", {}).get(symbol)
        if not isinstance(value, dict):
            return None
        if str(value.get("side", "")).upper() in ("LONG", "SHORT"):
            return value if str(value.get("side")).upper() == side.upper() else None
        record = value.get(side.upper())
        return record if isinstance(record, dict) else None

    def set_stopped_thesis(self, symbol, side, record):
        side = side.upper()
        bucket = self.state.setdefault("stopped_theses", {})
        existing = bucket.get(symbol)
        if isinstance(existing, dict) and str(existing.get("side", "")).upper() in ("LONG", "SHORT"):
            previous_side = str(existing["side"]).upper()
            bucket[symbol] = {previous_side: existing}
        elif not isinstance(existing, dict):
            bucket[symbol] = {}
        if side not in ("LONG", "SHORT"):
                                                                             
                                                                            
            for unknown_side in ("LONG", "SHORT"):
                conservative = dict(record)
                conservative.update({"side": unknown_side, "supporters": [], "origin_unknown": True})
                bucket[symbol][unknown_side] = conservative
            return
        bucket[symbol][side] = record

    def clear_stopped_thesis(self, symbol, side):
        side = side.upper()
        bucket = self.state.setdefault("stopped_theses", {})
        existing = bucket.get(symbol)
        if not isinstance(existing, dict):
            return
        if str(existing.get("side", "")).upper() in ("LONG", "SHORT"):
            if str(existing.get("side")).upper() == side:
                bucket.pop(symbol, None)
            return
        existing.pop(side, None)
        if not existing:
            bucket.pop(symbol, None)

    @staticmethod
    def _api_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() == "true"
        return bool(value)

    def _new_algo_client_id(self, purpose):
                                                                                       
        return f"ag2_{str(purpose).lower()}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"

    @staticmethod
    def _owned_algo_ids(metadata):
        ids = {str(value) for value in (metadata or {}).get("algo_ids", []) if value is not None}
                                                                              
                                                                             
        return ids

    def _algo_is_owned(self, order, metadata):
        """Require a persisted confirmed Algo ID and, when present, its client ID."""
        if not isinstance(order, dict) or order.get("algoId") is None:
            return False
        algo_id = str(order["algoId"])
        if algo_id not in self._owned_algo_ids(metadata):
            return False
        client_ids = (metadata or {}).get("algo_client_ids", {})
        expected_client_id = client_ids.get(algo_id) if isinstance(client_ids, dict) else None
        return expected_client_id is None or order.get("clientAlgoId") == expected_client_id

    def _owned_algo_order_matches(self, order, metadata, **match_args):
        """Match live Binance fields and persisted AG ownership together."""
        if not self._algo_is_owned(order, metadata):
            return False
        algo_id = str(order["algoId"])
        client_ids = (metadata or {}).get("algo_client_ids", {})
        stored_client_id = client_ids.get(algo_id) if isinstance(client_ids, dict) else None
        requested_client_id = match_args.pop("client_algo_id", None)
        if (stored_client_id is not None and requested_client_id is not None
                and stored_client_id != requested_client_id):
            return False
        return self._algo_order_matches(
            order, **match_args,
            client_algo_id=requested_client_id or stored_client_id
        )

    def _cancel_owned_algos(self, symbol, metadata, *, exclude_algo_ids=None):
        """Cancel only Algo IDs persisted from this bot's confirmed submissions."""
        return self.client.cancel_algo_orders(
            symbol, algo_ids=self._owned_algo_ids(metadata),
            exclude_algo_ids=exclude_algo_ids
        )

    def _algo_order_matches(self, order, *, symbol, order_type, side, position_side,
                             trigger_price, close_position, quantity=None, reduce_only=False,
                             client_algo_id=None):
        """Strictly reconcile a live Binance Algo order against its submitted intent."""
        if not isinstance(order, dict) or order.get("algoId") is None:
            return False
        try:
            rules = self.get_symbol_filters(symbol)
            tick = Decimal(str(rules["tickSize"]))
            actual_trigger = Decimal(str(order.get("triggerPrice")))
            wanted_trigger = Decimal(str(trigger_price))
            if abs(actual_trigger - wanted_trigger) > tick / Decimal(2):
                return False
        except Exception:
            return False
        if (order.get("symbol") != symbol
                or order.get("algoType") != "CONDITIONAL"
                or order.get("orderType") != order_type
                or order.get("side") != side
                or str(order.get("positionSide", "BOTH")).upper() != str(position_side).upper()
                or order.get("workingType") != "MARK_PRICE"
                or order.get("algoStatus") not in ("NEW", "TRIGGERING")):
            return False
        if client_algo_id is not None and order.get("clientAlgoId") != client_algo_id:
            return False
                                                                                
                                                                               
        if "closePosition" not in order:
            return False
        actual_close = self._api_bool(order.get("closePosition"))
        if actual_close != bool(close_position):
            return False
        if close_position:
                                                                                 
                                                                          
                                                                              
                                                                            
            try:
                reported_qty = Decimal(str(order.get("quantity") or "0"))
            except Exception:
                return False
            if reported_qty != 0:
                return False
        else:
            if "reduceOnly" not in order:
                return False
            actual_reduce = self._api_bool(order.get("reduceOnly"))
            if actual_reduce != bool(reduce_only) or quantity is None:
                return False
            try:
                step = Decimal(str(rules["stepSize"]))
                actual_qty = Decimal(str(order.get("quantity")))
                wanted_qty = Decimal(str(quantity))
                if abs(actual_qty - wanted_qty) > step / Decimal(2):
                    return False
            except Exception:
                return False
        return True

    def _retry_read(self, read_call, *, label, valid_result):
        """Retry a bounded, read-only Binance check before treating state as absent."""
        operations = CONFIG["operations"]
        attempts = max(1, int(operations.get("protection_read_retry_attempts", 2)))
        delay = max(0.0, float(operations.get("protection_read_retry_delay_seconds", 0.5)))
        last_error = None
        for attempt in range(attempts):
            try:
                result = read_call()
                if valid_result(result):
                    return result
                last_error = RuntimeError(f"Binance devolvió una respuesta inválida al leer {label}.")
            except Exception as exc:
                last_error = exc
            if attempt + 1 < attempts and delay:
                time.sleep(delay)
        raise RuntimeError(f"No se pudo confirmar {label} tras {attempts} lecturas seguras: {last_error}") from last_error

    def _read_open_algo_orders(self, symbol=None):
        return self._retry_read(
            lambda: self.client.get_open_algo_orders(symbol),
            label=f"openAlgoOrders de {symbol or 'la cuenta'}",
            valid_result=lambda value: isinstance(value, list),
        )

    def _read_positions(self):
        return self._retry_read(
            self.client.get_positions,
            label="posiciones Futures",
            valid_result=lambda value: isinstance(value, list),
        )

    def _read_position_mode(self, force_refresh=False):
        return self._retry_read(
            lambda: self.client.get_position_mode(force_refresh=force_refresh),
            label="modo de posición Futures",
            valid_result=lambda value: isinstance(value, bool),
        )

    def _revalidate_position_protection(self, symbol, metadata):
        """Refresh position and owned Algo evidence before any protection-failure close."""
        positions = self._read_positions()
        hedge_mode = self._read_position_mode(force_refresh=True)
        live_position = next((item for item in positions
                              if item.get("symbol") == symbol
                              and self._is_open_position(item)
                              and self._position_matches_metadata(item, metadata, hedge_mode)), None)
        if live_position is None:
            return {"position": None, "algos": [], "is_protected": False}

        amount = float(live_position.get("positionAmt", 0) or 0)
        side = "LONG" if amount > 0 else "SHORT"
        close_side = "SELL" if amount > 0 else "BUY"
        position_side = str(metadata.get("position_side") or
                             (str(live_position.get("positionSide", "BOTH")).upper()
                              if hedge_mode else "BOTH"))
        entry_price = float(live_position.get("entryPrice", 0) or 0)
        initial_qty = float(metadata.get("initial_qty", 0) or 0)
        partial_qty = float(metadata.get("partial_qty", 0) or 0)
        step_size = float(self.get_symbol_filters(symbol)["stepSize"])
        tp1_executed = bool(metadata.get("tp1_executed") or
                            str(metadata.get("status", "")).upper() == "RUNNER_ACTIVE")
        if (not tp1_executed and initial_qty > 0 and partial_qty > 0
                and abs(amount) <= initial_qty - partial_qty + step_size / 2.0):
            tp1_executed = True

        stop_target = metadata.get("current_sl_price") or metadata.get("initial_sl_price")
        if stop_target is None and tp1_executed and entry_price > 0:
            be_buffer = float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0
            stop_target = entry_price * (1.0 + be_buffer if side == "LONG" else 1.0 - be_buffer)
        tp_target = metadata.get("tp_price")
        algos = self._read_open_algo_orders(symbol)
        sl = next((order for order in algos if stop_target is not None
                   and self._owned_algo_order_matches(
                       order, metadata, symbol=symbol, order_type="STOP_MARKET",
                       side=close_side, position_side=position_side,
                       trigger_price=stop_target, close_position=True)), None)
        tp = None
        if not tp1_executed and tp_target is not None and partial_qty > 0:
            tp_qty = self.format_quantity(symbol, partial_qty)
            tp = next((order for order in algos if self._owned_algo_order_matches(
                order, metadata, symbol=symbol, order_type="TAKE_PROFIT_MARKET",
                side=close_side, position_side=position_side,
                trigger_price=tp_target, close_position=False, quantity=tp_qty,
                reduce_only=(position_side == "BOTH"))), None)
        return {"position": live_position, "algos": algos, "sl": sl, "tp": tp,
                "tp1_executed": tp1_executed,
                "is_protected": bool(sl is not None and (tp is not None or tp1_executed))}

    def _validate_algo_order_request(self, *, symbol, order_type, side, position_side,
                                     trigger_price, close_position, quantity,
                                     reduce_only, metadata):
        """Check live account mode and current exchange filters before an Algo POST."""
        if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise ValueError(f"Tipo Algo de protección no admitido: {order_type}.")
        mode = self._read_position_mode(force_refresh=True)
        expected_position_side = str(position_side or "").upper()
        if (not mode and expected_position_side != "BOTH") or (mode and expected_position_side not in {"LONG", "SHORT"}):
            raise ValueError(f"positionSide {expected_position_side or 'vacío'} no corresponde al modo Binance actual.")
        thesis_side = str((metadata or {}).get("side", "")).upper()
        expected_close_side = "SELL" if thesis_side == "LONG" else "BUY" if thesis_side == "SHORT" else None
        if expected_close_side and side != expected_close_side:
            raise ValueError(f"Lado {side} incorrecto para proteger {thesis_side} en {symbol}.")
        if mode and thesis_side in {"LONG", "SHORT"} and expected_position_side != thesis_side:
            raise ValueError(f"positionSide {expected_position_side} no protege la pierna {thesis_side} de {symbol}.")

        rules = self.get_symbol_filters(symbol)
        trigger = Decimal(str(trigger_price))
        tick = Decimal(str(rules["tickSize"]))
        minimum_price = Decimal(str(rules.get("minPrice") or "0"))
        maximum_price = Decimal(str(rules.get("maxPrice") or "0"))
        if trigger <= 0 or tick <= 0 or (trigger - minimum_price) % tick != 0:
            raise ValueError(f"triggerPrice {trigger} no está alineado al tickSize actual de {symbol}.")
        if ((minimum_price > 0 and trigger < minimum_price)
                or (maximum_price > 0 and trigger > maximum_price)):
            raise ValueError(f"triggerPrice {trigger} está fuera de los filtros de precio de {symbol}.")

        if close_position:
            if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"} or quantity is not None or reduce_only:
                raise ValueError("closePosition=true debe enviarse sin quantity ni reduceOnly.")
            return
        if quantity is None:
            raise ValueError("Una orden Algo que no usa closePosition requiere quantity explícita.")
        qty = Decimal(str(quantity))
        step = Decimal(str(rules["stepSize"]))
        minimum_qty = Decimal(str(rules["minQty"]))
        maximum_qty = Decimal(str(rules.get("maxQty") or "0"))
        if qty <= 0 or step <= 0 or qty < minimum_qty or qty % step != 0:
            raise ValueError(f"quantity {qty} no cumple minQty/stepSize actuales de {symbol}.")
        if maximum_qty > 0 and qty > maximum_qty:
            raise ValueError(f"quantity {qty} supera maxQty actual de {symbol}.")
        mark = Decimal(str(self.client.get_mark_price(symbol) or "0"))
        if mark <= 0:
            raise RuntimeError(f"Mark Price no disponible para validar quantity en {symbol}.")
        min_notional = rules.get("minNotional")
        if min_notional is not None and qty * mark < Decimal(str(min_notional)):
            raise ValueError(f"quantity {qty} queda bajo minNotional actual de {symbol}.")

                                                                                                                                
                                                             
    TP_LIMIT_FRACTION = 0.9

    def cap_take_profit_trigger(self, symbol, tp_price, is_long, mark=None):
        """El TP de respaldo (2 ATR) no puede quedar más lejos del Mark Price que el límite PERCENT_PRICE de Binance (ETH/BTC/SOL: 3 %
        desde el 29-10-2026). Si lo supera, se acerca al 90 % de ese límite en lugar de bloquear la entrada; el escudo híbrido (+1,5 %)
        sigue haciendo el TP1. Nunca aleja el TP ni toca el stop."""
        rules = self.get_symbol_filters(symbol)
        mark = float(mark if mark is not None else (self.client.get_mark_price(symbol) or 0))
        if not math.isfinite(mark) or mark <= 0:
            raise RuntimeError(f"Mark Price no disponible para acotar el TP de {symbol}.")
        tp = float(tp_price)
        if is_long and rules.get("multiplierUp") is not None:
            return min(tp, mark * (1.0 + (float(rules["multiplierUp"]) - 1.0) * self.TP_LIMIT_FRACTION))
        if not is_long and rules.get("multiplierDown") is not None:
            return max(tp, mark * (1.0 - (1.0 - float(rules["multiplierDown"])) * self.TP_LIMIT_FRACTION))
        return tp

    def cap_stop_trigger(self, symbol, sl_price, is_long, mark=None):
        """01-11-2026: el stop (1,5 ATR) no puede quedar más lejos del Mark Price que el límite PERCENT_PRICE de Binance (ETH/BTC/SOL:
        3 %). Si lo supera, se acerca al 90 % de ese límite en lugar de bloquear la entrada. El tamaño se sigue calculando con el
        stop de 1,5 ATR, así que la pérdida si salta es menor, nunca mayor. Nunca aleja el stop."""
        rules = self.get_symbol_filters(symbol)
        mark = float(mark if mark is not None else (self.client.get_mark_price(symbol) or 0))
        if not math.isfinite(mark) or mark <= 0:
            raise RuntimeError(f"Mark Price no disponible para acotar el stop de {symbol}.")
        sl = float(sl_price)
        if is_long and rules.get("multiplierDown") is not None:
            return max(sl, mark * (1.0 - (1.0 - float(rules["multiplierDown"])) * self.TP_LIMIT_FRACTION))
        if not is_long and rules.get("multiplierUp") is not None:
            return min(sl, mark * (1.0 + (float(rules["multiplierUp"]) - 1.0) * self.TP_LIMIT_FRACTION))
        return sl

    def _validate_trigger_side(self, symbol, order_type, side, trigger_price):
        mark = float(self.client.get_mark_price(symbol) or 0)
        trigger = float(trigger_price)
        if not math.isfinite(mark) or mark <= 0 or not math.isfinite(trigger) or trigger <= 0:
            raise RuntimeError(f"Mark Price o triggerPrice inválido para {symbol}.")
        must_be_above = (order_type == "STOP_MARKET" and side == "BUY") or (
            order_type == "TAKE_PROFIT_MARKET" and side == "SELL"
        )
        if (must_be_above and trigger <= mark) or (not must_be_above and trigger >= mark):
            raise ValueError(f"El trigger {trigger} de {order_type} {side} ya no está del lado válido del Mark Price {mark} para {symbol}.")
        rules = self.get_symbol_filters(symbol)
        if rules.get("multiplierUp") is not None and trigger > mark * float(rules["multiplierUp"]):
            raise ValueError(f"El trigger excede multiplierUp de Binance para {symbol}.")
        if rules.get("multiplierDown") is not None and trigger < mark * float(rules["multiplierDown"]):
            raise ValueError(f"El trigger queda por debajo de multiplierDown de Binance para {symbol}.")

    def _submit_algo_and_verify(self, *, symbol, order_type, side, position_side,
                                trigger_price, metadata, metadata_key,
                                quantity=None, reduce_only=False):
        """Submit once, then require an exact live Binance order before accepting it."""
        close_position = order_type == "STOP_MARKET" and quantity is None
        try:
            self._validate_algo_order_request(
                symbol=symbol, order_type=order_type, side=side,
                position_side=position_side, trigger_price=trigger_price,
                close_position=close_position, quantity=quantity,
                reduce_only=bool(reduce_only and position_side == "BOTH"),
                metadata=metadata
            )
            self._validate_trigger_side(symbol, order_type, side, trigger_price)
        except Exception as exc:
            return None, {"error": str(exc), "outcome": "pre_submit_error"}
        client_algo_id = self._new_algo_client_id(metadata_key)
        expected = {
            "symbol": symbol, "order_type": order_type, "side": side,
            "position_side": position_side, "trigger_price": str(trigger_price),
            "close_position": close_position,
            "quantity": None if quantity is None else str(quantity),
            "reduce_only": bool(reduce_only and position_side == "BOTH"),
            "client_algo_id": client_algo_id, "metadata_key": metadata_key
        }
        if quantity is not None:
            min_notional = self.get_symbol_filters(symbol).get("minNotional")
            mark_price = float(self.client.get_mark_price(symbol) or 0)
            if not math.isfinite(mark_price) or mark_price <= 0:
                return None, {"error": "mark_price_unavailable", "outcome": "pre_submit_error"}
            if min_notional is not None and float(quantity) * mark_price < float(min_notional):
                return None, {"error": "minimum_notional", "outcome": "pre_submit_error"}
        try:
            open_before_submit = self._read_open_algo_orders(symbol)
        except Exception as exc:
            return None, {"error": type(exc).__name__, "outcome": "pre_submit_error"}
        same_leg_type = [order for order in open_before_submit
                         if order.get("symbol") == symbol
                         and order.get("orderType") == order_type
                         and order.get("side") == side
                         and str(order.get("positionSide", "BOTH")).upper() == str(position_side).upper()
                         and order.get("algoStatus") in ("NEW", "TRIGGERING")]
        exact_existing = next((order for order in same_leg_type if self._owned_algo_order_matches(
            order, metadata,
            symbol=symbol, order_type=order_type, side=side,
            position_side=position_side, trigger_price=trigger_price,
            close_position=close_position, quantity=quantity,
            reduce_only=expected["reduce_only"]
        )), None)
        if exact_existing:
            return exact_existing, {"outcome": "existing_owned", "algoId": exact_existing["algoId"]}
        if same_leg_type:
            return None, {"error": "conflicting_open_algo", "outcome": "pre_submit_error"}
        pending = metadata.setdefault("pending_algo_orders", {})
        pending[client_algo_id] = expected
        self.save_state()
        try:
            if order_type == "STOP_MARKET":
                response = self.client.place_stop_loss(
                    symbol, side, trigger_price, position_side=position_side,
                    working_type="MARK_PRICE", client_algo_id=client_algo_id
                )
            else:
                response = self.client.place_take_profit(
                    symbol, side, trigger_price, quantity,
                    position_side=position_side, reduce_only=reduce_only,
                    working_type="MARK_PRICE", client_algo_id=client_algo_id
                )
        except Exception as exc:
            response = {"error": type(exc).__name__, "outcome": "pre_submit_error"}
        outcome = response.get("outcome") if isinstance(response, dict) else None
        if outcome == "explicit_rejection":
            pending.pop(client_algo_id, None)
            self.save_state()
            return None, response
        if outcome == "pre_submit_error":
            pending.pop(client_algo_id, None)
            self.save_state()
            return None, response
        time.sleep(float(CONFIG["operations"]["settle_delay_seconds"]))
        try:
            active_orders = self._read_open_algo_orders(symbol)
        except Exception:
            active_orders = None
        candidate = next((order for order in (active_orders or [])
                          if order.get("clientAlgoId") == client_algo_id), None)
        acknowledged_algo_id = response.get("algoId") if isinstance(response, dict) else None
        if (candidate is not None and acknowledged_algo_id is not None
                and str(candidate.get("algoId")) != str(acknowledged_algo_id)):
            candidate = None
                                                                                 
                                                                                   
                                                                             
                                                                                     
                                                                              
        confirmed = candidate if self._algo_order_matches(
            candidate, symbol=symbol, order_type=order_type, side=side,
            position_side=position_side, trigger_price=trigger_price,
            close_position=close_position, quantity=quantity,
            reduce_only=expected["reduce_only"], client_algo_id=client_algo_id
        ) else None
        verification_source = "openAlgoOrders" if confirmed is not None else None
        query_result = None
        if confirmed is None:
            query_algo_id = acknowledged_algo_id
            if query_algo_id is not None:
                try:
                    queried = self.client.get_algo_order(symbol, algo_id=query_algo_id)
                except Exception:
                    queried = None
                query_result = queried
                if (isinstance(queried, dict)
                        and str(queried.get("algoId")) == str(query_algo_id)
                        and queried.get("clientAlgoId") == client_algo_id
                        and (acknowledged_algo_id is None
                             or str(queried.get("algoId")) == str(acknowledged_algo_id))):
                    candidate = queried
                else:
                    candidate = None
                confirmed = candidate if self._algo_order_matches(
                    candidate, symbol=symbol, order_type=order_type, side=side,
                    position_side=position_side, trigger_price=trigger_price,
                    close_position=close_position, quantity=quantity,
                    reduce_only=expected["reduce_only"], client_algo_id=client_algo_id
                ) else None
                if confirmed is not None:
                    verification_source = "algoOrder"
                                                                                
                                                                                 
                                                                          
        if confirmed is None:
            time.sleep(float(CONFIG["operations"]["settle_delay_seconds"]))
            try:
                refreshed_orders = self._read_open_algo_orders(symbol)
            except Exception:
                refreshed_orders = None
            refreshed = next((order for order in (refreshed_orders or [])
                              if order.get("clientAlgoId") == client_algo_id), None)
            if (refreshed is not None and acknowledged_algo_id is not None
                    and str(refreshed.get("algoId")) != str(acknowledged_algo_id)):
                refreshed = None
            if self._algo_order_matches(
                refreshed, symbol=symbol, order_type=order_type, side=side,
                position_side=position_side, trigger_price=trigger_price,
                close_position=close_position, quantity=quantity,
                reduce_only=expected["reduce_only"], client_algo_id=client_algo_id
            ):
                candidate = refreshed
                confirmed = refreshed
                verification_source = "openAlgoOrders_retry"
        recorder = getattr(self.client, "record_algo_audit_event", None)
        if callable(recorder):
            recorder("algo_confirmation", {
                "symbol": symbol,
                "orderType": order_type,
                "side": side,
                "positionSide": position_side,
                "clientAlgoId": client_algo_id,
                "algoId": candidate.get("algoId") if isinstance(candidate, dict) else (
                    response.get("algoId") if isinstance(response, dict) else None
                ),
                "candidate": candidate,
                "verification_source": verification_source,
                "single_order_query": query_result,
                "confirmed_active": confirmed is not None,
                "outcome": "protected_order_confirmed" if confirmed else (
                    response.get("outcome") if isinstance(response, dict) else "ambiguous_after_submit"
                ),
            })
        if confirmed:
            pending.pop(client_algo_id, None)
            algo_ids = metadata.setdefault("algo_ids", [])
            if str(confirmed["algoId"]) not in {str(value) for value in algo_ids}:
                algo_ids.append(str(confirmed["algoId"]))
            metadata[metadata_key] = str(confirmed["algoId"])
            metadata.setdefault("algo_client_ids", {})[str(confirmed["algoId"])] = client_algo_id
            self.save_state()
            return confirmed, response
        if isinstance(response, dict) and not response.get("error"):
            response.setdefault("outcome", "ambiguous_after_submit")
        metadata["status"] = "RECONCILING"
        self.save_state()
        return None, response

    def _reconcile_pending_algo_orders(self, symbol, metadata):
        pending = metadata.get("pending_algo_orders", {})
        if not pending:
            return True
        active_orders = self._read_open_algo_orders(symbol)
        for client_algo_id, expected in list(pending.items()):
            order = next((item for item in active_orders
                          if item.get("clientAlgoId") == client_algo_id), None)
            if order is None:
                try:
                    queried = self.client.get_algo_order(symbol, client_algo_id=client_algo_id)
                except Exception:
                    return False
                try:
                    queried_code = int(queried.get("code", 0) or 0) if isinstance(queried, dict) else None
                except (TypeError, ValueError):
                    queried_code = None
                if queried_code == -2013:
                                                                                        
                                                                                     
                    pending.pop(client_algo_id, None)
                    metadata["status"] = "OPEN"
                    self.save_state()
                    continue
                if isinstance(queried, dict) and queried.get("algoStatus") in {"CANCELED", "EXPIRED", "FINISHED", "REJECTED"}:
                    pending.pop(client_algo_id, None)
                    metadata["status"] = "OPEN"
                    self.save_state()
                    continue
                order = queried
            else:
                try:
                    queried = self.client.get_algo_order(symbol, algo_id=order.get("algoId"))
                except Exception:
                    return False
                if (not isinstance(queried, dict)
                        or str(queried.get("algoId")) != str(order.get("algoId"))
                        or queried.get("clientAlgoId") != client_algo_id):
                    return False
                order = queried
            if not self._algo_order_matches(
                order, symbol=expected["symbol"], order_type=expected["order_type"],
                side=expected["side"], position_side=expected["position_side"],
                trigger_price=expected["trigger_price"],
                close_position=expected["close_position"], quantity=expected["quantity"],
                reduce_only=expected["reduce_only"], client_algo_id=client_algo_id
            ):
                return False
            pending.pop(client_algo_id, None)
            algo_id = str(order["algoId"])
            owned = metadata.setdefault("algo_ids", [])
            if algo_id not in {str(value) for value in owned}:
                owned.append(algo_id)
            metadata[expected["metadata_key"]] = algo_id
            metadata.setdefault("algo_client_ids", {})[algo_id] = client_algo_id
            self.save_state()
        return not bool(pending)

    def _replace_owned_stop(self, symbol, side, position_side, new_trigger, metadata):
        """Replace one confirmed bot-owned stop; restore it after a definite rejection."""
                                                                                    
        try:
            self._validate_algo_order_request(
                symbol=symbol, order_type="STOP_MARKET", side=side,
                position_side=position_side, trigger_price=new_trigger,
                close_position=True, quantity=None, reduce_only=False, metadata=metadata)
            self._validate_trigger_side(symbol, "STOP_MARKET", side, new_trigger)
        except Exception as exc:
            return None, {"outcome": "pre_submit_error", "error": str(exc)}
        old_id = metadata.get("sl_algo_id")
        owned_ids = self._owned_algo_ids(metadata)
        old_trigger = metadata.get("current_sl_price") or metadata.get("initial_sl_price")
        old_order = None
        if old_id is not None:
            active = self._read_open_algo_orders(symbol)
            old_order = next((item for item in active if str(item.get("algoId")) == str(old_id)), None)
            if old_order and str(old_id) not in owned_ids:
                return None, {"outcome": "pre_submit_error", "error": "stop_ownership_unconfirmed"}
            if old_order and not self._owned_algo_order_matches(
                old_order, metadata,
                symbol=symbol, order_type="STOP_MARKET", side=side,
                position_side=position_side, trigger_price=old_order.get("triggerPrice"),
                close_position=True
            ):
                return None, {"outcome": "pre_submit_error", "error": "existing_stop_fields_mismatch"}
        if old_order:
            old_trigger = old_order.get("triggerPrice")
            self.client.cancel_algo_orders(
                symbol, algo_ids={old_id}, position_side=position_side,
                side=side, order_type="STOP_MARKET"
            )
        confirmed, response = self._submit_algo_and_verify(
            symbol=symbol, order_type="STOP_MARKET", side=side,
            position_side=position_side, trigger_price=new_trigger,
            metadata=metadata, metadata_key="sl_algo_id"
        )
        if confirmed:
            metadata["current_sl_price"] = float(confirmed["triggerPrice"])
            self._cancel_owned_algos(symbol, metadata, exclude_algo_ids={confirmed["algoId"]})
            return confirmed, response
        if (old_trigger is not None and not (isinstance(response, dict)
                                             and response.get("outcome") == "ambiguous_after_submit")):
            restored, restore_response = self._submit_algo_and_verify(
                symbol=symbol, order_type="STOP_MARKET", side=side,
                position_side=position_side, trigger_price=old_trigger,
                metadata=metadata, metadata_key="sl_algo_id"
            )
            if restored:
                metadata["current_sl_price"] = float(restored["triggerPrice"])
                return None, {"outcome": "rejected_restored", "response": response}
            return None, {"outcome": "restore_failed", "response": response,
                          "restore": restore_response}
        return None, response

    def _mark_symbol_active(self, symbol, initial_qty=None, sl_algo_id=None, tp_algo_id=None, position_side=None):
        if symbol not in self.state["active_symbols"]:
            self.state["active_symbols"].append(symbol)
        if "positions" not in self.state or not isinstance(self.state["positions"], dict):
            self.state["positions"] = {}
        if symbol not in self.state["positions"]:
            self.state["positions"][symbol] = {}
        if initial_qty is not None:
            self.state["positions"][symbol]["initial_qty"] = float(initial_qty)
        if sl_algo_id is not None:
            self.state["positions"][symbol]["sl_algo_id"] = str(sl_algo_id)
        if tp_algo_id is not None:
            self.state["positions"][symbol]["tp_algo_id"] = str(tp_algo_id)
        if position_side is not None:
            self.state["positions"][symbol]["position_side"] = str(position_side)
        self.state["positions"][symbol].setdefault("tp1_executed", False)
        self.save_state()

    @staticmethod
    def _is_open_position(position):
        try:
            return float(position.get("positionAmt", 0) or 0) != 0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _position_matches_metadata(position, metadata, hedge_mode):
        """Match the registered AG leg, not merely the symbol (Hedge Mode has two legs)."""
        if not isinstance(metadata, dict):
            return False
        actual_side = str(position.get("positionSide", "BOTH")).upper()
        expected_side = metadata.get("position_side")
        if expected_side:
            return actual_side == str(expected_side).upper()
                                                                               
        thesis_side = str(metadata.get("side", "")).upper()
        if hedge_mode and thesis_side in ("LONG", "SHORT"):
            return actual_side == thesis_side
        return actual_side == "BOTH" or not hedge_mode

    def get_untracked_positions(self, account_positions=None):
        """Return live account legs without a matching Antigravity state record."""
        positions = self.client.get_positions() if account_positions is None else account_positions
        if not isinstance(positions, list):
            return None
        hedge_mode = self.client.get_position_mode()
        if hedge_mode is None:
            return None
        tracked = self.state.get("positions", {})
        untracked = []
        for position in positions:
            if not self._is_open_position(position):
                continue
            symbol = position.get("symbol", "")
            metadata = tracked.get(symbol, {})
            if symbol not in self.state.get("active_symbols", []) or not self._position_matches_metadata(position, metadata, hedge_mode):
                untracked.append(position)
        return untracked

    def _mark_symbol_closed(self, symbol):
        if symbol in self.state["active_symbols"]:
            self.state["active_symbols"].remove(symbol)
        if "positions" in self.state and symbol in self.state["positions"]:
            closed_meta = self.state["positions"].pop(symbol)
            if self.is_operational_close(closed_meta):
                closed_meta["status"] = "CLOSED"
                closed_meta["closed_at"] = time.time()
                for failure in self.state.get("protection_failures", []):
                    if (isinstance(failure, dict) and failure.get("symbol") == symbol
                            and failure.get("entry_client_order_id") == closed_meta.get("entry_client_order_id")):
                        failure["status"] = "CLOSED"
                        failure["closed_at"] = closed_meta["closed_at"]
            self.state.setdefault("closed_positions", {})[symbol] = {
                "metadata": closed_meta,
                "detected_at": time.time()
            }
        self.save_state()

    def _reconcile_entry_without_position(self, symbol, metadata):
        """Resolve an entry that Binance has not yet exposed in positionRisk."""
        client_id = metadata.get("entry_client_order_id")
        if not client_id:
            return "unknown"
        order = self.client.get_order(symbol, client_id)
        if not isinstance(order, dict):
            return "unknown"
        try:
            code = int(order.get("code", 0) or 0)
        except (TypeError, ValueError):
            code = None
        if code == -2013:
            return "rejected"
        if order.get("orderId") is None:
            return "unknown"
        status = str(order.get("status", "")).upper()
        executed = float(order.get("executedQty", 0) or 0)
        if status in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}:
            return "pending"
        if status in {"REJECTED", "CANCELED", "EXPIRED"} and executed <= 0:
            return "rejected"
        if status in {"FILLED", "CANCELED", "EXPIRED"} and executed > 0:
            return "closed"
        return "unknown"

    def get_agent_positions(self):
        with self.execution_lock():
            return self._get_agent_positions_unlocked()

    def _get_agent_positions_unlocked(self):
        """
        Devuelve SOLO las posiciones atribuidas al agente Antigravity.
        Ignora posiciones manuales de la cuenta maestra u otros bots.
        """
        self.state = self.load_state()
        all_positions = self.client.get_positions()
        if all_positions is None:
            return None
            
        agent_positions = []
        hedge_mode = self.client.get_position_mode()
        if hedge_mode is None:
            return None
        observed_legs = set()
        for pos in all_positions:
            symbol = pos["symbol"]
            observed_legs.add((symbol, str(pos.get("positionSide", "BOTH")).upper()))
            pos_amt = float(pos["positionAmt"])
            metadata = self.state.get("positions", {}).get(symbol, {})
            matches_agent_leg = self._position_matches_metadata(pos, metadata, hedge_mode)
            
                                                                                        
            if pos_amt != 0 and symbol in self.state["active_symbols"] and matches_agent_leg:
                status_before_reconcile = str(metadata.get("status", "")).upper()
                needs_entry_reconcile = (metadata.get("entry_submit_uncertain") is True
                                         or ("entry_submit_uncertain" not in metadata
                                             and status_before_reconcile in {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}))
                if needs_entry_reconcile:
                    client_order_id = metadata.get("entry_client_order_id")
                    if client_order_id:
                        entry_order = self.client.get_order(symbol, client_order_id)
                        entry_status = str(entry_order.get("status", "")).upper() if isinstance(entry_order, dict) else ""
                        entry_filled = float(entry_order.get("executedQty", 0) or 0) if isinstance(entry_order, dict) else 0.0
                        if isinstance(entry_order, dict) and entry_order.get("orderId") is not None and entry_filled > 0:
                            self.state.pop("binance_cooling_off", None)
                            if entry_status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                                metadata["entry_submit_uncertain"] = False
                                metadata["status"] = "ENTRY_CONFIRMED"
                            self.save_state()
                agent_positions.append(pos)
            
                                                                                     
            if pos_amt == 0 and symbol in self.state["active_symbols"] and matches_agent_leg:
                if str(metadata.get("status", "")).upper() in {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}:
                    entry_state = self._reconcile_entry_without_position(symbol, metadata)
                    if entry_state == "pending" or entry_state == "unknown":
                        continue
                    if entry_state == "rejected":
                        self._forget_executed_fingerprint(metadata)
                        self.state.get("positions", {}).pop(symbol, None)
                        self.state["active_symbols"].remove(symbol)
                        self.save_state()
                    else:
                        self._mark_symbol_closed(symbol)
                else:
                    self._mark_symbol_closed(symbol)

                                                                                                  
                                                                                         
        for symbol in list(self.state.get("active_symbols", [])):
            metadata = self.state.get("positions", {}).get(symbol, {})
            side_hint = str(metadata.get("position_side") or metadata.get("side") or "BOTH").upper()
            expected_side = side_hint if hedge_mode and side_hint in ("LONG", "SHORT") else "BOTH"
            if (symbol, expected_side) in observed_legs:
                continue
            if str(metadata.get("status", "")).upper() in {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}:
                entry_state = self._reconcile_entry_without_position(symbol, metadata)
                if entry_state == "pending" or entry_state == "unknown":
                    continue
                if entry_state == "rejected":
                    self._forget_executed_fingerprint(metadata)
                    self.state.get("positions", {}).pop(symbol, None)
                    self.state["active_symbols"].remove(symbol)
                    self.save_state()
                    continue
                self._mark_symbol_closed(symbol)
                continue
            self._mark_symbol_closed(symbol)
                
        return agent_positions

    def get_wallet_summary(self, account=None):
        """
        Obtiene el balance. El balance es compartido (cuenta maestra).
        """
        account = account or self.client.get_account_info()
        if not account:
            return None
            
        total_margin = float(account.get("totalMarginBalance", 0))
        available = float(account.get("availableBalance", 0))
        unrealized_pnl = float(account.get("totalUnrealizedProfit", 0))
        
        return {
            "total_balance": total_margin,
            "available_balance": available,
            "unrealized_pnl": unrealized_pnl,
            "scope": "SHARED_MASTER_ACCOUNT",
            "agent": self.agent_id
        }

    def calculate_atr_4h(self, symbol, current_price):
        """Calcula ATR con velas 4H cerradas; errores de datos bloquean la entrada."""
        period = int(CONFIG["atr"]["period"])
        if current_price <= 0:
            raise ValueError(f"Precio inválido para calcular ATR de {symbol}.")
        klines = self.client._request(
            "GET", "/fapi/v1/klines",
            {"symbol": symbol, "interval": CONFIG["atr"]["interval"], "limit": period + 2}
        )
        if not isinstance(klines, list) or len(klines) < period + 2:
            raise RuntimeError(f"Binance no devolvió suficientes velas {CONFIG['atr']['interval']} para {symbol}.")
        closed_klines = klines[:-1]
        true_ranges = []
        for index in range(1, len(closed_klines)):
            high = float(closed_klines[index][2])
            low = float(closed_klines[index][3])
            previous_close = float(closed_klines[index - 1][4])
            true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        if len(true_ranges) < period:
            raise RuntimeError(f"ATR {period} incompleto para {symbol}.")
        atr = sum(true_ranges[-period:]) / period
        if not math.isfinite(atr) or atr <= 0:
            raise RuntimeError(f"ATR inválido para {symbol}.")
        stop_pct = atr * float(CONFIG["atr"]["stop_multiple"]) / current_price
        tp_pct = atr * float(CONFIG["exits"]["take_profit_atr_multiple"]) / current_price
        if not math.isfinite(stop_pct) or not math.isfinite(tp_pct) or stop_pct <= 0 or tp_pct <= 0:
            raise RuntimeError(f"Distancias ATR inválidas para {symbol}.")
        return stop_pct, tp_pct, atr

    def calculate_atr_15m(self, symbol, current_price):
        """ATR del filtro anti-persecución; una respuesta inválida no se sustituye por datos sintéticos."""
        period = int(CONFIG["atr"]["period"])
        if current_price <= 0:
            raise ValueError(f"Precio inválido para calcular ATR anti-persecución de {symbol}.")
        klines = self.client._request(
            "GET", "/fapi/v1/klines",
            {"symbol": symbol, "interval": CONFIG["atr"]["anti_chase_interval"], "limit": period + 2}
        )
        if not isinstance(klines, list) or len(klines) < period + 2:
            raise RuntimeError(f"Binance no devolvió suficientes velas anti-persecución para {symbol}.")
        closed_klines = klines[:-1]
        true_ranges = []
        for index in range(1, len(closed_klines)):
            high = float(closed_klines[index][2])
            low = float(closed_klines[index][3])
            previous_close = float(closed_klines[index - 1][4])
            true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        if len(true_ranges) < period:
            raise RuntimeError(f"ATR anti-persecución incompleto para {symbol}.")
        return sum(true_ranges[-period:]) / period

    def calculate_risk_adjusted_margin(self, symbol, leverage, risk_pct, equity=None, available_balance=None, side="BUY"):
        """Calcula margen desde el porcentaje de equity y el stop ATR 4H, sin montos fijos."""
        if equity is None or available_balance is None:
            wallet = self.get_wallet_summary()
            if not wallet:
                raise RuntimeError("No se obtuvo equity/margen actual de Binance; se bloquea el dimensionamiento.")
            equity = wallet["total_balance"] if equity is None else equity
            available_balance = wallet["available_balance"] if available_balance is None else available_balance
        equity = float(equity)
        available_balance = float(available_balance)
        leverage = int(leverage)
        risk_pct = float(risk_pct)
        max_risk_pct = float(CONFIG["risk"]["high_confidence_trade_risk_pct"])
        if (not all(math.isfinite(value) for value in (equity, available_balance, risk_pct))
                or equity <= 0 or available_balance <= 0 or leverage <= 0 or not 0 < risk_pct <= max_risk_pct):
            raise ValueError("Equity, margen disponible, apalancamiento o riesgo porcentual inválido.")
        mark_price = float(self.client.get_mark_price(symbol) or 0)
        if mark_price <= 0:
            raise RuntimeError(f"No se obtuvo Mark Price de Binance para {symbol}; entrada bloqueada.")
        stop_pct, _, _ = self.calculate_atr_4h(symbol, mark_price)
        risk_budget = equity * risk_pct / 100.0
        is_long = side in ("BUY", "LONG")
        raw_stop = mark_price * (1.0 - stop_pct) if is_long else mark_price * (1.0 + stop_pct)
        stop_price = float(self.format_price(symbol, raw_stop, round_up=is_long))
        commission_rate = self.client.get_commission_rate(symbol)
        if commission_rate is None:
            raise RuntimeError(f"No se pudo consultar la comisión taker de Binance para {symbol}; entrada bloqueada.")
        slippage_pct = float(CONFIG["risk"]["slippage_reserve_pct"])
        if not math.isfinite(slippage_pct) or slippage_pct < 0:
            raise ValueError("Reserva de slippage porcentual inválida.")
        unit_risk = self.planned_unit_risk(
            mark_price, mark_price * stop_pct, is_long, commission_rate, slippage_pct)
        if unit_risk <= 0:
            raise ValueError(f"Riesgo unitario inválido para {symbol}.")
        target_notional = (risk_budget / unit_risk) * mark_price
        target_margin = target_notional / leverage
        margin_limit = available_balance * float(CONFIG["risk"]["max_margin_pct_of_available_balance"]) / 100.0
        margin = min(target_margin, margin_limit)
        if margin <= 0:
            raise ValueError("El margen proporcional calculado no permite abrir una posición.")

        rules = self.get_symbol_filters(symbol)
        raw_quantity = margin * leverage / mark_price
        quantity = float(self.format_quantity(symbol, raw_quantity))
        partial_quantity = float(self.split_position_quantities(symbol, quantity)[0])
        if rules.get("minNotional") is not None and quantity * mark_price < float(rules["minNotional"]):
            raise ValueError(f"El tamaño ajustado al riesgo queda por debajo del mínimo de notional de Binance para {symbol}.")
        if partial_quantity <= 0 or partial_quantity >= quantity:
            raise ValueError(f"El tamaño de TP parcial {CONFIG['exits']['partial_close_pct']:g}/{CONFIG['exits']['runner_pct']:g} no es viable para {symbol}.")
        quantity = float(self.format_quantity(symbol, margin * leverage / mark_price))
        modeled_risk = quantity * unit_risk
        if modeled_risk > risk_budget:
            raise ValueError(f"El tamaño redondeado excede el presupuesto de riesgo porcentual para {symbol}.")
        return margin

    def get_symbol_filters(self, symbol):
        """Obtiene filtros del símbolo desde exchangeInfo; no inventa mínimos si falla Binance."""
        cache_ttl = float(CONFIG["operations"].get("exchange_info_cache_seconds", 30))
        cache_expired = (not getattr(self, "_exchange_info_cache", None)
                         or time.monotonic() - getattr(self, "_exchange_info_cache_at", 0) >= cache_ttl)
        if cache_expired:
            fresh_info = self.client.get_exchange_info()
            if not isinstance(fresh_info, dict) or not isinstance(fresh_info.get("symbols"), list):
                raise RuntimeError("Binance exchangeInfo no está disponible; no se puede validar tamaño/precio.")
            self._exchange_info_cache = fresh_info
            self._exchange_info_cache_at = time.monotonic()
        info = self._exchange_info_cache
        if not isinstance(info, dict) or not isinstance(info.get("symbols"), list):
            raise RuntimeError("Binance exchangeInfo no está disponible; no se puede validar tamaño/precio.")
        symbol_info = next((item for item in info["symbols"] if item.get("symbol") == symbol), None)
        if not symbol_info:
            raise ValueError(f"Símbolo {symbol} no encontrado en exchangeInfo de Binance.")
        rules = {}
        for rule in symbol_info.get("filters", []):
            kind = rule.get("filterType")
            if kind == "LOT_SIZE":
                rules["lotStepSize"] = rule.get("stepSize")
                rules["lotMinQty"] = rule.get("minQty")
                rules["lotMaxQty"] = rule.get("maxQty")
            elif kind == "MARKET_LOT_SIZE":
                rules["marketStepSize"] = rule.get("stepSize")
                rules["marketMinQty"] = rule.get("minQty")
                rules["marketMaxQty"] = rule.get("maxQty")
            elif kind == "PRICE_FILTER":
                rules["tickSize"] = rule.get("tickSize")
                rules["minPrice"] = rule.get("minPrice")
                rules["maxPrice"] = rule.get("maxPrice")
            elif kind == "MIN_NOTIONAL":
                rules["minNotional"] = rule.get("notional")
            elif kind == "PERCENT_PRICE":
                rules["multiplierUp"] = rule.get("multiplierUp")
                rules["multiplierDown"] = rule.get("multiplierDown")
        rules["stepSize"] = rules.get("marketStepSize") or rules.get("lotStepSize")
        rules["minQty"] = rules.get("marketMinQty") or rules.get("lotMinQty")
        rules["maxQty"] = rules.get("marketMaxQty") or rules.get("lotMaxQty")
        try:
            if float(rules.get("marketStepSize") or 0) <= 0:
                rules["stepSize"] = rules.get("lotStepSize")
            if float(rules.get("marketMinQty") or 0) <= 0:
                rules["minQty"] = rules.get("lotMinQty")
            if float(rules.get("marketMaxQty") or 0) <= 0:
                rules["maxQty"] = rules.get("lotMaxQty")
        except (TypeError, ValueError):
            raise RuntimeError(f"Filtros MARKET_LOT_SIZE inválidos para {symbol}.")
        missing = [key for key in ("stepSize", "minQty", "tickSize") if not rules.get(key)]
        if missing:
            raise RuntimeError(f"exchangeInfo incompleto para {symbol}: {', '.join(missing)}")
        try:
            if Decimal(str(rules["stepSize"])) <= 0 or Decimal(str(rules["tickSize"])) <= 0 or Decimal(str(rules["minQty"])) < 0:
                raise ValueError("filtro no positivo")
        except Exception as exc:
            raise RuntimeError(f"exchangeInfo contiene step/tick/minQty inválidos para {symbol}.") from exc
        return rules

    def format_quantity(self, symbol, qty):
        """Floors market quantity to current MARKET_LOT_SIZE (LOT_SIZE fallback)."""
        rules = self.get_symbol_filters(symbol)
        step = Decimal(str(rules["stepSize"]))
        min_qty = Decimal(str(rules["minQty"]))
        requested = Decimal(str(qty))
        if requested <= 0 or step <= 0:
            raise ValueError(f"Cantidad inválida para {symbol}.")
        def decimal_places(value):
            normalized = format(Decimal(str(value)).normalize(), "f")
            return len(normalized.split(".", 1)[1]) if "." in normalized else 0
        decimals = max(decimal_places(rules["stepSize"]), decimal_places(rules["minQty"]))
        if requested < min_qty:
            raise ValueError(f"Cantidad proporcional menor que minQty de Binance para {symbol}; entrada/salida cancelada.")
        qty_floored = min_qty + ((requested - min_qty) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        max_qty = Decimal(str(rules.get("maxQty") or "0"))
        if max_qty > 0 and qty_floored > max_qty:
            raise ValueError(f"Cantidad {qty_floored} supera maxQty de Binance ({max_qty}) para {symbol}.")
        return format(qty_floored, f".{decimals}f") if decimals else str(int(qty_floored))

    def split_position_quantities(self, symbol, quantity):
        """Return the TP1 tranche and exact exchange-step remainder for the runner."""
        initial = Decimal(self.format_quantity(symbol, quantity))
        partial_raw = initial * Decimal(str(CONFIG["exits"]["partial_close_pct"])) / Decimal("100")
        min_qty = Decimal(str(self.get_symbol_filters(symbol)["minQty"]))
        if partial_raw < min_qty and initial - min_qty >= min_qty:
                                                                                                                             
                                                                                                                
            partial_raw = min_qty
        partial = Decimal(self.format_quantity(symbol, partial_raw))
        if partial >= initial:
            raise ValueError(f"El parcial TP1 no deja runner para {symbol}.")
        runner = Decimal(self.format_quantity(symbol, initial - partial))
        if partial + runner != initial:
            raise ValueError(f"Los tramos TP1/runner no cuadran con el tamaño ejecutable de {symbol}.")
        return format(partial, "f"), format(runner, "f")

    def format_price(self, symbol, price, round_up=False):
        """Alinea el precio a un múltiplo válido del tickSize de Binance."""
        rules = self.get_symbol_filters(symbol)
        tick = Decimal(str(rules["tickSize"]))
        price_decimal = Decimal(str(price))
        minimum = Decimal(str(rules.get("minPrice") or "0"))
        maximum = Decimal(str(rules.get("maxPrice") or "0"))
        rounding = ROUND_CEILING if round_up else ROUND_FLOOR
        ticks = ((price_decimal - minimum) / tick).to_integral_value(rounding=rounding)
        aligned = minimum + ticks * tick
        if aligned <= 0 or (minimum > 0 and aligned < minimum) or (maximum > 0 and aligned > maximum):
            raise ValueError(f"Precio {aligned} fuera de minPrice/maxPrice de Binance para {symbol}.")
        return format(aligned, "f")

    def open_position(self, symbol, side, leverage, risk_pct, equity=None, available_balance=None, signal_metadata=None):
        with self.execution_lock():
            self.state = self.load_state()
            return self._open_position_unlocked(
                symbol, side, leverage, risk_pct, equity, available_balance, signal_metadata
            )

    def preview_capacity_entry(self, symbol, side, leverage, risk_pct, equity, available_balance, signal_metadata):
        """Read-only plan at a full portfolio; all normal read checks still apply."""
        with self.execution_lock():
            return self._open_position_unlocked(symbol, side, leverage, risk_pct, equity,
                                                available_balance, signal_metadata, preview_only=True)

    def _open_position_unlocked(self, symbol, side, leverage, risk_pct, equity=None, available_balance=None, signal_metadata=None, *, preview_only=False):
        """Abre una posición dimensionada exclusivamente como porcentaje del equity en vivo."""
        self.state = self.load_state()
        if not self.entries_armed():
            return {"status": "entry_disarmed", "symbol": symbol,
                    "reason": "Mainnet está DISARMED o el circuit breaker global de protección está activo."}
        cooling_off = self.state.get("binance_cooling_off", {})
        retry_after = float(cooling_off.get("retry_after", 0) or 0) if isinstance(cooling_off, dict) else 0.0
        if retry_after > time.time():
            return {"status": "account_cooling_off", "retry_after": retry_after,
                    "retry_in_seconds": max(1, int(retry_after - time.time())), "code": -4192}
        if self.has_unresolved_reconciliation():
            return {"status": "entry_pending", "symbol": symbol, "state": "RECONCILING",
                    "reason": "Existe un submit de entrada sin reconciliar con Binance."}
        pending_statuses = {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING", "TP1_PENDING",
                            "EMERGENCY_CLOSE_PENDING", "UNPROTECTED", "CLOSE_PENDING",
                            "PROTECTION_FAILURE_CLOSE_PENDING"}
        position_states = self.state.get("positions", {})
        if str(position_states.get(symbol, {}).get("status", "")).upper() in pending_statuses:
            raise RuntimeError(f"{symbol} tiene una operación pendiente de reconciliación.")
        if symbol in self.state.get("closed_positions", {}):
            raise RuntimeError(f"{symbol} aún está pendiente de reconciliar en el registro de cierres.")
        last_exit = float(self.state.get("symbol_cooldowns", {}).get(symbol, 0) or 0)
        cooldown_seconds = float(CONFIG["operations"]["symbol_cooldown_minutes"]) * 60.0
        if time.time() - last_exit < cooldown_seconds:
            raise RuntimeError(f"Cooldown activo para {symbol}; entrada bloqueada.")
        account_positions = self.client.get_positions()
        if not isinstance(account_positions, list):
            raise RuntimeError("No se pudieron reconciliar posiciones actuales de Binance; entrada bloqueada.")
        untracked = self.get_untracked_positions(account_positions)
        if untracked is None:
            raise RuntimeError("No se pudo confirmar el modo de posiciones para reconciliar Binance; entrada bloqueada.")
        if untracked:
            legs = ", ".join(f"{p.get('symbol')}:{p.get('positionSide', 'BOTH')}" for p in untracked)
            raise RuntimeError(f"Hay posiciones Binance sin estado AG verificable ({legs}); entrada bloqueada.")
        live_positions = [p for p in account_positions if self._is_open_position(p)]
        max_positions = int(CONFIG["operations"]["max_concurrent_positions"])
        live_symbols = {p.get("symbol") for p in live_positions}
        reserved_entries = sum(
            1 for pending_symbol, metadata in position_states.items()
            if pending_symbol not in live_symbols and isinstance(metadata, dict)
            and str(metadata.get("status", "")).upper() in {"ENTRY_PENDING", "ENTRY_UNKNOWN", "RECONCILING"}
        )
        if len(live_positions) + reserved_entries >= max_positions and not preview_only:
            raise RuntimeError(f"Límite de {max_positions} posiciones simultáneas alcanzado en la cuenta Binance.")
        hedge_mode = self.client.get_position_mode()
        if hedge_mode is None:
            raise RuntimeError("No se pudo confirmar el modo de posición antes de verificar protecciones.")
        portfolio_algos = []
        for position in live_positions:
            current_symbol = position.get("symbol")
            metadata = self.state.get("positions", {}).get(current_symbol, {})
            current_amt = float(position.get("positionAmt", 0))
            position_side = str(metadata.get("position_side") or (str(position.get("positionSide", "BOTH")) if hedge_mode else "BOTH"))
            close_side = "SELL" if current_amt > 0 else "BUY"
            algos = self.client.get_open_algo_orders(current_symbol)
            portfolio_algos.extend(algos)
            runner_active = bool(metadata.get("tp1_executed"))
            stop_target = (metadata.get("current_sl_price") or metadata.get("initial_sl_price"))
            tp_target = metadata.get("tp_price")
            stop_matches = [a for a in algos if stop_target is not None and self._owned_algo_order_matches(
                a, metadata,
                symbol=current_symbol, order_type="STOP_MARKET", side=close_side,
                position_side=position_side, trigger_price=stop_target, close_position=True
            )]
            tp_qty = metadata.get("partial_qty")
            tp_matches = [a for a in algos if not runner_active and tp_target is not None and tp_qty is not None
                          and self._owned_algo_order_matches(
                              a, metadata,
                              symbol=current_symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
                              position_side=position_side, trigger_price=tp_target,
                              close_position=False, quantity=tp_qty,
                              reduce_only=(position_side == "BOTH")
                          )]
            has_stop = bool(stop_matches)
            has_take_profit = bool(tp_matches)
            if not has_stop or (not has_take_profit and not metadata.get("tp1_executed")):
                raise RuntimeError(f"La posición abierta {current_symbol}:{position_side} no tiene protección reconciliada; nueva entrada bloqueada.")
        if any(p.get("symbol") == symbol for p in live_positions):
            raise RuntimeError(f"Ya existe una posición abierta en {symbol}; no se permite duplicar ni abrir la pierna opuesta.")
        if any(order.get("symbol") == symbol for order in self.client.get_open_orders(symbol)):
            raise RuntimeError(f"Hay órdenes estándar pendientes en {symbol}; entrada bloqueada hasta reconciliarlas.")
        if self.client.get_open_algo_orders(symbol):
            raise RuntimeError(f"Hay órdenes condicionales pendientes en {symbol}; entrada bloqueada hasta reconciliarlas.")
        if equity is None or available_balance is None:
            wallet = self.get_wallet_summary()
            if not wallet or float(wallet.get("total_balance", 0)) <= 0:
                raise RuntimeError("Equity real de Binance no disponible; entrada bloqueada.")
            equity = float(wallet["total_balance"]) if equity is None else float(equity)
            available_balance = float(wallet["available_balance"]) if available_balance is None else float(available_balance)
        is_hedge = self.client.get_position_mode(force_refresh=True)
        if is_hedge is None:
            raise RuntimeError("No se pudo confirmar el modo de posición de Binance; entrada bloqueada.")
        pos_side = ("LONG" if side in ("BUY", "LONG") else "SHORT") if is_hedge else "BOTH"
        close_side = "SELL" if side in ("BUY", "LONG") else "BUY"
        mark_price = float(self.client.get_mark_price(symbol) or 0)
        if mark_price <= 0:
            raise ValueError(f"No se pudo obtener Mark Price de Binance para {symbol}.")
        stop_pct, tp_pct, atr_val = self.calculate_atr_4h(symbol, mark_price)
        if side in ("BUY", "LONG"):
            sl_price_val = mark_price * (1.0 - stop_pct)
            tp_price_val = mark_price * (1.0 + tp_pct)
            if not sl_price_val < mark_price < tp_price_val:
                raise ValueError(f"Niveles SL/TP inválidos para {symbol} LONG.")
        else:
            sl_price_val = mark_price * (1.0 + stop_pct)
            tp_price_val = mark_price * (1.0 - tp_pct)
            if not tp_price_val < mark_price < sl_price_val:
                raise ValueError(f"Niveles SL/TP inválidos para {symbol} SHORT.")

        is_long = side in ("BUY", "LONG")
        tp_price_val = self.cap_take_profit_trigger(symbol, tp_price_val, is_long, mark_price)                                  
        sl_price_val = self.cap_stop_trigger(symbol, sl_price_val, is_long, mark_price)                                  
                                                                                     
        sl_str = self.format_price(symbol, sl_price_val, round_up=is_long)
        tp_str = self.format_price(symbol, tp_price_val, round_up=is_long)
        margin = self.calculate_risk_adjusted_margin(symbol, leverage, risk_pct, equity, available_balance, side)
        rules = self.get_symbol_filters(symbol)
        quantity_str = self.format_quantity(symbol, margin * int(leverage) / mark_price)
        quantity = float(quantity_str)
        partial_quantity_str, runner_quantity_str = self.split_position_quantities(symbol, quantity_str)
        min_notional = rules.get("minNotional")
        if min_notional is not None and quantity * mark_price < float(min_notional):
            raise ValueError(f"La cantidad calculada no alcanza el notional mínimo de Binance para {symbol}.")
        if min_notional is not None and float(partial_quantity_str) * mark_price < float(min_notional):
            raise ValueError(f"El tramo TP1 de {symbol} quedaría bajo el notional mínimo de Binance; no se abre una posición que no pueda tomar el parcial configurado.")
        risk_budget = float(equity) * float(risk_pct) / 100.0
        commission_rate = self.client.get_commission_rate(symbol)
        if commission_rate is None:
            raise RuntimeError(f"No se pudo consultar la comisión taker de Binance para {symbol}; entrada bloqueada.")
        stop_price_rounded = float(sl_str)
        unit_risk = self.planned_unit_risk(
            mark_price, atr_val * float(CONFIG["atr"]["stop_multiple"]), is_long,
            commission_rate, float(CONFIG["risk"]["slippage_reserve_pct"]))
                                                                                     
        safe_quantity = float(self.format_quantity(symbol, min(quantity, risk_budget / unit_risk)))
        if safe_quantity < quantity:
            quantity = safe_quantity
            quantity_str = self.format_quantity(symbol, quantity)
            partial_quantity_str, runner_quantity_str = self.split_position_quantities(symbol, quantity_str)
            margin = quantity * mark_price / int(leverage)
            if min_notional is not None and (quantity * mark_price < float(min_notional)
                    or float(partial_quantity_str) * mark_price < float(min_notional)):
                raise ValueError("Risk-safe quantity cannot meet Binance minimums before submit.")
        stop_loss_amount = quantity * unit_risk
        if stop_loss_amount > risk_budget:
            raise ValueError(f"El redondeo al stepSize excedería el presupuesto de riesgo en {symbol}.")

                                                                             
                                                                                
                                                                              
                                                                 
        planned_metadata = {"side": "LONG" if is_long else "SHORT",
                            "position_side": pos_side}
        self._validate_algo_order_request(
            symbol=symbol, order_type="STOP_MARKET", side=close_side,
            position_side=pos_side, trigger_price=sl_str,
            close_position=True, quantity=None, reduce_only=False,
            metadata=planned_metadata,
        )
        self._validate_trigger_side(symbol, "STOP_MARKET", close_side, sl_str)
        self._validate_algo_order_request(
            symbol=symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
            position_side=pos_side, trigger_price=tp_str,
            close_position=False, quantity=partial_quantity_str,
            reduce_only=True, metadata=planned_metadata,
        )
        self._validate_trigger_side(symbol, "TAKE_PROFIT_MARKET", close_side, tp_str)

                                                                                
                                                                           
        capacity_advisory = None
        requested_quantity = quantity
        capacity_quantity_scale = 1.0
        capacity_assessment = None
        conditional_slot = len(live_positions) + reserved_entries >= int(CONFIG["portfolio_exposure"]["base_capacity"])
        try:
            from ag_portfolio_observer import assess_entry
            for sizing_attempt in range(4):
                candidate = {"symbol": symbol, "side": "LONG" if is_long else "SHORT",
                             "qty": quantity, "entry": mark_price, "mark": mark_price,
                             "sl": stop_price_rounded, "leverage": int(leverage),
                             "fee_rate": commission_rate}
                capacity_advisory = assess_entry(
                    self, live_positions, portfolio_algos, candidate,
                    equity=float(equity), available=float(available_balance),
                    signal_metadata=signal_metadata, preview_only=preview_only,
                    reserved_entries=reserved_entries)
                if (not conditional_slot or preview_only or not isinstance(capacity_advisory, dict)
                        or capacity_advisory.get("production_authorized") is True):
                    break
                decision = capacity_advisory.get("decision", capacity_advisory)
                reasons = decision.get("reasons", []) if isinstance(decision, dict) else []
                scale = decision.get("recommended_quantity_scale") if isinstance(decision, dict) else None
                if "SIZE_REDUCTION_REQUIRED" not in reasons or not isinstance(scale, (int, float)) or scale <= 0:
                    break
                if sizing_attempt >= 3:
                    break
                try:
                    reduced_str = self.format_quantity(symbol, quantity * float(scale))
                    reduced_qty = float(reduced_str)
                    if reduced_qty <= 0 or reduced_qty >= quantity:
                        raise ValueError("LOT_SIZE impide reducir más la cantidad")
                    reduced_partial, reduced_runner = self.split_position_quantities(symbol, reduced_str)
                    if min_notional is not None and reduced_qty * mark_price < float(min_notional):
                        raise ValueError("La cantidad reducida quedaría bajo el notional mínimo de Binance")
                    if min_notional is not None and float(reduced_partial) * mark_price < float(min_notional):
                        raise ValueError("El TP1 reducido quedaría bajo el notional mínimo de Binance")
                except Exception as sizing_exc:
                    capacity_advisory = {
                        "status": "capacity_rejected", "production_authorized": False,
                        "decision": decision,
                        "reasons": ["BINANCE_MINIMUMS_PREVENT_SAFE_DOWNSIZE", str(sizing_exc)[:240]],
                        "execution_effect": "NONE",
                    }
                    break
                quantity_str = reduced_str
                quantity = reduced_qty
                partial_quantity_str, runner_quantity_str = reduced_partial, reduced_runner
                margin = quantity * mark_price / int(leverage)
                stop_loss_amount = quantity * unit_risk
                capacity_quantity_scale = quantity / requested_quantity
        except Exception as exposure_exc:
            print(f"AG exposure advisory unavailable: {type(exposure_exc).__name__}: {exposure_exc}")
        if preview_only:
            return {"status": "portfolio_capacity_preview", "symbol": symbol,
                    "decision": capacity_advisory, "quantity": quantity,
                    "requested_quantity": requested_quantity,
                    "capacity_quantity_scale": capacity_quantity_scale,
                    "sl": stop_price_rounded, "tp": float(tp_str),
                    "venue_margin_leverage_confirmation": "REQUIRED_AT_REAL_SUBMIT",
                    "execution_effect": "NONE"}

        if conditional_slot:
            reasons = list(capacity_advisory.get("reasons", [])) if isinstance(capacity_advisory, dict) else ["EXPOSURE_UNAVAILABLE"]
            if not isinstance(capacity_advisory, dict) or capacity_advisory.get("production_authorized") is not True:
                if isinstance(capacity_advisory, dict) and isinstance(capacity_advisory.get("decision"), dict):
                    reasons = list(capacity_advisory["decision"].get("reasons", reasons)) + reasons
                return {"status": "capacity_rejected", "symbol": symbol, "decision": capacity_advisory,
                        "reasons": reasons,
                        "recommended_quantity_scale": (capacity_advisory.get("recommended_quantity_scale")
                            if isinstance(capacity_advisory, dict) else None),
                        "soft_warnings": (capacity_advisory.get("soft_warnings", [])
                            if isinstance(capacity_advisory, dict) else []),
                        "execution_effect": "NONE"}
            fresh_equity = capacity_advisory.get("fresh_equity_usdt")
            if not isinstance(fresh_equity, (int, float)) or fresh_equity <= 0 or stop_loss_amount > fresh_equity * float(risk_pct) / 100.0:
                return {"status": "capacity_rejected", "symbol": symbol,
                        "reasons": ["TRADE_RISK_EXCEEDS_FRESH_EQUITY"], "execution_effect": "NONE"}
                                                                            
                                                                        
            fresh_positions = self.client.get_positions()
            old_legs = sorted((str(p.get("symbol")), str(p.get("positionSide", "BOTH")),
                               str(p.get("positionAmt"))) for p in live_positions)
            new_legs = sorted((str(p.get("symbol")), str(p.get("positionSide", "BOTH")),
                               str(p.get("positionAmt"))) for p in fresh_positions if self._is_open_position(p)) if isinstance(fresh_positions, list) else None
            if new_legs != old_legs:
                return {"status": "capacity_rejected", "symbol": symbol,
                        "reasons": ["PORTFOLIO_CHANGED_DURING_ASSESSMENT"], "execution_effect": "NONE"}
            for current_symbol in sorted({p.get("symbol") for p in live_positions}):
                original_ids = {str(a.get("algoId")) for a in portfolio_algos if a.get("symbol") == current_symbol}
                latest_algos = self.client.get_open_algo_orders(current_symbol)
                latest_ids = {str(a.get("algoId")) for a in latest_algos} if isinstance(latest_algos, list) else None
                if latest_ids != original_ids:
                    return {"status": "capacity_rejected", "symbol": symbol,
                            "reasons": ["PROTECTION_CHANGED_DURING_ASSESSMENT"], "execution_effect": "NONE"}

        margin_result = self.client.change_margin_type(symbol, CONFIG["risk"]["margin_type"])
        if not isinstance(margin_result, dict):
            raise RuntimeError(f"Binance no confirmó el modo de margen {CONFIG['risk']['margin_type']} para {symbol}.")
        margin_error = " ".join(str(margin_result.get(key, "")) for key in ("error", "response", "msg"))
        already_configured = "-4046" in margin_error or "No need to change margin type" in margin_error
        if ("error" in margin_result or int(margin_result.get("code", 0) or 0) < 0) and not already_configured:
            raise RuntimeError(f"Binance rechazó el modo de margen {CONFIG['risk']['margin_type']} para {symbol}: {margin_result}")
        leverage_result = self.client.change_leverage(symbol, leverage)
        if not isinstance(leverage_result, dict) or int(leverage_result.get("leverage", 0)) != int(leverage):
            raise RuntimeError(f"Binance no confirmó el apalancamiento {leverage}x para {symbol}: {leverage_result}")

        client_order_id = f"agy_{self.agent_id[:8]}_{int(time.time() * 1000)}"
        opened_at = time.time()
        signal_metadata = signal_metadata if isinstance(signal_metadata, dict) else {}
        self._mark_symbol_active(symbol, initial_qty=quantity, position_side=pos_side)
        pos_meta = self.state.setdefault("positions", {}).setdefault(symbol, {})
        pos_meta.update({
            "status": "ENTRY_PENDING", "side": "LONG" if is_long else "SHORT",
            "position_side": pos_side, "opened_at": opened_at,
            "requested_qty": quantity, "initial_qty": quantity,
            "risk_pct": float(risk_pct), "risk_budget": risk_budget,
            "requested_policy_qty": requested_quantity,
            "capacity_quantity_scale": capacity_quantity_scale,
            "capacity_marginal_risk_budget_usdt": (capacity_advisory.get("marginal_risk_budget_usdt")
                if isinstance(capacity_advisory, dict) else None),
            "modeled_risk_usdt": stop_loss_amount,
            "modeled_risk_pct_equity": stop_loss_amount / float(equity) * 100.0,
            "portfolio_fee_rate": commission_rate,
            "risk_usdt": stop_loss_amount, "entry_client_order_id": client_order_id,
            "entry_submit_uncertain": True,
            "supporters": sorted({str(x).strip().lower() for x in signal_metadata.get("supporters", []) if x}),
            "supporter_names": signal_metadata.get("supporter_names", []),
            "reference_price": signal_metadata.get("reference_price"),
            "fingerprint": signal_metadata.get("fingerprint")
        })
        fingerprint = signal_metadata.get("fingerprint")
        if fingerprint:
            fingerprints = self.state.setdefault("executed_fingerprints", [])
            if fingerprint not in fingerprints:
                fingerprints.append(fingerprint)
            del fingerprints[:-int(CONFIG["operations"]["fingerprint_history_limit"])]
        self.save_state()
        entry_result = self.client.place_order(
            symbol=symbol,
            side="BUY" if side in ("BUY", "LONG") else "SELL",
            order_type="MARKET",
            quantity=quantity_str,
            position_side=pos_side,
            client_order_id=client_order_id
        )
        if not isinstance(entry_result, dict) or entry_result.get("error") or "orderId" not in entry_result:
            outcome = entry_result.get("outcome") if isinstance(entry_result, dict) else "ambiguous_after_submit"
            if outcome in {"pre_submit_error", "explicit_rejection"}:
                self._forget_executed_fingerprint(pos_meta)
                self.state.get("positions", {}).pop(symbol, None)
                if symbol in self.state.get("active_symbols", []):
                    self.state["active_symbols"].remove(symbol)
                cooling = None
                if outcome == "explicit_rejection" and self._exchange_error_code(entry_result) == -4192:
                    cooling = self._register_cooling_off_rejection(symbol)
                self.save_state()
                response = {"status": "entry_pre_submit_error" if outcome == "pre_submit_error" else "entry_rejected",
                            "symbol": symbol, "entry": entry_result}
                if cooling:
                    response.update({"cooling_off": True, "retry_after": cooling["retry_after"],
                                     "retry_delay_seconds": cooling["retry_delay_seconds"],
                                     "rejection_count": cooling["rejection_count"]})
                return response
            attempts = int(CONFIG["operations"]["entry_order_lookup_attempts"])
            interval = float(CONFIG["operations"]["entry_order_lookup_interval_seconds"])
            reconciled = None
            for attempt in range(attempts):
                if attempt:
                    time.sleep(interval)
                candidate = self.client.get_order(symbol, client_order_id)
                if isinstance(candidate, dict) and candidate.get("orderId") is not None:
                    reconciled = candidate
                    break
            if reconciled is None:
                pos_meta.update({"status": "RECONCILING", "entry_submit_uncertain": True})
                self.save_state()
                print(f"⚠️ [RECONCILING] El submit pudo aceptarse, pero Binance aún no confirma {client_order_id}; no se repite la entrada.")
                return {"status": "entry_pending", "symbol": symbol, "client_order_id": client_order_id,
                        "state": "RECONCILING"}
            entry_result = reconciled

        entry_status = str(entry_result.get("status", "")).upper()
        filled_qty = float(entry_result.get("executedQty", 0) or 0)
        if entry_status == "PARTIALLY_FILLED":
                                                                                  
                                                                         
            self.client.cancel_order(symbol, client_order_id=client_order_id)
            terminal = None
            for attempt in range(int(CONFIG["operations"]["entry_order_lookup_attempts"])):
                if attempt:
                    time.sleep(float(CONFIG["operations"]["entry_order_lookup_interval_seconds"]))
                candidate = self.client.get_order(symbol, client_order_id)
                if isinstance(candidate, dict) and str(candidate.get("status", "")).upper() in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                    terminal = candidate
                    break
            if terminal is None:
                pos_meta.update({"status": "RECONCILING", "executed_qty": filled_qty,
                                 "entry_submit_uncertain": True})
                self.save_state()
                return {"status": "entry_pending", "symbol": symbol,
                        "client_order_id": client_order_id, "state": "RECONCILING"}
            entry_result = terminal
            entry_status = str(entry_result.get("status", "")).upper()
            filled_qty = float(entry_result.get("executedQty", 0) or 0)
        if filled_qty <= 0 or entry_status not in ("FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"):
            if entry_status in ("REJECTED", "CANCELED", "EXPIRED") and filled_qty <= 0:
                self._forget_executed_fingerprint(pos_meta)
                self.state.get("positions", {}).pop(symbol, None)
                if symbol in self.state.get("active_symbols", []):
                    self.state["active_symbols"].remove(symbol)
                cooling = None
                if self._exchange_error_code(entry_result) == -4192:
                    cooling = self._register_cooling_off_rejection(symbol)
                self.save_state()
                response = {"status": "entry_rejected", "symbol": symbol, "entry": entry_result}
                if cooling:
                    response.update({"cooling_off": True, "retry_after": cooling["retry_after"],
                                     "retry_delay_seconds": cooling["retry_delay_seconds"],
                                     "rejection_count": cooling["rejection_count"]})
                return response
            pos_meta.update({"status": "ENTRY_PENDING" if entry_status in {"NEW", "PARTIALLY_FILLED"} else "RECONCILING",
                             "entry_submit_uncertain": True})
            self.save_state()
            return {"status": "entry_pending", "symbol": symbol, "entry": entry_result, "client_order_id": client_order_id}

                                                                                    
                                                                                    
        pos_meta["entry_submit_uncertain"] = False
        self.state.pop("binance_cooling_off", None)
        self.save_state()

        try:
            entry_price = float(entry_result.get("avgPrice", 0) or 0)
        except (TypeError, ValueError):
            entry_price = 0.0
        if entry_price <= 0:
            positions_after_entry = self.client.get_positions()
            matched_position = next((p for p in (positions_after_entry or [])
                                     if p.get("symbol") == symbol
                                     and str(p.get("positionSide", "BOTH")).upper() == pos_side), None)
            entry_price = float(matched_position.get("entryPrice", 0) or 0) if matched_position else 0.0
        if entry_price <= 0:
            pos_meta.update({"status": "EMERGENCY_CLOSE_PENDING", "executed_qty": filled_qty, "entry_order_id": str(entry_result.get("orderId", ""))})
            self.save_state()
            emergency_close = self._close_position_unlocked(symbol)
            if emergency_close and emergency_close.get("status") in {"closed", "already_closed"}:
                return {"status": "emergency_closed", "symbol": symbol, "reason": "entry fill price unavailable", "close": emergency_close}
            return {"status": "entry_pending", "symbol": symbol, "entry": entry_result, "client_order_id": client_order_id, "close_attempt": emergency_close}

        quantity_str = self.format_quantity(symbol, filled_qty)
        quantity = float(quantity_str)
        partial_quantity_str, runner_quantity_str = self.split_position_quantities(symbol, quantity_str)
        actual_stop_price = entry_price - atr_val * float(CONFIG["atr"]["stop_multiple"]) if is_long else entry_price + atr_val * float(CONFIG["atr"]["stop_multiple"])
        actual_tp_price = entry_price + atr_val * float(CONFIG["exits"]["take_profit_atr_multiple"]) if is_long else entry_price - atr_val * float(CONFIG["exits"]["take_profit_atr_multiple"])
        try:                                                                                                                  
            actual_stop_price = self.cap_stop_trigger(symbol, actual_stop_price, is_long)
        except Exception as exc_cap:
            print(f"Tope del stop de {symbol} no aplicado: {exc_cap}")
        sl_str = self.format_price(symbol, actual_stop_price, round_up=is_long)
        try:                                                                                                           
            actual_tp_price = self.cap_take_profit_trigger(symbol, actual_tp_price, is_long)
        except Exception as exc_cap:
            print(f"Tope del TP de {symbol} no aplicado: {exc_cap}")
        tp_str = self.format_price(symbol, actual_tp_price, round_up=not is_long)
        actual_unit_risk = self.unit_risk_to_stop(
            entry_price, float(sl_str), commission_rate,
            float(CONFIG["risk"]["slippage_reserve_pct"]))
        stop_loss_amount = quantity * actual_unit_risk
        if stop_loss_amount > risk_budget:
            pos_meta.update({"status": "EMERGENCY_CLOSE_PENDING", "initial_qty": quantity,
                             "executed_qty": filled_qty, "entry_price": entry_price,
                             "risk_usdt": stop_loss_amount, "entry_order_id": str(entry_result.get("orderId", "")),
                             "close_classification": "SAFETY_CLOSE",
                             "close_reason": "ACTUAL_FILL_RISK_BUDGET_EXCEEDED"})
            self.save_state()
            close_result = self._close_position_unlocked(symbol)
            status = "emergency_closed" if close_result and close_result.get("status") in {"already_closed", "closed"} else "critical_unprotected_failure"
            return {"status": status, "symbol": symbol, "reason": "actual fill excede el riesgo presupuestado",
                    "details": f"Filled risk {stop_loss_amount:.8f} exceeds budget {risk_budget:.8f}",
                    "classification": "SAFETY_CLOSE", "close": close_result}

        partial_pct = float(CONFIG["exits"]["partial_close_pct"])
        runner_pct = float(CONFIG["exits"]["runner_pct"])
        print(f"🚀 [ENTRADA EJECUTADA] {symbol} ({side} {leverage}x): {quantity_str}. Colocando SL y TP {partial_pct:g}/{runner_pct:g}.")
        confirmed_sl, res_sl = self._submit_algo_and_verify(
            symbol=symbol, order_type="STOP_MARKET", side=close_side,
            position_side=pos_side, trigger_price=sl_str, metadata=pos_meta,
            metadata_key="sl_algo_id"
        )
        confirmed_tp = None
        res_tp = None
        if confirmed_sl:
            confirmed_tp, res_tp = self._submit_algo_and_verify(
                symbol=symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
                position_side=pos_side, trigger_price=tp_str, quantity=partial_quantity_str,
                reduce_only=True, metadata=pos_meta, metadata_key="tp_algo_id"
            )
        if not (confirmed_sl and confirmed_tp):
            print(f"🚨 [FAILSAFE] Binance no confirmó todos los campos del SL/TP real de {symbol}; se intenta recuperar/cerrar.")
                                                                                    
                                                                                    
            self._mark_symbol_active(symbol, initial_qty=quantity, position_side=pos_side)
            pos_meta = self.state.setdefault("positions", {}).setdefault(symbol, {})
            pos_meta.update({
                "initial_qty": quantity,
                "partial_qty": float(partial_quantity_str),
                "runner_qty": float(runner_quantity_str),
                "risk_pct": float(risk_pct),
                "risk_budget": risk_budget,
                "risk_usdt": stop_loss_amount,
                "modeled_risk_pct_equity": stop_loss_amount / float(equity) * 100.0,
                "status": "RECONCILING" if any(item.get("outcome") == "ambiguous_after_submit" for item in (res_sl, res_tp) if isinstance(item, dict)) else "EMERGENCY_CLOSE_PENDING",
                "position_side": pos_side,
                "side": "LONG" if side in ("BUY", "LONG") else "SHORT",
                "opened_at": opened_at,
                "entry_price": entry_price,
                "entry_order_id": str(entry_result.get("orderId", "")),
                "entry_client_order_id": str(entry_result.get("clientOrderId", client_order_id))
            })
            self._trip_protection_circuit_breaker(
                symbol, "ENTRY_PROTECTION_NOT_CONFIRMED", pos_meta,
                {"stop": res_sl, "take_profit": res_tp}
            )
            try:
                if pos_meta.get("pending_algo_orders"):
                    self._reconcile_pending_algo_orders(symbol, pos_meta)
                    if not pos_meta.get("pending_algo_orders"):
                        pos_meta["status"] = "EMERGENCY_CLOSE_PENDING"
                        self.save_state()
                self._cancel_owned_algos(symbol, pos_meta)
            except Exception as cancel_exc:
                print(f"No se pudieron verificar/cancelar las protecciones residuales de {symbol}: {cancel_exc}")
            try:
                emergency_close = self._close_position_unlocked(symbol)
            except Exception as close_exc:
                emergency_close = {"error": type(close_exc).__name__}
            if emergency_close and emergency_close.get("status") in {"closed", "already_closed"}:
                return {"status": "emergency_closed", "symbol": symbol,
                        "reason": "SL/TP confirmation failed",
                        "classification": "PROTECTION_FAILURE",
                        "details": f"Cierre de emergencia confirmado plano para {symbol}."}
            return {"status": "critical_unprotected_failure", "symbol": symbol, "error": f"Falló confirmar protecciones y el cierre sigue pendiente: {emergency_close}"}

        sl_algo_id = confirmed_sl.get("algoId")
        tp_algo_id = confirmed_tp.get("algoId")
        self._mark_symbol_active(symbol, initial_qty=quantity, sl_algo_id=sl_algo_id, tp_algo_id=tp_algo_id, position_side=pos_side)
        pos_meta = self.state.setdefault("positions", {}).setdefault(symbol, {})
        entry_id = str(entry_result.get("orderId", ""))
        pos_meta.update({"initial_qty": quantity, "partial_qty": float(partial_quantity_str), "runner_qty": float(runner_quantity_str), "risk_pct": float(risk_pct), "risk_budget": risk_budget, "risk_usdt": stop_loss_amount, "modeled_risk_pct_equity": stop_loss_amount / float(equity) * 100.0, "status": "OPEN", "position_side": pos_side, "side": "LONG" if side in ("BUY", "LONG") else "SHORT", "opened_at": opened_at, "entry_price": entry_price, "initial_sl_price": float(sl_str), "tp_price": float(tp_str), "entry_order_id": entry_id, "entry_client_order_id": str(entry_result.get("clientOrderId", client_order_id)), "order_ids": [entry_id] if entry_id else []})
        self.save_state()
        return {
            "status": "ok", "symbol": symbol, "entry": entry_result,
            "sl_algo_id": str(sl_algo_id), "tp_algo_id": str(tp_algo_id),
            "sl_price": sl_str, "tp_price": tp_str, "is_protected": True,
            "margin": margin, "notional": quantity * mark_price,
            "requested_quantity": requested_quantity, "capacity_quantity_scale": capacity_quantity_scale,
            "quantity_reduced_for_capacity": capacity_quantity_scale < 1.0,
            "modeled_risk_usdt": stop_loss_amount,
            "modeled_risk_pct_equity": stop_loss_amount / float(equity) * 100.0,
            "capacity_soft_warnings": (capacity_advisory.get("soft_warnings", [])
                if isinstance(capacity_advisory, dict) else []),
            "risk_pct": float(risk_pct), "risk_budget": risk_budget,
            "leverage": int(leverage)
        }

    def ensure_position_protection(self, symbol, pos=None):
        with self.execution_lock():
            self.state = self.load_state()
            return self._ensure_position_protection_unlocked(symbol, pos)

    def _ensure_position_protection_unlocked(self, symbol, pos=None):
        """
        Reconciliador continuo: Garantiza que una posición abierta mantenga activas
        sus órdenes condicionales nativas de protección en Binance Futures.
        Si alguna falta, la coloca y confirma inmediatamente vía openAlgoOrders.
        Si la confirmación falla, activa el protocolo de cierre de emergencia con reduceOnly=true.
        """
                                                                                                                   
        if not isinstance(getattr(self, "last_protection_error", None), dict):
            self.last_protection_error = {}
        self.last_protection_error.pop(symbol, None)
        try:
            self.state = self.load_state()
            if not pos:
                positions = self._read_positions()
                is_hedge = self._read_position_mode()
                metadata = self.state.get("positions", {}).get(symbol, {})
                for p in (positions or []):
                    if p["symbol"] == symbol and self._is_open_position(p) and self._position_matches_metadata(p, metadata, is_hedge):
                        pos = p
                        break
            if not pos:
                return None

            amt = float(pos.get("positionAmt", 0))
            if amt == 0:
                return None

            pos_meta = self.state.setdefault("positions", {}).setdefault(symbol, {})
            if pos_meta.get("pending_algo_orders") and not self._reconcile_pending_algo_orders(symbol, pos_meta):
                self._trip_protection_circuit_breaker(
                    symbol, "PENDING_ALGO_PROTECTION_UNVERIFIABLE", pos_meta,
                    {"pending_client_algo_ids": list(pos_meta.get("pending_algo_orders", {}))}
                )
                close_result = self._close_position_unlocked(symbol)
                return {"symbol": symbol, "is_protected": False,
                        "classification": "PROTECTION_FAILURE",
                        "emergency_close": close_result}
            if pos_meta.get("close_client_order_id"):
                close_result = self._close_position_unlocked(symbol)
                return {"symbol": symbol, "is_protected": False, "close_pending": True,
                        "close_attempt": close_result}
            if str(pos_meta.get("status", "")).upper() in {
                "EMERGENCY_CLOSE_PENDING", "UNPROTECTED", "CLOSE_PENDING",
                "PROTECTION_FAILURE_CLOSE_PENDING"
            }:
                close_result = self._close_position_unlocked(symbol)
                return {"symbol": symbol, "sl_active": False, "is_protected": False,
                        "emergency_close": close_result}

            is_hedge = self._read_position_mode()
            pos_side = ("LONG" if amt > 0 else "SHORT") if is_hedge else "BOTH"
            close_side = "SELL" if amt > 0 else "BUY"
            abs_qty = abs(amt)
            entry_px = float(pos.get("entryPrice", 0))
            if entry_px <= 0:
                return None

                                                                  
            qty_str = self.format_quantity(symbol, abs_qty)
            min_qty = float(self.get_symbol_filters(symbol)["minQty"])
                                                                                                     
            try:
                tp_qty_str = self.split_position_quantities(symbol, abs_qty)[0]
            except ValueError:
                                                                                                                                       
                                                                                                                          
                tp_qty_str = None

            stop_pct, tp_pct, atr_val = self.calculate_atr_4h(symbol, entry_px)
            if amt > 0:
                sl_price_val = entry_px * (1.0 - stop_pct)
                tp_price_val = entry_px * (1.0 + tp_pct)
            else:
                sl_price_val = entry_px * (1.0 + stop_pct)
                tp_price_val = entry_px * (1.0 - tp_pct)

            tp_str = self.format_price(symbol, tp_price_val, round_up=amt < 0)
            if "initial_sl_price" not in pos_meta:
                try:                                                                         
                    sl_price_val = self.cap_stop_trigger(symbol, sl_price_val, amt > 0)
                except Exception as exc_cap:
                    print(f"Tope del stop de {symbol} no aplicado: {exc_cap}")
            sl_str = self.format_price(symbol, sl_price_val, round_up=amt > 0)

                                               
            existing_algos = self._read_open_algo_orders(symbol)
            leg_algos = [a for a in existing_algos if a.get("symbol") == symbol
                         and a.get("side") == close_side
                         and str(a.get("positionSide", "BOTH")).upper() == pos_side]

                                                                                                              
            pos_meta.setdefault("position_side", pos_side)
            pos_meta.setdefault("side", "LONG" if amt > 0 else "SHORT")
            initial_qty = float(pos_meta.get("initial_qty", 0.0) or 0.0)
            tp1_executed = pos_meta.get("tp1_executed", False)
            be_active = pos_meta.get("be_active", False)

                                                                                                    
                                                                      
            expected_partial = float(pos_meta.get("partial_qty", 0.0) or 0.0)
            if initial_qty > 0 and expected_partial > 0:
                                                                               
                                                                               
                exact_runner = Decimal(str(initial_qty)) - Decimal(str(expected_partial))
                if exact_runner > 0:
                    pos_meta["runner_qty"] = float(exact_runner)
                step_size = float(self.get_symbol_filters(symbol)["stepSize"])
                expected_remaining = initial_qty - expected_partial
                if abs_qty <= expected_remaining + step_size / 2.0 and abs_qty > 0:
                    tp1_executed = True
                    pos_meta["tp1_executed"] = True
            expected_sl = (pos_meta.get("current_sl_price") or
                           (entry_px * (1.0 + float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0)
                            if amt > 0 else entry_px * (1.0 - float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0))
                           if tp1_executed else pos_meta.get("initial_sl_price", sl_str))
            expected_sl = self.format_price(symbol, expected_sl, round_up=amt > 0)
            expected_tp = self.format_price(symbol, pos_meta.get("tp_price", tp_str), round_up=amt < 0)
            tp_qty_str = self.format_quantity(
                symbol, float(pos_meta.get("partial_qty", abs_qty * float(CONFIG["exits"]["partial_close_pct"]) / 100.0))
            )
            has_sl = any(self._owned_algo_order_matches(
                order, pos_meta,
                symbol=symbol, order_type="STOP_MARKET", side=close_side,
                position_side=pos_side, trigger_price=expected_sl, close_position=True
            ) for order in leg_algos)
            has_tp = any(self._owned_algo_order_matches(
                order, pos_meta,
                symbol=symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
                position_side=pos_side, trigger_price=expected_tp, close_position=False,
                quantity=tp_qty_str, reduce_only=(pos_side == "BOTH")
            ) for order in leg_algos)
            if tp1_executed and not be_active:
                                                                                                        
                has_sl = False

                                                                            
            if tp1_executed and not be_active:
                be_buffer = float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0
                be_price_val = entry_px * (1.0 + be_buffer) if amt > 0 else entry_px * (1.0 - be_buffer)
                be_str = self.format_price(symbol, be_price_val, round_up=amt > 0)
                print(f"🎯 [ESCUDO HÍBRIDO {CONFIG['exits']['partial_close_pct']:g}/{CONFIG['exits']['runner_pct']:g}] {symbol}: TP1 ({CONFIG['exits']['partial_close_pct']:g}%) ejecutado. Moviendo Stop Loss del runner a Break-Even ({be_str}).")
                try:
                    confirmed_be = next((order for order in leg_algos if self._owned_algo_order_matches(
                        order, pos_meta,
                        symbol=symbol, order_type="STOP_MARKET", side=close_side,
                        position_side=pos_side, trigger_price=be_str, close_position=True
                    )), None)
                    if not confirmed_be:
                        old_sl_id = pos_meta.get("sl_algo_id")
                        old_sl = next((order for order in leg_algos
                                       if old_sl_id is not None and str(order.get("algoId")) == str(old_sl_id)
                                       and order.get("orderType") == "STOP_MARKET"), None)
                        old_trigger = old_sl.get("triggerPrice") if old_sl else None
                        if old_sl:
                            self.client.cancel_algo_orders(
                                symbol, algo_ids={old_sl_id}, position_side=pos_side,
                                side=close_side, order_type="STOP_MARKET"
                            )
                        confirmed_be, be_response = self._submit_algo_and_verify(
                            symbol=symbol, order_type="STOP_MARKET", side=close_side,
                            position_side=pos_side, trigger_price=be_str,
                            metadata=pos_meta, metadata_key="sl_algo_id"
                        )
                        if not confirmed_be:
                            if old_trigger is not None and not (isinstance(be_response, dict)
                                                               and be_response.get("outcome") == "ambiguous_after_submit"):
                                restored, _ = self._submit_algo_and_verify(
                                    symbol=symbol, order_type="STOP_MARKET", side=close_side,
                                    position_side=pos_side, trigger_price=old_trigger,
                                    metadata=pos_meta, metadata_key="sl_algo_id"
                                )
                                if not restored:
                                    raise RuntimeError(f"No se confirmó Break-Even ni la restauración del SL anterior para {symbol}.")
                            raise RuntimeError(f"Binance no confirmó todos los campos del Stop Break-Even de {symbol}: {be_response}.")
                    self._cancel_owned_algos(symbol, pos_meta, exclude_algo_ids={confirmed_be.get("algoId")})
                    pos_meta["be_active"] = True
                    pos_meta["status"] = "RUNNER_ACTIVE"
                    pos_meta["risk_usdt"] = 0.0
                    pos_meta["sl_algo_id"] = str(confirmed_be["algoId"])
                    pos_meta["current_sl_price"] = float(be_str)
                    expected_sl = be_str
                    has_sl = True
                    be_active = True
                    sl_str = be_str
                    print(f"🛡️ [BREAK-EVEN CONFIRMADO EN BINANCE] {symbol}: Stop Loss trasladado exitosamente a {be_str} (AlgoID: {confirmed_be['algoId']}). Riesgo: $0.00.")
                except Exception as e_be:
                    print(f"Error actualizando SL a Break-Even para {symbol}: {e_be}")
                    has_sl = any(self._owned_algo_order_matches(
                        order, pos_meta,
                        symbol=symbol, order_type="STOP_MARKET", side=close_side,
                        position_side=pos_side, trigger_price=expected_sl, close_position=True
                    ) for order in self._read_open_algo_orders(symbol))
                self.save_state()

                                                                     
            if tp1_executed and be_active:
                mark_now = float(pos.get("markPrice", entry_px))
                peak_px = float(pos_meta.get("peak_price", entry_px))
                be_buffer = float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0
                be_floor = entry_px * (1.0 + be_buffer) if amt > 0 else entry_px * (1.0 - be_buffer)
                current_sl_val = float(pos_meta.get("current_sl_price", be_floor))

                if amt > 0:        
                    if mark_now > peak_px:
                        peak_px = mark_now
                        pos_meta["peak_price"] = peak_px
                    trail_target = peak_px - (float(CONFIG["atr"]["stop_multiple"]) * atr_val)
                    desired_sl = max(be_floor, trail_target)
                    trail_update = float(CONFIG["exits"]["trailing_stop_update_pct"]) / 100.0
                    if desired_sl > current_sl_val * (1.0 + trail_update):
                        new_sl_str = self.format_price(symbol, desired_sl)
                        print(f"🚀 [TRAILING UP ACTIVO] {symbol}: Nuevo pico ${peak_px:.2f}. Elevando Stop Loss a {new_sl_str} (Asegurando ganancias en el runner).")
                        try:
                            confirmed_trail, res_trail = self._replace_owned_stop(
                                symbol, close_side, pos_side, new_sl_str, pos_meta
                            )
                            if confirmed_trail:
                                pos_meta["current_sl_price"] = float(confirmed_trail["triggerPrice"])
                                pos_meta["sl_algo_id"] = str(confirmed_trail["algoId"])
                                expected_sl = str(confirmed_trail["triggerPrice"])
                                has_sl = True
                                print(f"🛡️ [TRAILING STOP CONFIRMADO EN BINANCE] {symbol} SL @ {new_sl_str} (AlgoID: {confirmed_trail['algoId']}).")
                        except Exception as e_trail:
                            print(f"Error actualizando Trailing SL para {symbol}: {e_trail}")
                        self.save_state()
                else:         
                    if mark_now < peak_px or peak_px == 0:
                        peak_px = mark_now
                        pos_meta["peak_price"] = peak_px
                    trail_target = peak_px + (float(CONFIG["atr"]["stop_multiple"]) * atr_val)
                    desired_sl = min(be_floor, trail_target)
                    trail_update = float(CONFIG["exits"]["trailing_stop_update_pct"]) / 100.0
                    if desired_sl < current_sl_val * (1.0 - trail_update):
                        new_sl_str = self.format_price(symbol, desired_sl, round_up=True)
                        print(f"🚀 [TRAILING DOWN ACTIVO] {symbol}: Nuevo fondo ${peak_px:.2f}. Bajando Stop Loss a {new_sl_str}.")
                        try:
                            confirmed_trail, res_trail = self._replace_owned_stop(
                                symbol, close_side, pos_side, new_sl_str, pos_meta
                            )
                            if confirmed_trail:
                                pos_meta["current_sl_price"] = float(confirmed_trail["triggerPrice"])
                                pos_meta["sl_algo_id"] = str(confirmed_trail["algoId"])
                                expected_sl = str(confirmed_trail["triggerPrice"])
                                has_sl = True
                        except Exception as e_trail:
                            print(f"Error actualizando Trailing SL para {symbol}: {e_trail}")
                        self.save_state()

                                 
            if not has_sl:
                confirmed_sl, res_sl = self._submit_algo_and_verify(
                    symbol=symbol, order_type="STOP_MARKET", side=close_side,
                    position_side=pos_side, trigger_price=expected_sl,
                    metadata=pos_meta, metadata_key="sl_algo_id"
                )
                has_sl = confirmed_sl is not None

                                                                     
            if not has_tp and not tp1_executed:
                try:                                                                                 
                    expected_tp = self.format_price(symbol, self.cap_take_profit_trigger(symbol, float(expected_tp), amt > 0), round_up=amt < 0)
                    pos_meta["tp_price"] = float(expected_tp)
                except Exception as exc_cap:
                    print(f"Tope del TP de {symbol} no aplicado: {exc_cap}")
                confirmed_tp, res_tp = self._submit_algo_and_verify(
                    symbol=symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
                    position_side=pos_side, trigger_price=expected_tp,
                    quantity=tp_qty_str, reduce_only=(pos_side == "BOTH"),
                    metadata=pos_meta, metadata_key="tp_algo_id"
                )
                has_tp = confirmed_tp is not None

                                              
            verified_algos = self._read_open_algo_orders(symbol)
            v_sl = next((order for order in verified_algos if self._owned_algo_order_matches(
                order, pos_meta,
                symbol=symbol, order_type="STOP_MARKET", side=close_side,
                position_side=pos_side, trigger_price=expected_sl, close_position=True
            )), None)
            v_tp = next((order for order in verified_algos if self._owned_algo_order_matches(
                order, pos_meta,
                symbol=symbol, order_type="TAKE_PROFIT_MARKET", side=close_side,
                position_side=pos_side, trigger_price=expected_tp, close_position=False,
                quantity=tp_qty_str, reduce_only=(pos_side == "BOTH")
            )), None)

            is_protected = v_sl is not None and (v_tp is not None or tp1_executed)
            if not is_protected:
                                                                                     
                                                                                       
                fresh = self._revalidate_position_protection(symbol, pos_meta)
                if fresh.get("position") is None:
                    return {"symbol": symbol, "is_protected": False,
                            "position_closed_during_verification": True,
                            "reason": "BINANCE_POSITION_FLAT_DURING_PROTECTION_CHECK"}
                if fresh.get("is_protected"):
                    v_sl, v_tp = fresh.get("sl"), fresh.get("tp")
                    tp1_executed = bool(fresh.get("tp1_executed"))
                    if tp1_executed:
                        pos_meta.update({"tp1_executed": True, "be_active": True,
                                         "status": "RUNNER_ACTIVE", "risk_usdt": 0.0,
                                         "sl_algo_id": str(v_sl.get("algoId")),
                                         "current_sl_price": float(v_sl.get("triggerPrice", expected_sl))})
                    else:
                        pos_meta.update({"status": "OPEN", "sl_algo_id": str(v_sl.get("algoId")),
                                         "tp_algo_id": str(v_tp.get("algoId"))})
                    self.save_state()
                    return {"symbol": symbol,
                            "sl_order_id": v_sl.get("algoId"),
                            "tp_order_id": v_tp.get("algoId") if v_tp else None,
                            "sl_active": True, "tp_active": v_tp is not None,
                            "is_protected": True,
                            "verification_source": "fresh_position_and_openAlgoOrders"}
                self._trip_protection_circuit_breaker(
                    symbol, "LIVE_PROTECTION_NOT_CONFIRMED", pos_meta,
                    {"active_algo_orders": fresh.get("algos", verified_algos),
                     "stop_confirmed": fresh.get("sl") is not None,
                     "take_profit_confirmed": fresh.get("tp") is not None,
                     "position_reconfirmed_open": True}
                )
                close_result = self._close_position_unlocked(symbol)
                return {"symbol": symbol, "sl_active": False, "tp_active": v_tp is not None,
                        "is_protected": False, "classification": "PROTECTION_FAILURE",
                        "emergency_close": close_result}
            if tp1_executed:
                pos_meta.update({"be_active": True, "status": "RUNNER_ACTIVE", "risk_usdt": 0.0,
                                 "sl_algo_id": str(v_sl.get("algoId")),
                                 "current_sl_price": float(v_sl.get("triggerPrice", expected_sl))})
                self._cancel_owned_algos(symbol, pos_meta, exclude_algo_ids={v_sl.get("algoId")})
            else:
                pos_meta.update({"status": "OPEN", "sl_algo_id": str(v_sl.get("algoId")),
                                 "tp_algo_id": str(v_tp.get("algoId"))})
                pos_meta.setdefault("initial_sl_price", float(expected_sl))
                pos_meta.setdefault("tp_price", float(expected_tp))
                pos_meta.setdefault("partial_qty", float(tp_qty_str))
            self.save_state()

            return {
                "symbol": symbol,
                "sl_order_id": v_sl.get("algoId") if v_sl else None,
                "tp_order_id": v_tp.get("algoId") if v_tp else None,
                "sl_price": sl_str,
                "tp_price": tp_str,
                "sl_active": v_sl is not None,
                "tp_active": v_tp is not None,
                "is_protected": is_protected
            }
        except Exception as e:
            print(f"Error en ensure_position_protection para {symbol}: {e}")
            self.last_protection_error[symbol] = str(e)
            if pos and self._is_open_position(pos):
                metadata = self.state.setdefault("positions", {}).setdefault(symbol, {})
                fresh = None
                fresh_read_error = None
                try:
                    fresh = self._revalidate_position_protection(symbol, metadata)
                except Exception as verify_exc:
                    fresh_read_error = verify_exc
                if fresh is not None and fresh.get("position") is None:
                    return {"symbol": symbol, "is_protected": False,
                            "position_closed_during_verification": True,
                            "reason": "BINANCE_POSITION_FLAT_DURING_PROTECTION_CHECK"}
                if fresh is not None and fresh.get("is_protected"):
                    self.save_state()
                    return {"symbol": symbol, "is_protected": True,
                            "sl_active": True, "tp_active": fresh.get("tp") is not None,
                            "sl_order_id": fresh.get("sl", {}).get("algoId"),
                            "tp_order_id": fresh.get("tp", {}).get("algoId") if fresh.get("tp") else None,
                            "verification_source": "fresh_position_and_openAlgoOrders_after_read_error"}
                if fresh is None and str(metadata.get("sl_algo_id") or "") in self._owned_algo_ids(metadata):
                                                                                     
                                                                                        
                    return {"symbol": symbol, "is_protected": None,
                            "read_retry_pending": True, "reason": "PROTECTION_READ_UNAVAILABLE",
                            "last_owned_sl_algo_id": metadata.get("sl_algo_id"),
                            "read_error": str(fresh_read_error or e)[:500]}
                failure_reason = ("LIVE_PROTECTION_NOT_CONFIRMED" if fresh is not None
                                  else "PROTECTION_STATE_UNVERIFIABLE")
                try:
                    self._trip_protection_circuit_breaker(
                        symbol, failure_reason, metadata,
                        {"error_type": type(e).__name__, "error": str(e)[:500],
                         "revalidation_error": str(fresh_read_error)[:500] if fresh_read_error else None,
                         "position_reconfirmed_open": bool(fresh and fresh.get("position")),
                         "active_algo_orders": fresh.get("algos") if fresh else None,
                         "stop_confirmed": bool(fresh and fresh.get("sl")),
                         "take_profit_confirmed": bool(fresh and fresh.get("tp"))}
                    )
                except Exception:
                    pass
                try:
                    close_result = self._close_position_unlocked(symbol)
                except Exception as close_exc:
                    close_result = {"status": "close_pending", "error_type": type(close_exc).__name__}
                return {"symbol": symbol, "sl_active": False, "is_protected": False,
                        "classification": "PROTECTION_FAILURE",
                        "emergency_close": close_result, "reason": type(e).__name__}
            return None

    def execute_tp1(self, symbol, pos=None):
        """Execute the configured market partial and atomically protect the runner at BE."""
        with self.execution_lock():
            self.state = self.load_state()
            return self._execute_tp1_unlocked(symbol, pos)

    def _execute_tp1_unlocked(self, symbol, pos=None):
        metadata = self.state.get("positions", {}).get(symbol, {})
        if metadata.get("tp1_executed"):
            return {"status": "already_executed", "symbol": symbol}
        if not pos:
            positions = self.client.get_positions()
            hedge = self.client.get_position_mode()
            if not isinstance(positions, list) or hedge is None:
                return {"status": "tp1_pending", "symbol": symbol, "reason": "position_reconciliation_unavailable"}
            pos = next((item for item in positions if item.get("symbol") == symbol
                        and self._position_matches_metadata(item, metadata, hedge)
                        and self._is_open_position(item)), None)
        if not pos:
            return {"status": "already_closed", "symbol": symbol}

        protection = self._ensure_position_protection_unlocked(symbol, pos)
        if not protection or not protection.get("is_protected"):
            return {"status": "unprotected", "symbol": symbol, "protection": protection}
        metadata = self.state.get("positions", {}).get(symbol, {})
        if metadata.get("tp1_executed"):
            return {"status": "already_executed", "symbol": symbol}
        amount = float(pos.get("positionAmt", 0) or 0)
        if amount == 0:
            return {"status": "already_closed", "symbol": symbol}
        side = "LONG" if amount > 0 else "SHORT"
        close_side = "SELL" if amount > 0 else "BUY"
        hedge = self.client.get_position_mode()
        if hedge is None:
            return {"status": "tp1_pending", "symbol": symbol, "reason": "position_mode_unavailable"}
        position_side = side if hedge else "BOTH"
        initial_qty = float(metadata.get("initial_qty", abs(amount)) or abs(amount))
        metadata.setdefault("initial_qty", initial_qty)
                                                                                                 
        desired_partial = float(self.split_position_quantities(symbol, initial_qty)[0])
        step_size = float(self.get_symbol_filters(symbol)["stepSize"])
        accounted_fill = float(metadata.get("tp1_filled_qty", 0.0) or 0.0)
        client_order_id = metadata.get("tp1_client_order_id")
        result = None
        if client_order_id:
            result = self.client.get_order(symbol, client_order_id)
            if not isinstance(result, dict) or result.get("orderId") is None:
                try:
                    not_found = int(result.get("code", 0) or 0) == -2013 if isinstance(result, dict) else False
                except (TypeError, ValueError):
                    not_found = False
                if not_found:
                    metadata.pop("tp1_client_order_id", None)
                    metadata.pop("tp1_requested_qty", None)
                    metadata["status"] = "OPEN"
                    self.save_state()
                    client_order_id = None
                    result = None
                else:
                    return {"status": "tp1_pending", "symbol": symbol, "client_order_id": client_order_id}
            if result is not None:
                order_status = str(result.get("status", "")).upper()
                if order_status in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}:
                    return {"status": "tp1_pending", "symbol": symbol, "order": result}
                if order_status not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                    return {"status": "tp1_pending", "symbol": symbol, "order": result}
                filled = float(result.get("executedQty", 0) or 0)
                metadata["tp1_filled_qty"] = accounted_fill + filled
                accounted_fill += filled
                order_id = str(result.get("orderId", ""))
                order_ids = metadata.setdefault("order_ids", [])
                if order_id and order_id not in {str(value) for value in order_ids}:
                    order_ids.append(order_id)
                metadata.pop("tp1_client_order_id", None)
                metadata.pop("tp1_requested_qty", None)
                self.save_state()
                if order_status in {"REJECTED", "CANCELED", "EXPIRED"} and filled <= 0:
                    return {"status": "tp1_retryable", "symbol": symbol, "order": result}
                result = None

        outstanding = max(0.0, desired_partial - accounted_fill)
        if outstanding > step_size / 2.0:
            partial_qty = self.format_quantity(symbol, outstanding)
            filters = self.get_symbol_filters(symbol)
            mark_price = float(self.client.get_mark_price(symbol) or 0)
            if not math.isfinite(mark_price) or mark_price <= 0:
                return {"status": "tp1_pending", "symbol": symbol, "reason": "mark_price_unavailable"}
            if filters.get("minNotional") is not None and float(partial_qty) * mark_price < float(filters["minNotional"]):
                return {"status": "tp1_pending", "symbol": symbol, "reason": "tp1_below_min_notional"}
            client_order_id = f"agy_tp1_{self.agent_id[2:10]}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"
            metadata["status"] = "TP1_PENDING"
            metadata["tp1_client_order_id"] = client_order_id
            metadata["tp1_requested_qty"] = partial_qty
            self.save_state()
            result = self.client.place_order(
                symbol=symbol, side=close_side, order_type="MARKET", quantity=partial_qty,
                reduce_only=True, position_side=position_side, client_order_id=client_order_id
            )
            if not isinstance(result, dict) or result.get("error") or result.get("orderId") is None:
                outcome = result.get("outcome") if isinstance(result, dict) else "ambiguous_after_submit"
                if outcome in {"pre_submit_error", "explicit_rejection"}:
                    metadata.pop("tp1_client_order_id", None)
                    metadata.pop("tp1_requested_qty", None)
                    metadata["status"] = "OPEN"
                    self.save_state()
                    return {"status": "tp1_retryable" if outcome == "pre_submit_error" else "tp1_rejected",
                            "symbol": symbol, "order": result}
                result = None
                for attempt in range(int(CONFIG["operations"]["entry_order_lookup_attempts"])):
                    if attempt:
                        time.sleep(float(CONFIG["operations"]["entry_order_lookup_interval_seconds"]))
                    candidate = self.client.get_order(symbol, client_order_id)
                    if isinstance(candidate, dict) and candidate.get("orderId") is not None:
                        result = candidate
                        break
                if result is None:
                    metadata["status"] = "RECONCILING"
                    self.save_state()
                    return {"status": "tp1_pending", "symbol": symbol, "client_order_id": client_order_id}
            order_status = str(result.get("status", "")).upper()
            if order_status in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}:
                metadata["status"] = "TP1_PENDING"
                self.save_state()
                return {"status": "tp1_pending", "symbol": symbol, "order": result}
            if order_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                filled = float(result.get("executedQty", 0) or 0)
                order_id = str(result.get("orderId", ""))
                if filled > 0:
                    metadata["tp1_filled_qty"] = accounted_fill + filled
                    order_ids = metadata.setdefault("order_ids", [])
                    if order_id and order_id not in {str(value) for value in order_ids}:
                        order_ids.append(order_id)
                metadata.pop("tp1_client_order_id", None)
                metadata.pop("tp1_requested_qty", None)
                metadata["status"] = "TP1_PENDING" if filled > 0 else "OPEN"
                self.save_state()
                return {"status": "tp1_pending" if filled > 0 else "tp1_retryable",
                        "symbol": symbol, "order": result}
            if order_status != "FILLED":
                metadata["status"] = "RECONCILING"
                self.save_state()
                return {"status": "tp1_pending", "symbol": symbol, "order": result}
            filled = float(result.get("executedQty", 0) or 0)
            metadata["tp1_filled_qty"] = accounted_fill + filled
            order_id = str(result.get("orderId", ""))
            order_ids = metadata.setdefault("order_ids", [])
            if order_id and order_id not in {str(value) for value in order_ids}:
                order_ids.append(order_id)
            metadata.pop("tp1_client_order_id", None)
            metadata.pop("tp1_requested_qty", None)
            self.save_state()

        time.sleep(float(CONFIG["operations"]["settle_delay_seconds"]))
        positions_after = self.client.get_positions()
        if not isinstance(positions_after, list):
            self.save_state()
            return {"status": "tp1_pending", "symbol": symbol, "order": result}
        position_after = next((item for item in positions_after if item.get("symbol") == symbol
                              and str(item.get("positionSide", "BOTH")).upper() == position_side), None)
        remaining = abs(float(position_after.get("positionAmt", 0) or 0)) if position_after else 0.0
        closed_qty = max(0.0, initial_qty - remaining)
        if closed_qty < desired_partial - step_size / 2.0:
            metadata["status"] = "TP1_PENDING"
            self.save_state()
            return {"status": "tp1_pending", "symbol": symbol, "remaining_qty": remaining,
                    "closed_qty": closed_qty, "required_qty": desired_partial}
        if remaining <= 0:
            metadata["tp1_executed"] = True
            self.save_state()
            return {"status": "already_closed", "symbol": symbol, "order": result}

        entry_px = float(position_after.get("entryPrice", metadata.get("entry_price", 0)) or 0)
        be_buffer = float(CONFIG["exits"]["break_even_buffer_pct"]) / 100.0
        be_price = entry_px * (1.0 + be_buffer) if side == "LONG" else entry_px * (1.0 - be_buffer)
        be_str = self.format_price(symbol, be_price, round_up=side == "LONG")
        open_algos = self.client.get_open_algo_orders(symbol)
        old_owned = self._owned_algo_ids(metadata)
        old_stop = next((item for item in open_algos
                         if str(item.get("algoId")) == str(metadata.get("sl_algo_id"))
                         and str(item.get("algoId")) in old_owned
                         and item.get("orderType") == "STOP_MARKET"), None)
        old_stop_id = old_stop.get("algoId") if old_stop else None
        if old_owned:
            self._cancel_owned_algos(
                symbol, metadata,
                exclude_algo_ids={old_stop_id} if old_stop_id is not None else set()
            )
        if old_stop:
            confirmed_be, be_result = self._replace_owned_stop(
                symbol, close_side, position_side, be_str, metadata
            )
        else:
            confirmed_be, be_result = self._submit_algo_and_verify(
                symbol=symbol, order_type="STOP_MARKET", side=close_side,
                position_side=position_side, trigger_price=be_str,
                metadata=metadata, metadata_key="sl_algo_id"
            )
        if not confirmed_be:
            if isinstance(be_result, dict) and be_result.get("outcome") == "ambiguous_after_submit":
                metadata.update({"tp1_executed": True})
                self._trip_protection_circuit_breaker(
                    symbol, "RUNNER_STOP_SUBMIT_AMBIGUOUS", metadata,
                    {"stop_result": be_result}
                )
                close_result = self._close_position_unlocked(symbol)
                return {"status": "emergency_closed" if close_result and close_result.get("status") in {"closed", "already_closed"} else "critical_unprotected_failure",
                        "symbol": symbol, "classification": "PROTECTION_FAILURE",
                        "reason": "runner stop submission could not be confirmed", "close": close_result}
            if old_stop:
                restored, _ = self._submit_algo_and_verify(
                    symbol=symbol, order_type="STOP_MARKET", side=close_side,
                    position_side=position_side, trigger_price=old_stop.get("triggerPrice"),
                    metadata=metadata, metadata_key="sl_algo_id"
                )
                if restored:
                    metadata["status"] = "TP1_PENDING"
                    self.save_state()
                    return {"status": "tp1_pending", "symbol": symbol, "reason": "runner_stop_rejected_old_stop_restored"}
            metadata.update({"tp1_executed": True})
            self._trip_protection_circuit_breaker(
                symbol, "RUNNER_BREAK_EVEN_STOP_NOT_CONFIRMED", metadata,
                {"stop_result": be_result}
            )
            close_result = self._close_position_unlocked(symbol)
            return {"status": "emergency_closed" if close_result and close_result.get("status") in {"closed", "already_closed"} else "critical_unprotected_failure",
                    "symbol": symbol, "classification": "PROTECTION_FAILURE",
                    "reason": "no se pudo confirmar stop Break-Even", "close": close_result}

                                                                                    
        metadata.update({
            "initial_qty": abs(amount), "runner_qty": remaining,
            "tp1_executed": True, "be_active": True, "be_price": be_str,
            "current_sl_price": float(be_str), "sl_algo_id": str(confirmed_be.get("algoId")),
            "status": "RUNNER_ACTIVE", "risk_usdt": 0.0,
            "tp1_order_id": str(metadata.get("order_ids", [""])[-1]) if metadata.get("order_ids") else None
        })
        self.save_state()
        return {"status": "runner_active", "symbol": symbol, "partial_order_id": metadata.get("tp1_order_id"),
                "be_algo_id": str(confirmed_be.get("algoId")), "be_price": be_str,
                "remaining_qty": remaining, "risk_usdt": 0.0}

    def close_position(self, symbol):
        with self.execution_lock():
            self.state = self.load_state()
            return self._close_position_unlocked(symbol)

    def _close_position_unlocked(self, symbol):
        """Cierra una posición del agente a mercado y cancela sus Algo Orders en Binance."""
        positions = self.client.get_positions()
        if not isinstance(positions, list):
            return None
        is_hedge = self.client.get_position_mode(force_refresh=True)
        if is_hedge is None:
            raise RuntimeError("No se pudo confirmar el modo de posición; cierre bloqueado.")
        metadata = self.state.get("positions", {}).get(symbol, {})
        target_pos = None
        for p in positions:
            if p.get("symbol") == symbol and self._position_matches_metadata(p, metadata, is_hedge):
                target_pos = p
                break
                
        if not target_pos:
            return None
            
        amt = float(target_pos["positionAmt"])
        thesis_side = str(metadata.get("side", "")).upper()
        pos_side = str(metadata.get("position_side") or (("LONG" if amt > 0 else "SHORT") if is_hedge else "BOTH"))
        is_long = thesis_side == "LONG" if thesis_side in ("LONG", "SHORT") else amt > 0
        close_side = "SELL" if is_long else "BUY"
        if amt == 0:
            self._cancel_owned_algos(symbol, metadata)
            self._mark_symbol_closed(symbol)
            return {"status": "already_closed"}
        side = close_side
        qty_str = self.format_quantity(symbol, abs(amt))
        previous_id = metadata.get("close_client_order_id")
        res = None
        if previous_id:
            previous = self.client.get_order(symbol, previous_id)
            if not isinstance(previous, dict) or previous.get("orderId") is None:
                try:
                    not_found = int(previous.get("code", 0) or 0) == -2013 if isinstance(previous, dict) else False
                except (TypeError, ValueError):
                    not_found = False
                if not_found:
                    metadata.pop("close_client_order_id", None)
                    metadata["status"] = "UNPROTECTED"
                    self.save_state()
                    previous_id = None
                else:
                    return {"status": "close_pending", "symbol": symbol, "client_order_id": previous_id}
            if previous_id:
                previous_status = str(previous.get("status", "")).upper()
                previous_filled = float(previous.get("executedQty", 0) or 0)
                if previous_status not in {"FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                    return {"status": "close_pending", "symbol": symbol, "order": previous}
                if previous_status in {"CANCELED", "REJECTED", "EXPIRED"} and previous_filled <= 0:
                    metadata.pop("close_client_order_id", None)
                else:
                    res = previous
        else:
            res = None

        if res is None:
            client_order_id = f"agy_close_{self.agent_id[:8]}_{int(time.time() * 1000)}"
            metadata["status"] = "CLOSE_PENDING"
            metadata["close_client_order_id"] = client_order_id
            self.save_state()
            res = self.client.place_order(
                symbol=symbol, side=side, order_type="MARKET", quantity=qty_str,
                reduce_only=True, position_side=pos_side, client_order_id=client_order_id
            )
            if not isinstance(res, dict) or res.get("error") or res.get("orderId") is None:
                outcome = res.get("outcome") if isinstance(res, dict) else "ambiguous_after_submit"
                if outcome in {"pre_submit_error", "explicit_rejection"}:
                    metadata.pop("close_client_order_id", None)
                    metadata["status"] = "UNPROTECTED"
                    self.save_state()
                    return {"status": "close_retryable" if outcome == "pre_submit_error" else "close_rejected",
                            "symbol": symbol, "order": res}
                attempts = int(CONFIG["operations"]["entry_order_lookup_attempts"])
                for attempt in range(attempts):
                    if attempt:
                        time.sleep(float(CONFIG["operations"]["entry_order_lookup_interval_seconds"]))
                    candidate = self.client.get_order(symbol, client_order_id)
                    if isinstance(candidate, dict) and candidate.get("orderId") is not None:
                        res = candidate
                        break
                else:
                    metadata["status"] = "RECONCILING"
                    self.save_state()
                    return {"status": "close_pending", "symbol": symbol, "client_order_id": client_order_id}

        if float(res.get("executedQty", 0) or 0) <= 0:
            return {"status": "close_pending", "symbol": symbol, "order": res}
        if str(res.get("status", "")).upper() in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}:
            self.save_state()
            return {"status": "close_pending", "symbol": symbol, "order": res}
        order_ids = metadata.setdefault("order_ids", [])
        order_id = str(res.get("orderId", ""))
        if order_id and order_id not in order_ids:
            order_ids.append(order_id)
        updated_positions = self.client.get_positions()
        if updated_positions is None:
            self.save_state()
            return {"status": "close_pending", "symbol": symbol, "order": res}
        updated_position = next((p for p in updated_positions if p.get("symbol") == symbol and self._position_matches_metadata(p, metadata, is_hedge)), None)
        if updated_position and float(updated_position.get("positionAmt", 0)) != 0:
            metadata.pop("close_client_order_id", None)
            self.save_state()
            return {"status": "close_pending", "symbol": symbol, "order": res}
        self._cancel_owned_algos(symbol, metadata)
        self._mark_symbol_closed(symbol)
        return {"status": "closed", "symbol": symbol, "order": res}
