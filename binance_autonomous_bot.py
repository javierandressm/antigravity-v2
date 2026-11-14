"""
ANTIGRAVITY V2 · AUTONOMOUS TRADING BOT DAEMON (BINANCE FUTURES USDT-M)
=======================================================================
Motor de ejecución autónoma en vivo para Binance Futures:
- Monitoreo continuo de señales de consenso híbrido (HL Whales + Binance Lead Traders)
- Ejecución automática de órdenes a mercado al detectar confirmación híbrida
- Reglas cuantitativas nativas parametrizadas: ATR 4H y sizing porcentual sobre equity
- Apalancamiento escalonado por categoría del activo y confirmaciones
- Re-entradas con cooldown de 1h
- Reconciliación continua y registro persistente en antigravity_agent_state.json
"""


import os
import sys
import time
import json
import requests
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parent

from binance_futures_client import BinanceFuturesClient
from binance_execution_bridge import BinanceExecutionBridge, CONFIG
from antigravity_policy import evaluate_signal_policy
import antigravity_flip
import antigravity_rebote
import antigravity_cierre_manual
import antigravity_arranque
import antigravity_freno_shorts
import antigravity_alarma_proteccion
from antigravity_accounting import reconciliation_start_ms
from live_whale_scanner import CONSENSUS_CACHE_TTL_SECONDS, get_latest_consensus, start_scanner_daemon
from antigravity_runtime import DAEMON_HEALTH_PATH, SUPERVISOR_STATUS_PATH, atomic_write_json, pid_is_alive, read_json

LOG_FILE = ROOT / "antigravity_bot_events.log"
_last_signal_error_log_at = 0.0
_last_reconciliation_error_log_at = 0.0
_last_cooling_off_log_at = 0.0
_bot_lock_handle = None
_last_valid_signal_at = None


def _publish_daemon_health_raw(state, *, reason="", accepting_entries=False,
                          startup_reconciliation_complete=False,
                          active_position_count=None, pending_reconciliations=None,
                          last_reconciliation_at=None, entries_armed=None):
    now = time.time()
    atomic_write_json(DAEMON_HEALTH_PATH, {
        "pid": os.getpid(),
        "state": str(state),
        "health": "HEALTHY" if state == "HEALTHY" else str(state),
        "reason": str(reason or ""),
        "heartbeat_at": now,
        "heartbeat_at_utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "accepting_entries": bool(accepting_entries),
        "entries_armed": entries_armed,
        "startup_reconciliation_complete": bool(startup_reconciliation_complete),
        "active_position_count": active_position_count,
        "pending_reconciliations": pending_reconciliations or [],
        "last_reconciliation_at": last_reconciliation_at,
        "last_valid_signal_at": _last_valid_signal_at,
        "network": "BINANCE_TESTNET" if CONFIG["network"]["testnet"] else "BINANCE_MAINNET",
    })

_ultimo_aviso_salud = 0.0


def publish_daemon_health(*args, **kwargs):
    """03-11-2026: si Windows bloquea el archivo de salud más de lo que aguantan los reintentos, se avisa y el bot sigue (ese día
    se apagó 4 veces así y el supervisor quedó en FAIL_CLOSED). Si el bloqueo dura, el supervisor ve el latido viejo y lo reinicia."""
    global _ultimo_aviso_salud
    try:
        _publish_daemon_health_raw(*args, **kwargs)
        return True
    except OSError as exc:
        if time.time() - _ultimo_aviso_salud >= 300:
            log(f"⚠️ [SALUD] No se pudo escribir antigravity_daemon_health.json ({type(exc).__name__}: {exc}); el bot sigue.")
            _ultimo_aviso_salud = time.time()
        return False


def log(msg):
    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now_str}] {msg}"
    try:
                                        
        print(line.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8"), flush=True)
    except Exception:
        try:
            print(line.encode("ascii", "replace").decode("ascii"), flush=True)
        except Exception:
            pass
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def acquire_bot_lock():
    """Prevents two daemons from trading the same account at the same time."""
    global _bot_lock_handle
    lock_path = ROOT / ".antigravity_bot.lock"
    handle = lock_path.open("a+b")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, ImportError):
        handle.close()
        return False
    _bot_lock_handle = handle
    return True

def fetch_onchain_signals():
    """Consulta señales validadas; ante error devuelve lista vacía y bloquea nuevas entradas."""
    global _last_signal_error_log_at, _last_valid_signal_at
    try:
        start_scanner_daemon()
        payload = get_latest_consensus()
        if not isinstance(payload, dict) or payload.get("status") != "ok" or not isinstance(payload.get("signals"), list):
            raise ValueError("respuesta del motor de consenso incompleta o degradada")
        snapshot_age = time.time() - float(payload.get("timestamp", 0))
        if not 0 <= snapshot_age < float(CONSENSUS_CACHE_TTL_SECONDS):
            raise ValueError("snapshot de consenso vencido o con timestamp inválido")
        _last_valid_signal_at = float(payload.get("timestamp", 0))
        return payload["signals"]
    except Exception as exc:
        now = time.monotonic()
        if now - _last_signal_error_log_at >= float(CONFIG["operations"]["signal_error_log_interval_seconds"]):
            log(f"Señales no disponibles; nuevas entradas bloqueadas hasta recuperar el consenso: {exc}")
            _last_signal_error_log_at = now
        return []

def ordenar_por_ballenas(signals):
    """22-10-2026 (usuario; research 11_suerte): si llegan varias señales en la misma vuelta, primero la que tiene más
    ballenas (los mismos votos que usa la política); a igualdad se mantiene el orden del consenso."""
    def votos(sig):
        try:
            return int(sig.get("hl_votes", sig.get("total_votes", 0)) or 0)
        except (TypeError, ValueError, AttributeError):
            return 0
    return sorted(signals, key=lambda sig: -votos(sig))
def main():
    global _last_reconciliation_error_log_at, _last_cooling_off_log_at
    if not acquire_bot_lock():
        log("Ya existe otro daemon Antigravity activo; esta instancia no iniciará.")
        return 73
    publish_daemon_health("STARTING", reason="VALIDATING_BINANCE_AND_LOCAL_STATE")
    retry_seconds = float(CONFIG["operations"]["startup_retry_seconds"])
    startup_options = CONFIG.get("runtime_supervisor", {})
    startup_attempts = max(1, int(startup_options.get("startup_attempts_per_process", 1)))
    startup_ready = False
    last_startup_error = ""
    for startup_attempt in range(startup_attempts):
        client = None
        try:
            client = BinanceFuturesClient(testnet=CONFIG["network"]["testnet"])
            bridge = BinanceExecutionBridge(client)
            account = client.get_account_info()
            if not isinstance(account, dict) or account.get("canTrade") is not True:
                raise RuntimeError("Binance no confirma acceso Futures habilitado con estas credenciales.")
            if not client.testnet:
                key_permissions = client.get_api_key_permissions()
                if not isinstance(key_permissions, dict) or key_permissions.get("enableFutures") is not True:
                    raise RuntimeError("La API key no confirma permiso Futures habilitado en Binance.")
            wallet = bridge.get_wallet_summary(account)
            if not wallet or float(wallet.get("total_balance", 0)) <= 0:
                raise RuntimeError("Binance no confirma equity Futures positivo.")
            account_positions = client.get_positions()
            if not isinstance(account_positions, list):
                raise RuntimeError("Binance no permite reconciliar las posiciones actuales.")
            untracked_positions = bridge.get_untracked_positions(account_positions)
            if untracked_positions is None:
                raise RuntimeError("Binance no permite confirmar el modo de posiciones; conciliación bloqueada.")
            if untracked_positions:
                untracked_symbols = sorted({f"{p.get('symbol')}:{p.get('positionSide', 'BOTH')}" for p in untracked_positions})
                raise RuntimeError(
                    "Posiciones Mainnet sin estado Antigravity: "
                    + ", ".join(untracked_symbols)
                    + ". Se requiere reconciliar antes de permitir entradas."
                )
            live_positions = [p for p in account_positions if bridge._is_open_position(p)]
            if len(live_positions) > int(CONFIG["operations"]["max_concurrent_positions"]):
                raise RuntimeError("La cuenta supera el máximo configurado de posiciones; daemon bloqueado.")
                                                                                
                                                                                    
            standard_orders = client.get_open_orders()
            if not isinstance(standard_orders, list):
                raise RuntimeError("Binance no confirmó el listado de órdenes estándar abiertas.")
            if standard_orders:
                raise RuntimeError("Hay órdenes estándar abiertas en la cuenta; reconcílialas antes de iniciar el daemon.")
            open_algos = client.get_open_algo_orders()
            if not isinstance(open_algos, list):
                raise RuntimeError("Binance no confirmó el listado de Algo Orders abiertas.")
            tracked_symbols = set(bridge.state.get("active_symbols", []))
            hedge_mode = client.get_position_mode()
            if hedge_mode is None:
                raise RuntimeError("No se pudo confirmar el modo de posición al validar órdenes condicionales.")
            for order in open_algos:
                symbol = order.get("symbol")
                metadata = bridge.state.get("positions", {}).get(symbol, {})
                thesis_side = str(metadata.get("side", "")).upper()
                expected_position_side = str(metadata.get("position_side") or ((thesis_side if hedge_mode else "BOTH")))
                expected_close_side = "SELL" if thesis_side == "LONG" else "BUY" if thesis_side == "SHORT" else None
                if (symbol not in tracked_symbols or not expected_close_side
                        or order.get("positionSide", "BOTH") != expected_position_side
                        or order.get("side") != expected_close_side):
                    raise RuntimeError("Hay órdenes condicionales sin correspondencia exacta con el estado AG; reconcílialas antes de iniciar el daemon.")

            if not bridge.state_file.exists():
                                                                                    
                                                                                   
                if live_positions or open_algos:
                    raise RuntimeError("No existe estado Mainnet y la cuenta tiene posiciones u órdenes abiertas; no se inicializará estado vacío.")
                bridge.save_state()
            startup_ready = True
            break
        except Exception as e:
            last_startup_error = f"{type(e).__name__}: {e}"
            if client:
                client.session.close()
            remaining_attempts = startup_attempts - startup_attempt - 1
            retry_text = f"Reintentando una vez en {retry_seconds:g}s." if remaining_attempts else "El supervisor decidirá si procede un reinicio limitado."
            log(f"[EN ESPERA] Binance Futures/estado Mainnet aún no está listo: {e}. {retry_text}")
            publish_daemon_health("STARTUP_BLOCKED", reason=last_startup_error)
            if remaining_attempts:
                time.sleep(retry_seconds)

    if not startup_ready:
        publish_daemon_health("STARTUP_FAILED", reason=last_startup_error)
        return 2

    net_label = "BINANCE_TESTNET" if client.testnet else "BINANCE_MAINNET (CUENTA REAL)"
    endpoint_url = client.base_url
    log("==================================================================")
    log("INICIANDO BOT AUTÓNOMO ANTIGRAVITY V2 (BINANCE FUTURES USDT-M)")
    log("Scope: ANTIGRAVITY_AGENT (DEMO_ID_REMOVED)")
    log(f"Network: {net_label} [{endpoint_url}] | Consenso On-Chain 40 Ballenas: ACTIVO")
    log("Execution mode: ARMED (entradas sujetas a reconciliación inicial)" if bridge.entries_armed() else "Execution mode: DISARMED")
    if bridge.state.get("protection_circuit_breaker", {}).get("active"):
        log("Global protection circuit breaker: ACTIVE")
    log("==================================================================")

    log(f"Conexión exitosa ({net_label}). Equity: ${wallet['total_balance']:.2f} USDT | Margen libre: ${wallet['available_balance']:.2f} USDT")

    cooldowns = {symbol: float(stamp) for symbol, stamp in bridge.state.get("symbol_cooldowns", {}).items()}
    max_concurrent_positions = int(CONFIG["operations"]["max_concurrent_positions"])
    base_capacity = int(CONFIG["portfolio_exposure"]["base_capacity"])
    cooldown_seconds = float(CONFIG["operations"]["symbol_cooldown_minutes"]) * 60.0
    espera_manual_h = antigravity_cierre_manual.espera_config(CONFIG)                                          
    _aviso_manual = {}
    alarma_proteccion = antigravity_alarma_proteccion.AlarmaProteccion()               
                                                                                                  
    filtro_arranque = antigravity_arranque.FiltroArranque(_last_valid_signal_at, antigravity_arranque.activo_config(CONFIG))
    network_key = "testnet" if client.testnet else "mainnet"
    baselines = bridge.state.setdefault("equity_baselines", {})
    if network_key not in baselines:
        baselines[network_key] = float(wallet["total_balance"])
        bridge.save_state()

    iteration = 0
    previous_symbols = set()
    startup_reconciliation_complete = False
    startup_reconciliation_started_at = time.time()
    startup_ambiguity_disarmed = False
    supervisor_unavailable_since = None
    last_reconciliation_at = None
    last_capacity_observation_at = 0.0
    try:
        from ag_portfolio_observer import start_observer
        start_observer()
        log("AG Portfolio Exposure: observador activo; capacidad productiva 3, slots #4/#5 deshabilitados.")
    except Exception as observer_exc:
        log(f"AG Portfolio Exposure: instrumentación no disponible ({observer_exc}); ejecución normal continúa.")
    while True:
        try:
            iteration += 1
            now = time.time()

                                                                            
            agent_positions = bridge.get_agent_positions()
            if agent_positions is None:
                publish_daemon_health("READ_RETRY_PENDING", reason="BINANCE_POSITION_OR_MODE_READ_FAILED",
                                      accepting_entries=False,
                                      startup_reconciliation_complete=startup_reconciliation_complete,
                                      pending_reconciliations=list(bridge.state.get("closed_positions", {})))
                if now - _last_reconciliation_error_log_at >= float(CONFIG["operations"]["signal_error_log_interval_seconds"]):
                    log("⚠️ [RECONCILIATION] Lecturas Binance no disponibles. Preservando órdenes y bloqueando nuevas entradas...")
                    _last_reconciliation_error_log_at = now
                time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                continue

            active_symbols = set(p["symbol"] for p in agent_positions)
            
                                                                                                  
                                                                                   
            for sym, closed_event in list(bridge.state.setdefault("closed_positions", {}).items()):
                raw_check = bridge.client.get_positions()
                if raw_check is None:
                    continue
                position_mode = bridge.client.get_position_mode()
                if position_mode is None:
                    continue
                pos_match = next((p for p in raw_check if p.get("symbol") == sym and bridge._position_matches_metadata(p, closed_event.get("metadata", {}), position_mode)), None)
                if pos_match and float(pos_match.get("positionAmt", 0)) != 0:
                    continue

                pos_meta = closed_event.get("metadata", {})
                cooldown_at = float(closed_event.setdefault("cooldown_at", now))
                cooldowns[sym] = cooldown_at
                bridge.state.setdefault("symbol_cooldowns", {})[sym] = cooldown_at
                try:
                    close_pos_side = pos_meta.get("position_side", "BOTH")
                    thesis_side = pos_meta.get("side")
                    close_order_side = "SELL" if thesis_side == "LONG" else "BUY" if thesis_side == "SHORT" else None
                    if close_order_side:
                        bridge._cancel_owned_algos(sym, pos_meta)
                    remaining_algos = bridge.client.get_open_algo_orders(sym)
                    if not isinstance(remaining_algos, list):
                        raise RuntimeError("Binance no confirmó Algo Orders residuales de la posición cerrada.")
                    remaining_for_leg = [order for order in remaining_algos
                                         if order.get("symbol") == sym
                                         and order.get("side") == close_order_side
                                         and str(order.get("positionSide", "BOTH")).upper() == str(close_pos_side).upper()
                                         and str(order.get("algoStatus", "")).upper() in {"NEW", "TRIGGERING"}]
                    if remaining_for_leg:
                        raise RuntimeError("Persisten Algo Orders para la pierna cerrada; ownership incierto o cancelación sin confirmar.")
                except Exception as exc:
                    log(f"No se pudieron cancelar las órdenes condicionales de la pierna cerrada {sym}: {exc}")
                    closed_event["reconciliation_status"] = "RECONCILIATION_REQUIRED"
                    closed_event["reconciliation_error"] = str(exc)
                    bridge.save_state()
                    continue

                try:
                    entry_order = bridge.client._request("GET", "/fapi/v1/order", {
                        "symbol": sym, "orderId": str(pos_meta.get("entry_order_id", ""))})
                    opened_at_ms = reconciliation_start_ms(sym, pos_meta, entry_order)
                except Exception as exc:
                    closed_event["reconciliation_status"] = "RECONCILIATION_REQUIRED"
                    closed_event["reconciliation_error"] = f"ENTRY_FILL_HISTORY_UNCONFIRMED: {exc}"
                    bridge.save_state()
                    continue
                end_at = float(closed_event.get("detected_at", now))
                end_at_ms = max(opened_at_ms, int(end_at * 1000))
                trade_rows = bridge.client.get_user_trades(sym, opened_at_ms, end_at_ms)
                algo_rows = bridge.client.get_all_algo_orders(sym, opened_at_ms, end_at_ms)
                income_rows = bridge.client._request(
                    "GET", "/fapi/v1/income",
                    {"symbol": sym, "startTime": opened_at_ms, "endTime": end_at_ms,
                     "limit": int(CONFIG["operations"]["income_history_limit"])}
                )
                if not isinstance(trade_rows, list) or not isinstance(algo_rows, list) or not isinstance(income_rows, list):
                    closed_event["reconciliation_status"] = "RECONCILIATION_REQUIRED"
                    closed_event["reconciliation_error"] = "BINANCE_CLOSE_HISTORY_UNAVAILABLE"
                    bridge.save_state()
                    continue

                tracked_order_ids = {str(value) for value in pos_meta.get("order_ids", []) if value}
                tracked_order_ids.update(str(pos_meta.get(key)) for key in ("entry_order_id", "tp1_order_id") if pos_meta.get(key))
                tracked_algo_ids = {str(value) for value in pos_meta.get("algo_ids", []) if value}
                tracked_algo_ids.update(str(pos_meta.get(key)) for key in ("sl_algo_id", "tp_algo_id") if pos_meta.get(key))
                for algo in algo_rows:
                    if str(algo.get("algoId", "")) in tracked_algo_ids and algo.get("actualOrderId") is not None:
                        tracked_order_ids.add(str(algo["actualOrderId"]))

                position_side = str(pos_meta.get("position_side", "BOTH")).upper()
                bounded_all_trades = [row for row in trade_rows
                                      if opened_at_ms <= int(row.get("time", 0)) <= end_at_ms]
                bounded_trades = [row for row in bounded_all_trades
                                  if str(row.get("positionSide", "BOTH")).upper() == position_side]
                matched_trades = [row for row in bounded_trades if str(row.get("orderId", "")) in tracked_order_ids]
                external_trades = [row for row in bounded_all_trades if str(row.get("orderId", "")) not in tracked_order_ids]
                entry_present = any(str(row.get("orderId")) == str(pos_meta.get("entry_order_id")) for row in matched_trades)
                buy_qty = sum(float(row.get("qty", 0)) for row in matched_trades if row.get("side") == "BUY")
                sell_qty = sum(float(row.get("qty", 0)) for row in matched_trades if row.get("side") == "SELL")
                complete = entry_present and buy_qty > 0 and abs(buy_qty - sell_qty) <= max(1e-10, buy_qty * 1e-9)
                if external_trades or not matched_trades or not complete:
                    closed_event["reconciliation_status"] = "RECONCILIATION_REQUIRED"
                    closed_event["reconciliation_error"] = (
                        "UNTRACKED_BINANCE_TRADES_IN_CLOSE_WINDOW" if external_trades
                        else "BINANCE_ENTRY_OR_BALANCED_CLOSE_FILLS_MISSING"
                    )
                    bridge.save_state()
                    log(f"⚠️ [RECONCILIACIÓN PENDIENTE] {sym}: Binance ya confirma la posición flat, pero la evidencia de fills no permite atribuir el cierre con seguridad; entradas globales bloqueadas.")
                    continue
                income_funding = sum(float(row.get("income", 0) or 0) for row in income_rows
                                     if opened_at_ms <= int(row.get("time", 0)) <= end_at_ms
                                     and row.get("incomeType") == "FUNDING_FEE")
                attribution_unknown = bool(external_trades or not matched_trades)
                closure_net_pnl = None
                closure_pnl_attribution = "unknown"
                closure_classification = None
                if bridge.is_operational_close(pos_meta):
                    net_pnl = None
                    if not attribution_unknown:
                        net_pnl = sum(float(row.get("realizedPnl", 0) or 0)
                                      - float(row.get("commission", 0) or 0) for row in matched_trades)
                        net_pnl += income_funding
                    closure_net_pnl = net_pnl
                    closure_pnl_attribution = "unknown" if attribution_unknown else "BINANCE_FUTURES_USER_TRADES"
                    closure_classification = "PROTECTION_FAILURE_SAFETY_CLOSE"
                    failure = next((item for item in reversed(bridge.state.get("protection_failures", []))
                                    if isinstance(item, dict) and item.get("symbol") == sym
                                    and item.get("entry_client_order_id") == pos_meta.get("entry_client_order_id")), None)
                    if failure is not None:
                        failure.update({
                            "status": "CLOSED",
                            "closed_at": dt.datetime.fromtimestamp(end_at, dt.timezone.utc).isoformat(),
                            "pnl_attribution": "unknown" if attribution_unknown else "BINANCE_FUTURES_USER_TRADES",
                        })
                        if net_pnl is not None:
                            failure["net_pnl_usdt"] = net_pnl
                    log(f"🧰 [PROTECTION_FAILURE] {sym}: cierre técnico registrado fuera de STOPPED_THESIS"
                        + (f"; PnL neto auditado {net_pnl:.4f} USDT." if net_pnl is not None else "; PnL no atribuible con seguridad."))
                elif attribution_unknown:
                    closed_at = end_at
                    closure_pnl_attribution = "ambiguous_external_or_missing_order_history"
                    closure_classification = "CLOSE_ATTRIBUTION_UNKNOWN"
                    bridge.set_stopped_thesis(sym, str(pos_meta.get("side", "")).upper(), {
                        "symbol": sym, "side": pos_meta.get("side"),
                        "stopped_at": closed_at,
                        "closed_at": dt.datetime.fromtimestamp(closed_at, dt.timezone.utc).isoformat(),
                        "supporters": list(pos_meta.get("supporters", [])),
                        "origin_unknown": True,
                        "pnl_attribution": "ambiguous_external_or_missing_order_history",
                        "source": "BINANCE_FUTURES_USER_TRADES"
                    })
                    log(f"⚠️ [STOPPED_THESIS CONSERVADORA] {sym}: no se pudo atribuir el PnL exclusivamente a las órdenes AG; se aplica bloqueo por 24h sin afirmar un PnL.")
                else:
                    net_pnl = sum(float(row.get("realizedPnl", 0) or 0)
                                  - float(row.get("commission", 0) or 0) for row in matched_trades)
                    net_pnl += income_funding
                    closure_net_pnl = net_pnl
                    closure_pnl_attribution = "BINANCE_FUTURES_USER_TRADES"
                    closed_at = max(int(row.get("time", 0)) for row in matched_trades) / 1000.0
                    if net_pnl < 0:
                        bridge.set_stopped_thesis(sym, str(pos_meta.get("side", "")).upper(), {
                            "symbol": sym,
                            "side": pos_meta.get("side"),
                            "stopped_at": closed_at,
                            "closed_at": dt.datetime.fromtimestamp(closed_at, dt.timezone.utc).isoformat(),
                            "supporters": list(pos_meta.get("supporters", [])),
                            "net_pnl_usdt": net_pnl,
                            "source": "BINANCE_FUTURES_USER_TRADES"
                        })
                        log(f"🛑 [STOPPED_THESIS REGISTRADA] {sym} cerró con PnL neto atribuible {net_pnl:.4f} USDT. Se bloqueará 24h salvo 2 wallets nuevas.")
                    else:
                        bridge.clear_stopped_thesis(sym, str(pos_meta.get("side", "")).upper())

                closure_record = bridge.archive_reconciled_closure(
                    symbol=sym,
                    closed_event=closed_event,
                    matched_trades=matched_trades,
                    algo_rows=algo_rows,
                    tracked_order_ids=tracked_order_ids,
                    tracked_algo_ids=tracked_algo_ids,
                    net_pnl_usdt=closure_net_pnl,
                    pnl_attribution=closure_pnl_attribution,
                    classification=closure_classification,
                )
                if closure_record.get("net_pnl_usdt") is None:
                    close_summary = "PnL pendiente de atribución"
                else:
                    close_summary = f"PnL neto {closure_record['net_pnl_usdt']:.4f} USDT"
                log(f"✅ [RECONCILIACIÓN RESUELTA] {sym}: {closure_record['classification']} | "
                    f"órdenes {len(closure_record['matched_order_ids'])} | "
                    f"Algo disparado {','.join(closure_record['triggered_algo_ids']) or 'ninguno'} | {close_summary}.")
                if closure_record.get("net_pnl_usdt") is None or closure_record.get("pnl_attribution") != "BINANCE_FUTURES_USER_TRADES":
                    closed_event["reconciliation_status"] = "RECONCILIATION_REQUIRED"
                    closed_event["reconciliation_error"] = "CLOSURE_PNL_ATTRIBUTION_INCOMPLETE"
                    bridge.save_state()
                    continue
                bridge.state["closed_positions"].pop(sym, None)
                bridge.save_state()
                last_reconciliation_at = time.time()
            
            previous_symbols = active_symbols.copy()

                                                                                              
            all_positions_protected = True
            protected_symbols = set()
            for pos in agent_positions:
                protection = bridge.ensure_position_protection(pos["symbol"], pos)
                try:                                                                                        
                    _msg_alarma = alarma_proteccion.registrar(pos["symbol"], (getattr(bridge, "last_protection_error", None) or {}).get(pos["symbol"]), time.time())
                    if _msg_alarma:
                        log(_msg_alarma)
                except Exception:
                    pass
                if protection and protection.get("position_closed_during_verification"):
                    log(f"ℹ️ {pos['symbol']} quedó flat en Binance durante la verificación; el cierre se concilia con los fills del siguiente snapshot.")
                elif protection and protection.get("is_protected"):
                    protected_symbols.add(pos["symbol"])
                else:
                    all_positions_protected = False
                    log(f"⚠️ Protección SL no confirmada para {pos['symbol']}; se reintentará en el próximo ciclo.")

                                                                                  
                                                                                     
                                                                             
            if not startup_reconciliation_complete:
                blockers = []
                account_snapshot = bridge.client.get_positions()
                if not isinstance(account_snapshot, list):
                    blockers.append("BINANCE_POSITIONS_UNAVAILABLE")
                else:
                    untracked = bridge.get_untracked_positions(account_snapshot)
                    if untracked is None:
                        blockers.append("POSITION_MODE_UNAVAILABLE")
                    elif untracked:
                        blockers.append("UNTRACKED_LIVE_POSITIONS")
                standard_snapshot = bridge.client.get_open_orders()
                if not isinstance(standard_snapshot, list):
                    blockers.append("STANDARD_ORDERS_UNAVAILABLE")
                elif standard_snapshot:
                    blockers.append("UNRECONCILED_STANDARD_ORDERS")
                algo_snapshot = bridge.client.get_open_algo_orders()
                if not isinstance(algo_snapshot, list):
                    blockers.append("ALGO_ORDERS_UNAVAILABLE")
                else:
                    owned_ids = set()
                    for active_pos in agent_positions:
                        active_meta = bridge.state.get("positions", {}).get(active_pos.get("symbol"), {})
                        owned_ids.update(bridge._owned_algo_ids(active_meta))
                    unknown_algos = [row for row in algo_snapshot
                                     if str(row.get("algoStatus", "")).upper() in {"NEW", "TRIGGERING"}
                                     and (str(row.get("algoId", "")) not in owned_ids
                                          or not bridge._algo_is_owned(
                                              row,
                                              bridge.state.get("positions", {}).get(row.get("symbol"), {}),
                                          ))]
                    if unknown_algos:
                        blockers.append("UNOWNED_OR_UNTRACKED_ALGO_ORDERS")
                pending = list(bridge.state.get("closed_positions", {}))
                if pending:
                    blockers.append("CLOSED_POSITIONS_PENDING_RECONCILIATION")
                if bridge.has_unresolved_reconciliation():
                    blockers.append("UNRESOLVED_POSITION_OR_ORDER_STATE")
                if not all_positions_protected:
                    blockers.append("POSITION_PROTECTION_NOT_CONFIRMED")

                if not blockers:
                    startup_reconciliation_complete = True
                    last_reconciliation_at = time.time()
                    log("✅ Reconciliación de arranque completa: Binance → posiciones → órdenes/Algo → estado local. Se habilita el ciclo orgánico de señales.")
                else:
                    definitive_ambiguity = any(code in blockers for code in (
                        "UNTRACKED_LIVE_POSITIONS", "UNRECONCILED_STANDARD_ORDERS",
                        "UNOWNED_OR_UNTRACKED_ALGO_ORDERS", "CLOSED_POSITIONS_PENDING_RECONCILIATION",
                        "UNRESOLVED_POSITION_OR_ORDER_STATE",
                    ))
                    ambiguity_age = time.time() - startup_reconciliation_started_at
                    if (definitive_ambiguity or ambiguity_age >= float(startup_options.get("startup_ambiguity_disarm_seconds", 30))) and not startup_ambiguity_disarmed:
                        bridge.disarm_for_runtime_failure("STARTUP_RECONCILIATION_AMBIGUOUS", {"blockers": blockers})
                        startup_ambiguity_disarmed = True
                    publish_daemon_health(
                        "RECONCILIATION_BLOCKED", reason=", ".join(blockers),
                        accepting_entries=False, startup_reconciliation_complete=False,
                        active_position_count=len(agent_positions), pending_reconciliations=pending,
                        last_reconciliation_at=last_reconciliation_at,
                    )
                    if iteration % int(CONFIG["operations"]["heartbeat_interval_cycles"]) == int(CONFIG["operations"]["heartbeat_offset_cycles"]):
                        log(f"⏸️ Entradas bloqueadas durante reconciliación de arranque: {', '.join(blockers)}")
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue

                                                                              
            for pos in agent_positions:
                symbol = pos["symbol"]
                if symbol not in protected_symbols:
                    continue
                size = float(pos["positionAmt"])
                entry_px = float(pos["entryPrice"])
                mark_px = float(pos["markPrice"])
                upnl = float(pos["unRealizedProfit"])
                leverage = int(pos["leverage"])
                side = "LONG" if size > 0 else "SHORT"

                notional = abs(size) * entry_px
                margin = notional / leverage if leverage > 0 else notional
                pnl_pct = (upnl / margin * 100.0) if margin > 0 else 0.0
                price_move_pct = ((mark_px - entry_px) / entry_px * 100.0) if side == "LONG" else ((entry_px - mark_px) / entry_px * 100.0)

                pos_meta = bridge.state.setdefault("positions", {}).setdefault(symbol, {})
                tp1_done = pos_meta.get("tp1_executed", False)

                                                                                               
                if price_move_pct >= float(CONFIG["exits"]["tp1_trigger_move_pct"]) and not tp1_done:
                    log(f"🎯 [ESCUDO HÍBRIDO {CONFIG['exits']['partial_close_pct']:.0f}/{CONFIG['exits']['runner_pct']:.0f} DISPARADO] {symbol} avanzó +{price_move_pct:.2f}% a favor (uPnL: +${upnl:.2f}). Ejecutando toma parcial y moviendo a Break-Even...")
                    try:
                        tp1_result = bridge.execute_tp1(symbol, pos)
                        if tp1_result.get("status") == "runner_active":
                            log(f"🛡️ TP1 confirmado para {symbol}; runner protegido en Break-Even @ {tp1_result.get('be_price')}.")
                        elif tp1_result.get("status") in {"emergency_closed", "critical_unprotected_failure"}:
                            log(f"🚨 Recuperación TP1 {symbol}: {tp1_result}")
                        else:
                            log(f"TP1 de {symbol} queda pendiente de reconciliación: {tp1_result}")
                    except Exception as e_tp1:
                        log(f"Error ejecutando TP1 activo para {symbol}: {e_tp1}")
                        try:
                            recovery = bridge.ensure_position_protection(symbol, pos)
                            if not recovery or not recovery.get("sl_active"):
                                log(f"🚨 Stop Loss no confirmado para {symbol}; cierre de emergencia solicitado.")
                                bridge.close_position(symbol)
                        except Exception as e_recovery:
                            log(f"🔥 Falló la recuperación de protección/cierre de {symbol}: {e_recovery}")

                                                                                         
            if not startup_reconciliation_complete:
                time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                continue
            if not client.testnet:
                supervisor = {}
                for read_attempt in range(3):
                    supervisor = read_json(SUPERVISOR_STATUS_PATH)
                    if (int(supervisor.get("supervisor_pid", 0) or 0) > 0
                            and float(supervisor.get("updated_at", 0) or 0) > 0):
                        break
                    if read_attempt < 2:
                        time.sleep(0.05)
                supervisor_updated = float(supervisor.get("updated_at", 0) or 0)
                supervisor_age = time.time() - supervisor_updated if supervisor_updated else float("inf")
                supervisor_pid = int(supervisor.get("supervisor_pid", 0) or 0)
                supervisor_state = str(supervisor.get("state", "")).upper()
                status_valid = supervisor_pid > 0 and supervisor_updated > 0
                supervisor_alive = pid_is_alive(supervisor_pid) if status_valid else False
                supervisor_stale_limit = float(startup_options.get("health_stale_seconds", 60))
                terminal_supervisor_state = supervisor_state in {"FAIL_CLOSED", "BLOCKED", "STOPPED"}
                supervisor_ok = (status_valid and supervisor_alive
                                 and 0 <= supervisor_age <= supervisor_stale_limit
                                 and supervisor_state == "RUNNING")
                if not supervisor_ok:
                    if supervisor_unavailable_since is None:
                        supervisor_unavailable_since = time.monotonic()
                    unavailable_for = time.monotonic() - supervisor_unavailable_since
                    definitive_failure = (terminal_supervisor_state
                                          or (status_valid and not supervisor_alive)
                                          or (status_valid and supervisor_age > supervisor_stale_limit)
                                          or unavailable_for >= supervisor_stale_limit)
                    if not definitive_failure:
                        publish_daemon_health(
                            "SUPERVISOR_CHECK_PENDING", reason="SUPERVISOR_STATUS_TEMPORARILY_UNAVAILABLE",
                            accepting_entries=False, startup_reconciliation_complete=True,
                            active_position_count=len(agent_positions),
                            pending_reconciliations=list(bridge.state.get("closed_positions", {})),
                            last_reconciliation_at=last_reconciliation_at,
                            entries_armed=bridge.entries_armed(),
                        )
                        time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                        continue
                    details = {"supervisor_state": supervisor_state or "UNKNOWN",
                               "supervisor_age_seconds": supervisor_age if supervisor_age != float("inf") else None,
                               "supervisor_status_valid": status_valid,
                               "supervisor_pid_alive": supervisor_alive,
                               "unavailable_for_seconds": round(unavailable_for, 3)}
                    if not startup_ambiguity_disarmed:
                        bridge.disarm_for_runtime_failure("SUPERVISOR_HEARTBEAT_STALE", details)
                        startup_ambiguity_disarmed = True
                    publish_daemon_health(
                        "SUPERVISOR_OFFLINE", reason="SUPERVISOR_HEARTBEAT_STALE",
                        accepting_entries=False, startup_reconciliation_complete=True,
                        active_position_count=len(agent_positions),
                        pending_reconciliations=list(bridge.state.get("closed_positions", {})),
                        last_reconciliation_at=last_reconciliation_at,
                        entries_armed=False,
                    )
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue
                supervisor_unavailable_since = None
                control = bridge.state.get("execution_control", {})
                if (isinstance(control, dict)
                        and control.get("armed") is not True
                        and control.get("disarmed_reason") in {
                            "SUPERVISOR_HEARTBEAT_STALE",
                            "SUPERVISOR_RESTART_BUDGET_EXHAUSTED",
                            "STARTUP_RECONCILIATION_AMBIGUOUS",
                        }
                        and startup_reconciliation_complete
                        and all_positions_protected
                        and not bridge.state.get("closed_positions")
                        and not bridge.has_unresolved_reconciliation()):
                    fresh_recovery = bridge.audit_runtime_recovery()
                    recovered_supervisor = read_json(SUPERVISOR_STATUS_PATH)
                    recovered_supervisor_age = time.time() - float(recovered_supervisor.get("updated_at", 0) or 0)
                    supervisor_still_healthy = (
                        recovered_supervisor.get("state") == "RUNNING"
                        and int(recovered_supervisor.get("supervisor_pid", 0) or 0) == supervisor_pid
                        and pid_is_alive(supervisor_pid)
                        and 0 <= recovered_supervisor_age <= supervisor_stale_limit
                    )
                    recovery = bridge.restore_supervisor_false_disarm({
                        **fresh_recovery,
                        "all_positions_protected": fresh_recovery["all_positions_protected"] and supervisor_still_healthy,
                        "supervisor_pid": supervisor_pid,
                        "supervisor_age_seconds": round(supervisor_age, 3),
                        "startup_reconciliation_complete": True,
                        "disarm_reason": control.get("disarmed_reason"),
                    })
                    if recovery.get("restored"):
                        startup_ambiguity_disarmed = False
                        log("✅ Recuperación automática de armado: Binance confirmó posiciones, ownership y protecciones; bloqueo temporal de runtime resuelto.")
            if not bridge.entries_armed():
                control = bridge.state.get("execution_control", {})
                disarmed_reason = (control.get("disarmed_reason", "entries_disarmed")
                                   if isinstance(control, dict) else "entries_disarmed")
                publish_daemon_health(
                    "DISARMED", reason=str(disarmed_reason),
                    accepting_entries=False, startup_reconciliation_complete=True,
                    active_position_count=len(agent_positions),
                    pending_reconciliations=list(bridge.state.get("closed_positions", {})),
                    last_reconciliation_at=last_reconciliation_at,
                    entries_armed=False,
                )
                if iteration % int(CONFIG["operations"]["heartbeat_interval_cycles"]) == int(CONFIG["operations"]["heartbeat_offset_cycles"]):
                    breaker = bridge.state.get("protection_circuit_breaker", {})
                    reason = breaker.get("reason", "entradas desarmadas") if isinstance(breaker, dict) else "entradas desarmadas"
                    log(f"⏸️ Mainnet DISARMED: se monitorea Binance, pero no se evalúan ni envían entradas. Motivo: {reason}.")
                time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                continue
            conditional_zone = len(agent_positions) >= base_capacity
            capacity_due = time.monotonic() - last_capacity_observation_at >= float(CONFIG["portfolio_exposure"]["observer_interval_seconds"])
            if all_positions_protected and (not conditional_zone or capacity_due):
                if conditional_zone:
                    last_capacity_observation_at = time.monotonic()
                cooling = bridge.get_cooling_off_state()
                retry_after = float(cooling.get("retry_after", 0) or 0)
                if retry_after > time.time():
                    monotonic_now = time.monotonic()
                    if monotonic_now - _last_cooling_off_log_at >= float(CONFIG["operations"]["cooling_off_log_interval_seconds"]):
                        remaining = max(1, int(retry_after - time.time()))
                        log(f"⏸️ Binance rechazó una entrada con -4192; pausa global de cuenta activa por {remaining}s más. No se reenvían señales ni se prueban otros símbolos.")
                        _last_cooling_off_log_at = monotonic_now
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue
                if bridge.has_unresolved_reconciliation():
                    monotonic_now = time.monotonic()
                    if monotonic_now - _last_cooling_off_log_at >= float(CONFIG["operations"]["cooling_off_log_interval_seconds"]):
                        log("⏸️ Hay un submit de entrada sin resultado definitivo; se reconcilia con Binance antes de permitir cualquier otra entrada.")
                        _last_cooling_off_log_at = monotonic_now
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue

                                                                                          
                wallet_now = bridge.get_wallet_summary()
                if not wallet_now or float(wallet_now.get("total_balance", 0)) <= 0:
                    if iteration % 60 == 1:
                        log("Sin equity actualizado de Binance; entradas bloqueadas hasta recuperar la consulta de cuenta.")
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue
                curr_equity = float(wallet_now["total_balance"])
                available_balance = float(wallet_now["available_balance"])
                base_eq = float(bridge.state["equity_baselines"][network_key])
                max_drawdown_limit_pct = float(CONFIG["risk"]["max_equity_drawdown_pct"])
                current_dd_pct = ((base_eq - curr_equity) / base_eq * 100.0) if base_eq > 0 else 0.0

                if current_dd_pct >= max_drawdown_limit_pct:
                    if iteration % 60 == 1:
                        log(f"🛑 [CIRCUIT BREAKER] Drawdown: {current_dd_pct:.2f}% >= {max_drawdown_limit_pct}%; nuevas entradas congeladas.")
                    time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))
                    continue

                _ts_senales_antes = _last_valid_signal_at
                signals = fetch_onchain_signals()
                if _last_valid_signal_at is not None and _last_valid_signal_at != _ts_senales_antes:
                    if filtro_arranque.observar(signals, _last_valid_signal_at):
                        log(f"🧊 [ARRANQUE] Señales confirmadas al arrancar: {len(filtro_arranque.viejas)}; no abrirán posición mientras sigan iguales.")
                    filtro_arranque.presentes(signals)
                flip_cfg = antigravity_flip.flip_config(CONFIG)
                if antigravity_flip.limpiar_vencidos(bridge.state, flip_cfg, now):
                    bridge.save_state()
                for sig in ordenar_por_ballenas(signals):
                    status = sig.get("status")
                    if status not in ("CONFIRMED_ONCHAIN", "CONFIRMED_HYBRID"):
                        continue

                    raw_sym = sig.get("raw_symbol", sig.get("symbol", "").replace("/", ""))              
                    side_str = sig.get("side", "")                   
                    order_side = "BUY" if side_str == "LONG" else ("SELL" if side_str == "SHORT" else None)
                    if not order_side:
                        continue

                                                  
                    if raw_sym in active_symbols:
                                                                                                                                  
                        pos_meta_f = bridge.state.get("positions", {}).get(raw_sym, {})
                        if (raw_sym in protected_symbols and raw_sym not in bridge.state.get("closed_positions", {})
                                and antigravity_flip.debe_voltear(flip_cfg, sig, pos_meta_f, bridge.state.get("executed_fingerprints", []))):
                            log(f"🔄 [FLIP] {raw_sym}: señal {side_str} con {antigravity_flip.votos(sig)} ballenas contra la posición "
                                f"{pos_meta_f.get('side')} abierta. Cerrando a mercado para voltear...")
                            pos_meta_f["cierre_por_flip"] = {"side_nuevo": side_str, "votos": antigravity_flip.votos(sig),
                                                             "fingerprint": sig.get("fingerprint"), "at": now}
                            antigravity_flip.anotar_flip(bridge.state, raw_sym, sig, now)
                            bridge.save_state()
                            try:
                                res_flip = bridge.close_position(raw_sym)
                            except Exception as exc_flip:
                                res_flip = {"status": "error", "error": str(exc_flip)}
                            log(f"🔄 [FLIP] {raw_sym}: resultado del cierre {res_flip.get('status') if isinstance(res_flip, dict) else res_flip}; "
                                f"la entrada {side_str} se evaluará al reconciliar el cierre.")
                            active_symbols = set(bridge.state.get("active_symbols", []))
                        continue

                                                                                                                                    
                    if filtro_arranque.bloqueada(sig):
                        if filtro_arranque.viejas is not None and filtro_arranque.avisar(sig):
                            log(f"🧊 [ARRANQUE GATE] {raw_sym} {side_str} rechazado: la señal ya estaba confirmada al arrancar el bot; se espera a una señal nueva.")
                        continue

                                                                                                                                  
                    _fs_cfg = antigravity_freno_shorts.config(CONFIG)
                    if _fs_cfg["enabled"] and str(side_str).upper() in ("LONG", "BUY"):
                        _fs_est = antigravity_freno_shorts.leer_estado(os.path.join(os.path.dirname(os.path.abspath(__file__)), _fs_cfg["archivo"]), max_edad_min=_fs_cfg["max_edad_min"])
                        _fs_b, _fs_n, _fs_u = antigravity_freno_shorts.bloquea(_fs_est, raw_sym, side_str)
                        if _fs_b:
                            if filtro_arranque.avisar({"fingerprint": f"freno|{sig.get('fingerprint')}"}):
                                log(f"🧯 [FRENO SHORTS] {raw_sym} LONG rechazado: {_fs_n} ballenas abrieron short en las últimas 12 h (umbral {_fs_u}).")
                            continue

                                                                                                      
                    fp = sig.get("fingerprint")
                    if fp and fp in bridge.state.get("executed_fingerprints", []):
                        continue

                                                                                                       
                    if raw_sym in bridge.state.get("closed_positions", {}):
                        continue

                                                                                                     
                                                                                               
                    stopped_thesis = bridge.get_stopped_thesis(raw_sym, side_str)
                    thesis_lock_seconds = float(CONFIG["risk"]["stopped_thesis_lock_hours"]) * 3600.0
                    if stopped_thesis and now - float(stopped_thesis.get("stopped_at", now)) >= thesis_lock_seconds:
                        bridge.clear_stopped_thesis(raw_sym, side_str)
                        bridge.save_state()
                        stopped_thesis = None
                    if stopped_thesis and stopped_thesis.get("side") == side_str:
                        failed_whales = {str(address).strip().lower() for address in stopped_thesis.get("supporters", []) if address}
                        current_whales = {str(address).strip().lower() for address in sig.get("supporters", []) if address}
                        new_whales = current_whales - failed_whales
                                                                                         
                                                                                          
                        origin_known = len(failed_whales) >= 2 and not stopped_thesis.get("origin_unknown")
                        if not origin_known or len(new_whales) < 2:
                            log(f"🛑 [STOPPED_THESIS GATE] {raw_sym} rechazado: Tesis previa en {side_str} tocó Stop Loss. Solo {len(new_whales)} ballena(s) nueva(s) ({list(new_whales)}). Se exigen >= 2 ballenas nuevas distintas.")
                            continue

                                                                                                  
                    ref_price = float(sig.get("reference_price", 0.0))
                    clean_sym = raw_sym.replace("USDT", "")
                    if ref_price > 0:
                        mark_px = float(bridge.client.get_mark_price(raw_sym) or 0)
                        if mark_px <= 0:
                            log(f"Mark Price inválido para {raw_sym}; entrada bloqueada.")
                            continue
                        adverse_dist = max(0.0, mark_px - ref_price) if order_side == "BUY" else max(0.0, ref_price - mark_px)
                        adverse_dist_pct = (adverse_dist / ref_price * 100.0) if ref_price > 0 else 0.0
                        atr_15m = bridge.calculate_atr_15m(raw_sym, mark_px)
                        max_adverse_atr = float(CONFIG["atr"]["anti_chase_multiple"]) * atr_15m
                        max_adverse_pct = float(CONFIG["atr"]["anti_chase_max_pct_major"]) if clean_sym in CONFIG["assets"]["major"] else float(CONFIG["atr"]["anti_chase_max_pct_alt"])

                        if adverse_dist > max_adverse_atr or adverse_dist_pct > max_adverse_pct:
                            log(f"⚠️ [ANTI-CHASE GATE] Entrada descartada en {raw_sym} {side_str}: Precio perseguido en exceso (+{adverse_dist_pct:.2f}% adverso > máx {max_adverse_pct}% o ${adverse_dist:.2f} > {CONFIG['atr']['anti_chase_multiple']} ATR {CONFIG['atr']['anti_chase_interval']} ${max_adverse_atr:.2f}). Ref: ${ref_price:.2f}, Mark: ${mark_px:.2f}")
                            continue

                                                 
                    last_exit = cooldowns.get(raw_sym, 0)
                    if now - last_exit < cooldown_seconds and not antigravity_flip.cooldown_exento(bridge.state, flip_cfg, raw_sym, side_str, now):
                        continue

                                                                                                                                         
                    falta_manual = antigravity_cierre_manual.bloqueo(bridge.state, raw_sym, side_str, now, espera_manual_h)
                    if falta_manual is not None:
                        if now - _aviso_manual.get((raw_sym, side_str), 0) >= 3600:
                            log(f"⏸️ [CIERRE MANUAL GATE] {raw_sym} {side_str} rechazado: se cerró a mano hace poco; faltan {falta_manual:.1f} h de espera.")
                            _aviso_manual[(raw_sym, side_str)] = now
                        continue

                                                                                                                                  
                                                                                                                                   
                    rebote_cfg = antigravity_rebote.rebote_config(CONFIG)
                    if rebote_cfg and side_str in rebote_cfg["lados"]:
                        try:
                            rebote = antigravity_rebote.revisar(bridge, raw_sym, side_str, rebote_cfg)
                        except Exception as exc_rebote:
                            log(f"🛑 [REBOTE GATE] {raw_sym} {side_str}: no se pudo medir el rebote ({exc_rebote}); entrada bloqueada.")
                            continue
                        if rebote["bloquear"]:
                            log(f"🛑 [REBOTE GATE] {raw_sym} {side_str} rechazado: movimiento previo {rebote['movimiento_atr']} ATR(4h) y el precio "
                                f"ya devolvió {rebote['devuelto_pct']}% (máx {rebote_cfg['rebote_min'] * 100:.0f}%). Es un rebote en marcha.")
                            continue

                                                                                        
                    tot_votes = int(sig.get("hl_votes", sig.get("total_votes", 0)))
                    
                    policy = evaluate_signal_policy(raw_sym, tot_votes, curr_equity)
                    if not policy["eligible"]:
                        continue
                        
                    lev = policy["leverage"]
                    risk_pct = policy["risk_pct"]
                    category = policy["category"]
                    dollar_risk = policy["dollar_risk"]

                    if len(active_symbols) >= max_concurrent_positions:
                        try:
                            preview = bridge.preview_capacity_entry(raw_sym, order_side, lev, risk_pct,
                                curr_equity, available_balance,
                                {"supporters": sig.get("supporters", []), "fingerprint": fp,
                                 "reference_price": ref_price, "side": side_str, "organic_ag_filters_passed": True})
                            log(f"AG Delta-Aware research: {raw_sym} {side_str} | {preview.get('decision') if preview else 'plan pendiente'} | límite productivo {max_concurrent_positions}; sin envío.")
                        except Exception as preview_exc:
                            log(f"AG Delta-Aware research: {raw_sym} descartada por filtros normales: {preview_exc}")
                            try:
                                from ag_portfolio_observer import append_event, ENTRY_PATH
                                append_event(ENTRY_PATH, {"recorded_at": time.time(), "source": "AG_CAPACITY_PREVIEW_FILTER_REJECTION",
                                    "network": "BINANCE_TESTNET" if client.testnet else "BINANCE_MAINNET",
                                    "symbol": raw_sym, "side": side_str, "fingerprint": fp,
                                    "normal_filters_passed": False, "reason": str(preview_exc)[:400],
                                    "execution_effect": "NONE"})
                            except Exception:
                                pass
                        continue

                    account_positions = bridge.client.get_positions()
                    if account_positions is None:
                        log("No se pudieron reconciliar posiciones actuales; entradas bloqueadas.")
                        break
                    untracked_positions = bridge.get_untracked_positions(account_positions)
                    if untracked_positions is None:
                        log("No se pudo confirmar el modo de posición; nuevas entradas bloqueadas.")
                        break
                    if untracked_positions:
                        untracked_labels = sorted({f"{p.get('symbol')}:{p.get('positionSide', 'BOTH')}" for p in untracked_positions})
                        log(f"Entradas bloqueadas: posiciones Binance sin estado AG: {', '.join(untracked_labels)}")
                        break
                    live_positions = [p for p in account_positions if bridge._is_open_position(p)]
                    if len(live_positions) >= max_concurrent_positions:
                        log("Límite de posiciones simultáneas de la cuenta alcanzado; nuevas entradas bloqueadas.")
                        break
                    if any(p.get("symbol") == raw_sym for p in live_positions):
                        continue
                    if any(order.get("symbol") == raw_sym for order in bridge.client.get_open_orders(raw_sym)) or bridge.client.get_open_algo_orders(raw_sym):
                        log(f"Entradas bloqueadas en {raw_sym}: hay órdenes pendientes que requieren reconciliación.")
                        break

                    log(f"🚀 SEÑAL CONFIRMADA: {raw_sym} {side_str} ({tot_votes} ballenas | {category} | {lev}x | riesgo {risk_pct:.2f}% del equity = ${dollar_risk:.2f} USDT). Enviando entrada...")
                    try:
                        res = bridge.open_position(
                            symbol=raw_sym,
                            side=order_side,
                            leverage=lev,
                            risk_pct=risk_pct,
                            equity=curr_equity,
                            available_balance=available_balance,
                            signal_metadata={
                                "organic_ag_filters_passed": True,
                                "supporters": sig.get("supporters", []),
                                "supporter_names": sig.get("supporter_names", []),
                                "side": side_str,
                                "reference_price": ref_price,
                                "fingerprint": fp
                            }
                        )
                        if res and res.get("status") == "ok":
                            entry_result = res.get("entry", {})
                            capacity_note = ""
                            if res.get("quantity_reduced_for_capacity"):
                                capacity_note = (f" | Delta-Aware ajustó cantidad {res.get('requested_quantity')}→"
                                                 f"{res.get('requested_quantity', 0) * res.get('capacity_quantity_scale', 1):.8g} "
                                                 f"({res.get('capacity_quantity_scale', 1) * 100:.1f}% del sizing inicial)")
                            log(f"✅ POSICIÓN PROTEGIDA: {raw_sym} {order_side} | Entry Order #{entry_result.get('orderId')} | Margen: ${res.get('margin', 0):.2f} USDT ({res.get('margin', 0) / curr_equity * 100:.2f}% del equity) @ {lev}x | SL Algo #{res.get('sl_algo_id')} | TP Algo #{res.get('tp_algo_id')}{capacity_note}")
                            if res.get("capacity_soft_warnings"):
                                log(f"AG Delta-Aware observación de exposición {raw_sym}: {res.get('capacity_soft_warnings')} (dentro de límites duros).")
                            active_symbols.add(raw_sym)
                                                                                                         
                            pos_rec = bridge.state.setdefault("positions", {}).setdefault(raw_sym, {})
                            pos_rec["supporters"] = sig.get("supporters", [])
                            pos_rec["supporter_names"] = sig.get("supporter_names", [])
                            pos_rec["side"] = side_str
                            pos_rec["reference_price"] = ref_price
                            if (bridge.state.get("flip_pendiente") or {}).pop(raw_sym, None):
                                pos_rec["entrada_por_flip"] = True
                                log(f"🔄 [FLIP] {raw_sym}: volteo completado, nueva posición {side_str}.")
                            if fp:
                                fps = bridge.state.setdefault("executed_fingerprints", [])
                                if fp not in fps:
                                    fps.append(fp)
                                if len(fps) > int(CONFIG["operations"]["fingerprint_history_limit"]):
                                    fps.pop(0)
                            bridge.save_state()
                        elif res and res.get("status") == "emergency_closed":
                            log(f"⚠️ {res.get('details') or res.get('reason') or 'Safety-close needs reconciliation'}")
                            cooldowns[raw_sym] = now
                            bridge.state.setdefault("symbol_cooldowns", {})[raw_sym] = now
                            closed_event = bridge.state.setdefault("closed_positions", {}).setdefault(
                                raw_sym, {"metadata": {}, "detected_at": now}
                            )
                            pos_rec = closed_event.setdefault("metadata", {})
                            pos_rec.update({
                                "supporters": sig.get("supporters", []),
                                "supporter_names": sig.get("supporter_names", []),
                                "side": side_str, "reference_price": ref_price,
                                "close_classification": res.get("classification", "SAFETY_CLOSE"),
                                "status": "CLOSED",
                            })
                            bridge.save_state()
                            if res.get("classification") == "PROTECTION_FAILURE":
                                break
                        elif res and res.get("status") == "critical_unprotected_failure":
                            log(f"🔥 {res.get('error')}")
                            pos_rec = bridge.state.setdefault("positions", {}).setdefault(raw_sym, {})
                            pos_rec.setdefault("supporters", sig.get("supporters", []))
                            pos_rec.setdefault("supporter_names", sig.get("supporter_names", []))
                            pos_rec.setdefault("side", side_str)
                            pos_rec.setdefault("reference_price", ref_price)
                            active_symbols.add(raw_sym)
                            bridge.save_state()
                            if not bridge.entries_armed():
                                break
                        elif res and res.get("status") == "entry_pending":
                            log(f"⏳ Orden de entrada {raw_sym} incierta ({res.get('client_order_id')}); nuevas entradas bloqueadas hasta reconciliar con Binance.")
                            break
                        elif res and res.get("status") == "account_cooling_off":
                            log(f"⏸️ Bloqueo cooling-off global de Binance: quedan {res.get('retry_in_seconds')}s; se detiene el escaneo de otros símbolos.")
                            break
                        elif res and res.get("status") == "entry_rejected":
                            if res.get("cooling_off"):
                                log(f"Binance rechazó explícitamente {raw_sym} con -4192 (Cooling-off Period). El episodio no se consume; pausa global {res.get('retry_delay_seconds')}s antes de volver a leer una señal vigente y recalcular los filtros.")
                                break
                            log(f"Orden de entrada rechazada por Binance para {raw_sym}; no se registró posición: {res.get('entry')}")
                        elif res and res.get("status") == "capacity_rejected":
                            scale = res.get("recommended_quantity_scale")
                            sizing_note = (f"; escala máxima sugerida {float(scale) * 100:.1f}%" if isinstance(scale, (int, float)) else "")
                            soft_note = f"; avisos soft {res.get('soft_warnings')}" if res.get("soft_warnings") else ""
                            log(f"AG Delta-Aware denegó el cupo adicional para {raw_sym} {side_str}: {res.get('reasons')}{sizing_note}{soft_note}. Sin orden; la señal queda disponible para otra evaluación vigente.")
                        elif res and ("orderId" in res or "clientOrderId" in res):
                            log(f"✅ Posición enviada a Binance: {raw_sym} {order_side} | OrderID: {res.get('orderId')}")
                            active_symbols.add(raw_sym)
                            pos_rec = bridge.state.setdefault("positions", {}).setdefault(raw_sym, {})
                            pos_rec["supporters"] = sig.get("supporters", [])
                            pos_rec["side"] = side_str
                            pos_rec["reference_price"] = ref_price
                            bridge.save_state()
                        else:
                            log(f"⚠️ Error o rechazo de orden en {raw_sym}: {res}")
                    except Exception as exc:
                        log(f"Error ejecutando orden para {raw_sym}: {exc}")

                                 
            heartbeat_interval = int(CONFIG["operations"]["heartbeat_interval_cycles"])
            if iteration % heartbeat_interval == int(CONFIG["operations"]["heartbeat_offset_cycles"]):
                if agent_positions:
                    pos_summary = ", ".join([f"{p['symbol']} ({p['positionAmt']} @ {p['entryPrice']})" for p in agent_positions])
                    log(f"Heartbeat #{iteration} · Posiciones Activas ({len(agent_positions)}): {pos_summary}")
                else:
                    log(f"Heartbeat #{iteration} · Monitoreando 40 Ballenas On-Chain L1... (0 posiciones abiertas)")

            publish_daemon_health(
                "HEALTHY" if startup_reconciliation_complete and all_positions_protected else "DEGRADED",
                reason="" if startup_reconciliation_complete and all_positions_protected else "POSITION_PROTECTION_OR_RECONCILIATION_PENDING",
                accepting_entries=bool(startup_reconciliation_complete and bridge.entries_armed()
                                       and all_positions_protected and not bridge.has_unresolved_reconciliation()),
                startup_reconciliation_complete=startup_reconciliation_complete,
                active_position_count=len(agent_positions),
                pending_reconciliations=list(bridge.state.get("closed_positions", {})),
                last_reconciliation_at=last_reconciliation_at,
                entries_armed=bridge.entries_armed(),
            )

        except Exception as e:
            log(f"Error en ciclo de monitoreo: {e}")
            publish_daemon_health("DEGRADED", reason=f"MONITOR_CYCLE_ERROR:{type(e).__name__}",
                                  accepting_entries=False,
                                  startup_reconciliation_complete=startup_reconciliation_complete,
                                  pending_reconciliations=list(bridge.state.get("closed_positions", {})) if bridge else [])

        time.sleep(float(CONFIG["operations"]["poll_interval_seconds"]))

if __name__ == "__main__":
    raise SystemExit(main() or 0)
