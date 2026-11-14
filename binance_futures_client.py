import os
import time
import hmac
import hashlib
import json
import re
import threading
import uuid
import requests
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlencode

from antigravity_config import load_bot_config

RATE_LIMIT_STATE_PATH = Path(__file__).resolve().with_name(".binance_api_cooldown.json")
RATE_LIMIT_LOCK_PATH = RATE_LIMIT_STATE_PATH.with_suffix(".lock")
_RATE_LIMIT_PROCESS_LOCK = threading.Lock()
_RATE_LIMIT_LOCAL_STATES = {}
ALGO_AUDIT_LOG_PATH = Path(__file__).resolve().with_name("antigravity_execution_audit.jsonl")
_ALGO_AUDIT_LOCK = threading.Lock()
TIMESTAMP_RETRY_LIMIT = 1


def _safe_audit_value(value):
    """Remove credential-like values before writing exchange diagnostics locally."""
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            normalized = str(key).replace("-", "").replace("_", "").lower()
            if normalized in {"apikey", "apisecret", "signature", "authorization", "xmbxapikey"}:
                continue
            cleaned[str(key)] = _safe_audit_value(item)
        return cleaned
    if isinstance(value, (list, tuple)):
        return [_safe_audit_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)

class BinanceFuturesClient:
    def __init__(self, api_key=None, api_secret=None, testnet=None):
        if testnet is False or not load_bot_config()["network"]["testnet"]:
            raise RuntimeError("Esta edición local solo admite Binance Testnet.")
        config = load_bot_config()
        self.testnet = bool(config["network"]["testnet"]) if testnet is None else bool(testnet)

                                                                                        
        dotenv_values = {}
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        try:
            if os.path.exists(env_path):
                with open(env_path, "r", encoding="utf-8") as env_file:
                    for line in env_file:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            name, value = line.split("=", 1)
                            dotenv_values[name.strip()] = value.strip().strip('"').strip("'")
        except OSError as exc:
            raise RuntimeError(f"No se pudo leer el archivo local de credenciales: {type(exc).__name__}") from None

        self.api_key = (api_key or os.getenv("BINANCE_API_KEY", "").strip() or dotenv_values.get("BINANCE_API_KEY", "").strip()) or None
        self.api_secret = (api_secret or os.getenv("BINANCE_API_SECRET", "").strip() or dotenv_values.get("BINANCE_API_SECRET", "").strip()) or None
        if not self.testnet and (not self.api_key or not self.api_secret):
            raise RuntimeError("Mainnet requiere BINANCE_API_KEY y BINANCE_API_SECRET en .env.")
        self.config = config
        self.rate_limit_state_path = RATE_LIMIT_STATE_PATH
        self.rate_limit_lock_path = RATE_LIMIT_LOCK_PATH
        self.rate_limit_fallback_seconds = max(
            1.0, float(config["operations"].get("rate_limit_fallback_seconds", 60))
        )
        
        if self.testnet:
            self.base_url = "https://testnet.binancefuture.com"
        else:
            self.base_url = "https://fapi.binance.com"
            
        self.session = requests.Session()
        self.session.headers.update({
            "X-MBX-APIKEY": self.api_key or "",
            "Content-Type": "application/json"
        })
        self._server_time_offset_ms = 0.0
        self._server_time_sync_lock = threading.Lock()

    def _sign(self, params):
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return f"{query_string}&signature={signature}"

    def _sync_server_time(self, request_base, request_endpoint="/fapi/v1/account"):
        """Measure Binance clock offset from an unsigned server-time request."""
        if request_endpoint.startswith("/sapi/"):
            time_url = "https://api.binance.com/api/v3/time"
        else:
            time_url = f"{request_base.rstrip('/')}/fapi/v1/time"

        try:
            with self._server_time_sync_lock:
                started_ms = time.time() * 1000.0
                response = self.session.get(
                    time_url,
                    timeout=float(self.config["operations"]["request_timeout_seconds"]),
                )
                response.raise_for_status()
                body = response.json()
                completed_ms = time.time() * 1000.0
                if not isinstance(body, dict) or body.get("serverTime") is None:
                    return {"ok": False, "error_type": "InvalidServerTimeResponse"}
                server_time_ms = int(body["serverTime"])
                offset_ms = server_time_ms - ((started_ms + completed_ms) / 2.0)
                self._server_time_offset_ms = offset_ms
                self._server_time_sync_at_ms = completed_ms
                return {
                    "ok": True,
                    "offset_ms": round(offset_ms, 1),
                    "round_trip_ms": round(completed_ms - started_ms, 1),
                }
        except Exception as exc:
            return {"ok": False, "error_type": type(exc).__name__}

    def sync_server_time(self):
        """Synchronize this client against the configured Binance Futures host."""
        return self._sync_server_time(self.base_url, "/fapi/v1/account")

    def record_algo_audit_event(self, event_type, details):
        """Persist sanitized Algo submit/confirmation evidence without keys or signatures."""
        event = {
            "recorded_at": time.time(),
            "event_type": str(event_type),
            "details": _safe_audit_value(details),
        }
        try:
            with _ALGO_AUDIT_LOCK:
                with ALGO_AUDIT_LOG_PATH.open("a", encoding="utf-8") as audit_file:
                    audit_file.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
                    audit_file.flush()
        except OSError:
                                                                                             
            pass

    @contextmanager
    def _rate_limit_file_lock(self):
        """Coordinate REST cooldown state across the daemon and dashboard processes."""
        with _RATE_LIMIT_PROCESS_LOCK:
            with self.rate_limit_lock_path.open("a+b") as handle:
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    unlock = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                    unlock = lambda: fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                try:
                    yield
                finally:
                    unlock()

    def _read_rate_limit_state_unlocked(self):
        try:
            state = json.loads(self.rate_limit_state_path.read_text(encoding="utf-8"))
            return state if isinstance(state, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError, TypeError):
                                                                                      
                                                                                     
            try:
                fallback_until = self.rate_limit_state_path.stat().st_mtime + self.rate_limit_fallback_seconds
            except OSError:
                fallback_until = time.time() + self.rate_limit_fallback_seconds
            return {
                "until_epoch": fallback_until,
                "http_status": 429,
                "code": -1003,
            }

    def _shared_rate_limit_state(self):
        state_key = str(self.rate_limit_state_path.resolve())
        try:
            with self._rate_limit_file_lock():
                shared = self._read_rate_limit_state_unlocked()
        except OSError:
            shared = {}
        local = _RATE_LIMIT_LOCAL_STATES.get(state_key, {})
        return local if float(local.get("until_epoch", 0) or 0) > float(shared.get("until_epoch", 0) or 0) else shared

    def _extend_shared_rate_limit(self, until_epoch, http_status, code):
        state_key = str(self.rate_limit_state_path.resolve())
        local = _RATE_LIMIT_LOCAL_STATES.get(state_key, {})
        deadline = max(float(local.get("until_epoch", 0) or 0), float(until_epoch))
        local_state = {
            "until_epoch": deadline,
            "http_status": int(http_status),
            "code": int(code) if code is not None else -1003,
            "updated_at": time.time(),
        }
        _RATE_LIMIT_LOCAL_STATES[state_key] = local_state
        try:
            with self._rate_limit_file_lock():
                previous = self._read_rate_limit_state_unlocked()
                deadline = max(float(previous.get("until_epoch", 0) or 0), deadline)
                state = {
                    "until_epoch": deadline,
                    "http_status": int(http_status),
                    "code": int(code) if code is not None else -1003,
                    "updated_at": time.time(),
                }
                temp_path = self.rate_limit_state_path.with_name(
                    f"{self.rate_limit_state_path.name}.{os.getpid()}.tmp"
                )
                temp_path.write_text(json.dumps(state), encoding="utf-8")
                os.replace(temp_path, self.rate_limit_state_path)
                _RATE_LIMIT_LOCAL_STATES[state_key] = state
                return state
        except OSError:
                                                                                 
                                                        
            return local_state

    def _rate_limit_deadline(self, response, body):
        now = time.time()
        deadlines = []
        message = str(body.get("msg", "")) if isinstance(body, dict) else ""
        banned_until = re.search(r"banned until\s+(\d+)", message, re.IGNORECASE)
        if banned_until:
            deadlines.append(int(banned_until.group(1)) / 1000.0)
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after:
            try:
                deadlines.append(now + max(0.0, float(retry_after)))
            except (TypeError, ValueError):
                try:
                    parsed = parsedate_to_datetime(str(retry_after))
                    deadlines.append(parsed.timestamp())
                except (TypeError, ValueError, OverflowError):
                    pass
        if not deadlines:
            deadlines.append(now + self.rate_limit_fallback_seconds)
        return max(now, max(deadlines))

    def _retry_rate_limited_read(self, error):
        """Respect Binance's cooldown before one bounded GET retry; never replay writes."""
        if error.get("http_status") != 429:
            return False
        deadline = float(error.get("cooldown_until_epoch", 0) or 0)
        if not deadline:
            deadline = float(self._shared_rate_limit_state().get("until_epoch", 0) or 0)
        wait = max(0.0, deadline - time.time())
        budget = float(self.config["operations"].get("rate_limit_read_retry_max_wait_seconds", 10))
        if not deadline or wait > budget:
            return False
        if wait:
            time.sleep(wait + 0.05)
        return True

    def _request(self, method, endpoint, params=None, signed=True, base_url=None,
                 _rate_limit_retry=False):
        audit_algo_submit = method == "POST" and endpoint == "/fapi/v1/algoOrder"
        audit_request_id = uuid.uuid4().hex if audit_algo_submit else None
        cooldown = self._shared_rate_limit_state()
        cooldown_until = float(cooldown.get("until_epoch", 0) or 0)
        if cooldown_until > time.time():
            safe_error = {
                "error": "rate_limit_cooldown",
                "http_status": cooldown.get("http_status", 429),
                "code": cooldown.get("code", -1003),
                "retry_after_seconds": max(1, int(cooldown_until - time.time())),
                "msg": "Binance request suppressed until the shared cooldown expires.",
            }
            if method == "POST":
                safe_error["outcome"] = "pre_submit_error"
            else:
                safe_error["outcome"] = "request_error"
            if audit_algo_submit:
                self.record_algo_audit_event("algo_submit", {
                    "request_id": audit_request_id,
                    "method": method,
                    "endpoint": endpoint,
                    "request_params": params or {},
                    "http_status": None,
                    "response": safe_error,
                    "outcome": safe_error["outcome"],
                })
            if (method == "GET" and not _rate_limit_retry
                    and self._retry_rate_limited_read(safe_error)):
                return self._request(method, endpoint, params, signed, base_url,
                                     _rate_limit_retry=True)
            return safe_error

        params = dict(params or {})
        request_base = base_url or self.base_url

        retry_count = 0
        timestamp_sync = None
        while True:
            request_params = dict(params)
            if signed:
                offset_ms = float(getattr(self, "_server_time_offset_ms", 0.0) or 0.0)
                request_params["timestamp"] = int(time.time() * 1000 + offset_ms)
                request_params["recvWindow"] = int(self.config["operations"]["recv_window_ms"])
                if audit_algo_submit:
                    self.record_algo_audit_event("algo_submit_intent", {
                        "request_id": audit_request_id,
                        "method": method,
                        "endpoint": endpoint,
                        "request_params": request_params,
                        "signature_in_query": True,
                    })
                query_string = self._sign(request_params)
                url = f"{request_base}{endpoint}?{query_string}"
            else:
                query_string = urlencode(request_params) if request_params else ""
                url = f"{request_base}{endpoint}" + (f"?{query_string}" if query_string else "")

            try:
                if method == "GET":
                    response = self.session.get(url, timeout=float(self.config["operations"]["request_timeout_seconds"]))
                elif method == "POST":
                    response = self.session.post(url, timeout=float(self.config["operations"]["request_timeout_seconds"]))
                elif method == "DELETE":
                    response = self.session.delete(url, timeout=float(self.config["operations"]["request_timeout_seconds"]))
                else:
                    raise ValueError("Unsupported HTTP method")

                response.raise_for_status()
                body = response.json()
                if audit_algo_submit:
                    self.record_algo_audit_event("algo_submit_response", {
                        "request_id": audit_request_id,
                        "method": method,
                        "endpoint": endpoint,
                        "http_status": response.status_code,
                        "response": body,
                        "outcome": "acknowledged" if isinstance(body, dict) and body.get("algoId") is not None else "response_without_algo_id",
                    })
                return body
            except requests.exceptions.RequestException as e:
                response = getattr(e, "response", None)
                body = None
                exchange_code = None
                if response is not None:
                    try:
                        body = response.json()
                    except Exception:
                        body = None
                    if isinstance(body, dict):
                        try:
                            exchange_code = int(body.get("code"))
                        except (TypeError, ValueError):
                            exchange_code = None

                if (method == "GET" and signed and exchange_code == -1021
                        and retry_count < TIMESTAMP_RETRY_LIMIT):
                    timestamp_sync = self._sync_server_time(request_base, endpoint)
                    if timestamp_sync.get("ok"):
                        retry_count += 1
                        continue

                safe_error = {"error": type(e).__name__}
                if response is not None:
                    safe_error["http_status"] = response.status_code
                    if isinstance(body, dict):
                        if "code" in body:
                            safe_error["code"] = body["code"]
                        if "msg" in body:
                            safe_error["msg"] = body["msg"]
                    if response.status_code in (418, 429):
                        deadline = self._rate_limit_deadline(response, body)
                        cooldown = self._extend_shared_rate_limit(
                            deadline, response.status_code, exchange_code
                        )
                        safe_error["retry_after_seconds"] = max(
                            1, int(float(cooldown.get("until_epoch", deadline)) - time.time())
                        )
                        safe_error["cooldown_until_epoch"] = cooldown.get("until_epoch", deadline)
                        safe_error["used_weight"] = next((
                            value for key, value in response.headers.items()
                            if key.upper().startswith("X-MBX-USED-WEIGHT-")
                        ), None)
                    if method == "POST" and (
                        response.status_code >= 500
                        or response.status_code < 400
                        or exchange_code in {-1006, -1007}
                    ):
                        safe_error["outcome"] = "ambiguous_after_submit"
                    elif method == "POST" and 400 <= response.status_code < 500:
                        safe_error["outcome"] = "explicit_rejection"
                    else:
                        safe_error["outcome"] = "request_error"
                    if audit_algo_submit:
                        self.record_algo_audit_event("algo_submit_response", {
                            "request_id": audit_request_id,
                            "method": method,
                            "endpoint": endpoint,
                            "http_status": safe_error.get("http_status"),
                            "response": body,
                            "outcome": safe_error["outcome"],
                        })
                elif method == "POST" and isinstance(e, requests.exceptions.ConnectTimeout):
                    safe_error["outcome"] = "pre_submit_error"
                elif method == "POST":
                    safe_error["outcome"] = "ambiguous_after_submit"
                else:
                    safe_error["outcome"] = "request_error"

                if timestamp_sync is not None:
                    safe_error["timestamp_sync"] = timestamp_sync
                    safe_error["timestamp_retry_attempted"] = retry_count > 0
                    safe_error["timestamp_retry_exhausted"] = retry_count > 0 and exchange_code == -1021
                if audit_algo_submit and response is None:
                    self.record_algo_audit_event("algo_submit_response", {
                        "request_id": audit_request_id,
                        "method": method,
                        "endpoint": endpoint,
                        "http_status": None,
                        "response": safe_error,
                        "outcome": safe_error["outcome"],
                    })
                print(f"Binance API Error ({endpoint}): {safe_error}")
                if (method == "GET" and not _rate_limit_retry
                        and self._retry_rate_limited_read(safe_error)):
                    return self._request(method, endpoint, params, signed, base_url,
                                         _rate_limit_retry=True)
                return safe_error

                                                                                     
    def get_account_info(self):
        return self._request('GET', '/fapi/v2/account')

    def get_api_key_permissions(self):
        """Read Mainnet API-key restrictions without making an order."""
        if self.testnet:
            return None
        return self._request(
            'GET', '/sapi/v1/account/apiRestrictions', {},
            base_url='https://api.binance.com'
        )

    def get_positions(self):
        res = self._request('GET', '/fapi/v2/positionRisk')
        return res if isinstance(res, list) else None

    def get_order(self, symbol, client_order_id):
        """Reconcile a possibly timed-out standard order by its unique client ID."""
        return self._request('GET', '/fapi/v1/order', {
            'symbol': symbol,
            'origClientOrderId': client_order_id
        })

    def cancel_order(self, symbol, client_order_id=None, order_id=None):
        """Cancel one standard order by an explicit Binance order/client ID."""
        if not client_order_id and order_id is None:
            raise ValueError("Se requiere orderId o origClientOrderId para cancelar.")
        params = {"symbol": symbol}
        if client_order_id:
            params["origClientOrderId"] = str(client_order_id)
        else:
            params["orderId"] = int(order_id)
        return self._request('DELETE', '/fapi/v1/order', params)

    def get_user_trades(self, symbol, start_time, end_time=None):
        params = {'symbol': symbol, 'startTime': int(start_time)}
        if end_time is not None:
            params['endTime'] = int(end_time)
        result = self._request('GET', '/fapi/v1/userTrades', params)
        return result if isinstance(result, list) else None

    def get_all_algo_orders(self, symbol, start_time, end_time=None):
        params = {'symbol': symbol, 'startTime': int(start_time)}
        if end_time is not None:
            params['endTime'] = int(end_time)
        result = self._request('GET', '/fapi/v1/allAlgoOrders', params)
        return result if isinstance(result, list) else None

    def get_commission_rate(self, symbol):
        """Return the account's live maker/taker commission rates for a symbol."""
        result = self._request('GET', '/fapi/v1/commissionRate', {'symbol': symbol})
        if not isinstance(result, dict) or result.get('error') or result.get('takerCommissionRate') is None:
            return None
        try:
            rate = float(result['takerCommissionRate'])
        except (TypeError, ValueError):
            return None
        return rate if rate >= 0 else None

    def get_position_mode(self, force_refresh=False):
        """Consulta el modo de posición de la cuenta: False = One-way (BOTH), True = Hedge (LONG/SHORT)."""
        cached = getattr(self, "_position_mode_cache", None)
        if not force_refresh and cached and time.monotonic() < cached[1]:
            return cached[0]
        res = self._request('GET', '/fapi/v1/positionSide/dual')
        if isinstance(res, dict) and isinstance(res.get('dualSidePosition'), bool):
            ttl = float(self.config["operations"].get("position_mode_cache_seconds", 10))
            self._position_mode_cache = (res['dualSidePosition'], time.monotonic() + ttl)
            return res['dualSidePosition']
        self._position_mode_cache = None
        return None

                                                                                                               
    def get_exchange_info(self):
        """Consulta reglas y filtros de trading oficiales de Binance Futures."""
        return self._request('GET', '/fapi/v1/exchangeInfo', signed=False)

    def get_ticker_price(self, symbol=None):
        """Consulta el precio actual de mercado."""
        params = {"symbol": symbol} if symbol else {}
        return self._request('GET', '/fapi/v1/ticker/price', params=params, signed=False)

    def get_mark_price(self, symbol):
        """Consulta el Mark Price oficial vigente en Binance Futures."""
        res = self._request('GET', '/fapi/v1/premiumIndex', params={'symbol': symbol}, signed=False)
        if isinstance(res, dict) and 'markPrice' in res:
            try:
                return float(res['markPrice'])
            except Exception:
                pass
        return 0.0

    def get_ticker_24h(self, symbol=None):
        params = {"symbol": symbol} if symbol else {}
        return self._request('GET', '/fapi/v1/ticker/24hr', params=params, signed=False)

                                                                                               
    def change_margin_type(self, symbol, margin_type="ISOLATED"):
        params = {
            "symbol": symbol,
            "marginType": margin_type
        }
        try:
            return self._request('POST', '/fapi/v1/marginType', params)
        except Exception:
            return None

    def change_leverage(self, symbol, leverage):
        params = {
            "symbol": symbol,
            "leverage": int(leverage)
        }
        return self._request('POST', '/fapi/v1/leverage', params)

                                                             
    def place_order(self, symbol, side, order_type="MARKET", quantity=None, price=None, stop_price=None, reduce_only=False, close_position=False, position_side="BOTH", working_type="MARK_PRICE", client_order_id=None):
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type
        }
        if position_side:
            params["positionSide"] = position_side
        if quantity is not None and not close_position:
            params["quantity"] = str(quantity)
        if price:
            params["price"] = str(price)
            params["timeInForce"] = "GTC"
        if stop_price:
            params["stopPrice"] = str(stop_price)
                                                                                       
        if reduce_only and not close_position and position_side == "BOTH":
            params["reduceOnly"] = "true"
        if close_position:
            params["closePosition"] = "true"
        if working_type and order_type in {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}:
            params["workingType"] = working_type
        if order_type == "MARKET":
                                                                               
                                                                             
            params["newOrderRespType"] = "RESULT"
        if client_order_id:
            params["newClientOrderId"] = client_order_id
            
        return self._request('POST', '/fapi/v1/order', params)

                                                         
    def place_algo_order(self, symbol, side, order_type, trigger_price, position_side="BOTH", quantity=None, close_position=False, reduce_only=False, working_type="MARK_PRICE", client_algo_id=None):
        """Coloca órdenes condicionales nativas (STOP_MARKET, TAKE_PROFIT_MARKET) en Binance Futures Algo API."""
        if close_position and quantity is not None:
            raise ValueError("Binance no permite quantity junto con closePosition=true.")
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "algoType": "CONDITIONAL",
            "triggerPrice": str(trigger_price),
            "workingType": working_type
        }
        if position_side:
            params["positionSide"] = position_side
        if client_algo_id:
            params["clientAlgoId"] = str(client_algo_id)
        if close_position:
            params["closePosition"] = "true"
        else:
            if quantity is not None:
                params["quantity"] = str(quantity)
                                                                                           
            if reduce_only and position_side == "BOTH":
                params["reduceOnly"] = "true"
                
        return self._request('POST', '/fapi/v1/algoOrder', params)

    def place_stop_loss(self, symbol, side, stop_price, position_side="BOTH", working_type="MARK_PRICE", client_algo_id=None):
        """
        Coloca orden de Stop Loss condicional nativa en Binance Futures vía /fapi/v1/algoOrder:
        type=STOP_MARKET, closePosition=true, workingType=MARK_PRICE.
        """
        return self.place_algo_order(
            symbol=symbol,
            side=side,
            order_type="STOP_MARKET",
            trigger_price=stop_price,
            position_side=position_side,
            close_position=True,
            reduce_only=False,
            working_type=working_type,
            client_algo_id=client_algo_id
        )

    def place_take_profit(self, symbol, side, stop_price, quantity, position_side="BOTH", reduce_only=True, working_type="MARK_PRICE", client_algo_id=None):
        """
        Coloca orden de Take Profit condicional nativa en Binance Futures vía /fapi/v1/algoOrder:
        type=TAKE_PROFIT_MARKET, reduceOnly=true, workingType=MARK_PRICE.
        """
        return self.place_algo_order(
            symbol=symbol,
            side=side,
            order_type="TAKE_PROFIT_MARKET",
            trigger_price=stop_price,
            position_side=position_side,
            quantity=quantity,
            close_position=False,
            reduce_only=reduce_only,
            working_type=working_type,
            client_algo_id=client_algo_id
        )

    def get_open_algo_orders(self, symbol=None):
        """Consulta todas las órdenes condicionales (algo orders) activas en Binance Futures."""
        params = {"symbol": symbol} if symbol else {}
        res = self._request('GET', '/fapi/v1/openAlgoOrders', params)
        if not isinstance(res, list):
            raise RuntimeError(f"No se pudo verificar openAlgoOrders de Binance para {symbol or 'la cuenta'}: {res}")
        return res

    def get_algo_order(self, symbol, client_algo_id=None, algo_id=None):
        """Query one USDⓈ-M conditional order by its Binance identifier."""
        if not client_algo_id and algo_id is None:
            raise ValueError("Se requiere clientAlgoId o algoId para consultar una orden Algo.")
                                                                           
                                                                        
        params = {}
        if client_algo_id:
            params["clientAlgoId"] = str(client_algo_id)
        else:
            params["algoId"] = int(algo_id)
        return self._request('GET', '/fapi/v1/algoOrder', params)

    def get_open_orders(self, symbol=None):
        """Consulta órdenes estándar abiertas sin realizar cambios en Binance."""
        params = {"symbol": symbol} if symbol else {}
        res = self._request('GET', '/fapi/v1/openOrders', params)
        if not isinstance(res, list):
            raise RuntimeError(f"No se pudo verificar openOrders de Binance para {symbol or 'la cuenta'}: {res}")
        return res

    def cancel_algo_orders(self, symbol, algo_ids, position_side=None, side=None, order_type=None, exclude_algo_ids=None):
        """Cancela únicamente IDs confirmados como propios; jamás cancela por coincidencia de lado."""
        if algo_ids is None:
            raise ValueError("La cancelación Algo requiere IDs explícitos de órdenes cuya propiedad esté confirmada.")
        requested_ids = {str(item) for item in algo_ids if item is not None}
        excluded_ids = {str(item) for item in (exclude_algo_ids or set()) if item is not None}
        requested_ids -= excluded_ids
        if not requested_ids:
            return True
        orders = self.get_open_algo_orders(symbol)
        targets = []
        for order in orders:
            algo_id = order.get("algoId")
            if algo_id is None or str(algo_id) not in requested_ids:
                continue
            if ((position_side is not None and order.get("positionSide", "BOTH") != position_side)
                    or (side is not None and order.get("side") != side)
                    or (order_type is not None and order.get("orderType") != order_type)):
                raise RuntimeError(f"El algoId {algo_id} no coincide con la pierna/filtro solicitado; no se canceló.")
            targets.append(order)
        for order in targets:
            algo_id = order["algoId"]
            result = self._request('DELETE', '/fapi/v1/algoOrder', {'algoId': algo_id})
            try:
                response_code = int(result.get("code", 200)) if isinstance(result, dict) else None
            except (TypeError, ValueError):
                response_code = None
            if (not isinstance(result, dict) or result.get("error") or response_code not in (200,)):
                raise RuntimeError(f"Binance no confirmó la cancelación del algoId {algo_id} para {symbol}: {result}")
            if str(result.get("algoId", "")) != str(algo_id):
                raise RuntimeError(f"La respuesta de cancelación de {symbol} no confirma el algoId solicitado {algo_id}: {result}")
        remaining = self.get_open_algo_orders(symbol)
        remaining_targets = [order for order in remaining
                             if order.get("algoId") is not None
                             and str(order.get("algoId")) in requested_ids]
        if remaining_targets:
            raise RuntimeError(f"Persisten {len(remaining_targets)} órdenes Algo propias abiertas para {symbol} tras cancelarlas.")
        return True

    def cancel_all_orders(self, symbol):
        """Cancela órdenes estándar abiertas."""
        params = {"symbol": symbol}
        return self._request('DELETE', '/fapi/v1/allOpenOrders', params)
