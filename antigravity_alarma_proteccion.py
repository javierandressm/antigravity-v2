"""05-11-2026 (usuario: "agrega una alarma"). El trailing del BTC estuvo roto ~1,5 h: ensure_position_protection fallaba en cada ciclo con el
mismo error y solo quedaba en la consola, sin aviso. Esta alarma cuenta los fallos seguidos de la protección de cada posición y, si el mismo
problema sigue >= 10 ciclos y >= 10 min, lo escribe en el registro de eventos (se ve en Actividad del panel). Repite cada 60 min mientras
siga, y avisa cuando se resuelve. Solo avisa: no toca posiciones, órdenes, SL/TP ni tamaños."""

MIN_CICLOS = 10
MIN_SEGUNDOS = 600
REPETIR_SEGUNDOS = 3600


class AlarmaProteccion:
    def __init__(self, min_ciclos=MIN_CICLOS, min_segundos=MIN_SEGUNDOS, repetir_segundos=REPETIR_SEGUNDOS):
        self.min_ciclos, self.min_segundos, self.repetir = min_ciclos, min_segundos, repetir_segundos
        self.estado = {}                                                    

    def registrar(self, symbol, error, ahora):
        """error = texto del fallo de este ciclo o None si la protección corrió bien. Devuelve el mensaje a registrar o None."""
        e = self.estado.get(symbol)
        if not error:
            if e and e.get("avisado_en") is not None:
                self.estado.pop(symbol, None)
                return f"✅ [ALARMA PROTECCIÓN] {symbol}: resuelto tras {e['n']} ciclos con fallo ({round((ahora - e['desde']) / 60)} min)."
            self.estado.pop(symbol, None)
            return None
        error = str(error)[:200]
        if not e or e["error"] != error:
            e = self.estado[symbol] = {"error": error, "n": 0, "desde": ahora, "avisado_en": None}
        e["n"] += 1
        if e["n"] < self.min_ciclos or ahora - e["desde"] < self.min_segundos:
            return None
        if e["avisado_en"] is not None and ahora - e["avisado_en"] < self.repetir:
            return None
        e["avisado_en"] = ahora
        return (f"🚨 [ALARMA PROTECCIÓN] {symbol}: la gestión de la posición falla en cada ciclo desde hace {round((ahora - e['desde']) / 60)} min "
                f"({e['n']} veces): {error}. El stop puesto sigue en Binance, pero trailing/break-even pueden no estar actualizándose. Revisar.")

    def olvidar_cerradas(self, abiertas):
        for s in [s for s in self.estado if s not in abiertas]:
            self.estado.pop(s, None)
