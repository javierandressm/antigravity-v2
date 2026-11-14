"""04-11-2026 (usuario: "evitar la suerte y que las entradas lleguen en el momento indicado"). Filtro de arranque.
Al arrancar, el bot veía señales confirmadas que se habían formado mientras estaba apagado y entraba a los 9-12 s, tarde y
persiguiendo el precio. Con dinero real (06-10 -> 04-11, 89 reinicios): 9 entradas a menos de 5 min de arrancar sumaron
-27,38 $ (acierto 44 %); 27 entradas a menos de 2 h, -48,43 $ (48 %); el resto, +21,31 $ (75 %). Ej.: BNB 29-10 -13,27 $.
Regla: en la primera respuesta válida del consenso tras arrancar se anotan los fingerprints de las señales confirmadas que ya
existen; esas no abren posición nueva mientras sigan iguales. Si cambian las ballenas (fingerprint nuevo) o la señal desaparece y
vuelve a formarse, es una señal nueva y entra normal. No toca posiciones abiertas, flips, órdenes, SL/TP ni tamaños."""

CONFIRMADAS = ("CONFIRMED_ONCHAIN", "CONFIRMED_HYBRID")


def activo_config(config):
    """antigravity_config.json puede apagarlo con {"arranque": {"ignorar_senales_viejas": false}}."""
    try:
        return bool((config or {}).get("arranque", {}).get("ignorar_senales_viejas", True))
    except AttributeError:
        return True


class FiltroArranque:
    def __init__(self, ts_inicial=None, activo=True):
        self.ts_inicial = ts_inicial                                                                     
        self.activo = activo
        self.viejas = None                                                                          
        self.avisadas = set()

    def observar(self, signals, ts_valido):
        """Llamar tras cada consulta. La primera vez que llega una respuesta válida nueva, anota las confirmadas que ya estaban.
        Devuelve True si en esta llamada se tomó la foto de arranque."""
        if self.viejas is not None or not ts_valido or ts_valido == self.ts_inicial:
            return False
        self.viejas = set()
        for s in signals or []:
            if isinstance(s, dict) and s.get("status") in CONFIRMADAS and s.get("fingerprint"):
                self.viejas.add(s["fingerprint"])
        return True

    def presentes(self, signals):
        """Olvida las viejas que ya no están (si vuelven a formarse, son señales nuevas)."""
        if not self.viejas:
            return
        vivas = {s.get("fingerprint") for s in signals or [] if isinstance(s, dict)}
        self.viejas &= vivas

    def bloqueada(self, sig):
        """True si la señal ya estaba confirmada al arrancar. Sin foto de arranque todavía, bloquea (no se sabe qué es viejo)."""
        if not self.activo:
            return False
        if self.viejas is None:
            return True
        return bool(sig.get("fingerprint")) and sig.get("fingerprint") in self.viejas

    def avisar(self, sig):
        """True solo la primera vez por fingerprint (para no llenar el log)."""
        fp = sig.get("fingerprint")
        if fp in self.avisadas:
            return False
        self.avisadas.add(fp)
        return True
