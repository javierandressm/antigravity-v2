"""Shared runtime configuration for Antigravity."""

import json
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().with_name("antigravity_config.json")

def load_bot_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    exits = config["exits"]
    if abs(float(exits["partial_close_pct"]) + float(exits["runner_pct"]) - 100.0) > 1e-9:
        raise ValueError("partial_close_pct y runner_pct deben sumar 100%")
    if not 0 < float(config["risk"]["standard_trade_risk_pct"]) <= float(config["risk"]["high_confidence_trade_risk_pct"]) <= 100:
        raise ValueError("Porcentajes de riesgo inválidos en antigravity_config.json")
    operations = config["operations"]
    retry_initial = float(operations["cooling_off_retry_initial_seconds"])
    retry_multiplier = float(operations["cooling_off_retry_multiplier"])
    retry_max = float(operations["cooling_off_retry_max_seconds"])
    if retry_initial <= 0 or retry_multiplier < 1 or retry_max < retry_initial:
        raise ValueError("Parámetros de reintento cooling-off inválidos en antigravity_config.json")
    if float(operations["cooling_off_log_interval_seconds"]) <= 0:
        raise ValueError("cooling_off_log_interval_seconds debe ser positivo")
    capacity = config["portfolio_exposure"]
    base = int(capacity["base_capacity"])
    maximum = int(capacity["research_capacity"])
    hard_limit = int(operations["max_concurrent_positions"])
    enabled = capacity["conditional_slots_enabled"] is True
    if base != 3 or maximum != 5 or hard_limit != (maximum if enabled else base):
        raise ValueError("Capacidad AG inconsistente: base 3 y máximo 5 solo con Delta-Aware habilitado")
    return config
