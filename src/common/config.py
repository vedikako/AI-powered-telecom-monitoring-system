from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    database_url: str = _env(
        "DATABASE_URL",
        "postgresql://telecom:telecom@localhost:5432/telecom",
    )
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    metrics_topic: str = _env("KAFKA_METRICS_TOPIC", "network.metrics")
    dlq_topic: str = _env("KAFKA_DLQ_TOPIC", "network.metrics.dlq")
    consumer_group: str = _env("KAFKA_CONSUMER_GROUP", "metrics-processor")
    interval_seconds: float = float(_env("SIMULATOR_INTERVAL_SECONDS", "10"))
    backfill_hours: float = float(_env("BACKFILL_HOURS", "6"))
    invalid_rate: float = float(_env("INVALID_RATE", "0.015"))
    log_level: str = _env("LOG_LEVEL", "INFO")
    model_version: str = _env("MODEL_VERSION", "iforest_v1")
    model_path: str = _env("MODEL_PATH", "/app/models/iforest_v1.joblib")
    etl_interval_seconds: float = float(_env("ETL_INTERVAL_SECONDS", "60"))
    ml_min_train_rows: int = int(_env("ML_MIN_TRAIN_ROWS", "40000"))
    ml_score_interval_seconds: float = float(_env("ML_SCORE_INTERVAL_SECONDS", "20"))
    batch_size: int = int(_env("PROCESSOR_BATCH_SIZE", "100"))
    batch_wait_seconds: float = float(_env("PROCESSOR_BATCH_WAIT_SECONDS", "2"))
    quality_interval_seconds: float = float(_env("QUALITY_INTERVAL_SECONDS", "30"))
    max_event_age_seconds: int = int(_env("MAX_EVENT_AGE_SECONDS", "3600"))
    alert_interval_seconds: float = float(_env("ALERT_INTERVAL_SECONDS", "20"))
    alert_lookback_minutes: float = float(_env("ALERT_LOOKBACK_MINUTES", "45"))
    alert_cooldown_minutes: float = float(_env("ALERT_COOLDOWN_MINUTES", "15"))
    servicenow_instance: str = _env("SERVICENOW_INSTANCE", "")
    servicenow_user: str = _env("SERVICENOW_USER", "")
    servicenow_password: str = _env("SERVICENOW_PASSWORD", "")
    llm_enabled: bool = _env("LLM_ENABLED", "false").lower() in {"1", "true", "yes"}
    openai_api_key: str = _env("OPENAI_API_KEY", "")
    llm_model: str = _env("LLM_MODEL", "gpt-4o-mini")


settings = Settings()
