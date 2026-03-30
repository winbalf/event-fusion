import json
import os
import random
import time
from hashlib import sha256
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from kafka import KafkaProducer


def getenv_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_json(
    url: str,
    *,
    timeout_seconds: int = 20,
    params: Optional[dict[str, Any]] = None,
) -> Any:
    response = requests.get(url, timeout=timeout_seconds, params=params)
    response.raise_for_status()
    return response.json()


def getenv_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value


def getenv_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_json_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_envelope(
    *,
    source: str,
    source_type: str,
    metadata: dict[str, Any],
    payload: Any,
) -> dict[str, Any]:
    schema_version = "1.0.0"
    contract_validation = validate_contract(source_type, metadata, payload)
    idempotency_key = build_idempotency_key(
        source=source,
        source_type=source_type,
        metadata=metadata,
        payload=payload,
    )
    return {
        "ingested_at": now_utc_iso(),
        "source": source,
        "source_type": source_type,
        "schema_version": schema_version,
        "idempotency_key": idempotency_key,
        "contract_validation": contract_validation,
        "metadata": metadata,
        "payload": payload,
    }


def publish_json(
    producer: KafkaProducer,
    *,
    topic: str,
    message: dict[str, Any],
) -> None:
    producer.send(topic, message)
    producer.flush()


def sleep_seconds(seconds: int) -> None:
    time.sleep(seconds)


def retry_with_backoff(
    fn,
    *,
    max_retries: int,
    base_backoff_seconds: int,
    operation_label: str,
):
    attempt = 0
    while True:
        try:
            return fn(), attempt
        except Exception as exc:
            if attempt >= max_retries:
                raise RuntimeError(
                    f"{operation_label} failed after {attempt + 1} attempt(s)"
                ) from exc
            sleep_for = (base_backoff_seconds * (2**attempt)) + random.uniform(0, 0.5)
            print(
                f"[retry] {operation_label} failed on attempt={attempt + 1}, "
                f"sleeping {sleep_for:.2f}s before retry: {exc}",
                flush=True,
            )
            sleep_seconds(int(max(1, round(sleep_for))))
            attempt += 1


def build_idempotency_key(
    *,
    source: str,
    source_type: str,
    metadata: dict[str, Any],
    payload: Any,
) -> str:
    canonical = json.dumps(
        {
            "source": source,
            "source_type": source_type,
            "metadata": metadata,
            "payload": payload,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def validate_contract(
    source_type: str,
    metadata: dict[str, Any],
    payload: Any,
) -> dict[str, Any]:
    required_metadata_fields: dict[str, dict[str, type]] = {
        "weather": {
            "country": str,
            "city": str,
            "latitude": (float, int),
            "longitude": (float, int),
            "timezone": str,
            "historical_mode": bool,
        },
        "football": {
            "country": str,
            "league_code": str,
            "league_name": str,
            "historical_mode": bool,
        },
    }

    errors: list[str] = []
    source_required = required_metadata_fields.get(source_type, {})
    for field_name, field_type in source_required.items():
        if field_name not in metadata:
            errors.append(f"metadata.{field_name} is required")
            continue
        value = metadata[field_name]
        if value is None:
            errors.append(f"metadata.{field_name} cannot be null")
            continue
        if not isinstance(value, field_type):
            errors.append(
                f"metadata.{field_name} must be {field_type}, got {type(value).__name__}"
            )

    if not isinstance(payload, dict):
        errors.append("payload must be an object")
    else:
        if source_type == "weather":
            if "latitude" not in payload or "longitude" not in payload:
                errors.append("payload must include latitude and longitude for weather")
            if "current" not in payload and "hourly" not in payload:
                errors.append("payload must include current or hourly weather data")
        if source_type == "football":
            if "events" not in payload:
                errors.append("payload.events is required for football")
            elif not isinstance(payload.get("events"), list):
                errors.append("payload.events must be an array")

    return {
        "status": "valid" if not errors else "invalid",
        "errors": errors,
    }
