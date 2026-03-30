import os
import time
from datetime import date, timedelta

from common import (
    build_envelope,
    build_kafka_producer,
    fetch_json,
    getenv_bool,
    getenv_int,
    getenv_str,
    load_json_config,
    publish_json,
    retry_with_backoff,
    sleep_seconds,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
PRODUCER_TOPIC = os.getenv("PRODUCER_TOPIC", "raw.weather.v1")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.weather.v1")
POLL_INTERVAL_SECONDS = getenv_int("POLL_INTERVAL_SECONDS", 60)
RETRY_WAIT_SECONDS = getenv_int("RETRY_WAIT_SECONDS", 10)
MAX_RETRIES = getenv_int("MAX_RETRIES", 3)
RETRY_BACKOFF_BASE_SECONDS = getenv_int("RETRY_BACKOFF_BASE_SECONDS", 2)
PUBLISH_DEDUP_TTL_SECONDS = getenv_int("PUBLISH_DEDUP_TTL_SECONDS", 3600)
METRICS_LOG_EVERY_SECONDS = getenv_int("METRICS_LOG_EVERY_SECONDS", 60)
SOURCES_CONFIG_PATH = getenv_str(
    "SOURCES_CONFIG_PATH",
    "/app/src/producers/sources.json",
)
HISTORY_START_DATE = os.getenv("HISTORY_START_DATE", "").strip()
HISTORY_END_DATE = os.getenv("HISTORY_END_DATE", "").strip()
WEATHER_CURRENT_VARS = getenv_str(
    "WEATHER_CURRENT_VARS",
    "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,wind_speed_10m,wind_direction_10m,weather_code,cloud_cover,is_day",
)
WEATHER_HOURLY_VARS = getenv_str(
    "WEATHER_HOURLY_VARS",
    "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,wind_speed_10m,wind_direction_10m,weather_code,cloud_cover",
)
HISTORICAL_RUN_ONCE = getenv_bool("HISTORICAL_RUN_ONCE", False)
HISTORICAL_DATE_CURSOR_ENABLED = getenv_bool("HISTORICAL_DATE_CURSOR_ENABLED", False)


def parse_iso_date(label: str, value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD, got {value!r}") from exc


def build_request_params(
    location: dict[str, str],
    historical_mode: bool,
    history_start_date: str,
    history_end_date: str,
) -> dict[str, str]:
    timezone = location.get("timezone", "auto")
    params = {
        "latitude": str(location["latitude"]),
        "longitude": str(location["longitude"]),
        "timezone": timezone,
    }
    if historical_mode:
        params["start_date"] = history_start_date
        params["end_date"] = history_end_date
        params["hourly"] = WEATHER_HOURLY_VARS
    else:
        params["current"] = WEATHER_CURRENT_VARS
    return params


def main() -> None:
    config = load_json_config(SOURCES_CONFIG_PATH)
    weather_config = config.get("weather", {})
    locations = weather_config.get("locations", [])
    forecast_api_url = weather_config.get(
        "forecast_api_url",
        "https://api.open-meteo.com/v1/forecast",
    )
    archive_api_url = weather_config.get(
        "archive_api_url",
        "https://archive-api.open-meteo.com/v1/archive",
    )
    historical_mode = bool(HISTORY_START_DATE and HISTORY_END_DATE)
    stop_after_first_loop = bool(
        HISTORICAL_RUN_ONCE and historical_mode and not HISTORICAL_DATE_CURSOR_ENABLED
    )
    selected_api_url = archive_api_url if historical_mode else forecast_api_url
    history_start_date_obj: date | None = None
    history_end_date_obj: date | None = None
    current_cursor_date_obj: date | None = None
    if historical_mode:
        history_start_date_obj = parse_iso_date("HISTORY_START_DATE", HISTORY_START_DATE)
        history_end_date_obj = parse_iso_date("HISTORY_END_DATE", HISTORY_END_DATE)
        if history_start_date_obj > history_end_date_obj:
            raise ValueError("HISTORY_START_DATE must be <= HISTORY_END_DATE")
        if HISTORICAL_DATE_CURSOR_ENABLED:
            current_cursor_date_obj = history_start_date_obj

    producer = build_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    seen_idempotency_keys: dict[str, float] = {}
    published_total = 0
    failed_total = 0
    retried_total = 0
    deduplicated_total = 0
    api_calls_total = 0
    api_duration_total_seconds = 0.0
    metrics_window_started_at = time.monotonic()

    def cleanup_dedup_cache(now_monotonic: float) -> None:
        expired = [
            key
            for key, first_seen_at in seen_idempotency_keys.items()
            if (now_monotonic - first_seen_at) > PUBLISH_DEDUP_TTL_SECONDS
        ]
        for key in expired:
            seen_idempotency_keys.pop(key, None)

    def publish_metrics_if_due(now_monotonic: float) -> None:
        nonlocal metrics_window_started_at
        elapsed_seconds = now_monotonic - metrics_window_started_at
        if elapsed_seconds < METRICS_LOG_EVERY_SECONDS:
            return
        throughput_per_minute = (published_total / elapsed_seconds) * 60 if elapsed_seconds else 0.0
        avg_api_seconds = (api_duration_total_seconds / api_calls_total) if api_calls_total else 0.0
        print(
            "[weather][metrics] "
            f"published={published_total} failed={failed_total} retried={retried_total} "
            f"deduplicated={deduplicated_total} avg_api_seconds={avg_api_seconds:.2f} "
            f"throughput_per_min={throughput_per_minute:.2f} dedup_cache_size={len(seen_idempotency_keys)}",
            flush=True,
        )
        metrics_window_started_at = now_monotonic

    while True:
        loop_history_start_date = HISTORY_START_DATE
        loop_history_end_date = HISTORY_END_DATE
        if historical_mode and HISTORICAL_DATE_CURSOR_ENABLED:
            assert current_cursor_date_obj is not None
            current_iso_date = current_cursor_date_obj.isoformat()
            loop_history_start_date = current_iso_date
            loop_history_end_date = current_iso_date
            print(
                f"[weather] historical cursor day={current_iso_date}",
                flush=True,
            )

        loop_started_at = time.monotonic()
        for location in locations:
            metadata = {
                "country": location.get("country"),
                "city": location.get("city"),
                "latitude": location.get("latitude"),
                "longitude": location.get("longitude"),
                "timezone": location.get("timezone", "auto"),
                "historical_mode": historical_mode,
                "start_date": loop_history_start_date or None,
                "end_date": loop_history_end_date or None,
            }
            try:
                request_params = build_request_params(
                    location,
                    historical_mode,
                    loop_history_start_date,
                    loop_history_end_date,
                )
                request_started_at = time.monotonic()
                payload, attempts_used = retry_with_backoff(
                    lambda: fetch_json(
                        selected_api_url,
                        params=request_params,
                    ),
                    max_retries=MAX_RETRIES,
                    base_backoff_seconds=RETRY_BACKOFF_BASE_SECONDS,
                    operation_label=f"weather_fetch city={metadata['city']}",
                )
                api_duration_total_seconds += time.monotonic() - request_started_at
                api_calls_total += 1
                retried_total += attempts_used
                message = build_envelope(
                    source="open-meteo",
                    source_type="weather",
                    metadata=metadata,
                    payload=payload,
                )

                idempotency_key = message["idempotency_key"]
                now_monotonic = time.monotonic()
                cleanup_dedup_cache(now_monotonic)
                if idempotency_key in seen_idempotency_keys:
                    deduplicated_total += 1
                    print(
                        f"[weather] deduplicated event for {metadata['city']}, {metadata['country']} "
                        f"idempotency_key={idempotency_key[:12]}",
                        flush=True,
                    )
                    continue

                publish_json(producer, topic=PRODUCER_TOPIC, message=message)
                seen_idempotency_keys[idempotency_key] = now_monotonic
                published_total += 1
                print(
                    f"[weather] published for {metadata['city']}, {metadata['country']}",
                    flush=True,
                )
            except Exception as exc:
                failed_total += 1
                error_message = str(exc)
                dlq_message = {
                    "failed_at": time.time(),
                    "source": "open-meteo",
                    "source_type": "weather",
                    "target_topic": PRODUCER_TOPIC,
                    "dlq_topic": DLQ_TOPIC,
                    "metadata": metadata,
                    "error": {
                        "type": exc.__class__.__name__,
                        "message": error_message,
                    },
                }
                try:
                    publish_json(producer, topic=DLQ_TOPIC, message=dlq_message)
                    print(
                        f"[weather] sent failed event to DLQ city={metadata['city']} "
                        f"topic={DLQ_TOPIC}",
                        flush=True,
                    )
                except Exception as dlq_exc:
                    print(
                        f"[weather] failed to publish to DLQ for city={metadata['city']}: {dlq_exc}",
                        flush=True,
                    )
                print(
                    f"[weather] error for {metadata['city']}, {metadata['country']}: {exc}",
                    flush=True,
                )
                sleep_seconds(RETRY_WAIT_SECONDS)
            finally:
                publish_metrics_if_due(time.monotonic())
        loop_elapsed = time.monotonic() - loop_started_at
        print(f"[weather] ingestion loop completed in {loop_elapsed:.2f}s", flush=True)
        if historical_mode and HISTORICAL_DATE_CURSOR_ENABLED:
            assert current_cursor_date_obj is not None
            assert history_end_date_obj is not None
            if current_cursor_date_obj >= history_end_date_obj:
                print(
                    "[weather] historical cursor reached HISTORY_END_DATE; exiting producer",
                    flush=True,
                )
                break
            current_cursor_date_obj = current_cursor_date_obj + timedelta(days=1)
            next_cursor_iso = current_cursor_date_obj.isoformat()
            print(f"[weather] advancing historical cursor to {next_cursor_iso}", flush=True)
            sleep_seconds(POLL_INTERVAL_SECONDS)
            continue
        if stop_after_first_loop:
            print(
                "[weather] HISTORICAL_RUN_ONCE=true and historical dates configured; exiting after first loop",
                flush=True,
            )
            break
        sleep_seconds(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
