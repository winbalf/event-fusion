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
PRODUCER_TOPIC = os.getenv("PRODUCER_TOPIC", "raw.football.v1")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.football.v1")
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
HISTORICAL_RUN_ONCE = getenv_bool("HISTORICAL_RUN_ONCE", False)
HISTORICAL_DATE_CURSOR_ENABLED = getenv_bool("HISTORICAL_DATE_CURSOR_ENABLED", False)


def parse_iso_date(label: str, value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD, got {value!r}") from exc


def build_date_range_query(history_start_date: str, history_end_date: str) -> str:
    if not (history_start_date and history_end_date):
        return ""
    compact_start = history_start_date.replace("-", "")
    compact_end = history_end_date.replace("-", "")
    return f"{compact_start}-{compact_end}"


def main() -> None:
    config = load_json_config(SOURCES_CONFIG_PATH)
    football_config = config.get("football", {})
    leagues = football_config.get("leagues", [])
    base_url = football_config.get(
        "base_url",
        "https://site.api.espn.com/apis/site/v2/sports/soccer",
    )
    historical_mode = bool(HISTORY_START_DATE and HISTORY_END_DATE)
    stop_after_first_loop = bool(
        HISTORICAL_RUN_ONCE and historical_mode and not HISTORICAL_DATE_CURSOR_ENABLED
    )
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
            "[football][metrics] "
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
                f"[football] historical cursor day={current_iso_date}",
                flush=True,
            )
        date_range_query = build_date_range_query(loop_history_start_date, loop_history_end_date)

        loop_started_at = time.monotonic()
        for league in leagues:
            league_code = league["league_code"]
            url = f"{base_url}/{league_code}/scoreboard"
            params = {"dates": date_range_query} if date_range_query else None
            metadata = {
                "country": league.get("country"),
                "league_code": league_code,
                "league_name": league.get("league_name"),
                "historical_mode": historical_mode,
                "start_date": loop_history_start_date or None,
                "end_date": loop_history_end_date or None,
            }
            try:
                request_started_at = time.monotonic()
                payload, attempts_used = retry_with_backoff(
                    lambda: fetch_json(url, params=params),
                    max_retries=MAX_RETRIES,
                    base_backoff_seconds=RETRY_BACKOFF_BASE_SECONDS,
                    operation_label=f"football_fetch league={league_code}",
                )
                api_duration_total_seconds += time.monotonic() - request_started_at
                api_calls_total += 1
                retried_total += attempts_used
                message = build_envelope(
                    source="espn",
                    source_type="football",
                    metadata=metadata,
                    payload=payload,
                )

                idempotency_key = message["idempotency_key"]
                now_monotonic = time.monotonic()
                cleanup_dedup_cache(now_monotonic)
                if idempotency_key in seen_idempotency_keys:
                    deduplicated_total += 1
                    print(
                        f"[football] deduplicated event for {league_code} "
                        f"idempotency_key={idempotency_key[:12]}",
                        flush=True,
                    )
                    continue

                publish_json(producer, topic=PRODUCER_TOPIC, message=message)
                seen_idempotency_keys[idempotency_key] = now_monotonic
                published_total += 1
                print(
                    f"[football] published for {metadata['league_code']} ({metadata['country']})",
                    flush=True,
                )
            except Exception as exc:
                failed_total += 1
                error_message = str(exc)
                dlq_message = {
                    "failed_at": time.time(),
                    "source": "espn",
                    "source_type": "football",
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
                        f"[football] sent failed event to DLQ league={league_code} topic={DLQ_TOPIC}",
                        flush=True,
                    )
                except Exception as dlq_exc:
                    print(
                        f"[football] failed to publish to DLQ for league={league_code}: {dlq_exc}",
                        flush=True,
                    )
                print(
                    f"[football] error for {metadata['league_code']}: {exc}",
                    flush=True,
                )
                sleep_seconds(RETRY_WAIT_SECONDS)
            finally:
                publish_metrics_if_due(time.monotonic())
        loop_elapsed = time.monotonic() - loop_started_at
        print(f"[football] ingestion loop completed in {loop_elapsed:.2f}s", flush=True)
        if historical_mode and HISTORICAL_DATE_CURSOR_ENABLED:
            assert current_cursor_date_obj is not None
            assert history_end_date_obj is not None
            if current_cursor_date_obj >= history_end_date_obj:
                print(
                    "[football] historical cursor reached HISTORY_END_DATE; exiting producer",
                    flush=True,
                )
                break
            current_cursor_date_obj = current_cursor_date_obj + timedelta(days=1)
            next_cursor_iso = current_cursor_date_obj.isoformat()
            print(f"[football] advancing historical cursor to {next_cursor_iso}", flush=True)
            sleep_seconds(POLL_INTERVAL_SECONDS)
            continue
        if stop_after_first_loop:
            print(
                "[football] HISTORICAL_RUN_ONCE=true and historical dates configured; exiting after first loop",
                flush=True,
            )
            break
        sleep_seconds(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
