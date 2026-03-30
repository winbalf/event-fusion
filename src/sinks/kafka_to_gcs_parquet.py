import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from kafka import KafkaConsumer


def getenv_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
KAFKA_TOPICS = [
    topic.strip()
    for topic in os.getenv("KAFKA_TOPICS", "raw.weather.v1,raw.football.v1").split(",")
    if topic.strip()
]
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "parquet-writer-v1")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
KAFKA_POLL_TIMEOUT_MS = getenv_int("KAFKA_POLL_TIMEOUT_MS", 1000)
MAX_BATCH_ROWS_PER_TOPIC = getenv_int("MAX_BATCH_ROWS_PER_TOPIC", 100)
MIN_FLUSH_ROWS_PER_TOPIC = getenv_int("MIN_FLUSH_ROWS_PER_TOPIC", 25)
FLUSH_INTERVAL_SECONDS = getenv_int("FLUSH_INTERVAL_SECONDS", 30)
FORCE_FLUSH_INTERVAL_SECONDS = getenv_int("FORCE_FLUSH_INTERVAL_SECONDS", 180)
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "bronze").strip().strip("/")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_json_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def sanitize_partition_value(value: Any, *, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if not text:
        return fallback
    normalized = re.sub(r"[^a-z0-9]+", "_", text)
    normalized = normalized.strip("_")
    return normalized or fallback


def partition_event_date(ingested_at: Any, consumed_at: datetime) -> str:
    if isinstance(ingested_at, str):
        date_prefix = ingested_at.strip()[:10]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_prefix):
            return date_prefix
    return consumed_at.date().isoformat()


def normalize_message(topic: str, message: dict[str, Any], consumed_at: datetime) -> dict[str, Any]:
    payload = message.get("payload")
    metadata = message.get("metadata")
    metadata_obj = metadata if isinstance(metadata, dict) else {}
    event_date = partition_event_date(message.get("ingested_at"), consumed_at)
    country = sanitize_partition_value(metadata_obj.get("country"))
    league = sanitize_partition_value(metadata_obj.get("league_code"), fallback="all")
    return {
        "kafka_topic": topic,
        "ingested_at": message.get("ingested_at"),
        "consumed_at": consumed_at.isoformat(),
        "source": message.get("source"),
        "source_type": message.get("source_type"),
        "idempotency_key": message.get("idempotency_key"),
        "schema_version": message.get("schema_version"),
        "partition_event_date": event_date,
        "partition_country": country,
        "partition_league": league,
        "metadata_json": to_json_safe(metadata),
        "payload_json": to_json_safe(payload),
    }


def build_batch_id(rows: list[dict[str, Any]]) -> str:
    normalized_rows = sorted(
        [
            {
                "idempotency_key": row.get("idempotency_key"),
                "source": row.get("source"),
                "source_type": row.get("source_type"),
                "payload_json": row.get("payload_json"),
            }
            for row in rows
        ],
        key=lambda row: f"{row.get('idempotency_key')}|{row.get('source')}|{row.get('source_type')}",
    )
    canonical = json.dumps(normalized_rows, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(canonical.encode("utf-8")).hexdigest()[:16]


def upload_rows(
    gcs_client: storage.Client,
    *,
    bucket_name: str,
    prefix: str,
    topic: str,
    event_date: str,
    country: str,
    league: str,
    rows: list[dict[str, Any]],
) -> tuple[str, bool]:
    batch_id = build_batch_id(rows)
    blob_path = (
        f"{prefix}/topic={topic}/event_date={event_date}/country={country}/league={league}/"
        f"batch={batch_id}.parquet"
    )

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if blob.exists():
        return blob_path, False

    table = pa.Table.from_pylist(rows)
    parquet_bytes = BytesIO()
    pq.write_table(table, parquet_bytes, compression="snappy")
    parquet_bytes.seek(0)
    blob.upload_from_file(parquet_bytes, content_type="application/octet-stream")
    return blob_path, True


def main() -> None:
    if not GCS_BUCKET:
        raise ValueError("Missing required environment variable: GCS_BUCKET")
    if not KAFKA_TOPICS:
        raise ValueError("KAFKA_TOPICS is empty; provide at least one topic")

    print(
        f"[parquet-writer] starting with topics={KAFKA_TOPICS}, bucket={GCS_BUCKET}, prefix={GCS_PREFIX}",
        flush=True,
    )

    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    gcs_client = storage.Client()

    buffered_by_partition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    first_buffered_at_by_partition: dict[str, float] = {}
    skipped_uploads_total = 0
    uploaded_files_total = 0

    while True:
        records_map = consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT_MS)
        consumed_at = utc_now()
        for topic_partition, records in records_map.items():
            topic = topic_partition.topic
            for record in records:
                normalized = normalize_message(topic, record.value, consumed_at)
                partition_key = (
                    f"{topic}|{normalized['partition_event_date']}|"
                    f"{normalized['partition_country']}|{normalized['partition_league']}"
                )
                if partition_key not in first_buffered_at_by_partition:
                    first_buffered_at_by_partition[partition_key] = time.monotonic()
                buffered_by_partition[partition_key].append(normalized)

        total_buffered = sum(len(rows) for rows in buffered_by_partition.values())
        if not total_buffered:
            continue

        now_monotonic = time.monotonic()
        for partition_key, rows in list(buffered_by_partition.items()):
            if not rows:
                continue
            buffered_age = now_monotonic - first_buffered_at_by_partition.get(partition_key, now_monotonic)
            flush_due_to_size = len(rows) >= MAX_BATCH_ROWS_PER_TOPIC
            flush_due_to_time = (
                buffered_age >= FLUSH_INTERVAL_SECONDS
                and len(rows) >= MIN_FLUSH_ROWS_PER_TOPIC
            )
            force_flush_due_to_age = buffered_age >= FORCE_FLUSH_INTERVAL_SECONDS
            if not (flush_due_to_size or flush_due_to_time or force_flush_due_to_age):
                continue

            topic, event_date, country, league = partition_key.split("|", 3)
            blob_path, uploaded = upload_rows(
                gcs_client,
                bucket_name=GCS_BUCKET,
                prefix=GCS_PREFIX,
                topic=topic,
                event_date=event_date,
                country=country,
                league=league,
                rows=rows,
            )
            if uploaded:
                uploaded_files_total += 1
                print(
                    f"[parquet-writer] uploaded topic={topic} event_date={event_date} "
                    f"country={country} league={league} rows={len(rows)} "
                    f"to gs://{GCS_BUCKET}/{blob_path}",
                    flush=True,
                )
            else:
                skipped_uploads_total += 1
                print(
                    f"[parquet-writer] skipped duplicate batch topic={topic} event_date={event_date} "
                    f"country={country} league={league} rows={len(rows)} "
                    f"existing gs://{GCS_BUCKET}/{blob_path}",
                    flush=True,
                )
            buffered_by_partition[partition_key] = []
            first_buffered_at_by_partition.pop(partition_key, None)

        print(
            f"[parquet-writer][metrics] buffered_rows={sum(len(rows) for rows in buffered_by_partition.values())} "
            f"uploaded_files_total={uploaded_files_total} skipped_duplicate_uploads={skipped_uploads_total}",
            flush=True,
        )


if __name__ == "__main__":
    main()
