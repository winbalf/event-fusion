import os
from typing import Iterable

from google.cloud import bigquery
from google.cloud import storage


def getenv_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def list_matching_parquet_uris(
    storage_client: storage.Client,
    *,
    bucket_name: str,
    prefixes: Iterable[str],
) -> list[str]:
    uris: list[str] = []
    bucket = storage_client.bucket(bucket_name)
    for prefix in prefixes:
        for blob in storage_client.list_blobs(bucket, prefix=prefix):
            if blob.name.endswith(".parquet"):
                uris.append(f"gs://{bucket_name}/{blob.name}")
    return sorted(set(uris))


def create_or_replace_external_table(
    bq_client: bigquery.Client,
    *,
    table_fqn: str,
    source_uris: list[str],
    schema: list[bigquery.SchemaField],
) -> None:
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = source_uris
    external_config.autodetect = False

    table = bigquery.Table(table_fqn)
    table.schema = schema
    table.external_data_configuration = external_config
    bq_client.delete_table(table_fqn, not_found_ok=True)
    bq_client.create_table(table)
    print(f"created external table: {table_fqn}")


def main() -> None:
    project_id = getenv_required("GCP_PROJECT_ID")
    dataset = getenv_required("BIGQUERY_DATASET")
    bucket = getenv_required("GCS_BUCKET")

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
    bronze_schema = [
        bigquery.SchemaField("kafka_topic", "STRING"),
        bigquery.SchemaField("ingested_at", "STRING"),
        bigquery.SchemaField("consumed_at", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("source_type", "STRING"),
        bigquery.SchemaField("schema_version", "STRING"),
        bigquery.SchemaField("idempotency_key", "STRING"),
        bigquery.SchemaField("metadata_json", "STRING"),
        bigquery.SchemaField("payload_json", "STRING"),
    ]

    dataset_id = f"{project_id}.{dataset}"
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = "us-central1"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    print(f"ensured dataset exists: {dataset_id}")

    weather_prefixes = [
        "bronze/topic=raw.weather.v1/",
    ]
    football_prefixes = [
        "bronze/topic=raw.football.v1/",
    ]

    weather_uris = list_matching_parquet_uris(
        storage_client,
        bucket_name=bucket,
        prefixes=weather_prefixes,
    )
    football_uris = list_matching_parquet_uris(
        storage_client,
        bucket_name=bucket,
        prefixes=football_prefixes,
    )

    print(f"weather parquet files found: {len(weather_uris)}")
    print(f"football parquet files found: {len(football_uris)}")
    if not weather_uris:
        raise RuntimeError("No weather parquet files found under bronze prefixes")
    if not football_uris:
        raise RuntimeError("No football parquet files found under bronze prefixes")

    create_or_replace_external_table(
        bq_client,
        table_fqn=f"{dataset_id}.bronze_weather_raw",
        source_uris=weather_uris,
        schema=bronze_schema,
    )
    create_or_replace_external_table(
        bq_client,
        table_fqn=f"{dataset_id}.bronze_football_raw",
        source_uris=football_uris,
        schema=bronze_schema,
    )

    print("bronze external tables are ready")


if __name__ == "__main__":
    main()
