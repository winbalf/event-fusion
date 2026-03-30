with bronze as (
    select
        kafka_topic,
        safe_cast(ingested_at as timestamp) as ingested_at_utc,
        safe_cast(consumed_at as timestamp) as consumed_at_utc,
        source,
        source_type,
        schema_version,
        idempotency_key,
        metadata_json,
        payload_json
    from {{ source("bronze", "weather_raw") }}
),
base as (
    select
        kafka_topic,
        ingested_at_utc,
        consumed_at_utc,
        source,
        source_type,
        schema_version,
        idempotency_key,
        payload_json,
        json_value(metadata_json, "$.country") as country,
        json_value(metadata_json, "$.city") as city,
        safe_cast(json_value(metadata_json, "$.latitude") as float64) as latitude,
        safe_cast(json_value(metadata_json, "$.longitude") as float64) as longitude,
        coalesce(nullif(json_value(metadata_json, "$.timezone"), ""), "UTC") as timezone_name,
        coalesce(safe_cast(json_value(metadata_json, "$.historical_mode") as bool), false) as historical_mode,
        nullif(json_value(metadata_json, "$.start_date"), "") as start_date,
        nullif(json_value(metadata_json, "$.end_date"), "") as end_date
    from bronze
),
weather_current as (
    select
        kafka_topic,
        ingested_at_utc,
        consumed_at_utc,
        source,
        source_type,
        schema_version,
        idempotency_key,
        country,
        city,
        latitude,
        longitude,
        timezone_name,
        historical_mode,
        start_date,
        end_date,
        "current" as record_grain,
        coalesce(
            safe.parse_datetime("%Y-%m-%dT%H:%M:%E*S", json_value(payload_json, "$.current.time")),
            safe.parse_datetime("%Y-%m-%dT%H:%M", json_value(payload_json, "$.current.time"))
        ) as observed_at_local_dt,
        safe_cast(json_value(payload_json, "$.current.temperature_2m") as float64) as temperature_c,
        safe_cast(json_value(payload_json, "$.current.apparent_temperature") as float64) as apparent_temperature_c,
        safe_cast(json_value(payload_json, "$.current.relative_humidity_2m") as float64) as relative_humidity_pct,
        safe_cast(json_value(payload_json, "$.current.precipitation") as float64) as precipitation_mm,
        safe_cast(json_value(payload_json, "$.current.wind_speed_10m") as float64) as wind_speed_10m,
        safe_cast(json_value(payload_json, "$.current.wind_direction_10m") as int64) as wind_direction_10m,
        safe_cast(json_value(payload_json, "$.current.weather_code") as int64) as weather_code,
        safe_cast(json_value(payload_json, "$.current.cloud_cover") as int64) as cloud_cover_pct,
        safe_cast(json_value(payload_json, "$.current.is_day") as bool) as is_day
    from base
    where json_query(payload_json, "$.current") is not null
),
hourly_arrays as (
    select
        b.*,
        json_query_array(b.payload_json, "$.hourly.time") as hourly_time,
        json_query_array(b.payload_json, "$.hourly.temperature_2m") as hourly_temperature_2m,
        json_query_array(b.payload_json, "$.hourly.apparent_temperature") as hourly_apparent_temperature,
        json_query_array(b.payload_json, "$.hourly.relative_humidity_2m") as hourly_relative_humidity_2m,
        json_query_array(b.payload_json, "$.hourly.precipitation") as hourly_precipitation,
        json_query_array(b.payload_json, "$.hourly.wind_speed_10m") as hourly_wind_speed_10m,
        json_query_array(b.payload_json, "$.hourly.wind_direction_10m") as hourly_wind_direction_10m,
        json_query_array(b.payload_json, "$.hourly.weather_code") as hourly_weather_code,
        json_query_array(b.payload_json, "$.hourly.cloud_cover") as hourly_cloud_cover
    from base b
    where json_query(b.payload_json, "$.hourly") is not null
),
weather_hourly as (
    select
        b.kafka_topic,
        b.ingested_at_utc,
        b.consumed_at_utc,
        b.source,
        b.source_type,
        b.schema_version,
        b.idempotency_key,
        b.country,
        b.city,
        b.latitude,
        b.longitude,
        b.timezone_name,
        b.historical_mode,
        b.start_date,
        b.end_date,
        "hourly" as record_grain,
        coalesce(
            safe.parse_datetime(
                "%Y-%m-%dT%H:%M:%E*S",
                json_value(time_item, "$")
            ),
            safe.parse_datetime(
                "%Y-%m-%dT%H:%M",
                json_value(time_item, "$")
            )
        ) as observed_at_local_dt,
        safe_cast(json_value(b.hourly_temperature_2m[safe_offset(hour_idx)], "$") as float64) as temperature_c,
        safe_cast(json_value(b.hourly_apparent_temperature[safe_offset(hour_idx)], "$") as float64) as apparent_temperature_c,
        safe_cast(json_value(b.hourly_relative_humidity_2m[safe_offset(hour_idx)], "$") as float64) as relative_humidity_pct,
        safe_cast(json_value(b.hourly_precipitation[safe_offset(hour_idx)], "$") as float64) as precipitation_mm,
        safe_cast(json_value(b.hourly_wind_speed_10m[safe_offset(hour_idx)], "$") as float64) as wind_speed_10m,
        safe_cast(json_value(b.hourly_wind_direction_10m[safe_offset(hour_idx)], "$") as int64) as wind_direction_10m,
        safe_cast(json_value(b.hourly_weather_code[safe_offset(hour_idx)], "$") as int64) as weather_code,
        safe_cast(json_value(b.hourly_cloud_cover[safe_offset(hour_idx)], "$") as int64) as cloud_cover_pct,
        cast(null as bool) as is_day
    from hourly_arrays b
    cross join unnest(b.hourly_time) as time_item with offset as hour_idx
),
unioned as (
    select * from weather_current
    union all
    select * from weather_hourly
)
select
    kafka_topic,
    source,
    source_type,
    schema_version,
    idempotency_key,
    ingested_at_utc,
    consumed_at_utc,
    country,
    city,
    latitude,
    longitude,
    timezone_name,
    historical_mode,
    start_date,
    end_date,
    record_grain,
    observed_at_local_dt,
    case
        when observed_at_local_dt is null then null
        else timestamp(observed_at_local_dt, timezone_name)
    end as observed_at_utc,
    date(observed_at_local_dt) as business_date_local,
    temperature_c,
    apparent_temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    wind_speed_10m,
    wind_direction_10m,
    weather_code,
    cloud_cover_pct,
    is_day
from unioned
