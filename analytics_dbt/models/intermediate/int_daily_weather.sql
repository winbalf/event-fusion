{{ config(
    partition_by={"field": "date_day", "data_type": "date"},
    cluster_by=["country", "city"]
) }}

with weather_deduped as (
    select
        business_date_local,
        country,
        city,
        observed_at_utc,
        temperature_c,
        apparent_temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        wind_speed_10m,
        weather_code,
        cloud_cover_pct,
        consumed_at_utc,
        row_number() over (
            partition by business_date_local, country, city, observed_at_utc
            order by consumed_at_utc desc
        ) as rn
    from {{ ref("stg_weather") }}
    where business_date_local is not null
      and country is not null
      and city is not null
      and observed_at_utc is not null
),
latest_observation as (
    select *
    from weather_deduped
    where rn = 1
)
select
    business_date_local as date_day,
    country,
    city,
    count(*) as weather_observations,
    round(avg(temperature_c), 2) as avg_temperature_c,
    round(min(temperature_c), 2) as min_temperature_c,
    round(max(temperature_c), 2) as max_temperature_c,
    round(avg(apparent_temperature_c), 2) as avg_apparent_temperature_c,
    round(avg(relative_humidity_pct), 2) as avg_relative_humidity_pct,
    round(sum(coalesce(precipitation_mm, 0)), 2) as total_precipitation_mm,
    round(avg(wind_speed_10m), 2) as avg_wind_speed_10m,
    round(max(wind_speed_10m), 2) as max_wind_speed_10m,
    round(avg(cloud_cover_pct), 2) as avg_cloud_cover_pct,
    countif(weather_code is not null) as coded_weather_observations
from latest_observation
group by 1, 2, 3
