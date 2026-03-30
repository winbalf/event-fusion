{{ config(
    partition_by={"field": "date_day", "data_type": "date"},
    cluster_by=["country", "league_code"]
) }}

with weather_country_day as (
    select
        date_day,
        country,
        count(distinct city) as weather_cities_covered,
        sum(weather_observations) as weather_observations,
        round(avg(avg_temperature_c), 2) as avg_temperature_c,
        round(min(min_temperature_c), 2) as min_temperature_c,
        round(max(max_temperature_c), 2) as max_temperature_c,
        round(avg(avg_apparent_temperature_c), 2) as avg_apparent_temperature_c,
        round(avg(avg_relative_humidity_pct), 2) as avg_relative_humidity_pct,
        round(sum(total_precipitation_mm), 2) as total_precipitation_mm,
        round(avg(avg_wind_speed_10m), 2) as avg_wind_speed_10m,
        round(max(max_wind_speed_10m), 2) as max_wind_speed_10m,
        round(avg(avg_cloud_cover_pct), 2) as avg_cloud_cover_pct
    from {{ ref("int_daily_weather") }}
    group by 1, 2
),
football_league_day as (
    select
        date_day,
        country,
        league_code,
        league_name,
        matches_count,
        completed_matches_count,
        scheduled_matches_count,
        in_progress_matches_count,
        total_home_goals,
        total_away_goals,
        total_goals,
        avg_goals_per_match,
        home_wins,
        away_wins,
        draws
    from {{ ref("int_daily_football") }}
),
joined as (
    select
        f.date_day,
        f.country,
        f.league_code,
        f.league_name,
        w.weather_cities_covered,
        w.weather_observations,
        w.avg_temperature_c,
        w.min_temperature_c,
        w.max_temperature_c,
        w.avg_apparent_temperature_c,
        w.avg_relative_humidity_pct,
        w.total_precipitation_mm,
        w.avg_wind_speed_10m,
        w.max_wind_speed_10m,
        w.avg_cloud_cover_pct,
        f.matches_count,
        f.completed_matches_count,
        f.scheduled_matches_count,
        f.in_progress_matches_count,
        f.total_home_goals,
        f.total_away_goals,
        f.total_goals,
        f.avg_goals_per_match,
        f.home_wins,
        f.away_wins,
        f.draws,
        safe_divide(f.total_goals, nullif(w.total_precipitation_mm, 0)) as goals_per_precip_mm
    from football_league_day f
    left join weather_country_day w
        on f.date_day = w.date_day
       and f.country = w.country
)
select
    concat(cast(date_day as string), "|", country, "|", league_code) as mart_row_id,
    date_day,
    country,
    league_code,
    league_name,
    weather_cities_covered,
    weather_observations,
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_apparent_temperature_c,
    avg_relative_humidity_pct,
    total_precipitation_mm,
    avg_wind_speed_10m,
    max_wind_speed_10m,
    avg_cloud_cover_pct,
    matches_count,
    completed_matches_count,
    scheduled_matches_count,
    in_progress_matches_count,
    total_home_goals,
    total_away_goals,
    total_goals,
    avg_goals_per_match,
    home_wins,
    away_wins,
    draws,
    goals_per_precip_mm,
    weather_cities_covered is not null as has_weather_data
from joined
