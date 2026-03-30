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
    from {{ source("bronze", "football_raw") }}
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
        json_value(metadata_json, "$.league_code") as league_code,
        json_value(metadata_json, "$.league_name") as league_name,
        coalesce(safe_cast(json_value(metadata_json, "$.historical_mode") as bool), false) as historical_mode,
        nullif(json_value(metadata_json, "$.start_date"), "") as start_date,
        nullif(json_value(metadata_json, "$.end_date"), "") as end_date
    from bronze
),
events_flattened as (
    select
        b.*,
        event_json,
        json_query(event_json, "$.competitions[0]") as competition_json
    from base b
    cross join unnest(json_query_array(b.payload_json, "$.events")) as event_json
),
typed as (
    select
        kafka_topic,
        source,
        source_type,
        schema_version,
        idempotency_key,
        ingested_at_utc,
        consumed_at_utc,
        country,
        league_code,
        league_name,
        historical_mode,
        start_date,
        end_date,
        json_value(event_json, "$.id") as event_id,
        json_value(event_json, "$.uid") as event_uid,
        json_value(event_json, "$.name") as event_name,
        json_value(event_json, "$.shortName") as event_short_name,
        coalesce(
            safe.parse_timestamp("%Y-%m-%dT%H:%M%Ez", json_value(event_json, "$.date")),
            safe.parse_timestamp("%Y-%m-%dT%H:%M:%E*S%Ez", json_value(event_json, "$.date")),
            safe.parse_timestamp("%Y-%m-%dT%H:%M%Ez", json_value(competition_json, "$.date")),
            safe.parse_timestamp("%Y-%m-%dT%H:%M:%E*S%Ez", json_value(competition_json, "$.date"))
        ) as event_start_utc,
        json_value(event_json, "$.status.type.name") as event_status_type,
        json_value(event_json, "$.status.type.state") as event_status_state,
        json_value(event_json, "$.status.type.detail") as event_status_detail,
        safe_cast(json_value(event_json, "$.season.year") as int64) as season_year,
        json_value(event_json, "$.season.slug") as season_slug,
        json_value(competition_json, "$.venue.fullName") as venue_name,
        json_value(competition_json, "$.venue.address.city") as venue_city,
        json_value(competition_json, "$.venue.address.country") as venue_country,
        (
            select json_value(comp, "$.team.id")
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "home"
            limit 1
        ) as home_team_id,
        (
            select json_value(comp, "$.team.displayName")
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "home"
            limit 1
        ) as home_team_name,
        (
            select safe_cast(json_value(comp, "$.score") as int64)
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "home"
            limit 1
        ) as home_team_score,
        (
            select json_value(comp, "$.team.id")
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "away"
            limit 1
        ) as away_team_id,
        (
            select json_value(comp, "$.team.displayName")
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "away"
            limit 1
        ) as away_team_name,
        (
            select safe_cast(json_value(comp, "$.score") as int64)
            from unnest(json_query_array(competition_json, "$.competitors")) as comp
            where json_value(comp, "$.homeAway") = "away"
            limit 1
        ) as away_team_score
    from events_flattened
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
    league_code,
    league_name,
    historical_mode,
    start_date,
    end_date,
    event_id,
    event_uid,
    event_name,
    event_short_name,
    event_start_utc,
    date(event_start_utc) as business_date_local,
    event_status_type,
    event_status_state,
    event_status_detail,
    season_year,
    season_slug,
    venue_name,
    venue_city,
    venue_country,
    home_team_id,
    home_team_name,
    home_team_score,
    away_team_id,
    away_team_name,
    away_team_score,
    safe_cast(home_team_score + away_team_score as int64) as total_goals
from typed
