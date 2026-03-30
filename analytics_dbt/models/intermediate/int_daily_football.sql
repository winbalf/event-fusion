{{ config(
    partition_by={"field": "date_day", "data_type": "date"},
    cluster_by=["country", "league_code"]
) }}

with football_latest_event as (
    select
        business_date_local,
        country,
        league_code,
        league_name,
        event_id,
        event_start_utc,
        event_status_type,
        event_status_state,
        home_team_score,
        away_team_score,
        total_goals,
        consumed_at_utc,
        row_number() over (
            partition by business_date_local, league_code, event_id
            order by consumed_at_utc desc
        ) as rn
    from {{ ref("stg_football") }}
    where business_date_local is not null
      and league_code is not null
      and event_id is not null
),
latest_event_snapshot as (
    select *
    from football_latest_event
    where rn = 1
)
select
    business_date_local as date_day,
    country,
    league_code,
    league_name,
    count(*) as matches_count,
    countif(lower(coalesce(event_status_state, "")) = "post") as completed_matches_count,
    countif(lower(coalesce(event_status_state, "")) = "pre") as scheduled_matches_count,
    countif(lower(coalesce(event_status_state, "")) = "in") as in_progress_matches_count,
    sum(coalesce(home_team_score, 0)) as total_home_goals,
    sum(coalesce(away_team_score, 0)) as total_away_goals,
    sum(coalesce(total_goals, 0)) as total_goals,
    round(avg(cast(total_goals as float64)), 2) as avg_goals_per_match,
    countif(coalesce(home_team_score, -1) > coalesce(away_team_score, -1)) as home_wins,
    countif(coalesce(away_team_score, -1) > coalesce(home_team_score, -1)) as away_wins,
    countif(
        home_team_score is not null
        and away_team_score is not null
        and home_team_score = away_team_score
    ) as draws
from latest_event_snapshot
group by 1, 2, 3, 4
