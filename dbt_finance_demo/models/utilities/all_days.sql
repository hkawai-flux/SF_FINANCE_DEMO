{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    ) }}
)
select
    date_day::date as date_day
from date_spine