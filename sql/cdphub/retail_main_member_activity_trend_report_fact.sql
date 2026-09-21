SELECT current_timestamp() as etl_run_timestamp,
    month_year,
    period_type,
    member_status,
    member_count,
    activity_percentage,
    total_members,
    change_vs_previous_period,
    member_count_last_year,
    yoy_delta,
    yoy_delta_percentage
FROM `mart_aeon_rt.fct_rt__opco_member_activity_trend`