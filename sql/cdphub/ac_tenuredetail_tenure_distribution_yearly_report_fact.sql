SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    cohort_year,
    active_members_count,
    inactive_members_count,
    total_members_count,
    active_percentage,
    inactive_percentage,
    _transform_at as created_at
FROM `mart_aeon_ac.fct_ac__opco_tenuredetail_tenure_distribution_yearly_report_fact`;