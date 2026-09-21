SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    tier,
    member_count,
    member_change,
    member_percentage,
    total_members
FROM `mart_aeon_360.fct_360_main__membership_tier_distribution_report`;