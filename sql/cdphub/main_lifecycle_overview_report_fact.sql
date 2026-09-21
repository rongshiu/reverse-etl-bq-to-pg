SELECT
    TIMESTAMP(snapshot_date, 'Asia/Kuala_Lumpur') AS etl_run_timestamp,
    total_members,
    new_members_count,
    active_members_count,
    active_members_percentage,
    at_risk_customers,
    churned_customers,
    weekly_member_change
FROM `mart_aeon_360.fct_360_main__lifecycle_overview`
WHERE snapshot_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND CURRENT_DATE()
ORDER BY snapshot_date DESC;