SELECT TIMESTAMP(snapshot_date, 'Asia/Kuala_Lumpur') AS etl_run_timestamp,
    new_count,
    new_change,
    established_count,
    established_change,
    trusted_count,
    trusted_change,
    veteran_count,
    veteran_change,
    _transform_at as created_at
FROM `mart_aeon_ac.fct_ac__opco_tenuredetail_members_tenure_overview_report_fact`
WHERE snapshot_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    AND CURRENT_DATE()
ORDER BY snapshot_date DESC;