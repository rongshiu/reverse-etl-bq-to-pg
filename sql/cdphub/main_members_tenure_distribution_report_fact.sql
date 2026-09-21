SELECT TIMESTAMP(snapshot_date, 'Asia/Kuala_Lumpur') AS etl_run_timestamp,
    type as member_type,
    new_bucket,
    established_bucket,
    trusted_bucket,
    veteran_bucket,
    total_index as total_members_index,
    index_change_percentage,
    _transform_at as created_at
FROM `mart_aeon_360.fct_360_main__tenure_distribution_report_fact`
WHERE snapshot_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    AND CURRENT_DATE()
ORDER BY snapshot_date DESC;