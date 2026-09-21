SELECT
    TIMESTAMP(snapshot_date, 'Asia/Kuala_Lumpur') AS etl_run_timestamp,
    lifecycle_stage,
    is_multi_opco,
    customer_count,
    weekly_change,
    change_direction,
    is_positive_stage
FROM `mart_aeon_360.fct_360_main__lifecycle_stage`
WHERE snapshot_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND CURRENT_DATE()
ORDER BY snapshot_date DESC;