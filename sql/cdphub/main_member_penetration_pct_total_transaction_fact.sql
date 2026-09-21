SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    month_year,
    total_transaction,
    member_penetration_percentage
FROM `mart_aeon_360.fct_360_main__member_penetration_pct_total_transaction_retail`;