SELECT
  CURRENT_TIMESTAMP() AS etl_run_timestamp,
  total_unified_member_count,
  total_member_count_at_retail,
  active_member_count_at_retail,
  inactive_member_count_at_retail,
  non_member_transaction_count_at_retail,
  member_active_percentage_at_retail,
  member_inactive_percentage_at_retail,
  member_penetration_percentage_at_retail,
  retail_transaction_start_date,
  retail_transaction_end_date,
  retail_transaction_date_range
FROM `mart_aeon_360.fct_360_main__customer_overview`;