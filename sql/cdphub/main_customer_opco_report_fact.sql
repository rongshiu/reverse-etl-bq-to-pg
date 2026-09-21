SELECT
  CURRENT_TIMESTAMP() AS etl_run_timestamp,
  customer_count,
  ROUND(SAFE_DIVIDE(member_count, customer_count) * 100, 2) AS member_percentage,
  ROUND(SAFE_DIVIDE(non_member_count, customer_count) * 100, 2) AS non_member_percentage,
  member_count,
  non_member_count,
  percentage_share,
  CASE
    WHEN opco_name = 'AMY' THEN 'AEON Retail'
    WHEN opco_name = 'ABG' THEN 'AEON Big'
    ELSE opco_name
  END AS opco_name
FROM `mart_aeon_360.fct_360_main__opco_customer`;