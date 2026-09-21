SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    new_percentage,
    established_percentage,
    trusted_percentage,
    veteran_percentage,
    _transform_at as created_at
FROM `mart_aeon_bg.fct_bg__opco_tenuredetail_tenure_frequency_report_fact`;