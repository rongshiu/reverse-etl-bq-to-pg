SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    male_count,
    male_percentage,
    female_count,
    female_percentage,
    not_specified_count,
    not_specified_percentage,
    total_members as total_customers
FROM `mart_aeon_ab.fct_ab__opco_depthdetail_gender_distribution_report`;