SELECT CURRENT_TIMESTAMP() AS etl_run_timestamp,
    total_customer as total_customers,
    male_count,
    male_percentage,
    female_count,
    female_percentage,
    not_specified_count,
    not_specified_percentage
FROM `mart_aeon_360.fct_360_main__gender_distribution_report`;