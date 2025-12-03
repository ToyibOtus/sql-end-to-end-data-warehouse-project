/*
=================================================================
Date Exploration
=================================================================
Script Purpose:
	This script aims to understand and explore the scope,
	and timespan of collected data. In addition, it checks the
	age range of our customers.
=================================================================
*/
-- When was the last time a new customer was added to the system?
SELECT
	MIN(signup_date) AS first_signup_date,
	MAX(signup_date) AS last_signup_date,
	DATEDIFF(year, MIN(signup_date), MAX(signup_date)) AS signup_timespan_year
FROM gold.dim_customers_view;

-- What is the timespan of our data?
SELECT
	MIN(YEAR(order_time)) AS first_order_year,
	MAX(YEAR(order_time)) AS last_order_year,
	DATEDIFF(year, MIN(order_time), MAX(order_time)) AS order_timespan_year
FROM gold.fact_orders_view;

SELECT
	MIN(age) AS youngest_customer,
	MAX(age) AS oldest_customer,
	MAX(age) - MIN(age) AS customer_age_range
FROM gold.dim_customers_view;
