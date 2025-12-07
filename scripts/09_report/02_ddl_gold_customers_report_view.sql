/*
======================================================================================
Customer Report
======================================================================================
Script Purpose:
	This script consolidates key customer metrics and behaviours. It performs the
	following operations:
	
	1. Retrieve essential fields from customers table such as customer's key,
	   customer's id, name, age etc.
	2. Performs data aggregations, these include:
		* total orders made,
		* total quantity of products purchased,
		* total revenue generated,
		* total distinct products ordered,
		* lifespan in month.
	3. Segment customers into:
		* Various age groups (such as 'Below 20', 20-21, etc.).
		* VIP, Regular, and New based on their loyalty and total revenue generated.
	4. Calculate valuable KPIs:
		* recency (months since last ordered)
		* average order value,
		* average monthly spend.
	5. Retrieve session & event related business merices:
		* total sessions,
		* total events,
		* avg event_per_session,
		* total checkouts,
		* total purchases,
		* checkout conversion rate,
		* checkout abandonment rate.
======================================================================================
*/
IF OBJECT_ID('gold.customers_report_view', 'V') IS NOT NULL
DROP VIEW gold.customers_report_view;
GO

CREATE VIEW gold.customers_report_view AS
-- Retrieve essential columns
WITH base_query AS
(
	SELECT
		c.customer_key,
		c.customer_id,
		c.customer_name,
		c.email,
		c.country_code,
		c.country_name,
		c.age,
		c.signup_date,
		c.marketing_opt_in,
		o.order_id,
		o.order_time,
		o.payment_method,
		oi.product_key,
		oi.quantity,
		oi.unit_price_usd,
		o.discount_pct,
		oi.line_total_usd
	FROM gold.dim_customers c
	LEFT JOIN gold.fact_orders o
	ON c.customer_key = o.customer_key
	LEFT JOIN gold.fact_order_items oi
	ON o.order_id = oi.order_id
	WHERE o.order_time IS NOT NULL
)
-- Aggregate key business metrics
, customer_aggregation AS
(
	SELECT
		customer_key,
		customer_id,
		customer_name,
		email,
		country_code,
		country_name,
		age,
		signup_date,
		marketing_opt_in,
		MIN(order_time) AS first_order_time,
		MAX(order_time) AS last_order_time,
		DATEDIFF(month, MIN(order_time), MAX(order_time)) AS lifespan_month,
		COUNT(DISTINCT order_id) AS total_orders,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		CAST(SUM((1 - (discount_pct * 1.0)/100) * line_total_usd) AS DECIMAL(8, 2)) AS total_sales
	FROM base_query
	GROUP BY
		customer_key,
		customer_id,
		customer_name,
		email,
		country_code,
		country_name,
		age,
		signup_date,
		marketing_opt_in
)
-- Segment customers into various categories
, customer_segmentation AS
(
	SELECT
		customer_key,
		customer_id,
		customer_name,
		email,
		country_code,
		country_name,
		age,
		CASE 
			WHEN age < 20 THEN 'Below 20'
			WHEN age BETWEEN 20 AND 29 THEN '20-29'
			WHEN age BETWEEN 30 AND 39 THEN '30-39'
			WHEN age BETWEEN 40 AND 49 THEN '40-49'
			WHEN age BETWEEN 50 AND 59 THEN '50-59'
			ELSE 'Above 59'
		END AS age_group,
		signup_date,
		marketing_opt_in,
		CASE 
			WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN lifespan_month >= 12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_status,
		first_order_time,
		last_order_time,
		lifespan_month,
		total_orders,
		total_quantity,
		total_products,
		total_sales
	FROM customer_aggregation
)
-- Calculate valuable KPIs
SELECT
	cs.customer_key,
	customer_id,
	customer_name,
	email,
	country_code,
	country_name,
	age,
	age_group,
	signup_date,
	marketing_opt_in,
	customer_status,
	first_order_time,
	last_order_time,
	lifespan_month,
	total_orders,
	total_quantity,
	total_products,
	total_sales,
	s.total_sessions,
	s.total_events,
	s.avg_event_per_session,
	s.total_checkouts,
	s.total_purchases,
	s.checkout_conversion_rate,
	s.checkout_abandonment_rate,
	DATEDIFF(month, last_order_time, GETDATE()) AS recency_month,
	CASE
		WHEN total_orders = 0 THEN 0 
		ELSE CAST(ROUND(total_sales/total_orders, 2) AS DECIMAL(8, 2))
	END AS order_value,
	CASE	
		WHEN lifespan_month = 0 THEN total_sales
		ELSE CAST(ROUND(total_sales/lifespan_month, 2) AS DECIMAL(8, 2))
	END AS average_monthly_spend
FROM customer_segmentation cs
LEFT JOIN
(
SELECT
	customer_key,
	COUNT(user_session_id) AS total_sessions,
	SUM(total_events) AS total_events,
	CAST((SUM(total_events) * 1.0)/NULLIF(COUNT(user_session_id), 0) AS DECIMAL(8, 2)) AS avg_event_per_session,
	SUM(total_nr_checkouts) AS total_checkouts,
	SUM(total_nr_purchases) AS total_purchases,
	CAST((SUM(total_nr_purchases) * 1.0)/NULLIF(SUM(total_nr_checkouts), 0) AS DECIMAL(8, 2)) AS checkout_conversion_rate,
	CAST(((SUM(total_nr_checkouts) * 1.0) - SUM(total_nr_purchases))/
	NULLIF(SUM(total_nr_checkouts), 0) AS DECIMAL(8, 2)) AS checkout_abandonment_rate
FROM gold.sessions_report_view
GROUP BY customer_key
)s
ON cs.customer_key = s.customer_key;
