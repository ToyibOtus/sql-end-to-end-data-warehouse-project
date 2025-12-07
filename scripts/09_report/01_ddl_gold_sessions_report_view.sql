/*
======================================================================================
Session Report
======================================================================================
Script Purpose:
	This script consolidates key session & event metrics. It performs the
	following operations:
	
	1. Retrieve essential fields from sessions table such as session_key,
		user_session_is, customer_key etc.
	2. Performs data aggregations, these include:
		* event timestamp start,
		* event timestamp end,
		* event duration minute,
		* total events,
		* total products interacted,
		* total products carted,
		* total quantity carted,
		* total nr checkouts,
		* cart size at check_out,
		* total nr purchases,
		* revenue generated.
	3. Segment products into:
		* High Performer, Mid Performer, and Low Performer based on their lifespan
		  and total revenue generated.
	4. Calculate valuable KPIs:
		* recency (months since last ordered)
		* average order revenue,
		* average monthly revenue.
	5. Retrieve average product ratings.
	6. Segments sessions into:
		* checkout_startedcheckout_started,
		* purchase_made,
		* completed_checkout.
======================================================================================
*/
IF OBJECT_ID('gold.sessions_report_view', 'V') IS NOT NULL
DROP VIEW gold.sessions_report_view;
GO

CREATE VIEW gold.sessions_report_view AS
-- Retrieve essential fields
WITH base_query AS
(
	SELECT
		s.session_key,
		s.user_session_id,
		s.customer_key,
		c.customer_name,
		s.country_code,
		c.country_name,
		c.marketing_opt_in,
		c.signup_date,
		s.start_time,
		s.device,
		s.traffic_source,
		e.event_id,
		e.event_timestamp,
		e.event_type,
		e.product_key,
		e.quantity,
		e.cart_size,
		e.payment_method,
		e.discount_pct,
		e.amount_usd
	FROM gold.dim_sessions s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	LEFT JOIN gold.fact_events e
	ON s.session_key = e.session_key
	WHERE e.event_timestamp IS NOT NULL
)
-- Aggregate key business metrics
, session_aggregation AS
(
	SELECT
		session_key,
		user_session_id,
		customer_key,
		customer_name,
		country_code,
		country_name,
		marketing_opt_in,
		signup_date,
		start_time,
		device,
		traffic_source,
		MIN(event_timestamp) AS event_timestamp_start,
		MAX(event_timestamp) AS event_timestamp_end,
		DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) AS event_duration_minute,
		COUNT(DISTINCT event_id) AS total_events,
		COUNT(DISTINCT product_key) AS total_products_interacted,
		SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_products_carted,
		SUM(quantity) AS total_quantity_carted,
		SUM(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS total_nr_checkouts,
		SUM(CASE WHEN event_type = 'checkout' THEN cart_size ELSE 0 END) AS cart_size_at_check_out,
		SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_nr_purchases,
		SUM(amount_usd) AS revenue_generated
	FROM base_query
	GROUP BY
		session_key,
		user_session_id,
		customer_key,
		customer_name,
		country_code,
		country_name,
		marketing_opt_in,
		signup_date,
		start_time,
		device,
		traffic_source
)
-- Segment based on checkouts, purchases, and completed checkouts
SELECT
	session_key,
	user_session_id,
	customer_key,
	customer_name,
	country_code,
	country_name,
	marketing_opt_in,
	signup_date,
	start_time,
	device,
	traffic_source,
	event_timestamp_start,
	event_timestamp_end,
	event_duration_minute,
	total_events,
	total_products_interacted,
	total_products_carted,
	total_quantity_carted,
	total_nr_checkouts,
	cart_size_at_check_out,
	total_nr_purchases,
	revenue_generated,
	CASE
		WHEN total_nr_checkouts > 0 THEN 'Yes'
		ELSE 'No'
	END AS checkout_started,
	CASE
		WHEN total_nr_purchases > 0 THEN 'Yes'
		ELSE 'No'
	END AS purchase_made,
	CASE
		WHEN total_nr_checkouts > 0 AND total_nr_purchases = 0 THEN 'Abandoned'
		WHEN total_nr_checkouts > 0 AND total_nr_purchases > 0 THEN 'Completed'
		ELSE 'No Checkout'
	END AS completed_checkout
FROM session_aggregation
