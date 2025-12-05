/*
==================================================================================
Data Segmentation
==================================================================================
Script Purpose:
	This script performs data segmentation. It segments customers and products
	based on their lifespan as well as revenue generated. It also segments 
	customers into various age groups.
==================================================================================
*/
-- What age group is most of our customers categorized into?
SELECT
	age_group,
	COUNT(customer_id) AS total_customers
FROM
(
SELECT
	customer_id,
	age,
	CASE
		WHEN age < 20 THEN 'Below 20'
		WHEN age BETWEEN 20 AND 29 THEN '20-29'
		WHEN age BETWEEN 30 AND 39 THEN '30-39'
		WHEN age BETWEEN 40 AND 49 THEN '40-49'
		WHEN age BETWEEN 50 AND 59 THEN '50-59'
		ELSE 'Above 59'
	END AS age_group
FROM gold.dim_customers_view
)SUB
GROUP BY age_group
ORDER BY total_customers DESC;

-- How many of our customers are VIPs?
SELECT
	customer_status,
	COUNT(customer_id) AS total_customers,
	ROUND((CAST(COUNT(customer_id) AS FLOAT)/SUM(COUNT(customer_id)) OVER()) * 100, 2) AS percent_cust_dist
FROM
(
	SELECT
		customer_id,
		customer_name,
		first_order_time,
		last_order_time,
		monthly_history,
		total_sales,
		CASE	
			WHEN monthly_history > 12 AND total_sales > 5000 THEN 'VIP'
			WHEN monthly_history > 12 AND total_sales < 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_status
	FROM
	(
		SELECT
			c.customer_id,
			c.customer_name,
			MIN(o.order_time) AS first_order_time,
			MAX(o.order_time) AS last_order_time,
			DATEDIFF(month, MIN(o.order_time), MAX(o.order_time)) AS monthly_history,
			SUM(o.total_usd) AS total_sales
		FROM gold.fact_orders_view o
		LEFT JOIN gold.dim_customers_view c
		ON o.customer_key = c.customer_key
		GROUP BY c.customer_id, c.customer_name
	)SUB1
)SUB2
GROUP BY customer_status
ORDER BY percent_cust_dist DESC;

-- Are most of our products are high performing products?
SELECT
	product_status,
	COUNT(product_id) AS total_products,
	ROUND((CAST(COUNT(product_id) AS FLOAT)/SUM(COUNT(product_id)) OVER()) * 100, 2) AS percent_product_dist
FROM
(
	SELECT
		product_id,
		product_name,
		first_order_time,
		last_order_time,
		lifespan,
		total_sales,
		CASE
			WHEN lifespan > 12 AND total_sales > 5000 THEN 'High Performer'
			WHEN lifespan > 12 AND total_sales < 5000 THEN 'Mid Performer'
			ELSE 'Low Performer'
		END AS product_status
	FROM
	(
		SELECT
			p.product_id,
			p.product_name,
			MIN(o.order_time) AS first_order_time,
			MAX(o.order_time) AS last_order_time,
			DATEDIFF(month, MIN(o.order_time), MAX(o.order_time)) AS lifespan,
			ROUND(SUM((1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd), 2) AS total_sales
		FROM gold.fact_order_items_view oi
		LEFT JOIN gold.fact_orders_view o
		ON oi.order_id = o.order_id
		LEFT JOIN gold.dim_products_view p
		ON oi.product_key = p.product_key
		GROUP BY p.product_id, p.product_name
	)SUB1
)SUB2
GROUP BY product_status
ORDER BY percent_product_dist DESC;
