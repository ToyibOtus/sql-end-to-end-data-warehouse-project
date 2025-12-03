/*
============================================================================
Magnitude Analysis
============================================================================
Script Purpose:
	This script carries out surface level analytics. It asks important 
	business questions, and dives into how key business metrics such as 
	orders, quantity, revenue, profit etc. are distributed across various 
	dimensions.
============================================================================
*/
-- Which country populates the highest number of customers?
SELECT
	country_code,
	country_name,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers_view 
GROUP BY country_code, country_name
ORDER BY total_customers DESC;

-- Are most of our customers in each country ordering?
SELECT 
	c.country_code,
	c.country_name,
	COUNT(DISTINCT c.customer_key) AS total_customers,
	COUNT(DISTINCT o.customer_key) AS total_customers_ordered,
	FORMAT(CAST(COUNT(DISTINCT o.customer_key) AS FLOAT)/
	COUNT(DISTINCT c.customer_key), 'P') AS percent_cust_ordered
FROM gold.dim_customers_view c 
LEFT JOIN gold.fact_orders_view o
ON o.customer_key = c.customer_key
GROUP BY c.country_code, c.country_name
ORDER BY total_customers_ordered DESC;

-- Which country has the highest percentage of customers that have not placed an order? 
SELECT 
	c.country_code,
	c.country_name,
	COUNT(DISTINCT c.customer_key) AS total_customers,
	COUNT(DISTINCT o.customer_key) AS total_customers_ordered,
	ROUND((CAST(COUNT(DISTINCT c.customer_key) AS FLOAT) - COUNT(DISTINCT o.customer_key))
	/COUNT(DISTINCT c.customer_key) * 100, 2) AS percent_customer_dev
FROM gold.dim_customers_view c 
LEFT JOIN gold.fact_orders_view o
ON o.customer_key = c.customer_key
GROUP BY c.country_code, c.country_name
ORDER BY percent_customer_dev DESC;

-- Which country brings in the most revenue?
SELECT
	c.country_code,
	c.country_name,
	SUM(o.total_usd) AS total_sales
FROM gold.fact_orders_view o
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.country_code, c.country_name
ORDER BY total_sales DESC;

-- Is high revenue in countries solely attributed to total orders?
WITH country_sales AS
(
SELECT 
	c.country_code,
	c.country_name,
	COUNT(o.order_id) AS total_orders,
	SUM(o.total_usd) AS total_sales
FROM gold.fact_orders_view o
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.country_code, c.country_name
)
-- Or are other factors such as average price a contributing factor?
SELECT
	cs.country_code,
	cs.country_name,
	cs.total_orders,
	p.weighted_avg_price,
	cs.total_sales
FROM country_sales cs
LEFT JOIN
(
	SELECT 
		country_code, 
		ROUND(SUM(weighted_price)/SUM(quantity), 2) AS weighted_avg_price 
	FROM
	(
		SELECT 
			o.country_code, oi.quantity, 
			CAST(oi.unit_price_usd * oi.quantity AS FLOAT) AS weighted_price 
		FROM gold.fact_orders_view o
		LEFT JOIN gold.fact_order_items_view oi
		ON o.order_id = oi.order_id
	)SUB
	GROUP BY country_code
)p
ON cs.country_code = p.country_code
ORDER BY total_sales DESC;

-- Who are our top customers based on total number of orders
SELECT 
	c.customer_id,
	c.customer_name,
	COUNT(DISTINCT o.order_id) AS total_orders
FROM gold.fact_orders_view o 
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.customer_id, c.customer_name
ORDER BY total_orders DESC;

-- Does the list differ in comparison to total revenue generated?
SELECT 
	c.customer_id,
	c.customer_name,
	COUNT(DISTINCT o.order_id) AS total_orders,
	SUM(o.total_usd) AS total_sales
FROM gold.fact_orders_view o 
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.customer_id, c.customer_name
ORDER BY total_sales DESC;

-- How many of our customers opted in for marketing information?
SELECT
	marketing_opt_in,
	COUNT(customer_id) AS total_customers
FROM gold.dim_customers_view
GROUP BY marketing_opt_in
ORDER BY total_customers DESC;

-- Which category of product is priotized by the organization?
SELECT
	category,
	COUNT(product_id) AS count_category
FROM gold.dim_products_view
GROUP BY category
ORDER BY count_category;

-- Which category of products generates the highest revenue?
SELECT
	category,
	ROUND(SUM(line_total_usd), 2) AS total_sales
FROM
(
	SELECT
		p.category,
		(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
	FROM gold.fact_orders_view o
	LEFT JOIN gold.fact_order_items_view oi
	ON o.order_id = oi.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)SUB1
GROUP BY category
ORDER BY total_sales DESC;

-- Does low price equate to higher demand? 
WITH category_data AS
(
	SELECT
		p.category,
		COUNT(DISTINCT oi.order_id) AS total_orders,
		ROUND(CAST(AVG(oi.unit_price_usd) AS FLOAT), 2) AS avg_price,
		ROUND(CAST(AVG(p.cost_usd) AS FLOAT), 2) AS avg_cost
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category
)
-- Which product category generates the highest profit, and is it solely driven by demand, high avg_price, or both?
SELECT
	cd.category,
	cd.total_orders,
	cd.avg_price,
	cd.avg_cost,
	tp.total_sales,
	tp.total_profit
FROM category_data cd
LEFT JOIN
(
	SELECT
		category,
		ROUND(SUM(line_total_usd), 2) AS total_sales,
		SUM(total_cost) AS total_cost,
		ROUND(SUM(line_total_usd) - SUM(total_cost), 2) total_profit
	FROM
	(
		SELECT
			p.category,
			o.discount_pct,
			oi.line_total_usd AS line_subtotal_usd,
			(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd,
			cost_usd * quantity AS total_cost
		FROM gold.fact_orders_view o
		LEFT JOIN gold.fact_order_items_view oi
		ON o.order_id = oi.order_id
		LEFT JOIN gold.dim_products_view p
		ON oi.product_key = p.product_key
	)SUB1
	GROUP BY category
)tp
ON cd.category = tp.category
ORDER BY total_profit DESC;

-- Are sales evenly distributed across products within the same category?
SELECT
	p.category,
	p.product_name,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_quantity
FROM gold.fact_order_items_view oi
LEFT JOIN gold.dim_products_view p
ON oi.product_key = p.product_key
GROUP BY p.category, p.product_name
ORDER BY p.category, total_quantity DESC;

-- Does high demand correlates with low price?
WITH product_data AS
(
	SELECT
		p.category,
		p.product_name,
		COUNT(DISTINCT order_id) AS total_orders,
		SUM(quantity) AS total_quantity,
		ROUND(CAST(AVG(oi.unit_price_usd) AS FLOAT), 2) AS avg_price,
		ROUND(CAST(AVG(p.cost_usd) AS FLOAT), 2) AS avg_cost
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category, p.product_name
)
-- Within each category, which product generates the highest revenue? and 
-- is it solely dependent on demand or are other factors at play?
SELECT
	pd.category,
	pd.product_name,
	pd.total_orders,
	pd.total_quantity,
	pd.avg_price,
	pd.avg_cost,
	tp.total_sales,
	tp.total_profit
FROM product_data pd
LEFT JOIN
(
	SELECT
		category,
		product_name,
		ROUND(SUM(line_total_usd), 2) AS total_sales,
		ROUND(SUM(line_cost_usd), 2) AS total_cost,
		ROUND(SUM(line_total_usd) - SUM(line_cost_usd), 2) AS total_profit
	FROM
	(
		SELECT
			p.category,
			p.product_name,
			(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd,
			p.cost_usd * oi.quantity AS line_cost_usd
		FROM gold.fact_order_items_view oi
		LEFT JOIN gold.fact_orders_view o
		ON oi.order_id = o.order_id
		LEFT JOIN gold.dim_products_view p
		ON oi.product_key = p.product_key
	)SUB
	GROUP BY category, product_name
) tp
ON pd.category = tp.category AND
pd.product_name = tp.product_name;

-- What traffic source drives most of our customers into a session?
SELECT
	traffic_source,
	total_sessions_per_source,
	FORMAT(CAST(total_sessions_per_source AS FLOAT)/total_sessions, 'P') AS percent_source_dist
FROM
(
	SELECT
		traffic_source,
		SUM(COUNT(user_session_id)) OVER() AS total_sessions,
		COUNT(user_session_id) AS total_sessions_per_source
	FROM gold.dim_sessions_view
	GROUP BY traffic_source
)SUB
ORDER BY total_sessions_per_source DESC;

-- How is this traffic source distributed across countries?
SELECT
	c.country_code,
	c.country_name,
	s.traffic_source,
	COUNT(user_session_id) total_nr_sessions
FROM gold.dim_sessions_view s
LEFT JOIN gold.dim_customers_view c
ON s.customer_key = c.customer_key
GROUP BY
	c.country_code,
	c.country_name,
	s.traffic_source
ORDER BY c.country_name, total_nr_sessions DESC;

-- What device is mostly used during sessions?
SELECT
	device,
	count_per_device,
	FORMAT(CAST(count_per_device AS FLOAT)/total_count, 'P') AS percent_count_dist
FROM
(
	SELECT
		device,
		COUNT(device) AS count_per_device,
		SUM(COUNT(device)) OVER() AS total_count
	FROM gold.dim_sessions_view
	GROUP BY device
)SUB
ORDER BY count_per_device DESC;

-- Are most of our customers coming in to buy?
SELECT
	event_type,
	count_per_event_type,
	FORMAT((CAST(count_per_event_type AS FLOAT)/total_event_type), 'P') AS percent_event_typ_dist
FROM
(
	SELECT
		event_type,
		COUNT(event_id) AS count_per_event_type,
		SUM(COUNT(event_id)) OVER() AS total_event_type
	FROM gold.fact_events_view
	GROUP BY event_type
)SUB
ORDER BY count_per_event_type DESC;

-- What product piques our customers' intrests?
SELECT
	p.product_name,
	COUNT(DISTINCT e.session_key) AS count_product
FROM gold.fact_events_view e
LEFT JOIN gold.dim_products_view p
ON e.product_key = p.product_key
WHERE p.product_name IS NOT NULL
GROUP BY p.product_name
ORDER BY count_product DESC;

-- Which of our products nearly resulted, or resulted to a purchase?
SELECT
	e.event_type,
	p.product_name,
	COUNT(event_id) AS count_product
FROM gold.fact_events_view e
LEFT JOIN gold.dim_products_view p
ON e.product_key = p.product_key
WHERE p.product_name IS NOT NULL AND e.event_type = 'add_to_cart'
GROUP BY e.event_type, p.product_name
ORDER BY count_product DESC;

-- What is the most common payment_method?
SELECT
	payment_method,
	count_payment_method,
	FORMAT(CAST(count_payment_method AS FLOAT)/total_count, 'P') AS percent_count_dist
FROM
(
	SELECT
		payment_method,
		COUNT(*) count_payment_method,
		SUM(COUNT(*)) OVER() AS total_count
	FROM gold.fact_events_view
	WHERE payment_method != 'N/A'
	GROUP BY payment_method
)SUB
ORDER BY count_payment_method DESC;

-- Are most of our products rated 5 stars?
SELECT
	rating,
	count_rating,
	FORMAT(CAST(count_rating AS FLOAT)/total_count, 'P') AS percent_count_dist
FROM
(
	SELECT
		rating,
		COUNT(*) AS count_rating,
		SUM(COUNT(*)) OVER() AS total_count
	FROM gold.fact_reviews_view
	GROUP BY rating
)SUB
ORDER BY count_rating DESC;

-- What are our most poorly rated products?
SELECT
	p.product_name,
	AVG(r.rating) AS avg_rating
FROM gold.fact_reviews_view r
LEFT JOIN gold.dim_products_view p
ON r.product_key = p.product_key
GROUP BY p.product_name
HAVING AVG(r.rating) BETWEEN 1 AND 2
ORDER BY avg_rating;
