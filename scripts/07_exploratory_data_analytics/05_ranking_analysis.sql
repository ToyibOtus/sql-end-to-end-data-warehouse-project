/*
============================================================================
Ranking Analysis
============================================================================
Script Purpose:
	This script ranks various dimensions using key business metrics.
	It aims to figure out the entities or dimensions that contribute both
	high and low to the total revenue.
============================================================================
*/
-- What are the top 5 countries with the largest number of customers?
SELECT
	country_code,
	country_name,
	total_customers,
	rank_country_customer_count
FROM
(
	SELECT
		country_code,
		country_name,
		COUNT(customer_key) AS total_customers,
		DENSE_RANK() OVER (ORDER BY COUNT(customer_key) DESC) AS rank_country_customer_count
	FROM gold.dim_customers_view 
	GROUP BY country_code, country_name
)SUB
WHERE rank_country_customer_count BETWEEN 1 AND 5;

-- What are the Top 5 countries with the smallest number of customers?
SELECT
	country_code,
	country_name,
	total_customers,
	rank_country_customer_count
FROM
(
	SELECT
		country_code,
		country_name,
		COUNT(customer_key) AS total_customers,
		DENSE_RANK() OVER (ORDER BY COUNT(customer_key)) AS rank_country_customer_count 
	FROM gold.dim_customers_view 
	GROUP BY country_code, country_name
)SUB
WHERE rank_country_customer_count BETWEEN 1 AND 5;

-- What are the top 5 countries with the largest number of customers ordered?
SELECT
	country_code,
	country_name,
	total_customers,
	total_customers_ordered,
	rank_country_orders
FROM
(
	SELECT 
		c.country_code,
		c.country_name,
		COUNT(DISTINCT c.customer_key) AS total_customers,
		COUNT(DISTINCT o.customer_key) AS total_customers_ordered,
		DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT o.customer_key) DESC) AS rank_country_orders 
	FROM gold.dim_customers_view c 
	LEFT JOIN gold.fact_orders_view o
	ON o.customer_key = c.customer_key
	GROUP BY c.country_code, c.country_name
)SUB
WHERE rank_country_orders BETWEEN 1 AND 5;

-- What are the top countries with the highest percentage of customers that haven't placed orders? 
SELECT
	country_code,
	country_name,
	total_customers,
	total_customers_ordered,
	percent_customer_dev AS percent_customer_dev,
	rank_country_orders
FROM
(
	SELECT
		country_code,
		country_name,
		total_customers,
		total_customers_ordered,
		percent_customer_dev AS percent_customer_dev,
		DENSE_RANK() OVER(ORDER BY percent_customer_dev DESC) AS rank_country_orders
	FROM
	(
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
	)SUB1
)SUB2
WHERE rank_country_orders BETWEEN 1 AND 5;

-- What are the top 5 countries that generate the most amount of revenue?
SELECT
	country_code,
	country_name,
	total_sales,
	rank_country_revenue
FROM
(
	SELECT
		c.country_code,
		c.country_name,
		SUM(o.total_usd) AS total_sales,
		DENSE_RANK() OVER(ORDER BY SUM(o.total_usd) DESC) rank_country_revenue
	FROM gold.fact_orders_view o
	LEFT JOIN gold.dim_customers_view c
	ON o.customer_key = c.customer_key
	GROUP BY c.country_code, c.country_name
)SUB
WHERE rank_country_revenue BETWEEN 1 AND 5;

-- What are the top 5 countries that generate the least amount of revenue?
SELECT
	country_code,
	country_name,
	total_sales,
	rank_country_revenue
FROM
(
	SELECT
		c.country_code,
		c.country_name,
		SUM(o.total_usd) AS total_sales,
		DENSE_RANK() OVER(ORDER BY SUM(o.total_usd)) rank_country_revenue
	FROM gold.fact_orders_view o
	LEFT JOIN gold.dim_customers_view c
	ON o.customer_key = c.customer_key
	GROUP BY c.country_code, c.country_name
)SUB
WHERE rank_country_revenue BETWEEN 1 AND 5;

-- What are our top 3 customers based on number of orders made?
SELECT
	customer_name,
	total_orders,
	rank_customers_orders
FROM
(
SELECT 
	c.customer_id,
	c.customer_name,
	COUNT(DISTINCT o.order_id) AS total_orders,
	DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT o.order_id) DESC) rank_customers_orders
FROM gold.fact_orders_view o 
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.customer_id, c.customer_name
)SUB
WHERE rank_customers_orders BETWEEN 1 AND 3;

-- What are our top 5 customers based on total revenue generated?
SELECT
	customer_name,
	total_sales,
	rank_customers_revenue
FROM
(
SELECT 
	c.customer_id,
	c.customer_name,
	SUM(o.total_usd) AS total_sales,
	DENSE_RANK() OVER(ORDER BY SUM(o.total_usd) DESC) rank_customers_revenue
FROM gold.fact_orders_view o 
LEFT JOIN gold.dim_customers_view c
ON o.customer_key = c.customer_key
GROUP BY c.customer_id, c.customer_name
)SUB
WHERE rank_customers_revenue BETWEEN 1 AND 5;

-- Within each category, what are the top 5 products with the highest orders?
SELECT
	category,
	product_name,
	total_orders,
	rank_product_orders
FROM
(
	SELECT
		p.category,
		p.product_name,
		COUNT(DISTINCT order_id) AS total_orders,
		DENSE_RANK() OVER(PARTITION BY p.category ORDER BY COUNT(DISTINCT order_id) DESC) rank_product_orders
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category, p.product_name
)SUB
WHERE rank_product_orders BETWEEN 1 AND 5;

-- Within each category, what are the top 5 products with the lowest orders?
SELECT
	category,
	product_name,
	total_orders,
	rank_product_orders
FROM
(
	SELECT
		p.category,
		p.product_name,
		COUNT(DISTINCT order_id) AS total_orders,
		DENSE_RANK() OVER(PARTITION BY p.category ORDER BY COUNT(DISTINCT order_id)) rank_product_orders
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category, p.product_name
)SUB
WHERE rank_product_orders BETWEEN 1 AND 5;

-- Within each category, what are the top 5 products with the highest quantity sold?
SELECT
	category,
	product_name,
	total_quantity,
	rank_product_quantity
FROM
(
	SELECT
		p.category,
		p.product_name,
		SUM(oi.quantity) AS total_quantity,
		DENSE_RANK() OVER(PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) rank_product_quantity
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category, p.product_name
)SUB
WHERE rank_product_quantity BETWEEN 1 AND 5;

-- Within each category, what are the top 5 products with the lowest quantity sold?
SELECT
	category,
	product_name,
	total_quantity,
	rank_product_quantity
FROM
(
	SELECT
		p.category,
		p.product_name,
		SUM(oi.quantity) AS total_quantity,
		DENSE_RANK() OVER(PARTITION BY p.category ORDER BY SUM(oi.quantity)) rank_product_quantity
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
	GROUP BY p.category, p.product_name
)SUB
WHERE rank_product_quantity BETWEEN 1 AND 5;

-- What are the top 5 products in each category that bring in the most revenue?
SELECT
	category,
	product_name,
	total_sales,
	rank_product_revenue
FROM
(
	SELECT
		category,
		product_name,
		ROUND(SUM(line_total_usd), 2) AS total_sales,
		DENSE_RANK() OVER(PARTITION BY category ORDER BY ROUND(SUM(line_total_usd), 2) DESC) AS rank_product_revenue
	FROM
	(
		SELECT
			p.category,
			p.product_name,
			(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
		FROM gold.fact_order_items_view oi
		LEFT JOIN gold.fact_orders_view o
		ON oi.order_id = o.order_id
		LEFT JOIN gold.dim_products_view p
		ON oi.product_key = p.product_key
	)SUB1
	GROUP BY category, product_name
)SUB2
WHERE rank_product_revenue BETWEEN 1 AND 5;

-- What are the top 5 products in each category that bring in the least revenue?
SELECT
	category,
	product_name,
	total_sales,
	rank_product_revenue
FROM
(
	SELECT
		category,
		product_name,
		ROUND(SUM(line_total_usd), 2) AS total_sales,
		DENSE_RANK() OVER(PARTITION BY category ORDER BY ROUND(SUM(line_total_usd), 2)) AS rank_product_revenue
	FROM
	(
		SELECT
			p.category,
			p.product_name,
			(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
		FROM gold.fact_order_items_view oi
		LEFT JOIN gold.fact_orders_view o
		ON oi.order_id = o.order_id
		LEFT JOIN gold.dim_products_view p
		ON oi.product_key = p.product_key
	)SUB1
	GROUP BY category, product_name
)SUB2
WHERE rank_product_revenue BETWEEN 1 AND 5;

-- What are the top 5 best performing products?
SELECT TOP 5
	product_name,
	ROUND(SUM(line_total_usd), 2) total_sales,
	DENSE_RANK() OVER(ORDER BY ROUND(SUM(line_total_usd), 2) DESC) AS rank_product_revenue
FROM
(
	SELECT
		p.product_name,
		(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.fact_orders_view o
	ON oi.order_id = o.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)SUB
GROUP BY product_name;

-- What are the top 5 worst performing products?
SELECT TOP 5
	product_name,
	ROUND(SUM(line_total_usd), 2) total_sales,
	DENSE_RANK() OVER(ORDER BY ROUND(SUM(line_total_usd), 2)) AS rank_product_revenue
FROM
(
	SELECT
		p.product_name,
		(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
	FROM gold.fact_order_items_view oi
	LEFT JOIN gold.fact_orders_view o
	ON oi.order_id = o.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)SUB
GROUP BY product_name;

-- What are the top 5 products that piqued our customers' interests?
SELECT TOP 5
	p.product_name,
	COUNT(DISTINCT e.session_key) AS count_product,
	DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT e.session_key) DESC) AS rank_product_interest
FROM gold.fact_events_view e
LEFT JOIN gold.dim_products_view p
ON e.product_key = p.product_key
WHERE p.product_name IS NOT NULL
GROUP BY p.product_name;

-- What are the top 5 products with the least interactions?
SELECT TOP 5
	p.product_name,
	COUNT(DISTINCT e.session_key) AS count_product,
	DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT e.session_key)) AS rank_product_interest
FROM gold.fact_events_view e
LEFT JOIN gold.dim_products_view p
ON e.product_key = p.product_key
WHERE p.product_name IS NOT NULL
GROUP BY p.product_name;
