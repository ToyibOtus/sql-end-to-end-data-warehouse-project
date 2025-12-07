/*
======================================================================================
Product Report
======================================================================================
Script Purpose:
	This script consolidates key product metrics and performance. It performs the
	following operations:
	
	1. Retrieve essential fields from products table such as product_key,
	   product_id, name etc.
	2. Performs data aggregations, these include:
		* total orders made,
		* total quantity of products purchased,
		* total revenue generated,
		* total cost,
		* total profit,
		* total distinct customers ordered,
		* lifespan in month.
	3. Segment products into:
		* High Performer, Mid Performer, and Low Performer based on their lifespan
		  and total revenue generated.
	4. Calculate valuable KPIs:
		* recency (months since last ordered)
		* average order revenue,
		* average monthly revenue.
	5. Retrieve average product ratings.
======================================================================================
*/
IF OBJECT_ID('gold.products_report_view', 'V') IS NOT NULL
DROP VIEW gold.products_report_view;
GO

CREATE VIEW gold.products_report_view AS
-- Retrieve essential columns
WITH base_query AS
(
	SELECT
		p.product_key,
		p.product_id,
		p.category,
		p.product_name,
		p.price_usd,
		p.cost_usd,
		p.margin_usd,
		o.order_id,
		o.customer_key,
		o.order_time,
		o.discount_pct,
		oi.unit_price_usd,
		oi.quantity,
		oi.line_total_usd
	FROM gold.dim_products p
	LEFT JOIN gold.fact_order_items oi
	ON p.product_key = oi.product_key
	LEFT JOIN gold.fact_orders o
	ON oi.order_id = o.order_id
	WHERE o.order_time IS NOT NULL
)
-- Aggregate key business metrics
, product_aggregation AS
(
	SELECT
		product_key,
		product_id,
		category,
		product_name,
		price_usd,
		cost_usd,
		margin_usd,
		MAX(order_time) AS last_order_time,
		DATEDIFF(month, MIN(order_time), MAX(order_time)) AS lifespan_month,
		COUNT(DISTINCT order_id) AS total_orders,
		SUM(quantity) AS total_quantity,
		CAST(SUM((1 - (discount_pct * 1.0)/100) * line_total_usd) AS DECIMAL(8, 2)) AS total_sales,
		CAST(SUM(cost_usd * quantity) * 1.0 AS DECIMAL(8, 2)) AS total_cost,
		CAST(SUM((1 - (discount_pct * 1.0)/100) * line_total_usd) - SUM(cost_usd * quantity) AS DECIMAL(8, 2)) AS total_profit,
		COUNT(DISTINCT customer_key) AS total_customers
	FROM base_query
	GROUP BY
		product_key,
		product_id,
		category,
		product_name,
		price_usd,
		cost_usd,
		margin_usd
)
-- Segment products into various categories
, product_segmentation AS
(
	SELECT
		product_key,
		product_id,
		category,
		product_name,
		CASE
			WHEN lifespan_month >= 12 AND total_sales > 5000 THEN 'High Performer'
			WHEN lifespan_month >= 12 AND total_sales <= 5000 THEN 'Mid Performer'
			ELSE 'Low Performer'
		END AS product_status,
		price_usd,
		cost_usd,
		margin_usd,
		last_order_time,
		lifespan_month,
		total_orders,
		total_quantity,
		total_sales,
		total_cost,
		total_profit,
		total_customers
	FROM product_aggregation
)
-- Calculate valuable KPIs
SELECT
	ps.product_key,
	product_id,
	category,
	product_name,
	product_status,
	price_usd,
	cost_usd,
	margin_usd,
	last_order_time,
	r.avg_product_ratings,
	lifespan_month,
	total_orders,
	total_quantity,
	total_sales,
	total_cost,
	total_profit,
	total_customers,
	DATEDIFF(month, last_order_time, GETDATE()) AS recency_month,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE CAST(total_sales/total_orders AS DECIMAL(8, 2))
	END AS avg_order_revenue,
	CASE
		WHEN lifespan_month = 0 THEN total_sales
		ELSE CAST(total_sales/lifespan_month AS DECIMAL(8, 2))
	END AS avg_monthly_revenue
FROM product_segmentation ps
LEFT JOIN
-- Retrieve average product ratings
(
	SELECT
		product_key,
		CAST(AVG(rating * 1.0) AS DECIMAL(8, 2)) AS avg_product_ratings
	FROM gold.fact_reviews
	GROUP BY product_key
)r
ON ps.product_key = r.product_key;
