/*
==================================================================================
Change Over Time Analysis
==================================================================================
Script Purpose:
	This script monitors how key business metrics change over time. It carries
	out both year-over-year and month-over-month analysis.
==================================================================================
*/
-- Year-Over-Year Analysis
SELECT
	order_time_year,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_quantity,
	ROUND(SUM(weighted_price)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(SUM(weighted_cost)/SUM(quantity), 2) AS weighted_avg_cost,
	ROUND(SUM(weighted_cost), 2) AS total_cost,
	ROUND(SUM(line_total_usd), 2) AS total_sales,
	ROUND(SUM(line_total_usd) - SUM(weighted_cost), 2) AS total_profit
FROM
(
	SELECT 
		o.order_id,
		YEAR(o.order_time) AS order_time_year,
		oi.quantity,
		oi.unit_price_usd * oi.quantity AS weighted_price,
		(1 - (CAST(o.discount_pct AS FLOAT)/100)) * oi.line_total_usd AS line_total_usd,
		p.cost_usd * oi.quantity AS weighted_cost
	FROM gold.fact_orders_view o
	LEFT JOIN gold.fact_order_items_view oi
	ON o.order_id = oi.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)SUB
GROUP BY order_time_year
ORDER BY order_time_year;

-- Month-Over-Month Analysis
SELECT
	order_time_month,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_quantity,
	ROUND(SUM(weighted_price)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(SUM(weighted_cost)/SUM(quantity), 2) AS weighted_avg_cost,
	ROUND(SUM(weighted_cost), 2) AS total_cost,
	ROUND(SUM(line_total_usd), 2) AS total_sales,
	ROUND(SUM(line_total_usd) - SUM(weighted_cost), 2) AS total_profit
FROM
(
	SELECT 
		o.order_id,
		DATETRUNC(month, o.order_time) AS order_time_month,
		oi.quantity,
		oi.unit_price_usd * oi.quantity AS weighted_price,
		(1 - (CAST(o.discount_pct AS FLOAT)/100)) * oi.line_total_usd AS line_total_usd,
		p.cost_usd * oi.quantity AS weighted_cost
	FROM gold.fact_orders_view o
	LEFT JOIN gold.fact_order_items_view oi
	ON o.order_id = oi.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)SUB
GROUP BY order_time_month
ORDER BY order_time_month;
