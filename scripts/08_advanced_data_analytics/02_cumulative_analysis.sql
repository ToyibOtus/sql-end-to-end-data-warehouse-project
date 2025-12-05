/*
==================================================================================
Cumulative Analysis
==================================================================================
Script Purpose:
	This script performs cumulative analysis. It aims to understand just how 
	much the business progresses over time.
==================================================================================
*/
-- Year-Over-Year Analysis
SELECT
	order_time_year,
	SUM(total_orders) OVER(ORDER BY order_time_year) AS running_total_orders,
	SUM(total_quantity) OVER(ORDER BY order_time_year) AS running_total_quantity,
	ROUND(SUM(weighted_price_by_year) OVER(ORDER BY order_time_year)
	/SUM(total_quantity) OVER(ORDER BY order_time_year), 2) AS moving_weighted_avg_price,
	ROUND(SUM(weighted_cost_by_year) OVER(ORDER BY order_time_year)
	/SUM(total_quantity) OVER(ORDER BY order_time_year), 2) AS moving_weighted_avg_cost,
	SUM(total_cost) OVER(ORDER BY order_time_year) AS running_total_cost,
	SUM(total_sales) OVER(ORDER BY order_time_year) AS running_total_sales,
	SUM(total_profit) OVER(ORDER BY order_time_year) AS running_total_profit
FROM
(
	SELECT
		order_time_year,
		COUNT(DISTINCT order_id) AS total_orders,
		SUM(quantity) AS total_quantity,
		SUM(weighted_price) AS weighted_price_by_year,
		SUM(weighted_cost) AS weighted_cost_by_year,
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
	)SUB1
	GROUP BY order_time_year
)SUB2;

-- Month-Over-Month Analysis
SELECT
	order_time_month,
	SUM(total_orders) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_orders,
	SUM(total_quantity) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_quantity,
	ROUND(SUM(weighted_price_by_month) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month)
	/SUM(total_quantity) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month), 2) AS moving_weighted_avg_price,
	ROUND(SUM(weighted_cost_by_month) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month)
	/SUM(total_quantity) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month), 2) AS moving_weighted_avg_cost,
	SUM(total_cost) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_cost,
	SUM(total_sales) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_sales,
	SUM(total_profit) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_profit
FROM
(
	SELECT
		order_time_month,
		COUNT(DISTINCT order_id) AS total_orders,
		SUM(quantity) AS total_quantity,
		SUM(weighted_price) AS weighted_price_by_month,
		SUM(weighted_cost) AS weighted_cost_by_month,
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
	)SUB1
	GROUP BY order_time_month
)SUB2;
