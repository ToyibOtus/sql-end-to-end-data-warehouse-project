/*
==================================================================================
Part-to-Whole Analysis
==================================================================================
Script Purpose:
	This script performs part-to-whole analysis. It checks how key business
	metrics are distributed across various dimensions.
==================================================================================
*/
-- What is our best year based on yearly revenue?
SELECT
	order_time_year,
	total_sales_all_years,
	yearly_sales,
	percent_sales_dist,
	DENSE_RANK() OVER(ORDER BY percent_sales_dist DESC) AS rank_year_sales
FROM
(
	SELECT
		order_time_year,
		SUM(total_sales) OVER() AS total_sales_all_years,
		total_sales AS yearly_sales,
		ROUND((total_sales/SUM(total_sales) OVER()) * 100, 2) AS percent_sales_dist
	FROM
	(
		SELECT
			YEAR(order_time) AS order_time_year,
			SUM(total_usd) AS total_sales
		FROM gold.fact_orders_view
		GROUP BY YEAR(order_time)
	)SUB1
)SUB2;

-- What do we attribute this high revenue to?
-- orders, quantity, weighted average price, or a combination of all metrics?
SELECT
	order_time_year,
	weighted_avg_price,
	total_orders,
	percent_orders_dist,
	DENSE_RANK() OVER(ORDER BY percent_orders_dist DESC) AS rank_orders,
	total_quantity,
	percent_quantity_dist,
	DENSE_RANK() OVER(ORDER BY percent_quantity_dist DESC) AS rank_quantity,
	total_sales,
	percent_sales_dist,
	DENSE_RANK() OVER(ORDER BY percent_sales_dist DESC) AS rank_sales
FROM
(
	SELECT
		order_time_year,
		weighted_avg_price,
		total_orders,
		total_quantity,
		total_sales,
		ROUND(CAST(total_orders AS FLOAT)/SUM(total_orders) OVER() * 100, 2) AS percent_orders_dist,
		ROUND(CAST(total_quantity AS FLOAT)/SUM(total_quantity) OVER() * 100, 2) AS percent_quantity_dist,
		ROUND(CAST(total_sales AS FLOAT)/SUM(total_sales) OVER() * 100, 2) AS percent_sales_dist
	FROM
		(
			SELECT
				YEAR(o.order_time) AS order_time_year,
				ROUND(SUM(oi.unit_price_usd * oi.quantity)/SUM(oi.quantity), 2) AS weighted_avg_price,
				COUNT(DISTINCT o.order_id) AS total_orders,
				SUM(oi.quantity) AS total_quantity,
				ROUND(SUM((1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd), 2) AS total_sales
			FROM gold.fact_orders_view o
			LEFT JOIN gold.fact_order_items_view oi
			ON o.order_id = oi.order_id
			GROUP BY YEAR(o.order_time)
		)SUB1
)SUB2;

-- Any pattern in monthly revenue across the years?
SELECT
	order_time_month,
	DATENAME(month, order_time_month) AS month_name,
	yearly_sales,
	monthly_sales,
	percent_sales_dist_month,
	DENSE_RANK() OVER(PARTITION BY YEAR(order_time_month) ORDER BY percent_sales_dist_month DESC) AS rank_monthly_sales
FROM
(
	SELECT
		order_time_month,
		SUM(total_sales) OVER(PARTITION BY YEAR(order_time_month)) AS yearly_sales,
		total_sales AS monthly_sales,
		ROUND((total_sales/SUM(total_sales) OVER(PARTITION BY YEAR(order_time_month))) * 100, 2) AS percent_sales_dist_month
	FROM
	(
		SELECT
			DATETRUNC(month, order_time) AS order_time_month,
			SUM(total_usd) AS total_sales
		FROM gold.fact_orders_view
		GROUP BY DATETRUNC(month, order_time)
	)SUB1
)SUB2;

-- Is high revenue solely attributed to high demand or high price?
SELECT
	order_time_month,
	DATENAME(month, order_time_month) AS month_name,
	weighted_avg_price_month,
	monthly_orders,
	percent_monthly_orders_dist,
	DENSE_RANK() OVER(PARTITION BY YEAR(order_time_month) ORDER BY percent_monthly_orders_dist DESC) AS rank_monthly_orders,
	monthly_quantity_ordered,
	percent_monthly_quantity_dist,
	DENSE_RANK() OVER(PARTITION BY YEAR(order_time_month) ORDER BY percent_monthly_quantity_dist DESC) AS rank_monthly_quantity,
	monthly_sales,
	percent_monthly_sales_dist,
	DENSE_RANK() OVER(PARTITION BY YEAR(order_time_month) ORDER BY percent_monthly_sales_dist DESC) AS rank_monthly_sales
FROM
(
	SELECT
		order_time_month,
		weighted_avg_price_month,
		monthly_orders,
		monthly_quantity_ordered,
		monthly_sales,
		ROUND(CAST(monthly_orders AS FLOAT)/SUM(monthly_orders) 
		OVER(PARTITION BY YEAR(order_time_month)) * 100, 2) AS percent_monthly_orders_dist,
		ROUND(CAST(monthly_quantity_ordered AS FLOAT)/SUM(monthly_quantity_ordered) 
		OVER(PARTITION BY YEAR(order_time_month)) * 100, 2) AS percent_monthly_quantity_dist,
		ROUND(CAST(monthly_sales AS FLOAT)/SUM(monthly_sales) 
		OVER(PARTITION BY YEAR(order_time_month)) * 100, 2) AS percent_monthly_sales_dist
	FROM
		(
			SELECT
				DATETRUNC(month, o.order_time) AS order_time_month,
				ROUND(SUM(oi.unit_price_usd * oi.quantity)/SUM(oi.quantity), 2) AS weighted_avg_price_month,
				COUNT(DISTINCT o.order_id) AS monthly_orders,
				SUM(oi.quantity) AS monthly_quantity_ordered,
				ROUND(SUM((1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd), 2) AS monthly_sales
			FROM gold.fact_orders_view o
			LEFT JOIN gold.fact_order_items_view oi
			ON o.order_id = oi.order_id
			GROUP BY DATETRUNC(month, o.order_time)
		)SUB1
)SUB2;
