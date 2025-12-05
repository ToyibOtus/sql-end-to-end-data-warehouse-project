/*
==================================================================================
Performance Analysis
==================================================================================
Script Purpose:
	This script performs performance analysis. It fetches data vital to the
	overall performance of the business.
==================================================================================
*/
-- Apply dicount on line_total_usd to get the actual amount paid in USD
WITH actual_revenue AS
(
	SELECT
		YEAR(o.order_time) AS order_time_year,
		product_name,
		(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
	FROM gold.fact_orders_view o
	LEFT JOIN gold.fact_order_items_view oi
	ON oi.order_id = o.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)
-- Generate the yearly revenue of each product, and average revenue of each product across the years
, yearly_revenue AS
(
	SELECT
		order_time_year,
		product_name,
		SUM(line_total_usd) AS total_sales,
		ROUND(AVG(SUM(line_total_usd)) OVER(PARTITION BY product_name), 2) AS avg_sales_years
	FROM actual_revenue
	GROUP BY order_time_year, product_name
)
-- Measure the product performace by comparing its yearly revenue with its average yearly revenue 
, sales_difference AS
(
	SELECT
		order_time_year,
		product_name,
		total_sales,
		avg_sales_years,
		ROUND(total_sales - avg_sales_years, 2) AS sales_diff,
		FORMAT((total_sales - avg_sales_years)/avg_sales_years, 'P') AS percent_diff
	FROM yearly_revenue
)
-- Segment each row based on yearly revenue status
SELECT
	order_time_year,
	product_name,
	total_sales,
	avg_sales_years,
	sales_diff,
	percent_diff,
	CASE
		WHEN sales_diff > 0 THEN 'Above Average'
		WHEN sales_diff < 0 THEN 'Below Average'
		ELSE 'Equal to Average'
	END AS sales_status
FROM sales_difference;

-- Apply dicount on line_total_usd to get the actual amount paid in USD
WITH actual_revenue AS
(
	SELECT
		YEAR(o.order_time) AS order_time_year,
		product_name,
		(1 - CAST(o.discount_pct AS FLOAT)/100) * oi.line_total_usd AS line_total_usd
	FROM gold.fact_orders_view o
	LEFT JOIN gold.fact_order_items_view oi
	ON oi.order_id = o.order_id
	LEFT JOIN gold.dim_products_view p
	ON oi.product_key = p.product_key
)
-- Generate current and previous yearly revenues of each product
, yearly_revenue AS
(
	SELECT
		order_time_year,
		product_name,
		ROUND(SUM(line_total_usd), 2) AS current_year_sales,
		LAG(ROUND(SUM(line_total_usd), 2)) OVER(PARTITION BY product_name ORDER BY order_time_year) AS previous_year_sales
	FROM actual_revenue
	GROUP BY order_time_year, product_name
)
-- Measure the product performace by comparing its current yearly revenue with its previous yearly revenue 
, sales_difference AS
(
	SELECT
		order_time_year,
		product_name,
		current_year_sales,
		previous_year_sales,
		current_year_sales - previous_year_sales AS sales_diff,
		FORMAT((current_year_sales - previous_year_sales)/previous_year_sales, 'P') AS percent_sales_diff
	FROM yearly_revenue
)
-- Segment each row based on current revenue status
SELECT
	order_time_year,
	product_name,
	current_year_sales,
	previous_year_sales,
	sales_diff,
	percent_sales_diff,
	CASE
		WHEN sales_diff < 0 THEN 'Below Previous Sale'
		WHEN sales_diff > 0 THEN 'Above Previous Sale'
		WHEN sales_diff = 0 THEN 'Equal to Previous Sale'
		ELSE NULL
	END AS current_sales_status
FROM sales_difference;

-- Is the progression of the business steady over the years?
SELECT
	order_time_year,
	running_total_sales AS current_cumul_sales_year,
	LAG(running_total_sales) OVER(ORDER BY order_time_year) AS previous_cumul_sales_year,
	FORMAT((running_total_sales - LAG(running_total_sales) OVER(ORDER BY order_time_year))/
	LAG(running_total_sales) OVER(ORDER BY order_time_year), 'P') AS percent_sales_diff
FROM
(
	SELECT
		order_time_year,
		SUM(total_sales) OVER(ORDER BY order_time_year) AS running_total_sales
	FROM
	(
		SELECT
			YEAR(order_time) AS order_time_year,
			SUM(total_usd) AS total_sales
		FROM gold.fact_orders_view
		GROUP BY YEAR(order_time)
	)SUB1
)SUB2;

-- Is the business progressing steadily over the months?
SELECT
	order_time_month,
	running_total_sales AS current_cumul_sales_month,
	LAG(running_total_sales) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS previous_cumul_sales_month,
	FORMAT((running_total_sales - LAG(running_total_sales) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month))/
	LAG(running_total_sales) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month), 'P') AS percent_sales_diff
FROM
(
	SELECT
		order_time_month,
		SUM(total_sales) OVER(PARTITION BY YEAR(order_time_month) ORDER BY order_time_month) AS running_total_sales
	FROM
	(
		SELECT
			DATETRUNC(month, order_time) AS order_time_month,
			SUM(total_usd) AS total_sales
		FROM gold.fact_orders_view
		GROUP BY DATETRUNC(month, order_time)
	)SUB1
)SUB2;
