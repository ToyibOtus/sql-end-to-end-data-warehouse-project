/*
============================================================================
DDL Script: Gold Layer Views
============================================================================
Script Purpose:
	These scripts create views for individual tables in the gold layer.
	It aims to provide analysts with vital columns needed for their 
	analysis.

	Run this script to redefine the structure of your views.
============================================================================
*/
-- Drop view [gold.dim_customers_view]
IF OBJECT_ID('gold.dim_customers_view', 'V') IS NOT NULL
DROP VIEW gold.dim_customers_view;
GO

-- Create view [gold.dim_customers_view]
CREATE VIEW gold.dim_customers_view AS
SELECT
	customer_key,
	customer_id,
	customer_name,
	email,
	country_code,
	country_name,
	age,
	signup_date,
	marketing_opt_in
FROM gold.dim_customers;
GO

-- Drop view [gold.dim_products_view]
IF OBJECT_ID('gold.dim_products_view', 'V') IS NOT NULL
DROP VIEW gold.dim_products_view;
GO

-- Create view [gold.dim_products_view]
CREATE VIEW gold.dim_products_view AS
SELECT
	product_key,
	product_id,
	category,
	product_name,
	price_usd,
	cost_usd,
	margin_usd
FROM gold.dim_products;
GO

-- Drop view [gold.dim_sessions_view]
IF OBJECT_ID('gold.dim_sessions_view', 'V') IS NOT NULL
DROP VIEW gold.dim_sessions_view;
GO

-- Create view [gold.dim_sessions_view]
CREATE VIEW gold.dim_sessions_view AS
SELECT
	session_key,
	user_session_id,
	customer_key,
	start_time,
	device,
	traffic_source,
	country_code
FROM gold.dim_sessions;
GO

-- Drop view [gold.fact_orders_view]
IF OBJECT_ID('gold.fact_orders_view', 'V') IS NOT NULL
DROP VIEW gold.fact_orders_view;
GO

-- Create view [gold.fact_orders_view]
CREATE VIEW gold.fact_orders_view AS
SELECT
	order_id,
	customer_key,
	order_time,
	payment_method,
	discount_pct,
	subtotal_usd,
	total_usd,
	country_code,
	device,
	traffic_source
FROM gold.fact_orders;
GO

-- Drop view [gold.fact_order_items_view]
IF OBJECT_ID('gold.fact_order_items_view', 'V') IS NOT NULL
DROP VIEW gold.fact_order_items_view;
GO

-- Create view [gold.fact_order_items_view]
CREATE VIEW gold.fact_order_items_view AS
SELECT
	order_id,
	product_key,
	unit_price_usd,
	quantity,
	line_total_usd
FROM gold.fact_order_items;
GO

-- Drop view [gold.fact_events_view]
IF OBJECT_ID('gold.fact_events_view', 'V') IS NOT NULL
DROP VIEW gold.fact_events_view;
GO

-- Create view [gold.fact_events_view]
CREATE VIEW gold.fact_events_view AS
SELECT
	event_id,
	session_key,
	event_timestamp,
	event_type,
	product_key,
	quantity,
	cart_size,
	payment_method,
	discount_pct,
	amount_usd
FROM gold.fact_events;
GO

-- Drop view [gold.fact_reviews_view]
IF OBJECT_ID('gold.fact_reviews_view', 'V') IS NOT NULL
DROP VIEW gold.fact_reviews_view;
GO

-- Create view [gold.fact_reviews_view]
CREATE VIEW gold.fact_reviews_view AS
SELECT
	review_id,
	order_id,
	product_key,
	rating,
	review_text,
	review_time
FROM gold.fact_reviews;
