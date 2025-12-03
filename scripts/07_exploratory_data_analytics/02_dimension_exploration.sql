/*
=================================================================
Dimension Exploration
=================================================================
Script Purpose:
	This script explores all dimensions in the gold layer, and
	asks basic but important questions about the business.
=================================================================
*/
-- Explore countries business is spreaded across
SELECT DISTINCT
	country_code,
	country_name
FROM gold.dim_customers_view;

-- Explore marketing opt in
SELECT DISTINCT marketing_opt_in FROM gold.dim_customers_view;

-- Explore product categories
SELECT DISTINCT category FROM gold.dim_products_view;

-- Explore products
SELECT DISTINCT product_name FROM gold.dim_products_view;

-- Explore payment methods
SELECT DISTINCT payment_method FROM gold.fact_orders_view;

-- Explore devices used in sessions
SELECT DISTINCT device FROM gold.dim_sessions_view;

-- Explore traffic sources
SELECT DISTINCT traffic_source FROM gold.dim_sessions_view;

-- Explore event types
SELECT DISTINCT event_type FROM gold.fact_events_view;

-- Explore ratings
SELECT DISTINCT rating FROM gold.fact_reviews_view;

-- Explore review texts
SELECT DISTINCT review_text FROM gold.fact_reviews_view;
