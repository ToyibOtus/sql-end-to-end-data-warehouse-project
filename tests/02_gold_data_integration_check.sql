/*
==================================================================================
Data Integration Check
==================================================================================
Script Purpose: 
	This script performs data integration checks on all fact and dimension
	tables.
==================================================================================
*/
-- Check gold.fact_orders
SELECT * FROM gold.fact_orders o
LEFT JOIN gold.dim_customers c
ON o.customer_key = c.customer_key;

-- Check gold.fact_order_items
SELECT * FROM gold.fact_order_items i
LEFT JOIN gold.dim_products p
ON i.product_key = p.product_key
LEFT JOIN gold.fact_orders o
ON i.order_id = o.order_id;

-- Check gold.dim_sessions
SELECT * FROM gold.dim_sessions s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key;

-- Check gold.fact_events
SELECT * FROM gold.fact_events e
LEFT JOIN gold.dim_products p
ON e.product_key = p.product_key
LEFT JOIN gold.dim_sessions s
ON e.session_key = s.session_key;

-- Check gold.fact_reviews
SELECT * FROM gold.fact_reviews r
LEFT JOIN gold.fact_orders o
ON r.order_id = o.order_id
LEFT JOIN gold.dim_products p
ON r.product_key = p.product_key;
