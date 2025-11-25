/*
===============================================================
Data Quality Checks
===============================================================
Script Purpose:
  This script performs data quality check on all silver tables.
  It also checks for connectivity between related tables, 
  preparing them for data integration/modeling 
  in the gold layer.
===============================================================
*/
-- ------------------------------------------------------------
-- Check [silver.archive_customers]
-- ------------------------------------------------------------

-- Check for duplicates and nulls in primary key
-- Expectation: No Result
SELECT 
customer_id,
COUNT(*) duplicate_check
FROM silver.archive_customers
GROUP BY customer_id
HAVING customer_id IS NULL OR COUNT(*) > 1;

-- Check for leading and trailing spaces in string fields
-- Expectation: No Result
SELECT
[name]
FROM silver.archive_customers
WHERE name <> TRIM(name);

SELECT
email
FROM silver.archive_customers
WHERE email <> TRIM(email);

SELECT
marketing_opt_in
FROM silver.archive_customers
WHERE marketing_opt_in <> TRIM(marketing_opt_in);

-- Check for invalid emails
-- Expectation: No Result
SELECT
email
FROM bronze.archive_customers
WHERE email NOT LIKE ('%@%');

-- Data Standardization & Consistence
-- Expectation: User friendly values and no nulls
SELECT DISTINCT
country, country_name
FROM silver.archive_customers;

SELECT DISTINCT
marketing_opt_in
FROM silver.archive_customers;

-- Check for invalid age
-- Expectation: No Result
SELECT
age
FROM silver.archive_customers
WHERE age < 0 OR age > 150;

-- Check connectivity
SELECT customer_id FROM silver.archive_customers WHERE customer_id
NOT IN(SELECT DISTINCT customer_id FROM silver.archive_orders);

-- ------------------------------------------------------------
-- Check [silver.archive_events]
-- ------------------------------------------------------------
-- Check for duplicates in primary key
-- Expectation: No Result
SELECT
event_id,
COUNT(*) duplicate_check
FROM silver.archive_events
GROUP BY event_id
HAVING event_id IS NULL OR COUNT(*) > 1;

-- Check Connectivity of session_id
SELECT [session_id] FROM silver.archive_events WHERE [session_id] NOT IN 
(SELECT [session_id] FROM silver.archive_sessions);

SELECT product_id FROM silver.archive_events WHERE product_id NOT IN 
(SELECT product_id FROM silver.archive_products);

-- Check for invalid timestamp
-- Expectation: No Result
SELECT
[timestamp]
FROM silver.archive_events
WHERE [timestamp] > GETDATE();

-- Check for trailing & leading spaces in field strings
-- Expectation: No Result
SELECT
event_type
FROM silver.archive_events
WHERE event_type <> TRIM(event_type);

-- Check for invalid values in quantity & cart size
-- Expectation: No Result
SELECT
qty
FROM silver.archive_events
WHERE qty < 0 OR qty IS NULL;

SELECT 
cart_size
FROM silver.archive_events
WHERE cart_size < 0 OR cart_size IS NULL;

-- Data standardization & consistency
-- Expectation: User friendly values & no nulls
SELECT DISTINCT 
event_type
FROM silver.archive_events

SELECT DISTINCT 
payment
FROM silver.archive_events;

-- Check for Invalid Values in discount_pct and amount_usd
-- Expectation: No Result
SELECT
discount_pct,
amount_usd
FROM silver.archive_events
WHERE discount_pct IS NULL OR discount_pct < 0 OR
amount_usd IS NULL OR amount_usd < 0

-- ------------------------------------------------------------
-- Check [silver.archive_order_items]
-- ------------------------------------------------------------
-- Check for nulls in column identifiers
-- Expectation: No Result
SELECT
order_id
FROM silver.archive_order_items
WHERE order_id IS NULL;

SELECT
product_id
FROM silver.archive_order_items
WHERE product_id IS NULL;

-- Check Connectivity between tables 
SELECT order_id FROM silver.archive_order_items WHERE order_id 
NOT IN(SELECT order_id FROM silver.archive_orders);

SELECT product_id FROM silver.archive_order_items WHERE product_id 
NOT IN(SELECT product_id FROM silver.archive_products);

-- Check for invalid values in price, quantity, and line total
-- Expectation: No Result
SELECT 
unit_price_usd,
quantity,
line_total_usd
FROM silver.archive_order_items
WHERE line_total_usd IS NULL OR line_total_usd <= 0 OR line_total_usd <> quantity * unit_price_usd OR
quantity IS NULL OR quantity <= 0 OR quantity <> line_total_usd / unit_price_usd OR
unit_price_usd IS NULL OR unit_price_usd <= 0 OR unit_price_usd <> line_total_usd / quantity;

-- --------------------------------------------------------------
-- Check [silver.archive_orders]
-- --------------------------------------------------------------
-- Check for nulls and duplicates in primary key
-- Expectation: No Result
SELECT 
order_id,
COUNT(*)
FROM silver.archive_orders
GROUP BY order_id
HAVING order_id IS NULL OR COUNT(*) > 1;

-- Check connectivity btw related tables
SELECT order_id FROM silver.archive_orders WHERE order_id 
NOT IN(SELECT DISTINCT order_id FROM silver.archive_order_items);

SELECT customer_id FROM silver.archive_orders WHERE customer_id
NOT IN(SELECT customer_id FROM silver.archive_customers);

-- Check for invalid dates
-- Expectation: No result
SELECT
order_time
FROM silver.archive_orders
WHERE order_time > GETDATE();

-- Data Standardization & Consistence
-- Exectation: No nulls and user friendly values
SELECT DISTINCT 
payment_method
FROM silver.archive_orders;

SELECT DISTINCT
country
FROM silver.archive_orders;

SELECT DISTINCT
device
FROM silver.archive_orders;

SELECT DISTINCT
[source]
FROM silver.archive_orders;

-- Check for unwanted spaces in string fields
-- Expectation: No result
SELECT
payment_method
FROM silver.archive_orders
WHERE payment_method != TRIM(payment_method);

SELECT
country
FROM silver.archive_orders
WHERE country != TRIM(country);

SELECT
device
FROM silver.archive_orders
WHERE device != TRIM(device);

SELECT
[source]
FROM silver.archive_orders
WHERE [source] != TRIM([source]);

-- Check for invalid values in discount pct, subtotal, and total
-- Expectation: No result
SELECT
discount_pct,
subtotal_usd,
total_usd,
subtotal_test,
total_usd_test,
ROUND(subtotal_usd - subtotal_test, 2),
ROUND(total_usd - total_usd_test, 2)
FROM
(
SELECT
discount_pct,
subtotal_usd,
total_usd,
ROUND(total_usd / (1 - CAST(discount_pct AS FLOAT)/100), 2) AS subtotal_test,
ROUND((1 - CAST(discount_pct AS FLOAT)/100) * subtotal_usd, 2) AS total_usd_test
FROM silver.archive_orders
WHERE discount_pct IS NULL OR discount_pct < 0 OR discount_pct != ROUND(100 - (total_usd/subtotal_usd * 100), 0) 
OR subtotal_usd IS NULL OR subtotal_usd <= 0 OR subtotal_usd != ROUND(total_usd / (1 - CAST(discount_pct AS FLOAT)/100), 2)
OR total_usd IS NULL OR total_usd <= 0 OR total_usd != ROUND((1 - CAST(discount_pct AS FLOAT)/100) * subtotal_usd, 2)
)SUB
WHERE ROUND(subtotal_usd - subtotal_test, 2) NOT BETWEEN -0.01 AND 0.01 
OR ROUND(total_usd - total_usd_test, 2) NOT BETWEEN -0.01 AND 0.01;

-- --------------------------------------------------------------
-- Check [silver.archive_products]
-- --------------------------------------------------------------
-- Check for nulls and duplicates in primary key
-- Expectation: No Result
SELECT
product_id,
COUNT(*)
FROM silver.archive_products
GROUP BY product_id
HAVING product_id IS NULL OR COUNT(*) > 1;

-- Check connectivity btw related tables
SELECT product_id FROM silver.archive_products WHERE product_id
NOT IN(SELECT DISTINCT product_id FROM silver.archive_order_items);

-- Data Standardization & Consistency
-- Expectation: User friendly values and no nulls
SELECT DISTINCT
category 
FROM silver.archive_products;

-- Check for unwanted spaces in string fields 
-- Expectation: No Result
SELECT
category
FROM silver.archive_products
WHERE category != TRIM(category);

SELECT
[name]
FROM silver.archive_products
WHERE [name] != TRIM([name]);

-- Check for invalid values in price, cost, and margin
-- Expectation: No Result
SELECT
price_usd,
cost_usd,
margin_usd
FROM silver.archive_products
WHERE price_usd IS NULL OR price_usd <= 0 OR price_usd != cost_usd + margin_usd
OR cost_usd IS NULL OR cost_usd <= 0 OR cost_usd != price_usd - margin_usd
OR margin_usd IS NULL OR margin_usd <= 0 OR margin_usd != price_usd - cost_usd;

-- --------------------------------------------------------------
-- Check [silver.archive_reviews]
-- --------------------------------------------------------------
-- Check for nulls and duplicates in primary key
-- Expectation: No Result
SELECT
review_id,
COUNT(*)
FROM silver.archive_reviews
GROUP BY review_id
HAVING review_id IS NULL OR COUNT(*) > 1;

-- Check connectivity btw related tables
SELECT DISTINCT order_id FROM silver.archive_reviews WHERE order_id
NOT IN(SELECT DISTINCT order_id FROM silver.archive_order_items);

SELECT DISTINCT product_id FROM silver.archive_reviews WHERE product_id
NOT IN(SELECT DISTINCT order_id FROM silver.archive_order_items);

-- Check for invalid rating
-- Expectation: No Result
SELECT
rating
FROM silver.archive_reviews
WHERE rating IS NULL OR rating <= 0;

-- Check for unwanted spaces in string fields
-- Expectation: No Result
SELECT
review_text
FROM silver.archive_reviews
WHERE review_text != TRIM(review_text);

-- Data Standardization and Consistency
-- Expectation: No Nulls
SELECT DISTINCT review_text FROM silver.archive_reviews;

-- Check for invalid review time
SELECT
review_time
FROM silver.archive_reviews
WHERE review_time > GETDATE();

-- --------------------------------------------------------------
-- Check [silver.archive_sessions]
-- --------------------------------------------------------------
-- Check for nulls and duplicates in primary key
-- Expectation: No Result
SELECT
[session_id],
COUNT(*)
FROM silver.archive_sessions
GROUP BY [session_id]
HAVING [session_id] IS NULL OR COUNT(*) > 1;

-- Check for connectivity btw related tables
SELECT [session_id] FROM silver.archive_sessions WHERE [session_id]
NOT IN(SELECT DISTINCT [session_id] FROM silver.archive_events);

SELECT customer_id FROM silver.archive_sessions WHERE customer_id
NOT IN(SELECT DISTINCT customer_id FROM silver.archive_customers);

-- Check for invalid start time
-- Expectation: No Result
SELECT
start_time
FROM silver.archive_sessions
WHERE start_time > GETDATE();

-- Data Standardization and Consistency
SELECT DISTINCT
device
FROM silver.archive_sessions;

SELECT DISTINCT
[source]
FROM silver.archive_sessions;

SELECT DISTINCT
country
FROM silver.archive_sessions;

-- Check for unwanted spaces in string fields
-- Expectation: No Result
SELECT
device
FROM silver.archive_sessions
WHERE device != TRIM(device);

SELECT
[source]
FROM silver.archive_sessions
WHERE [source] != TRIM([source]);

SELECT
country
FROM silver.archive_sessions
WHERE country != TRIM(country);
