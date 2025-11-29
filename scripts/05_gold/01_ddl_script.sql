/*
============================================================================================
DDL Script: Create Tables in Gold Layer
============================================================================================
Script Purpose: 
    This script creates 7 tables in the gold layer. It firstly checks the existence of
    each table, deletes them if they exist and recreates them.

    Run this script to redefine the structure of your tables.
============================================================================================
*/
-- Drop Table gold.dim_customers if it exists
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
DROP TABLE gold.dim_customers;
GO

-- Create Table gold.dim_customers
CREATE TABLE gold.dim_customers
(
    customer_key INT IDENTITY(101, 1) PRIMARY KEY,
	customer_id INT,
    customer_name NVARCHAR(50),
    email NVARCHAR(50),
    country_code NVARCHAR(50),
	country_name NVARCHAR(50),
    age INT,
    signup_date DATE,
    marketing_opt_in NVARCHAR(50),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table gold.fact_events if it exists
IF OBJECT_ID('gold.fact_events', 'U') IS NOT NULL
DROP TABLE gold.fact_events;
GO

-- Create Table gold.fact_events
CREATE TABLE gold.fact_events
(
	event_id INT,
    session_key INT,
    event_timestamp DATETIME,
    event_type NVARCHAR(50),
    product_key INT,
    quantity INT,
    cart_size INT,
    payment NVARCHAR(50),
    discount_pct INT,
    amount_usd DECIMAL(8, 1),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
	CONSTRAINT pk_events PRIMARY KEY(event_id)
);

-- Drop Table gold.fact_order_items if it exists
IF OBJECT_ID('gold.fact_order_items', 'U') IS NOT NULL
DROP TABLE gold.fact_order_items;
GO

-- Create Table gold.fact_order_items
CREATE TABLE gold.fact_order_items
(
    order_id INT,
    product_key INT,
    unit_price_usd DECIMAL(8, 2),
    quantity INT,
    line_total_usd DECIMAL(8, 2),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table gold.fact_orders if it exists
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
DROP TABLE gold.fact_orders;
GO

-- Create Table gold.fact_orders
CREATE TABLE gold.fact_orders
(
	order_id INT,
    customer_key INT,
    order_time DATETIME,
    payment_method NVARCHAR(50),
    discount_pct INT,
    subtotal_usd DECIMAL(8, 2),
    total_usd DECIMAL(8, 2),
    country_code NVARCHAR(50),
    device NVARCHAR(50),
    traffic_source NVARCHAR(50),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
	CONSTRAINT pk_orders PRIMARY KEY(order_id)
);

-- Drop Table gold.dim_products if it exists
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
DROP TABLE gold.dim_products;
GO

-- Create Table gold.dim_products
CREATE TABLE gold.dim_products
(
    product_key INT IDENTITY(101, 1) PRIMARY KEY,
	product_id INT,
    category NVARCHAR(50),
    product_name NVARCHAR(50),
    price_usd DECIMAL(8, 2),
    cost_usd DECIMAL(8, 2),
    margin_usd DECIMAL(8, 2),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table gold.fact_reviews if it exists
IF OBJECT_ID('gold.fact_reviews', 'U') IS NOT NULL
DROP TABLE gold.fact_reviews;
GO

-- Create Table gold.fact_reviews
CREATE TABLE gold.fact_reviews
(
    review_id INT,
    order_id INT,
    product_key INT,
    rating INT,
    review_text NVARCHAR(100),
    review_time DATETIME,

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
	CONSTRAINT pk_reviews PRIMARY KEY(review_id)
);

-- Drop Table gold.dim_sessions if it exists
IF OBJECT_ID('gold.dim_sessions', 'U') IS NOT NULL
DROP TABLE gold.dim_sessions;
GO

-- Create Table gold.dim_sessions
CREATE TABLE gold.dim_sessions
(
    session_key INT IDENTITY(101, 1) PRIMARY KEY,
	user_session_id INT,
    customer_key INT,
    start_time DATETIME,
    device NVARCHAR(50),
    traffic_source NVARCHAR(50),
    country_code NVARCHAR(50),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);
