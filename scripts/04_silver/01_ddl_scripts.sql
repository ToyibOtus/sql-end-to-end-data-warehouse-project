/*
============================================================================================
DDL Script: Create Tables in Silver Layer
============================================================================================
Script Purpose: 
    This script creates 7 tables in the silver layer. It firstly checks the existence of
    each table, deletes them if they exist and recreates them.
    
    Run this script to redefine the structure of your tables.
============================================================================================
*/
-- Drop Table [silver.archive_customers] if it exists
IF OBJECT_ID('silver.archive_customers', 'U') IS NOT NULL
DROP TABLE silver.archive_customers;
GO

-- Create Table [silver.archive_customers]
CREATE TABLE silver.archive_customers
(
    customer_id INT,
    [name] NVARCHAR(50),
    email NVARCHAR(50),
    country NVARCHAR(50),
	country_name NVARCHAR(50),
    age INT,
    signup_date DATE,
    marketing_opt_in NVARCHAR(50),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_events] if it exists
IF OBJECT_ID('silver.archive_events', 'U') IS NOT NULL
DROP TABLE silver.archive_events;
GO

-- Create Table [silver.archive_events]
CREATE TABLE silver.archive_events
(
    event_id INT,
    [session_id] INT,
    [timestamp] DATETIME,
    event_type NVARCHAR(50),
    product_id INT,
    qty INT,
    cart_size INT,
    payment NVARCHAR(50),
    discount_pct INT,
    amount_usd DECIMAL(8, 1),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_order_items] if it exists
IF OBJECT_ID('silver.archive_order_items', 'U') IS NOT NULL
DROP TABLE silver.archive_order_items;
GO

-- Create Table [silver.archive_order_items]
CREATE TABLE silver.archive_order_items
(
    order_id INT,
    product_id INT,
    unit_price_usd DECIMAL(8, 2),
    quantity INT,
    line_total_usd DECIMAL(8, 2),
	
	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_orders] if it exists
IF OBJECT_ID('silver.archive_orders', 'U') IS NOT NULL
DROP TABLE silver.archive_orders;
GO

-- Create Table [silver.archive_orders]
CREATE TABLE silver.archive_orders
(
    order_id INT,
    customer_id INT,
    order_time DATETIME,
    payment_method NVARCHAR(50),
    discount_pct INT,
    subtotal_usd DECIMAL(8, 2),
    total_usd DECIMAL(8, 2),
    country NVARCHAR(50),
    device NVARCHAR(50),
    [source] NVARCHAR(50),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_products] if it exists
IF OBJECT_ID('silver.archive_products', 'U') IS NOT NULL
DROP TABLE silver.archive_products;
GO

-- Create Table [silver.archive_products]
CREATE TABLE silver.archive_products
(
    product_id INT,
    category NVARCHAR(50),
    [name] NVARCHAR(50),
    price_usd DECIMAL(8, 2),
    cost_usd DECIMAL(8, 2),
    margin_usd DECIMAL(8, 2),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_reviews] if it exists
IF OBJECT_ID('silver.archive_reviews', 'U') IS NOT NULL
DROP TABLE silver.archive_reviews;
GO

-- Create Table [silver.archive_reviews]
CREATE TABLE silver.archive_reviews
(
    review_id INT,
    order_id INT,
    product_id INT,
    rating INT,
    review_text NVARCHAR(100),
    review_time DATE,

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [silver.archive_sessions] if it exists
IF OBJECT_ID('silver.archive_sessions', 'U') IS NOT NULL
DROP TABLE silver.archive_sessions;
GO

-- Create Table [silver.archive_sessions]
CREATE TABLE silver.archive_sessions
(
    [session_id] INT,
    customer_id INT,
    start_time DATETIME,
    device NVARCHAR(50),
    [source] NVARCHAR(50),
    country NVARCHAR(50),

	-- Metadata Columns
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);
