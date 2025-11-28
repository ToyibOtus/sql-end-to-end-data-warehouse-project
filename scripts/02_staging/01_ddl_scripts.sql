/*
============================================================================================
DDL Script: Create Tables in Bronze Layer
============================================================================================
Script Purpose: 
    This script creates 7 tables in the bronze layer. It firstly checks the existence of
    each table, deletes them if they exist and recreates them.
    
    Run this script to redefine the structure of your tables.
============================================================================================
*/
-- Drop Table [bronze.archive_customers] if it exists
IF OBJECT_ID('bronze.archive_customers', 'U') IS NOT NULL
DROP TABLE bronze.archive_customers;
GO

-- Create Table [bronze.archive_customers]
CREATE TABLE bronze.archive_customers
(
    customer_id INT,
    [name] NVARCHAR(50),
    email NVARCHAR(50),
    country NVARCHAR(50),
    age INT,
    signup_date DATE,
    marketing_opt_in NVARCHAR(50),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_events] if it exists
IF OBJECT_ID('bronze.archive_events', 'U') IS NOT NULL
DROP TABLE bronze.archive_events;
GO

-- Create Table [bronze.archive_events]
CREATE TABLE bronze.archive_events
(
    event_id INT,
    [session_id] INT,
    [timestamp] DATETIME,
    event_type NVARCHAR(50),
    product_id DECIMAL(8, 1),
    qty DECIMAL(8, 1),
    cart_size DECIMAL(8, 1),
    payment NVARCHAR(50),
    discount_pct DECIMAL(8, 1),
    amount_usd DECIMAL(8, 1),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_order_items] if it exists
IF OBJECT_ID('bronze.archive_order_items', 'U') IS NOT NULL
DROP TABLE bronze.archive_order_items;
GO

-- Create Table [bronze.archive_order_items]
CREATE TABLE bronze.archive_order_items
(
    order_id INT,
    product_id INT,
    unit_price_usd DECIMAL(8, 2),
    quantity INT,
    line_total_usd DECIMAL(8, 2),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_orders] if it exists
IF OBJECT_ID('bronze.archive_orders', 'U') IS NOT NULL
DROP TABLE bronze.archive_orders;
GO

-- Create Table [bronze.archive_orders]
CREATE TABLE bronze.archive_orders
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
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_products] if it exists
IF OBJECT_ID('bronze.archive_products', 'U') IS NOT NULL
DROP TABLE bronze.archive_products;
GO

-- Create Table [bronze.archive_products]
CREATE TABLE bronze.archive_products
(
    product_id INT,
    category NVARCHAR(50),
    [name] NVARCHAR(50),
    price_usd DECIMAL(8, 2),
    cost_usd DECIMAL(8, 2),
    margin_usd DECIMAL(8, 2),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_reviews] if it exists
IF OBJECT_ID('bronze.archive_reviews', 'U') IS NOT NULL
DROP TABLE bronze.archive_reviews;
GO

-- Create Table [bronze.archive_reviews]
CREATE TABLE bronze.archive_reviews
(
    review_id INT,
    order_id INT,
    product_id INT,
    rating INT,
    review_text NVARCHAR(100),
    review_time NVARCHAR(50),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);

-- Drop Table [bronze.archive_sessions] if it exists
IF OBJECT_ID('bronze.archive_sessions', 'U') IS NOT NULL
DROP TABLE bronze.archive_sessions;
GO

-- Create Table [bronze.archive_sessions]
CREATE TABLE bronze.archive_sessions
(
    [session_id] INT,
    customer_id INT,
    start_time DATETIME,
    device NVARCHAR(50),
    [source] NVARCHAR(50),
    country NVARCHAR(50),
	dwh_batch_id UNIQUEIDENTIFIER,
	dwh_raw_rows NVARCHAR(MAX),
	dwh_row_hash VARBINARY(64),
	dwh_create_time DATETIME DEFAULT GETDATE()
);
