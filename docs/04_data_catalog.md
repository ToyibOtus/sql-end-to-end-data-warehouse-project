# Data Dictionary for Gold Layer

## Overview
The **Gold layer** holds business-ready data, integrated to support analysis and analytical reporting. It comprises of the following:
* **dimension**, and
* **fact** tables/views.

These objects contain key business metrics that support analysis, and  thus enabling data-driven decisions.

---

### 1. gold.dim_customers_view
* **Purpose:** Stores customer information, enriched with geographic and demographic data.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|customer_key        |INT                           |Surrogate key uniquely identifying each customer record.
|customer_id         |INT                           |Unique numerical value assigned to each customer record.
|customer_name       |NVARCHAR(50)                  |The customer's full name recorded in the system. 
|email               |NVARCHAR(50)                  |The customer's email recorded in the system.
|country_code        |NVARCHAR(50)                  |The customer's country code of residence (e.g., 'CA').
|country_name        |NVARCHAR(50)                  |Name of customer's country of residence (e.g., 'Canada').
|age                 |NVARCHAR(50)                  |The customer's age recorded in the system (e.g., '40').
|signup_date         |DATE                          |The date the customer's record was created in the system (e.g., '2022-06-09').
|marketing_opt_in    |NVARCHAR(50)                  |A record identifying whether a customer agreed to receiving marketing information (e.g., 'True', 'False').

###  2. gold.dim_products_view
* **Purpose:** Provides detailed information about the products, their attributes, as well as key business metrics.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|product_key         |INT                           |Surrogate key uniquely identifying each product record.
|product_id          |INT                           |Unique numerical value assigned to each product record.
|category            |NVARCHAR(50)                  |A broader classification of products (e.g., 'Electronics', 'Home & Kitchen').
|product_name        |NVARCHAR(50)                  |Descriptive name of the product, including key details such as type and colour, and product code (e.g., Lamp Chocolate 506).
|price_usd           |DECIMAL(8, 2)                 |The base price of products in USD
|cost_usd            |DECIMAL(8, 2)                 |The cost of products in USD
|margin_usd          |DECIMAL(8, 2)                 |The profit in USD generated when a product is sold, calculated by subtracting product cost from price.

### 3. gold.dim_sessions_view
* **Purpose:** Provides detatiled information about customer session and their attributes.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|session_key         |INT                           |Surrogate key uniquely identifying each session record.
|user_session_id     |INT                           |Unique numerical value assigned to each record in the dimension table.
|customer_key        |INT                           |A surrogate key linking dim_sessions_view to dim_customer_view.
|start_time          |DATETIME                      |Timestamp of each session (e.g., '2023-11-05 04:22:17.000').
|device              |NVARCHAR(50)                  |The device the session was carried out (e.g., 'desktop', 'mobile').
|traffic_source      |NVARCHAR(50)                  |Channel that brought the user/customer into the session (e.g., 'paid', 'direct').
|country_code        |NVARCHAR(50)                  |The country code of the geographical location of the session (e.g., 'MX', 'US').

### 4. gold.fact_orders_view
* **Purpose:** Provides a broad information on each order made by customers.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|order_id            |INT                           |A primary key uniquely identifying each record in fact table.
|customer_key        |INT                           |A surrogate key linking fact_orders_view to dim_customer_view.
|order_time          |DATETIME                      |The time the order was entered into the system.
|payment_method      |NVARCHAR(50)                  |The method of payment made by the customer (e.g., 'card', 'wallet').
|discount_pct        |INT                           |The percentage discount of total price per order (e.g., '20'). 
|subtotal_usd        |DECIMAL(8, 2)                 |The base total price in USD before percentage discount is applied (e.g., '107.15').
|total_usd           |DECIMAL(8, 2)                 |The total price paid in USD after percentage discount is applied (e.g., '85.72').
|country_code        |NVARCHAR(50)                  |The country code of the geographical location the order was placed (e.g., 'PL').
|device              |NVARCHAR(50)                  |The device used to place the order (e.g., 'desktop').
|traffic_source      |NVARCHAR(50)                  |The channel through which the customer discovered the organization's site (e.g., 'organic').

### 5. gold.fact_order_items_view
* **Purpose:** Provides a more detailed information on each order, providing key information such as all products, and quantity of quantity of products purchased.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|order_id            |INT                           |A primary key linking fact_order_items_view to fact_orders_view.
|prduct_key          |INT                           |A surrogate key linking fact_order_items_view to dim_products_view.
|unit_price_usd      |DECIMAL(8, 2)                 |The base price of product ordered in USD.
|quantity            |INT                           |The quantity of products ordered. 
|line_total_usd      |DECIMAL(8, 2)                 |The total price of a product line, calculated by multiplying unit_price by quantity.

### 6. gold.fact_events_view
* **Purpose:** Provides detailed information about each session.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|event_id            |INT                           |A primary key uniquely identifying each event.
|session_key         |INT                           |A surrogate key linking fact_events_view to dim_sessions_view.
|event_timestamp     |DATETIME                      |A timestamp of each event, which progresses across a session.
|event_type          |NVARCHAR(50)                  |The type of event made in every session (e.g., 'page_view', 'add_to_cart', 'purchase', 'checkout').
|product_key         |INT                           |A surrogate key linking fact_events_view to dim_products_view.
|quantity            |INT                           |Quantity of products user interacted with for every event type.
|cart_size           |INT                           |A metric that reflects the total quantity of products added to carts after check_out.
|payment_method      |NVARCHAR(50)                  |The method of payment made by customer (e.g., 'card', 'cash_on_delivery').
|discount_pct        |INT                           |The percentage discount of total price of products ordered.
|amount_usd          |DECIMAL(8, 1)                 |The total amount in USD paid by the customer after percentage discount in applied.

### 7. gold.fact_reviews_view
* **Purpose:** Holds information on customer reviews about each product.
* **Columns:**

|  **Column Name**   |  **Data Type**               |  **Description**   |
|--------------------|------------------------------|--------------------|
|review_id           |INT                           |A primary key uniquely identifying each record in fact table.
|order_id            |INT                           |A primary key linking fact_reviews_view to fact_orders_view.
|product_key         |INT                           |A surrogate key linking fact_reviews_view to dim_products_view.
|rating              |INT                           |The rating on a scale of 1-5 a customer gives a product after order.
|review_text         |NVARCHAR(50)                  |The review text left by the customer, reflecting the rating.
|review_time         |DATE                          |The time  when a customer submitted a review.

### 8. gold.sessions_report_view
* **Purpose:** A report that holds detailed information about customers' sessions. It provides insight into customer's behaviour and interactions with products.
* **Columns:**

|  **Column Name**           |  **Data Type**                       |  **Description**   |
|----------------------------|--------------------------------------|--------------------|
|session_key                 |INT                                   |A surrogate key that uniquely identifies each session.
|user_session_id             |INT                                   |A unique numerical value assigned to each session.
|customer_key                |INT                                   |A surrogate key that links table to gold.dim_customers_view.
|customer_name               |NVARCHAR(50)                          |Customer's name recoreded in the system.
|country_code                |NVARCHAR(50)                          |The country code of the customer's country of residence.
|country_name                |NVARCHAR(50)                          |The name of customer's country of residence.
|marketing_opt_in            |NVARCHAR(50)                          |A boolean value that indicates if the customer opted for marketing information (e.g, 'True', 'False').
|signup_date                 |DATE                                  |The date the customer record entered the system.
|start_time                  |DATETIME                              |The time the session started.
|device                      |NVARCHAR(50)                          |The device the session took place on (e.g., 'mobile', 'desktop', etc.).
|traffic_source              |NVARCHAR(50)                          |The channel that brought the customer into the session (e.g., 'paid', 'email', etc.).
|event_timestamp_start       |DATETIME                              |The time an event started.
|event_timestamp_end         |DATETIME                              |The time an event ended.
|event_duration_minute       |INT                                   |The timespan or duration of the event in minute.
|total_events                |INT                                   |The total number of events per session
|total_products_interacted   |INT                                   |The number of unique products each customer interacted with per session.
|total_products_carted       |INT                                   |The number of unique products carted per session.
|total_quantity_carted       |INT                                   |The total quantity of products carted per session.
|total_nr_checkouts          |INT                                   |The total number of checkouts per session.
|cart_size_at_checkout       |INT                                   |The total quantity of products carted per session at checkout.
|total_nr_purchases          |INT                                   |The total number of purchases made per session.
|revenue_generated           |DECIMAL(38, 1)                        |The total revenue generated per session.
|checkout_started            |VARCHAR(3)                            |An alphabetical value indicating whether a checkout started or not (e.g., 'Yes', 'No').
|purchase_made               |VARCHAR(3)                            |An alphabetical value indicating whether a purchase was made or not (e.g., 'Yes', 'No').
|completed_checkout          |VARCHAR(11)                           |A alphabetical value indicating whether a checkout was completed or abandoned, or no checkout was made. (e.g., 'No Checkout', 'Completed', 'Abandoned'). 


   





