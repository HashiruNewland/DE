-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "cf6e5417-b842-4953-bd4a-8376d9e42932",
-- META       "default_lakehouse_name": "MaterializedLakeView",
-- META       "default_lakehouse_workspace_id": "8d919179-54dc-4df2-8d9f-940230d115c3",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "cf6e5417-b842-4953-bd4a-8376d9e42932"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

-- Welcome to your new notebook 
-- Type here in the cell editor to add code! 
-- CREATE MATERIALIZED LAKE VIEW <mlv_name> AS select_statement

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.products (
   product_id INT,
   product_name STRING,
   price DOUBLE
);

INSERT INTO bronze.products VALUES
(101, 'Laptop', 1200.50),
(102, 'Smartphone', 699.99),
(103, 'Tablet', 450.00);

CREATE TABLE IF NOT EXISTS bronze.orders (
   order_id INT,
   product_id INT,
   quantity INT,
   order_date DATE
   );

INSERT INTO bronze.orders VALUES
   (1001, 101, 2, '2025-06-01'),
   (1002, 103, 1, '2025-06-02'),
   (1003, 102, 3, '2025-06-03');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

ALTER TABLE bronze.products SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS silver;

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.cleaned_order_data AS
SELECT 
   o.order_id,
   o.order_date,
   o.product_id,
   p.product_name,
   o.quantity,
   p.price,
   o.quantity * p.price AS revenue
FROM bronze.orders o
JOIN bronze.products p
ON o.product_id = p.product_id;

CREATE SCHEMA IF NOT EXISTS GOLD;

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.product_sales_summary AS
SELECT
   product_id,
   product_name,
   SUM(quantity) AS total_quantity_sold,
   SUM(revenue) AS total_revenue,
   ROUND(AVG(revenue), 2) AS average_order_value
FROM
   silver.cleaned_order_data
GROUP BY
   product_id,
   product_name;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM gold.product_sales_summary;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
