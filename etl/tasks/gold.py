import duckdb
from prefect import task

@task(name="Build Olist Star Schema (Gold)")
def build_olist_star_schema(db_path):
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    
    print("Costruzione Layer Gold: Fact e Dimension tables")

    # DIM_CUSTOMERS
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_customers AS 
        SELECT 
            customer_id,
            customer_city,
            customer_state
        FROM silver.customers
    """)

    # DIM_PRODUCTS
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_products AS 
        SELECT 
            product_id,
            product_category_name
        FROM silver.products
    """)

    # DIM_TIME
    con.execute("""
        CREATE OR REPLACE TABLE gold.dim_time AS
        SELECT DISTINCT
            order_purchase_timestamp,
            EXTRACT(day FROM order_purchase_timestamp) AS day,
            EXTRACT(month FROM order_purchase_timestamp) AS month,
            EXTRACT(year FROM order_purchase_timestamp) AS year,
            EXTRACT(quarter FROM order_purchase_timestamp) AS quarter,
            DAYNAME(order_purchase_timestamp) AS day_of_week
        FROM silver.orders
        WHERE order_purchase_timestamp IS NOT NULL
    """)

    # FACT_SALES
    # Unisco gli ordini agli articoli e calcolo i tempi di consegna
    con.execute("""
        CREATE OR REPLACE TABLE gold.fact_sales AS 
        SELECT 
            o.order_id,
            o.customer_id,
            oi.product_id,
            oi.price,
            oi.freight_value,
            o.order_purchase_timestamp,
            -- Calcolo del tempo di consegna in giorni
            date_diff('day', o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_time_days
        FROM silver.orders o
        JOIN silver.order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status = 'delivered'
    """)

    con.close()
    return "Layer Gold costruito con successo"