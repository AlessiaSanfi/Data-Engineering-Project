import duckdb
from prefect import task

@task(name="Clean Olist Data (Silver)")
def clean_olist_data(db_path):
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    # TABELLA ORDERS
    # 1. Pulizia ordini: conversione date da stringa a TIMESTAMP
    con.execute("""
        CREATE OR REPLACE TABLE silver.orders AS 
        SELECT 
            order_id, 
            customer_id,
            order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) as order_purchase_timestamp,
            CAST(order_delivered_customer_date AS TIMESTAMP) as order_delivered_customer_date,
            CAST(order_estimated_delivery_date AS TIMESTAMP) as order_estimated_delivery_date
        FROM bronze.orders
        WHERE order_id IS NOT NULL
    """)

    # TABELLA ORDERS_ITEMS
    # 2. Pulizia articoli: prezzi come float
    con.execute("""
        CREATE OR REPLACE TABLE silver.order_items AS 
        SELECT 
            order_id,
            order_item_id,
            product_id,
            seller_id,
            CAST(price AS DOUBLE) as price,
            CAST(freight_value AS DOUBLE) as freight_value
        FROM bronze.order_items
    """)

    # 3. Altre tabelle: copia pulita
    con.execute("CREATE OR REPLACE TABLE silver.products AS SELECT * FROM bronze.products")
    con.execute("CREATE OR REPLACE TABLE silver.customers AS SELECT * FROM bronze.customers")
    con.execute("CREATE OR REPLACE TABLE silver.sellers AS SELECT * FROM bronze.sellers")
    
    con.close()
    return "Silver Layer completato con successo"