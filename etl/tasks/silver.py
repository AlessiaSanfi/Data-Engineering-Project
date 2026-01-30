import duckdb
import pandera.pandas as pa
from prefect import task

# --- DEFINIZIONE SCHEMI DI VALIDAZIONE ---
# Controllo colonne stringa con valori predefiniti (stati ordine)
orders_schema = pa.DataFrameSchema({
    "order_status": pa.Column(str, pa.Check.isin([
        'delivered', 'shipped', 'canceled', 'invoiced', 
        'processing', 'approved', 'unavailable', 'created'
    ]))
})

# Controllo colonne numeriche (prezzi >= 0) e ID non nulli
order_items_schema = pa.DataFrameSchema({
    "price": pa.Column(float, pa.Check.ge(0)), 
    "freight_value": pa.Column(float, pa.Check.ge(0)),
    "order_id": pa.Column(str, nullable=False)
})

@task(name="Clean Olist Data (Silver)")
def clean_olist_data(db_path):
    con = duckdb.connect(db_path)
    try:
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
        # Validazione Pandera: se fallisce, il task si blocca qui
        orders_schema.validate(con.execute("SELECT order_status FROM silver.orders").df())

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
        # Validazione Pandera
        order_items_schema.validate(con.execute("SELECT order_id, price, freight_value FROM silver.order_items").df())

        # 3. Altre tabelle: copia pulita
        con.execute("CREATE OR REPLACE TABLE silver.products AS SELECT * FROM bronze.products")
        con.execute("CREATE OR REPLACE TABLE silver.customers AS SELECT * FROM bronze.customers")
        ## con.execute("CREATE OR REPLACE TABLE silver.sellers AS SELECT * FROM bronze.sellers")
    
        return "Silver Layer validato e completato"
    finally:
        con.close()