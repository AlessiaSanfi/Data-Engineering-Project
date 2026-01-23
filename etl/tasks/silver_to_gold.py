#--------------------------------------------------------------
# Script per trasformare i dati dal layer Silver al layer Gold in uno schema a stella.
## Source of Truth: I dati finali (Gold) risiedono nel Data Lake come Parquet, non sono bloccati dentro DuckDB.
## Performance: DuckDB viene usato come motore di calcolo "volante" per trasformare i file, sfruttando la velocità dei formati colonnari.
## Flessibilità: Se il database DuckDB si corrompe, puoi ricostruire tutto in pochi secondi leggendo i file Gold.
#--------------------------------------------------------------

import duckdb
import os

#--------------------------------------------------------------
# PERCORSI
#--------------------------------------------------------------
# Percorsi specifici per evitare conflitti tra file diversi
SILVER_ORDERS = "data/lake/silver/orders_*.parquet" # Solo i file degli ordini partizionati
SILVER_ITEMS = "data/lake/silver/olist_order_items_dataset.parquet"
SILVER_PRODUCTS = "data/lake/silver/olist_products_dataset.parquet"
SILVER_CUSTOMERS = "data/lake/silver/olist_customers_dataset.parquet"

GOLD_DIR = "data/lake/gold/"
DB_PATH = "data/warehouse.duckdb"

#--------------------------------------------------------------
# FUNZIONE PRINCIPALE
#--------------------------------------------------------------
def run_silver_to_gold():
    con = duckdb.connect(DB_PATH)
    
    print("Creazione Layer Gold (Star Schema) dai file Silver...")

    # Mi assicuro che la cartella gold esista
    os.makedirs(GOLD_DIR, exist_ok=True)

    # 1. Caricamento mirato dei file e creazione Tabelle Temporanee dai Parquet Silver per elaborazione (DuckDB può fare JOIN direttamente tra file Parquet)
    con.execute(f"CREATE OR REPLACE TEMP TABLE orders AS SELECT * FROM read_parquet('{SILVER_ORDERS}')")
    con.execute(f"CREATE OR REPLACE TEMP TABLE items AS SELECT * FROM read_parquet('{SILVER_ITEMS}')")
    con.execute(f"CREATE OR REPLACE TEMP TABLE products AS SELECT * FROM read_parquet('{SILVER_PRODUCTS}')")
    con.execute(f"CREATE OR REPLACE TEMP TABLE customers AS SELECT * FROM read_parquet('{SILVER_CUSTOMERS}')")

    print("Generazione Fact Table e Dimensioni...")

    # 2. Fact Sales (unisco ordini e item)
    fact_sales_query = """
    SELECT 
        o.order_id, o.customer_id, i.product_id, i.price, i.freight_value,
        date_diff('day', CAST(o.order_purchase_timestamp AS TIMESTAMP), CAST(o.order_delivered_customer_date AS TIMESTAMP)) as delivery_time_days,
        o.order_purchase_timestamp
    FROM orders o
    JOIN items i ON o.order_id = i.order_id
    """
    con.execute(f"COPY ({fact_sales_query}) TO '{GOLD_DIR}fact_sales.parquet' (FORMAT PARQUET)")

    # 3. Dim Products
    con.execute(f"COPY (SELECT product_id, product_category_name FROM products) TO '{GOLD_DIR}dim_products.parquet' (FORMAT PARQUET)")

    # 4. Dim Customers
    con.execute(f"COPY (SELECT customer_id, customer_city, customer_state FROM customers) TO '{GOLD_DIR}dim_customers.parquet' (FORMAT PARQUET)")

    # 5. Dim Time
    dim_time_query = """
    SELECT DISTINCT 
        order_purchase_timestamp,
        extract(year from CAST(order_purchase_timestamp AS TIMESTAMP)) as year,
        extract(month from CAST(order_purchase_timestamp AS TIMESTAMP)) as month,
        dayname(CAST(order_purchase_timestamp AS TIMESTAMP)) as day_of_week
    FROM orders
    """
    con.execute(f"COPY ({dim_time_query}) TO '{GOLD_DIR}dim_time.parquet' (FORMAT PARQUET)")

    print(f"Layer Gold completato e salvato in: {GOLD_DIR}")
    con.close()

if __name__ == "__main__":
    run_silver_to_gold()