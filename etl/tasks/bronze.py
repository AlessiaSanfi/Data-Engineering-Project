import polars as pl
import duckdb
import os
from prefect import task

@task(name="Ingest All CSVs to Bronze")
def ingest_all_raw_data(db_path):
    datasets = [
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_products_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_sellers_dataset.csv"
    ]
    
    # Ricavo la cartella dal percorso del database
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Cartella {db_dir} creata.")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    for file in datasets:
        table_name = file.replace("olist_", "").replace("_dataset.csv", "")
        path = f"data/raw/{file}"
        
        # Carico con Polars e salvo in DuckDB
        df = pl.read_csv(path)
        con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM df")
        print(f"Caricato {table_name} nel layer Bronze")
    
    con.close()