import polars as pl
import duckdb
import os
from prefect import task

@task(name="Ingest All Olist CSVs")
def ingest_all_raw_data(db_path: str):
    """
    Legge tutti i CSV dalla cartella data/raw e li carica nello schema Bronze di DuckDB.
    """
    raw_dir = os.path.join("data", "raw")
    conn = duckdb.connect(db_path)
    
    # Crea lo schema bronze come richiesto [cite: 77]
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    # Lista i file nella cartella
    files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
    
    for file_name in files:
        # Crea un nome tabella pulito (es. olist_orders_dataset)
        table_name = file_name.replace(".csv", "")
        file_path = os.path.join(raw_dir, file_name)
        
        print(f"Caricamento di {file_name} in bronze.{table_name}...")
        
        # Ingestione rapida con Polars [cite: 25, 106]
        df = pl.read_csv(file_path)
        
        # Scrittura in DuckDB [cite: 24, 39]
        conn.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM df")
    
    conn.close()
    return "Ingestion Bronze completata correttamente."