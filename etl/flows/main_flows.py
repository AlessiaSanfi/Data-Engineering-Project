import os
from dotenv import load_dotenv
from prefect import flow

# Import dei task
from etl.tasks.bronze import ingest_all_raw_data
from etl.tasks.silver import clean_olist_data

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

@flow(name="Brazilian E-Commerce Pipeline")
def main_flow():
    # 1. BRONZE (fase raw)
    print("--- Avvio Fase Bronze ---")
    ingest_all_raw_data(DB_PATH)

    # 2. SILVER (pulizia e tipizzazione dati)
    print("--- Avvio Fase Silver ---")
    clean_olist_data(DB_PATH)
    
    print("--- PIPELINE COMPLETATA CON SUCCESSO ---")

if __name__ == "__main__":
    main_flow()