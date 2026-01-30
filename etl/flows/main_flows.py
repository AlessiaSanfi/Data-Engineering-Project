import os
from dotenv import load_dotenv
from prefect import flow

# Import dei task
from etl.tasks.bronze import ingest_all_raw_data
from etl.tasks.silver import clean_olist_data
from etl.tasks.gold import build_olist_star_schema

# Definizione del flusso principale
@flow(name="Brazilian E-Commerce Pipeline")
def main_flow(db_path: str):
    # 1. BRONZE (fase raw)
    print(f"--- Avvio Fase Bronze su {db_path} ---")
    ingest_all_raw_data(db_path)

    # 2. SILVER (pulizia e tipizzazione dati)
    print("--- Avvio Fase Silver ---")
    clean_olist_data(db_path)

    # 3. GOLD (costruzione star schema)
    print("--- Avvio Fase Gold ---")
    build_olist_star_schema(db_path)

    print("--- PIPELINE COMPLETATA CON SUCCESSO ---")
    

# Esecuzione del flusso principale se eseguito come script
if __name__ == "__main__":
    load_dotenv()
    DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")
    main_flow(DB_PATH)