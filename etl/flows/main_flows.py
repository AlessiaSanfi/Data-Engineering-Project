import os
from dotenv import load_dotenv
from prefect import flow

# Import dei task dalle cartelle corrette 
from etl.tasks.bronze import ingest_all_raw_data
# Una volta pronti, importerai anche questi:
# from etl.tasks.silver import clean_olist_data
# from etl.tasks.gold import build_olist_star_schema

# Carica le variabili d'ambiente (.env) 
load_dotenv()
DB_PATH = os.getenv("DB_PATH")

@flow(name="Brazilian E-Commerce")
def main_flow():
    """
    Orchestra il ciclo di vita del dato: Sorgente -> Bronze -> Silver -> Gold [cite: 2, 80]
    """
    
    # 1. STEP BRONZE: Ingestione Raw [cite: 81, 82]
    print("--- Avvio Fase Bronze ---")
    status_bronze = ingest_all_raw_data(DB_PATH)
    print(f"[OK] {status_bronze}")

    # 2. STEP SILVER: Pulizia e Tipizzazione [cite: 84, 85]
    # In futuro: status_silver = clean_olist_data(DB_PATH)
    
    # 3. STEP GOLD: Fact & Dimension Tables (Star Schema) [cite: 88, 89]
    # In futuro: status_gold = build_olist_star_schema(DB_PATH)

if __name__ == "__main__":
    main_flow()