from etl.flows.main_flows import main_flow
import os
from dotenv import load_dotenv

# Esecuzione del flusso principale se eseguito come script
if __name__ == "__main__":
    load_dotenv()
    db_path = os.getenv("DB_PATH", "data/warehouse.duckdb")
    
    # Avvia la pipeline completa
    print("Avvio della Pipeline Olist...")
    main_flow(db_path)
    print("Pipeline completata con successo!")