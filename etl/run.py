import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# --- AGGIUNTA LOGICA PER TROVARE LA ROOT ---
# Risale di un livello per vedere la cartella 'Data-Engineering-Project'
root_path = Path(__file__).parent.parent
sys.path.append(str(root_path))

# --- IMPOSTAZIONI AMBIENTALI PREFECT ---
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_API_SERVICES_RUN_IN_APP"] = "True"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "True"

from etl.flows.main_flows import main_flow

if __name__ == "__main__":
    # Cerca il .env nella root del progetto
    load_dotenv(dotenv_path=root_path / ".env")
    
    db_path = os.getenv("DB_PATH", "data/warehouse.duckdb")
    
    print(f"Avvio della Pipeline Olist dalla cartella 'etl'...")
    try:
        main_flow(db_path)
        print("\nPipeline completata con successo!")
    except Exception as e:
        print(f"\nErrore durante l'esecuzione: {e}")