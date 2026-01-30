import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from prefect import flow

ROOT = Path(__file__).resolve().parents[2]  # .../Data-Engineering-Project
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ------------------------------------------------------------
# IMPORT TASK / SCRIPT PHASE 2
# ------------------------------------------------------------

# Step 1: Esplosione dati CSV -> Parquet (Landing Zone)
from scripts.esplosione_dati import esplodi_dati

# Step 2: Bronze incrementale (Landing Zone -> DB)
from scripts.bronze_incremental import run_bronze_incremental

# Step 3 & 4: Silver + Gold (DB-based, già esistenti)
from etl.tasks.silver import clean_olist_data
from etl.tasks.gold import build_olist_star_schema


# ------------------------------------------------------------
# DEFINIZIONE FLOW PHASE 2
# ------------------------------------------------------------
@flow(name="Brazilian E-Commerce Pipeline - Phase 2 (Incremental)")
def main_flow_fase2(db_path: str):
    """
    Pipeline Phase 2 (Incrementale):

    1. CSV raw -> Parquet mensili (Landing Zone)
    2. Landing Zone -> Bronze (DB, incrementale anti-duplicate)
    3. Bronze -> Silver (pulizia + validazioni)
    4. Silver -> Gold (Star Schema)
    """

    # --------------------------------------------------------
    # STEP 1 - ESPLOSIONE DATI (FILE-BASED)
    # --------------------------------------------------------
    print("\n--- STEP 1: ESPLOSIONE DATI (CSV -> Landing Zone) ---")
    esplodi_dati()

    # --------------------------------------------------------
    # STEP 2 - BRONZE INCREMENTAL (DB-BASED)
    # --------------------------------------------------------
    print("\n--- STEP 2: BRONZE INCREMENTALE (Landing Zone -> DB) ---")
    run_bronze_incremental(db_path)

    # --------------------------------------------------------
    # STEP 3 - SILVER (PULIZIA + VALIDAZIONI)
    # --------------------------------------------------------
    print("\n--- STEP 3: SILVER LAYER ---")
    clean_olist_data(db_path)

    # --------------------------------------------------------
    # STEP 4 - GOLD (STAR SCHEMA)
    # --------------------------------------------------------
    print("\n--- STEP 4: GOLD LAYER (STAR SCHEMA) ---")
    build_olist_star_schema(db_path)

    print("\n--- PIPELINE PHASE 2 COMPLETATA CON SUCCESSO ---")


# ------------------------------------------------------------
# ENTRYPOINT SCRIPT
# ------------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()

    DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

    print("Avvio pipeline Phase 2 (incrementale)")
    print(f"Database target: {DB_PATH}")

    main_flow_fase2(DB_PATH)
