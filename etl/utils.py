import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

# Funzione per ottenere la connessione al database
def get_connection():
    return duckdb.connect(DB_PATH)