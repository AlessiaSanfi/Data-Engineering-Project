#--------------------------------------------------------------
# Questo script esegue un caricamento incrementale dei file Parquet dalla Landing Zone al layer Silver, assicurando che i file già processati non vengano riprocessati.
## Scansione: Lo script guarda tutti i file .parquet nella landing_zone.
## Confronto: Verifica quali file sono già stati processati (possiamo farlo salvando un piccolo file di registro o controllando le date nel DB).
## Ingestion: Carica solo i file mancanti, li converte in uno schema standard e li salva nel layer data/lake/silver/.
#--------------------------------------------------------------

import duckdb
import os
import glob
import pandas as pd

# Percorsi configurati
DB_PATH = "data/warehouse.duckdb"
LANDING_ZONE = "data/lake/bronze/landing_zone/*.parquet"
SILVER_PATH = "data/lake/silver/"

# Funzione di validazione dati: Data Quality Check (DQC)
def validate_data(df, file_name):
    """Esegue controlli di qualità base sui dati in ingresso"""
    
    # 1. Controllo record vuoti
    if df.empty:
        raise ValueError(f"Il file {file_name} è vuoto.")

    # 2. Esempio controllo prezzi negativi (solo se è il file degli item o ordini)
    if 'price' in df.columns:
        if (df['price'] < 0).any():
            raise ValueError(f"Trovati prezzi negativi nel file {file_name}!")

    # 3. Controllo integrità ID (non devono esserci nulli nelle chiavi primarie)
    chiavi_critiche = ['order_id', 'product_id', 'customer_id']
    for col in chiavi_critiche:
        if col in df.columns and df[col].isnull().any():
            print(f"⚠️ Avviso: Trovati valori nulli in {col} nel file {file_name}")
            # Qui potresti decidere di bloccare tutto con un raise o solo loggare

# Funzione principale per il caricamento incrementale
def run_bronze_incremental():
    # Creo le cartelle se non esistono
    os.makedirs("data/lake/bronze/landing_zone", exist_ok=True)
    os.makedirs("data/lake/silver", exist_ok=True)
    os.makedirs("data/lake/gold", exist_ok=True)

    con = duckdb.connect(DB_PATH)
    
    print("Scansione landing zone per nuovi file Parquet...")
    
    # 1. Recupero la lista di tutti i file nella landing zone
    files = glob.glob(LANDING_ZONE)
    
    if not files:
        print("Nessun file trovato nella landing zone.")
        return

    # 2. Logica di controllo duplicati
    # Creo una tabella tecnica nel DB per segnare i file già letti
    con.execute("""
        CREATE TABLE IF NOT EXISTS tech_processed_files (
            file_name VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    new_files_count = 0

    for file_path in sorted(files):
        file_name = os.path.basename(file_path)
        
        # Controllo se il file è già stato processato
        already_processed = con.execute(
            "SELECT 1 FROM tech_processed_files WHERE file_name = ?", [file_name]
        ).fetchone()

        if not already_processed:
            print(f"Processando nuovo file: {file_name}")
            
            # Leggo il file e lo carico nel layer Silver
            df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
            
            # Chiamata alla funzione di validazione (DQC)
            validate_data(df, file_name)

            # Salvo nel layer Silver
            silver_file = os.path.join(SILVER_PATH, file_name)
            df.to_parquet(silver_file, index=False)
            
            # Segno il file come processato per evitare duplicati futuri
            con.execute("INSERT INTO tech_processed_files (file_name) VALUES (?)", [file_name])
            new_files_count += 1
        else:
            # Se il file esiste già, lo salto (idempotenza)
            continue

    print(f"Task completato. File processati in questa run: {new_files_count}")
    con.close()

if __name__ == "__main__":
    try:
        run_bronze_incremental()
    except Exception as e:
        import sys
        print(f"ERRORE CRITICO durante l'automazione: {e}")
        sys.exit(1) # comunica a GitHub Actions che il job è fallito