#--------------------------------------------------------------
# Script per simulare l'invio giornaliero di dati nella Landing Zone.
## Formato Colonnare: Ho soddisfatto il requisito di usare Parquet, che è molto più efficiente dei CSV per i Data Lake.
## Organizzazione per data: I dati degli ordini non sono più un unico blocco, ma sono "partizionati" logicamente per mese (orders_2017-01.parquet, orders_2017-02.parquet, ecc.).
## Punto di Partenza: La landing_zone ora contiene la nostra "fonte di verità".
#--------------------------------------------------------------

import pandas as pd
import os
from pathlib import Path

# Configurazione percorsi
RAW_DATA_PATH = "data/raw/"
LANDING_ZONE = "data/lake/bronze/landing_zone/"

def simulate_daily_data():
    # Creo la cartella se non esiste
    Path(LANDING_ZONE).mkdir(parents=True, exist_ok=True)
    
    print("Inizio simulazione: Conversione CSV -> Parquet (Landing Zone)")
    
    # 1. Carico il dataset principale degli ordini
    orders_file = os.path.join(RAW_DATA_PATH, "olist_orders_dataset.csv")
    df_orders = pd.read_csv(orders_file)
    
    # Converto la colonna data per poter fare lo split
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    
    # 2. Divido i dati per mese/anno per simulare invii incrementali
    # Estraggo i periodi univoci (anno-mese)
    df_orders['periodo'] = df_orders['order_purchase_timestamp'].dt.to_period('M')
    periodi = sorted(df_orders['periodo'].unique())
    
    print(f"Trovati {len(periodi)} mesi di dati. Generazione file Parquet...")

    for p in periodi:
        # Filtro i dati per quel mese
        df_month = df_orders[df_orders['periodo'] == p].copy()
        df_month = df_month.drop(columns=['periodo']) # Pulizia colonna temporanea
        
        # Creo il nome del file
        filename = f"orders_{p}.parquet"
        file_path = os.path.join(LANDING_ZONE, filename)
        
        # Salvataggio in formato Parquet
        df_month.to_parquet(file_path, index=False)
        print(f"Creato: {filename}")

    # 3. Copio anche le altre tabelle (anagrafiche) come Parquet completi
    # Spesso le dimensioni (prodotti, clienti) arrivano come dump completi
    altri_files = ["olist_products_dataset.csv", "olist_customers_dataset.csv", "olist_order_items_dataset.csv"]
    
    for f in altri_files:
        path_csv = os.path.join(RAW_DATA_PATH, f)
        if os.path.exists(path_csv):
            df_temp = pd.read_csv(path_csv)
            new_name = f.replace(".csv", ".parquet")
            df_temp.to_parquet(os.path.join(LANDING_ZONE, new_name), index=False)
            print(f"Convertita anagrafica: {new_name}")

    print("\nStep 1 completato! La Landing Zone è pronta per essere processata.")

if __name__ == "__main__":
    simulate_daily_data()