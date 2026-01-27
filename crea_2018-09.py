import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid

def genera_file_settembre():
    # Basato sull'analisi dei tuoi file precedenti
    num_ordini = 1150  
    start_date = datetime(2018, 9, 1)
    
    data = {
        # Generiamo ID univoci usando uuid
        'order_id': [f'sep_{uuid.uuid4().hex[:10]}' for _ in range(num_ordini)],
        'customer_id': [uuid.uuid4().hex for _ in range(num_ordini)],
        'order_status': np.random.choice(['delivered', 'shipped', 'canceled'], num_ordini, p=[0.95, 0.03, 0.02]),
        'order_purchase_timestamp': [start_date + timedelta(days=np.random.randint(0, 30), hours=np.random.randint(0, 23), minutes=np.random.randint(0, 59)) for _ in range(num_ordini)],
    }
    
    df = pd.DataFrame(data)
    
    # Creazione date logistiche coerenti
    df['order_approved_at'] = df['order_purchase_timestamp'] + timedelta(hours=np.random.randint(1, 24))
    df['order_delivered_carrier_date'] = df['order_approved_at'] + timedelta(days=np.random.randint(1, 3))
    df['order_delivered_customer_date'] = df['order_delivered_carrier_date'] + timedelta(days=np.random.randint(2, 12))
    df['order_estimated_delivery_date'] = df['order_purchase_timestamp'] + timedelta(days=20)

    # Coerenza per ordini non consegnati
    df.loc[df['order_status'] != 'delivered', ['order_delivered_customer_date']] = np.nan
    
    # Conversione in stringa per mantenere lo schema originale (come visto nei file caricati)
    for col in df.columns:
        if 'date' in col or 'timestamp' in col:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Salvataggio nel percorso corretto
    output_path = 'orders_2018-09.parquet'
    df.to_parquet(output_path, index=False)
    print(f"File {output_path} creato con successo nella cartella principale!")

if __name__ == "__main__":
    genera_file_settembre()