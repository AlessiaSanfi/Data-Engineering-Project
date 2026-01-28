import pandas as pd
import numpy as np
import os

# Percorsi basati sulle tue immagini
PATH_ORDERS_SEP = 'data/lake/bronze/landing_zone/orders_2018-09.parquet'
PATH_ITEMS_ORIGINAL = 'data/lake/bronze/landing_zone/olist_order_items_dataset.parquet'

def integra_settembre():
    print("Inizio integrazione items per settembre...")
    
    # 1. Carichiamo gli ordini di settembre e gli items esistenti
    df_orders_sep = pd.read_parquet(PATH_ORDERS_SEP)
    df_items_old = pd.read_parquet(PATH_ITEMS_ORIGINAL)
    
    # Prendiamo un product_id e un seller_id reali dal tuo dataset per coerenza
    sample_product = df_items_old['product_id'].iloc[0]
    sample_seller = df_items_old['seller_id'].iloc[0]
    
    # 2. Creiamo i record degli items per i nuovi ordini
    new_items_list = []
    for _, row in df_orders_sep.iterrows():
        new_items_list.append({
            'order_id': row['order_id'],
            'order_item_id': 1,
            'product_id': sample_product,
            'seller_id': sample_seller,
            'shipping_limit_date': row['order_purchase_timestamp'],
            'price': round(np.random.uniform(30.0, 120.0), 2),
            'freight_value': round(np.random.uniform(5.0, 15.0), 2)
        })
    
    df_sep_items = pd.DataFrame(new_items_list)
    
    # 3. Uniamo tutto e salviamo sopra il file originale
    df_final = pd.concat([df_items_old, df_sep_items], ignore_index=True)
    df_final.to_parquet(PATH_ITEMS_ORIGINAL, index=False)
    
    print(f"Successo! Il file {PATH_ITEMS_ORIGINAL} ora contiene anche settembre.")

if __name__ == "__main__":
    integra_settembre()