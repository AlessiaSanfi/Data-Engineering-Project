import pandas as pd
import numpy as np

# 1. Carichiamo i tuoi dati reali e i nuovi ordini
path_items = 'data/lake/silver/olist_order_items_dataset.parquet'
df_items_old = pd.read_parquet(path_items)
df_orders_sep = pd.read_parquet('data/lake/bronze/landing_zone/orders_2018-09.parquet')

# 2. Creiamo i nuovi items basandoci sugli ID di settembre
new_items = []
for _, row in df_orders_sep.iterrows():
    new_items.append({
        'order_id': row['order_id'],
        'order_item_id': 1,
        'product_id': df_items_old['product_id'].iloc[0], # Usiamo un prodotto esistente
        'seller_id': df_items_old['seller_id'].iloc[0],   # Usiamo un venditore esistente
        'shipping_limit_date': row['order_purchase_timestamp'],
        'price': round(np.random.uniform(40, 120), 2),
        'freight_value': round(np.random.uniform(10, 20), 2)
    })

df_sep_items = pd.DataFrame(new_items)

# 3. Uniamo tutto e salviamo di nuovo nel tuo file originale
df_final = pd.concat([df_items_old, df_sep_items], ignore_index=True)
df_final.to_parquet(path_items, index=False)

print(f"Aggiornato {path_items} con i dati di settembre!")