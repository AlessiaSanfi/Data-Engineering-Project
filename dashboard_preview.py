# test per dashboard

import duckdb

con = duckdb.connect("data/warehouse.duckdb")

# Esempio di query per ottenere le top 5 categorie per fatturato
query = """
    SELECT 
        p.product_category_name,
        ROUND(SUM(f.price), 2) as totale_fatturato,
        ROUND(AVG(f.delivery_time_days), 1) as media_giorni_consegna
    FROM gold.fact_sales f
    JOIN gold.dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_category_name
    ORDER BY totale_fatturato DESC
    LIMIT 5;
"""

print("--- TOP 5 CATEGORIE PER FATTURATO ---")
print(con.execute(query).df())
con.close()