import duckdb

con = duckdb.connect("data/warehouse.duckdb")

print("--- VERIFICA SCHEMI ---")
print(con.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver')").fetchall())

print("\n--- ANTEPRIMA DATI SILVER (ORDERS) ---")
# Verifica date TIMESTAMP e non stringhe
df_orders = con.execute("DESCRIBE silver.orders").df()
print(df_orders[['column_name', 'column_type']])

print("\n--- CONTEGGIO RIGHE ---")
count = con.execute("SELECT count(*) FROM silver.orders").fetchone()[0]
print(f"Totale ordini pronti in Silver: {count}")

con.close()