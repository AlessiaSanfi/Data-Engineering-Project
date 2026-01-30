import duckdb
import os

con = duckdb.connect("data/warehouse.duckdb")

query = con.execute("""SELECT order_id, COUNT(*) c
FROM read_parquet('data/lake/silver/orders_*.parquet')
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY c DESC
LIMIT 20;

""")
print(query.df())