import duckdb

db_path = "data/warehouse.duckdb"
sql = """
UPDATE tech.tech_processed_files
SET
  status='SKIP',
  note=replace(note,'SKIP: ','')
WHERE note LIKE 'SKIP:%'
  AND rows_inserted=0
"""

con = duckdb.connect(db_path)
con.execute(sql)
con.close()
print("OK normalized")
