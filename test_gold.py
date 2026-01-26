#--------------------------------------------------------------
# Script di test per verificare la corretta costruzione del Gold Layer.
# Controlla che le tabelle Gold siano accessibili e che le query di base funzionino.
#--------------------------------------------------------------

import duckdb
import pandas as pd

def test_gold_layer():
    # 1. Connessione in memoria
    con = duckdb.connect(database=':memory:')
    
    print("--- TEST STRUTTURA GOLD LAYER ---")
    try:
        # 2. Creazione View
        con.execute("CREATE VIEW fact_sales AS SELECT * FROM read_parquet('data/lake/gold/fact_sales.parquet')")
        con.execute("CREATE VIEW dim_time AS SELECT * FROM read_parquet('data/lake/gold/dim_time.parquet')")
        print("View create correttamente.")

        # 3. Controllo colonne dim_time
        columns = con.execute("DESCRIBE dim_time").df()
        print("\nColonne trovate in dim_time:")
        print(columns[['column_name', 'column_type']])

        # 4. Test della Query di Trend (quella di queries.py)
        # Verifico se il JOIN funziona e se year/month sono utilizzabili
        test_query = """
            SELECT 
                CAST(t.year AS VARCHAR) || '-' || LPAD(CAST(t.month AS VARCHAR), 2, '0') as Periodo,
                SUM(f.price) as Fatturato
            FROM fact_sales f
            JOIN dim_time t ON f.order_purchase_timestamp = t.order_purchase_timestamp
            GROUP BY Periodo 
            ORDER BY Periodo 
            LIMIT 5
        """
        result = con.execute(test_query).df()
        
        print("\nRisultato test Trend (Top 5 mesi):")
        if not result.empty:
            print(result)
            print("\nTEST SUPERATO: Il sistema è pronto per la dashboard!")
        else:
            print("\nATTENZIONE: La query non ha restituito dati. Controlla i timestamp.")

    except Exception as e:
        print(f"\nERRORE DURANTE IL TEST: {e}")

if __name__ == "__main__":
    test_gold_layer()