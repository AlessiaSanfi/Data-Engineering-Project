from google import genai
from dotenv import load_dotenv
import os

# Carica le variabili dal file .env
load_dotenv()


def translate_text_to_sql(user_prompt: str) -> str:
    """
    Traduce una domanda in linguaggio naturale in SQL (DuckDB),
    puntando DIRETTAMENTE alle tabelle del Gold Layer (schema gold.*).
    Compatibile con Streamlit Cloud (DB in read-only).
    """
    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

    context = """
Sei un esperto SQL per DuckDB.

Il database usa uno Star Schema nel *Gold Layer* con queste tabelle (NOTA: hanno prefisso gold.):

1) gold.fact_sales
   colonne: [order_id, customer_id, product_id, price, freight_value, delivery_time_days, order_purchase_timestamp]

2) gold.dim_products
   colonne: [product_id, product_category_name]

3) gold.dim_customers
   colonne: [customer_id, customer_city, customer_state]

4) gold.dim_time
   colonne: [order_purchase_timestamp, year, month, day, day_of_week, quarter]

REGOLE:
- Restituisci SOLO la query SQL pura (niente testo extra).
- Non usare blocchi markdown (niente ```sql).
- Usa SEMPRE i nomi completi con schema: gold.fact_sales, gold.dim_products, gold.dim_customers, gold.dim_time.
- Se l'utente chiede "fatturato", usa: SUM(price) AS total_revenue.
- Se l'utente chiede "numero ordini", usa: COUNT(DISTINCT order_id) AS total_orders.
- Non inserire simboli di valuta nella query.
- Evita DDL/DML: niente CREATE/INSERT/UPDATE/DELETE. Solo SELECT/WITH.
"""

    response = client.models.generate_content(
        model="gemini-flash-lite-latest",
        contents=f"{context}\n\nDomanda: {user_prompt}"
    )

    raw = (response.text or "").strip()

    # pulizia markdown accidentale
    clean = raw.replace("```sql", "").replace("```", "").strip().rstrip(";").strip()

    # taglia eventuale testo introduttivo: cerca WITH o SELECT
    upper = clean.upper()
    candidates = []
    if "WITH" in upper:
        candidates.append(("WITH", upper.find("WITH")))
    if "SELECT" in upper:
        candidates.append(("SELECT", upper.find("SELECT")))

    if candidates:
        _, idx = sorted(candidates, key=lambda x: x[1])[0]
        clean = clean[idx:].strip()

    return clean
