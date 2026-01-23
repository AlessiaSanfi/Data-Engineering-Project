from google import genai
from dotenv import load_dotenv
import os

# Carica le variabili dal file .env
load_dotenv()

def translate_text_to_sql(user_prompt):
    # Inizializza il client usando la chiave dal .env
    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
    
    # Context per istruire il modello sullo Star Schema Gold
    context = """
    Sei un esperto SQL per DuckDB. Il database segue uno Star Schema con queste tabelle:
    1. gold.fact_sales: [order_id, customer_id, product_id, price, freight_value, delivery_time_days, order_purchase_timestamp]
    2. gold.dim_products: [product_id, product_category_name]
    3. gold.dim_customers: [customer_id, customer_city, customer_state]
    4. gold.dim_time: [order_purchase_timestamp, year, month, day_of_week]
    
    REGOLE:
    - Restituisci SOLO la query SQL pura.
    - Non usare blocchi di codice markdown (niente ```sql).
    - Usa i nomi completi delle tabelle con lo schema 'gold'.
    - Se l'utente chiede il fatturato, usa SUM(price).
    """
    
    response = client.models.generate_content(
        model="gemini-flash-lite-latest",
        contents=f"{context}\n\nDomanda: {user_prompt}"
    )
    
    # --- LOGICA DI PERFEZIONISMO ---
    raw_response = response.text.strip()
    
    # Rimuovo i tag markdown se l'IA li ha inseriti per errore
    clean_sql = raw_response.replace("```sql", "").replace("```", "").strip()
    
    # Se l'IA risponde con una riga di testo prima della query, prendo solo ciò che inizia con SELECT, WITH o l'istruzione SQL
    if "SELECT" in clean_sql.upper():
        start_index = clean_sql.upper().find("SELECT")
        clean_sql = clean_sql[start_index:]
    
    return clean_sql
