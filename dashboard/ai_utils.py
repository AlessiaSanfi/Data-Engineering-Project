import google.generativeai as genai
import os

# Configura la tua API KEY
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

def translate_text_to_sql(user_prompt):
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    # Il "System Prompt" istruisce Gemini sul tuo Database
    context = """
    Sei un esperto SQL per DuckDB. Il database segue uno Star Schema con queste tabelle:
    1. gold.fact_sales: colonne [order_id, customer_id, product_id, price, freight_value, delivery_time_days, order_purchase_timestamp]
    2. gold.dim_products: colonne [product_id, product_category_name]
    3. gold.dim_customers: colonne [customer_id, customer_city, customer_state]
    4. gold.dim_time: colonne [order_purchase_timestamp, year, month, day_of_week]
    
    Restituisci SOLO la query SQL, senza blocchi di codice markdown, senza spiegazioni.
    Usa sempre i nomi completi delle tabelle (es. gold.fact_sales).
    """
    
    full_prompt = f"{context}\n\nDomanda utente: {user_prompt}"
    response = model.generate_content(full_prompt)
    
    return response.text.strip()