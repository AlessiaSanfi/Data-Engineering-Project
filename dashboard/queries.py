import duckdb

# Funzioni per eseguire query sul data warehouse DuckDB
def get_connection(db_path):
    return duckdb.connect(db_path)

# Funzione per caricare i KPI principali
def load_kpis(con, query_where):
    return con.execute(f"""
        SELECT 
            SUM(price) as total_sales,
            AVG(delivery_time_days) as avg_delivery,
            COUNT(DISTINCT order_id) as total_orders,
            AVG(freight_value) as avg_freight
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
    """).fetchone()

# Funzione per caricare i dati delle categorie
def load_category_data(con, query_where):
    return con.execute(f"""
        SELECT p.product_category_name as Categoria, SUM(f.price) as Fatturato
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_id = p.product_id
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Categoria ORDER BY Fatturato DESC LIMIT 10
    """).df()

# Funzione per caricare i dati per stato
def load_state_data(con, query_where):
    return con.execute(f"""
        SELECT c.customer_state as Stato, COUNT(DISTINCT f.order_id) as Ordini
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato ORDER BY Ordini DESC
    """).df()

# Funzione per caricare i dati sui ritardi medi di consegna
def load_delay_data(con, query_where):
    return con.execute(f"""
        SELECT c.customer_state as Stato, AVG(f.delivery_time_days) as Media_Consegna
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato ORDER BY Media_Consegna DESC
    """).df()

# Funzione per caricare i dati di trend temporale
def load_trend_data(con, query_where):
    return con.execute(f"""
        SELECT CAST(t.year AS VARCHAR) || '-' || LPAD(CAST(t.month AS VARCHAR), 2, '0') as Periodo,
               SUM(f.price) as Fatturato
        FROM gold.fact_sales f
        JOIN gold.dim_time t ON f.order_purchase_timestamp = t.order_purchase_timestamp
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Periodo ORDER BY Periodo
    """).df()

# Funzione per caricare i dati sul costo medio di spedizione
def load_avg_shipping_data(con, query_where):
    return con.execute(f"""
        SELECT 
            c.customer_state as Stato,
            AVG(f.freight_value) as Media_Spedizione
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato
        ORDER BY Media_Spedizione DESC
    """).df()