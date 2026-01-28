import duckdb

# Funzioni per eseguire query sul data warehouse DuckDB
def get_connection(db_path):
    return duckdb.connect(db_path, read_only=True)

# Funzione per caricare i KPI principali
def load_kpis(con, query_where):
    return con.execute(f"""
        SELECT 
            SUM(price) as total_sales,
            AVG(delivery_time_days) as avg_delivery,
            COUNT(DISTINCT order_id) as total_orders,
            AVG(freight_value) as avg_freight
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
    """).fetchone()

# Funzione per caricare i dati delle categorie
def load_category_data(con, query_where):
    return con.execute(f"""
        SELECT p.product_category_name as Categoria, SUM(f.price) as Fatturato
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Categoria ORDER BY Fatturato DESC LIMIT 10
    """).df()

# Funzione per caricare i dati per stato
def load_state_data(con, query_where):
    return con.execute(f"""
        SELECT c.customer_state as Stato, COUNT(DISTINCT f.order_id) as Ordini
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato ORDER BY Ordini DESC
    """).df()

# Funzione per caricare i dati sui tempi di consegna
def load_shipping_time_data(con, query_where):
    return con.execute(f"""
        SELECT c.customer_state as Stato, AVG(f.delivery_time_days) as Tempi_Consegna
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato ORDER BY Tempi_Consegna DESC
    """).df()

# Funzione per caricare i dati sul costo medio di spedizione
def load_avg_shipping_data(con, query_where):
    return con.execute(f"""
        SELECT 
            c.customer_state as Stato,
            AVG(f.freight_value) as Costo_Spedizione
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato
        ORDER BY Costo_Spedizione DESC
    """).df()

# Funzione per caricare i dati di trend temporale
def load_trend_data(con, query_where):
    return con.execute(f"""
        SELECT 
            CAST(t.year AS VARCHAR) || '-' || LPAD(CAST(t.month AS VARCHAR), 2, '0') as Periodo,
            SUM(f.price) as Fatturato
        FROM fact_sales f
        -- TRICK: Castiamo entrambi i campi a DATE per ignorare l'orario nel JOIN
        JOIN dim_time t ON CAST(f.order_purchase_timestamp AS DATE) = CAST(t.order_purchase_timestamp AS DATE)
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Periodo 
        ORDER BY Periodo
    """).df()

# Funzione per caricare i dati di stagionalità settimanale
def load_weekly_seasonality(con, query_where):
    return con.execute(f"""
        SELECT 
            t.day_of_week as "day_of_week", 
            SUM(f.price) as Fatturato
        FROM fact_sales f
        -- Applichiamo il CAST a DATE anche qui per recuperare settembre
        JOIN dim_time t ON CAST(f.order_purchase_timestamp AS DATE) = CAST(t.order_purchase_timestamp AS DATE)
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY 1
        ORDER BY CASE 
            WHEN t.day_of_week = 'Monday' THEN 1 
            WHEN t.day_of_week = 'Tuesday' THEN 2
            WHEN t.day_of_week = 'Wednesday' THEN 3 
            WHEN t.day_of_week = 'Thursday' THEN 4
            WHEN t.day_of_week = 'Friday' THEN 5 
            WHEN t.day_of_week = 'Saturday' THEN 6
            WHEN t.day_of_week = 'Sunday' THEN 7 END
    """).df()