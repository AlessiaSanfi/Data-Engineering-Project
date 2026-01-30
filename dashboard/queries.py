import duckdb

# Funzioni per eseguire query sul data warehouse DuckDB
def get_connection(db_path):
    return duckdb.connect(db_path, read_only=True)

# -------------------------------------------------------------------
# KPI PRINCIPALI (coerenti a livello ORDINE)
# - total_sales: somma price per ordine e poi somma totale
# - avg_delivery: media delivery_time_days per ordine (1 ordine = 1 peso)
# - total_orders: numero ordini (dopo filtro)
# - avg_freight: media freight_value per ordine (1 ordine = 1 peso)
# - avg_order_value: media order_revenue per ordine
# -------------------------------------------------------------------
def load_kpis(con, query_where):
    return con.execute(f"""
        WITH per_order AS (
            SELECT
                f.order_id,
                SUM(f.price) AS order_revenue,
                MAX(f.delivery_time_days) AS delivery_time_days,
                SUM(f.freight_value) AS freight_value
            FROM fact_sales f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            {query_where}
            GROUP BY f.order_id
        )
        SELECT
            SUM(order_revenue) AS total_sales,
            AVG(delivery_time_days) AS avg_delivery,
            COUNT(*) AS total_orders,
            AVG(freight_value) AS avg_freight,
            AVG(order_revenue) AS avg_order_value
        FROM per_order
    """).fetchone()

# -------------------------------------------------------------------
# TOP CATEGORIE (grain item: corretto sommare price per product/category)
# -------------------------------------------------------------------
def load_category_data(con, query_where):
    return con.execute(f"""
        SELECT
            p.product_category_name AS Categoria,
            SUM(f.price) AS Fatturato
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Categoria
        ORDER BY Fatturato DESC
        LIMIT 10
    """).df()

# -------------------------------------------------------------------
# ORDINI PER STATO (conteggio ordini distinti)
# -------------------------------------------------------------------
def load_state_data(con, query_where):
    return con.execute(f"""
        SELECT
            c.customer_state AS Stato,
            COUNT(DISTINCT f.order_id) AS Ordini
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY Stato
        ORDER BY Ordini DESC
    """).df()

# -------------------------------------------------------------------
# TEMPI DI CONSEGNA PER STATO (coerenti a livello ORDINE)
# - prima collassiamo a 1 riga per ordine (per stato)
# - poi facciamo AVG sui soli ordini
# -------------------------------------------------------------------
def load_shipping_time_data(con, query_where):
    return con.execute(f"""
        WITH per_order AS (
            SELECT
                f.order_id,
                c.customer_state AS Stato,
                MAX(f.delivery_time_days) AS delivery_time_days
            FROM fact_sales f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            {query_where}
            GROUP BY f.order_id, c.customer_state
        )
        SELECT
            Stato,
            AVG(delivery_time_days) AS Tempi_Consegna
        FROM per_order
        GROUP BY Stato
        ORDER BY Tempi_Consegna DESC
    """).df()

# -------------------------------------------------------------------
# COSTO MEDIO SPEDIZIONE PER STATO (coerenti a livello ORDINE)
# - stessa logica: 1 riga per ordine (per stato), poi AVG
# -------------------------------------------------------------------
def load_avg_shipping_data(con, query_where):
    return con.execute(f"""
        WITH per_order AS (
            SELECT
                f.order_id,
                c.customer_state AS Stato,
                SUM(f.freight_value) AS freight_value
            FROM fact_sales f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            {query_where}
            GROUP BY f.order_id, c.customer_state
        )
        SELECT
            Stato,
            AVG(freight_value) AS Costo_Spedizione
        FROM per_order
        GROUP BY Stato
        ORDER BY Costo_Spedizione DESC
    """).df()

# -------------------------------------------------------------------
# TREND MENSILE (fatturato) - robusto: usa solo fact_sales (no join dim_time)
# Nota: somma price a grain item => corretto per fatturato
# -------------------------------------------------------------------
def load_trend_data(con, query_where):
    return con.execute(f"""
        SELECT
            strftime(CAST(f.order_purchase_timestamp AS TIMESTAMP), '%Y-%m') AS Periodo,
            SUM(f.price) AS Fatturato
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY 1
        ORDER BY 1
    """).df()

# -------------------------------------------------------------------
# STAGIONALITÀ SETTIMANALE (fatturato) - robusto: usa solo fact_sales
# -------------------------------------------------------------------
def load_weekly_seasonality(con, query_where):
    return con.execute(f"""
        SELECT
            strftime(CAST(f.order_purchase_timestamp AS TIMESTAMP), '%A') AS day_of_week,
            SUM(f.price) AS Fatturato
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        {query_where}
        GROUP BY 1
        ORDER BY CASE
            WHEN day_of_week = 'Monday' THEN 1
            WHEN day_of_week = 'Tuesday' THEN 2
            WHEN day_of_week = 'Wednesday' THEN 3
            WHEN day_of_week = 'Thursday' THEN 4
            WHEN day_of_week = 'Friday' THEN 5
            WHEN day_of_week = 'Saturday' THEN 6
            WHEN day_of_week = 'Sunday' THEN 7
        END
    """).df()
