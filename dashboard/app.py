import streamlit as st
import pandas as pd
import os
from queries import (
    get_connection, 
    load_kpis, 
    load_category_data,
    load_state_data, 
    load_delay_data, 
    load_trend_data,
    load_avg_shipping_data
)

# Configurazione pagina
st.set_page_config(page_title="Olist E-Commerce Dashboard", layout="wide")

st.title("Olist Business Intelligence Dashboard")
st.markdown("Analisi professionale delle vendite e della logistica basata sul dataset Olist.")

# Gestione percorso database
db_path = os.getenv("DB_PATH", "data/warehouse.duckdb")
con = get_connection(db_path)

# --- SIDEBAR PER FILTRI ---
st.sidebar.header("Filtri")
stati = con.execute("SELECT DISTINCT customer_state FROM gold.dim_customers ORDER BY customer_state").df()
stato_selezionato = st.sidebar.multiselect("Seleziona lo Stato (Customer State)", stati['customer_state'].tolist())

# Costruzione clausola WHERE dinamica
query_where = ""
if stato_selezionato:
    stati_str = "', '".join(stato_selezionato)
    query_where = f"WHERE c.customer_state IN ('{stati_str}')"

# --- KPI PRINCIPALI ---
st.subheader("Key Performance Indicators")
col1, col2, col3, col4, col5 = st.columns(5)

# Caricamento dati tramite queries.py
kpis = load_kpis(con, query_where)
total_sales, avg_delivery, total_orders, avg_freight = kpis
# Calcolo Ticket Medio sicuro
aov = total_sales / total_orders if total_orders and total_orders > 0 else 0

col1.metric("Fatturato Totale", f"€ {total_sales:,.2f}")
col2.metric("Ordini Totali", f"{total_orders:,}")
col3.metric("Consegna Media", f"{avg_delivery:.1f} giorni")
col4.metric("Spedizione Media", f"€ {avg_freight:.2f}")
col5.metric("Ticket Medio", f"€ {aov:.2f}")

# --- LAYOUT GRAFICI ---
st.divider()

# RIGA 1: categorie, ordini (geografia) e ritardi (logistica)
col_cat, col_ordini, col_ritardi = st.columns(3)

# Grafico 1: top 10 categorie per fatturato (bar chart)
with col_cat:
    st.subheader("Top 10 Categorie")
    df_cat = load_category_data(con, query_where)
    if not df_cat.empty:
        st.bar_chart(df_cat.set_index('Categoria'), color="#2E8B57")
    else:
        st.info("Nessun dato per le categorie con i filtri selezionati.")

# Grafico 2: distribuzione ordini per stato (donut chart)
with col_ordini:
    st.subheader("Distribuzione Ordini per Stato")
    df_state = load_state_data(con, query_where)
    
    if not df_state.empty:
        st.vega_lite_chart(df_state, {
            'width': 'container',
            'height': 400,
            'mark': {
                'type': 'arc', 
                'innerRadius': 70,
                'outerRadius': 120,
                'tooltip': True
            },
            'encoding': {
                'theta': {'field': 'Ordini', 'type': 'quantitative'},
                'color': {
                    'field': 'Stato', 
                    'type': 'nominal', 
                    'legend': {
                        'orient': 'bottom',
                        'columns': 9,
                        'title': None,
                        'labelFontSize': 10,
                        'symbolSize': 100
                    }
                }
            },
            'config': {
                'view': {'stroke': None}
            }
        }, use_container_width=True)
    else:
        st.info("Nessun dato disponibile.")

# Grafico 3: analisi ritardi medi per stato (bar chart)
with col_ritardi:
    st.subheader("Analisi Ritardi per Stato")
    df_ritardi = load_delay_data(con, query_where)
    if not df_ritardi.empty:
        st.bar_chart(df_ritardi.set_index('Stato'), color="#FF7F50")
    else:
        st.info("Nessun dato sui ritardi disponibile.")

st.divider()

# RIGA 2: Trend Temporale e Analisi Costi
row2_col1, row2_col2 = st.columns(2)

# Grafico 4: trend temporale delle vendite (line chart)
with row2_col1:
    st.subheader("Trend Temporale delle Vendite")
    df_trend = load_trend_data(con, query_where)
    if not df_trend.empty:
        st.line_chart(df_trend.set_index('Periodo'), color="#4682B4")
    else:
        st.info("Nessun dato temporale disponibile.")

# Grafico 5: costo medio spedizione per stato (bar chart)
with row2_col2:
    st.subheader("Costo Medio Spedizione per Stato")
    df_shipping = load_avg_shipping_data(con, query_where)
    
    if not df_shipping.empty:
        st.bar_chart(df_shipping.set_index('Stato'), color="#9370DB")
    else:
        st.info("Nessun dato disponibile per il calcolo della spedizione.")

con.close()