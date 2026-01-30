import streamlit as st
import pandas as pd
import duckdb
import os
import plotly.express as px
from ai_utils import translate_text_to_sql
from queries import (
    load_kpis,
    load_category_data,
    load_state_data,
    load_shipping_time_data,
    load_avg_shipping_data,
    load_trend_data,
    load_weekly_seasonality
)

CURRENCY_SYMBOL = "R$"

# ------------------------------------------------------------------
# --- CONFIGURAZIONE PAGINA ---
# ------------------------------------------------------------------

st.set_page_config(page_title="Olist E-Commerce Dashboard (Phase 2)", layout="wide")

st.title("Olist Business Intelligence Dashboard (Phase 2)")
st.markdown("Dashboard basata sul **Gold Layer nel DuckDB** (pipeline incrementale Phase 2).")

# ------------------------------------------------------------------
# --- CONNESSIONE DATABASE + CHECK TABELLE GOLD ---
# ------------------------------------------------------------------

DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

if not os.path.exists(DB_PATH):
    st.error("**Errore Critico: Database DuckDB mancante**")
    st.write(f"File non trovato: `{DB_PATH}`")
    st.info("Esegui la pipeline Phase 2 (etl/flows/main_flows_fase2.py) per creare/aggiornare il Gold Layer nel DB.")
    st.stop()

try:
    con = duckdb.connect(DB_PATH, read_only=True)
except Exception as e:
    st.error("**Errore Critico: Impossibile aprire il database DuckDB**")
    st.write(str(e))
    st.stop()

required_gold_tables = [
    ("gold", "fact_sales"),
    ("gold", "dim_products"),
    ("gold", "dim_customers"),
    ("gold", "dim_time"),
]

missing = []
for schema, table in required_gold_tables:
    exists = con.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
    """, [schema, table]).fetchone()
    if not exists:
        missing.append(f"{schema}.{table}")

if missing:
    st.error("**Errore Critico: Tabelle Gold mancanti nel DB**")
    st.write("Mancano queste tabelle:")
    st.write(", ".join(missing))
    st.info("Esegui la pipeline Phase 2 fino al Gold (etl/flows/main_flows_fase2.py).")
    con.close()
    st.stop()

# VIEW compatibili con queries.py (queries.py continua a usare fact_sales/dim_* senza conoscere gold.*)
con.execute("CREATE OR REPLACE VIEW fact_sales AS SELECT * FROM gold.fact_sales")
con.execute("CREATE OR REPLACE VIEW dim_products AS SELECT * FROM gold.dim_products")
con.execute("CREATE OR REPLACE VIEW dim_customers AS SELECT * FROM gold.dim_customers")
con.execute("CREATE OR REPLACE VIEW dim_time AS SELECT * FROM gold.dim_time")

# ------------------------------------------------------------------
# --- MAPPE DI DECODIFICA ---
# ------------------------------------------------------------------

mappa_stati = {
    'SP': 'San Paolo', 'RJ': 'Rio de Janeiro', 'MG': 'Minas Gerais', 'RS': 'Rio Grande do Sul',
    'PR': 'Paraná', 'SC': 'Santa Catarina', 'BA': 'Bahia', 'DF': 'Distrito Federal',
    'ES': 'Espírito Santo', 'GO': 'Goiás', 'PE': 'Pernambuco', 'CE': 'Ceará', 'PA': 'Pará',
    'MT': 'Mato Grosso', 'MA': 'Maranhão', 'MS': 'Mato Grosso do Sul', 'PB': 'Paraíba',
    'RN': 'Rio Grande do Norte', 'PI': 'Piauí', 'AL': 'Alagoas', 'SE': 'Sergipe',
    'TO': 'Tocantins', 'RO': 'Rondônia', 'AM': 'Amazonas', 'AC': 'Acre', 'AP': 'Amapá', 'RR': 'Roraima'
}

mappa_categorie = {
    'agro_industria_e_comercio': 'Agroindustria & Commercio',
    'alimentos': 'Alimentari',
    'alimentos_bebidas': 'Alimentari & Bevande',
    'artes': 'Arte',
    'artes_e_artesanato': 'Arte & Artigianato',
    'artigos_de_festas': 'Articoli per Feste',
    'artigos_de_natal': 'Articoli di Natale',
    'audio': 'Audio',
    'automotivo': 'Automotive',
    'bebes': 'Prima Infanzia',
    'bebidas': 'Bevande',
    'beleza_saude': 'Bellezza & Salute',
    'brinquedos': 'Giocattoli',
    'cama_mesa_banho': 'Casa & Arredo',
    'casa_conforto': 'Comfort Casa',
    'casa_conforto_2': 'Comfort Casa 2',
    'casa_construcao': 'Casa & Costruzioni',
    'cds_dvds_musicais': 'Musica (CD & DVD)',
    'cine_foto': 'Cinema & Fotografia',
    'climatizacao': 'Climatizzazione',
    'consoles_games': 'Console & Videogiochi',
    'construcao_ferramentas_construcao': 'Edilizia',
    'construcao_ferramentas_ferramentas': 'Ferramenta',
    'construcao_ferramentas_iluminacao': 'Illuminazione',
    'cool_stuff': 'Articoli Regalo',
    'esporte_lazer': 'Sport & Tempo Libero',
    'informatica_acessorios': 'Informatica & Accessori',
    'moveis_decoracao': 'Arredamento & Decoro',
    'relogios_presentes': 'Orologi & Regali',
    'utilidades_domesticas': 'Utensili per la Casa'
}

# ------------------------------------------------------------------
# --- FUNZIONI GRAFICHE ---
# ------------------------------------------------------------------

def draw_static_bar(df, x_col, y_col, color="#4682B4", orient="v", height=300, label_y=None, label_x=None):
    friendly_name_y = label_y if label_y else y_col
    friendly_name_x = label_x if label_x else x_col

    x_enc = {'field': x_col, 'type': 'nominal', 'title': None} if orient == "v" else {'field': y_col, 'type': 'quantitative', 'title': None}
    y_enc = {'field': y_col, 'type': 'quantitative', 'title': None} if orient == "v" else {'field': x_col, 'type': 'nominal', 'title': None}

    st.vega_lite_chart(df, {
        'width': 'container',
        'height': height,
        'mark': {'type': 'bar', 'color': color, 'tooltip': True},
        'encoding': {
            'x': x_enc,
            'y': y_enc,
            'tooltip': [
                {'field': x_col, 'type': 'nominal', 'title': friendly_name_x},
                {'field': y_col, 'type': 'quantitative', 'title': friendly_name_y, 'format': '.2f'}
            ]
        },
        'config': {
            'view': {'stroke': 'transparent'},
            'selection': {'grid': False}
        }
    }, width='stretch')

def draw_static_line(df, x_col, y_col, color="#4682B4", height=300):
    st.vega_lite_chart(df, {
        'width': 'container',
        'height': height,
        'mark': {'type': 'line', 'color': color, 'point': True, 'tooltip': True},
        'encoding': {
            'x': {'field': x_col, 'type': 'nominal', 'title': None},
            'y': {'field': y_col, 'type': 'quantitative', 'title': None}
        },
        'config': {'view': {'stroke': 'transparent'}, 'selection': {'grid': False}}
    }, width='stretch')

# ------------------------------------------------------------------
# --- SIDEBAR E FILTRI ---
# ------------------------------------------------------------------

st.sidebar.header("Filtri")
nomi_selezionati = st.sidebar.multiselect("Seleziona lo Stato", sorted(mappa_stati.values()))

mappa_inversa = {v: k for k, v in mappa_stati.items()}
sigle_selezionate = [mappa_inversa[n] for n in nomi_selezionati]

query_where = ""
if sigle_selezionate:
    stati_str = "', '".join(sigle_selezionate)
    query_where = f"WHERE c.customer_state IN ('{stati_str}')"

# ------------------------------------------------------------------
# --- KPI PRINCIPALI ---
# ------------------------------------------------------------------

st.subheader("Key Performance Indicators")

# ATTENZIONE: richiede queries.py aggiornato a 5 valori (vedi nota in fondo)
total_sales, avg_delivery, total_orders, avg_freight, avg_order_value = load_kpis(con, query_where)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Fatturato", f"{CURRENCY_SYMBOL} {total_sales:,.2f}")
c2.metric("Ordini", f"{total_orders:,}")
c3.metric("Consegna Media", f"{avg_delivery:.1f} gg")
c4.metric("Spedizione Media", f"{CURRENCY_SYMBOL} {avg_freight:,.2f}")
c5.metric("Spesa Media", f"{CURRENCY_SYMBOL} {avg_order_value:,.2f}")

st.divider()

# ------------------------------------------------------------------
# --- RIGA 1: CATEGORIE E ORDINI ---
# ------------------------------------------------------------------

col_cat, col_ordini = st.columns(2)

with col_cat:
    st.subheader("Top 10 Categorie")
    df_cat = load_category_data(con, query_where)
    if not df_cat.empty:
        df_cat['Categoria'] = df_cat['Categoria'].map(mappa_categorie).fillna(df_cat['Categoria'])
        st.vega_lite_chart(df_cat, {
            'width': 'container',
            'height': 450,
            'mark': {'type': 'arc', 'innerRadius': 80, 'outerRadius': 130, 'tooltip': True},
            'encoding': {
                'theta': {'field': 'Fatturato', 'type': 'quantitative'},
                'color': {
                    'field': 'Categoria',
                    'type': 'nominal',
                    'legend': {
                        'orient': 'bottom',
                        'columns': 3,
                        'labelFontSize': 15,
                        'symbolSize': 100,
                        'offset': 20
                    }
                }
            },
            'config': {'view': {'stroke': 'transparent'}}
        }, width='stretch')
    else:
        st.info("Nessun dato per le categorie.")

with col_ordini:
    st.subheader("Distribuzione Geografica Ordini")
    df_state = load_state_data(con, query_where)
    if not df_state.empty:
        df_plot = df_state.copy()
        df_plot['Nome Stato'] = df_plot['Stato'].map(mappa_stati)

        fig = px.choropleth(
            df_plot, locations='Stato', color='Ordini',
            geojson="https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson",
            featureidkey="properties.sigla", scope="south america",
            color_continuous_scale="YlOrRd", hover_name='Nome Stato'
        )
        fig.update_layout(height=450, margin={"r": 0, "t": 0, "l": 0, "b": 0}, dragmode=False)
        fig.update_geos(projection_scale=1.5, center={'lat': -15, 'lon': -55}, visible=False)

        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False, 'staticPlot': False})

# ------------------------------------------------------------------
# --- RIGA 2: TEMPI CONSEGNA E COSTI SPEDIZIONE ---
# ------------------------------------------------------------------

col_shipping_time, col_shipping_price = st.columns(2)

with col_shipping_time:
    st.subheader("Tempi di consegna per Stato")
    df_shipping_time = load_shipping_time_data(con, query_where)
    if not df_shipping_time.empty:
        df_shipping_time['Stato Esteso'] = df_shipping_time['Stato'].map(mappa_stati)
        draw_static_bar(df_shipping_time, 'Stato Esteso', 'Tempi_Consegna', color="#FF7F50", orient="h", label_y="Tempi di consegna (gg)")
    else:
        st.info("Dati sui tempi di consegna non disponibili.")

with col_shipping_price:
    st.subheader("Costo Medio Spedizione per Stato")
    df_shipping = load_avg_shipping_data(con, query_where)
    if not df_shipping.empty:
        df_shipping['Stato Esteso'] = df_shipping['Stato'].map(mappa_stati)
        draw_static_bar(df_shipping, 'Stato Esteso', 'Costo_Spedizione', color="#9370DB", orient="h", label_y="Costi di spedizione")
    else:
        st.info("Dati sui costi di spedizione non disponibili.")

# ------------------------------------------------------------------
# --- RIGA 3: TREND TEMPORALE E STAGIONALITÀ SETTIMANALE ---
# ------------------------------------------------------------------

col_trend, col_weekly = st.columns(2)

with col_trend:
    st.subheader("Trend Temporale Fatturato")
    df_trend = load_trend_data(con, query_where)
    if not df_trend.empty:
        fig_trend = px.line(df_trend, x='Periodo', y='Fatturato', markers=True)
        fig_trend.update_xaxes(type='category')
        st.plotly_chart(fig_trend, width='stretch')
    else:
        st.info("Trend temporale non disponibile per la selezione attuale.")

with col_weekly:
    st.subheader("Stagionalità Settimanale")
    df_weekly = load_weekly_seasonality(con, query_where)
    if not df_weekly.empty:
        ordine_giorni = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_weekly['day_of_week'] = pd.Categorical(df_weekly['day_of_week'], categories=ordine_giorni, ordered=True)
        draw_static_bar(df_weekly.sort_values('day_of_week'), 'day_of_week', 'Fatturato', color="#2E8B57", label_x="Giorno della settimana")
    else:
        st.info("Dati di stagionalità settimanale non disponibili.")

st.divider()

# ------------------------------------------------------------------
# --- RIGA 4: ASSISTENTE AI ---
# ------------------------------------------------------------------

st.header("Text-to-SQL Assistant")
user_query = st.text_input("Fai una domanda sui dati (es. 'Fatturato totale per città'):")

if user_query:
    sql_query = translate_text_to_sql(user_query)

    st.subheader("Query Generata")
    st.code(sql_query, language="sql")

    try:
        df_ai = con.execute(sql_query).df()

        if 'customer_state' in df_ai.columns:
            df_ai['customer_state'] = df_ai['customer_state'].map(mappa_stati).fillna(df_ai['customer_state'])

        if 'product_category_name' in df_ai.columns:
            df_ai['product_category_name'] = df_ai['product_category_name'].map(mappa_categorie).fillna(df_ai['product_category_name'])

        mappa_colonne_ai = {
            'customer_id': 'ID Cliente',
            'order_id': 'ID Ordine',
            'product_id': 'ID Prodotto',

            'customer_city': 'Città',
            'customer_state': 'Stato',
            'product_category_name': 'Categoria Prodotto',
            'city': 'Città',
            'state': 'Stato',
            'category': 'Categoria',

            'order_purchase_timestamp': 'Data Acquisto',
            'year': 'Anno',
            'quarter': 'Trimestre',
            'month': 'Mese',
            'day': 'Giorno',
            'day_of_week': 'Giorno della Settimana',

            'price': 'Prezzo',
            'freight_value': 'Costo Spedizione',

            'total_revenue': 'Fatturato Totale',
            'revenue': 'Fatturato',
            'sum(price)': 'Fatturato Totale',
            'sum(T1.price)': 'Fatturato Totale',
            'sum(order_revenue)': 'Fatturato Totale',

            'total_orders': 'Totale Ordini',
            'order_count': 'Numero Ordini',
            'orders': 'Numero Ordini',
            'customer_count': 'Numero Clienti',

            'avg_order_value': 'Spesa Media',
            'average_order_value': 'Spesa Media',
            'avg(order_revenue)': 'Spesa Media',
            'avg_revenue': 'Spesa Media',
            'aov': 'Spesa Media',

            'avg_price': 'Prezzo Medio',
            'avg_delivery_time': 'Media Giorni Consegna',
            'avg(delivery_time_days)': 'Media Giorni Consegna',

            'avg_freight': 'Media Spese Spedizione',
            'avg(freight_value)': 'Media Spese Spedizione',

            'max_delivery_time': 'Consegna Massima (gg)',
            'min_delivery_time': 'Consegna Minima (gg)',
            'max_price': 'Prezzo Massimo',
            'min_price': 'Prezzo Minimo',

            'total_freight': 'Costo Spedizione Totale'
        }

        df_ai = df_ai.rename(columns=mappa_colonne_ai)

        st.subheader("Risultato")
        if not df_ai.empty:
            format_dict = {}
            if 'Fatturato Totale' in df_ai.columns:
                format_dict['Fatturato Totale'] = f"{CURRENCY_SYMBOL} {{:,.2f}}"
            if 'Fatturato' in df_ai.columns:
                format_dict['Fatturato'] = f"{CURRENCY_SYMBOL} {{:,.2f}}"
            if 'Prezzo' in df_ai.columns:
                format_dict['Prezzo'] = f"{CURRENCY_SYMBOL} {{:,.2f}}"
            if 'Costo Spedizione' in df_ai.columns:
                format_dict['Costo Spedizione'] = f"{CURRENCY_SYMBOL} {{:,.2f}}"
            if 'Spesa Media' in df_ai.columns:
                format_dict['Spesa Media'] = f"{CURRENCY_SYMBOL} {{:,.2f}}"

            st.dataframe(df_ai.style.format(format_dict), width='stretch')
        else:
            st.warning("La query non ha prodotto risultati.")

    except Exception as e:
        st.error(f"Errore nell'esecuzione della query: {e}")

con.close()
