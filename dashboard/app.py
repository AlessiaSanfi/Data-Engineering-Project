import streamlit as st
import pandas as pd
import duckdb
import os
import plotly.express as px
from ai_utils import translate_text_to_sql
from queries import (
    get_connection,
    load_kpis,
    load_category_data,
    load_state_data,
    load_shipping_time_data,
    load_avg_shipping_data,
    load_trend_data,
    load_weekly_seasonality
)

# ------------------------------------------------------------------
# --- CONFIGURAZIONE PAGINA ---
# ------------------------------------------------------------------

st.set_page_config(page_title="Olist E-Commerce Dashboard", layout="wide")

st.title("Olist Business Intelligence Dashboard")
st.markdown("Analisi professionale delle vendite e della logistica basata sul dataset Olist.")

# ------------------------------------------------------------------
# --- TEST DI DEBUG CORRETTO ---
# ------------------------------------------------------------------
try:
    # Usiamo una connessione temporanea in memoria per leggere il file esterno
    temp_con = duckdb.connect(':memory:')
    path_to_check = 'data/lake/gold/fact_sales.parquet'
    
    if os.path.exists(path_to_check):
        count_sep = temp_con.execute(f"""
            SELECT count(*) 
            FROM read_parquet('{path_to_check}') 
            WHERE CAST(order_purchase_timestamp AS VARCHAR) LIKE '2018-09%'
        """).fetchone()[0]
        st.sidebar.metric("Righe Settembre (Debug)", count_sep)
    else:
        st.sidebar.error("File Gold non trovato sul server!")
except Exception as e:
    st.sidebar.warning(f"Errore durante il debug: {e}")
# ------------------------------

# ------------------------------------------------------------------
# --- GESTIONE ERRORI E FILTRI DI SICUREZZA ---
# ------------------------------------------------------------------
parquet_files = [
    'data/lake/gold/fact_sales.parquet',
    'data/lake/gold/dim_products.parquet',
    'data/lake/gold/dim_customers.parquet',
    'data/lake/gold/dim_time.parquet'
]

# Verifico se tutti i file necessari esistono
missing_files = [f for f in parquet_files if not os.path.exists(f)]

if missing_files:
    st.error("**Errore Critico: Dati mancanti nel Data Lake**")
    st.write(f"I seguenti file Gold non sono stati trovati: `{', '.join(missing_files)}`")
    st.info("Assicurati di aver eseguito correttamente la pipeline ETL prima di lanciare la dashboard.")
    st.stop() # Blocca l'esecuzione del resto dell'app

# ------------------------------------------------------------------
# --- CONNESSIONE DATABASE ---
# ------------------------------------------------------------------

# Creo una connessione DuckDB in memoria
con = duckdb.connect(database=':memory:')

# Registro i file Gold in modo che le tue query esistenti continuino a funzionare
con.execute("CREATE VIEW fact_sales AS SELECT * FROM read_parquet('data/lake/gold/fact_sales.parquet')")
con.execute("CREATE VIEW dim_products AS SELECT * FROM read_parquet('data/lake/gold/dim_products.parquet')")
con.execute("CREATE VIEW dim_customers AS SELECT * FROM read_parquet('data/lake/gold/dim_customers.parquet')")
con.execute("CREATE VIEW dim_time AS SELECT * FROM read_parquet('data/lake/gold/dim_time.parquet')")

# ------------------------------------------------------------------
# --- MAPPE DI DECODIFICA ---
# ------------------------------------------------------------------

# Mappa per decodificare le sigle degli stati brasiliani
mappa_stati = {
    'SP': 'San Paolo', 'RJ': 'Rio de Janeiro', 'MG': 'Minas Gerais', 'RS': 'Rio Grande do Sul',
    'PR': 'Paraná', 'SC': 'Santa Catarina', 'BA': 'Bahia', 'DF': 'Distrito Federal',
    'ES': 'Espírito Santo', 'GO': 'Goiás', 'PE': 'Pernambuco', 'CE': 'Ceará', 'PA': 'Pará',
    'MT': 'Mato Grosso', 'MA': 'Maranhão', 'MS': 'Mato Grosso do Sul', 'PB': 'Paraíba',
    'RN': 'Rio Grande do Norte', 'PI': 'Piauí', 'AL': 'Alagoas', 'SE': 'Sergipe',
    'TO': 'Tocantins', 'RO': 'Rondônia', 'AM': 'Amazonas', 'AC': 'Acre', 'AP': 'Amapá', 'RR': 'Roraima'
}

# Mappa per decodificare le categorie di prodotto
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

# Funzione per disegnare un bar chart statico con tooltip personalizzati
def draw_static_bar(df, x_col, y_col, color="#4682B4", orient="v", height=300, label_y=None, label_x=None):
    """Renderizza un bar chart statico con tooltip personalizzati."""
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

# Funzione per disegnare un line chart statico con tooltip personalizzati
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

# Logica di filtraggio pulita
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
total_sales, avg_delivery, total_orders, avg_freight = load_kpis(con, query_where)
spesa_media = total_sales / total_orders if total_orders > 0 else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Fatturato", f"R$ {total_sales:,.2f}")
c2.metric("Ordini", f"{total_orders:,}")
c3.metric("Consegna Media", f"{avg_delivery:.1f} gg")
c4.metric("Spedizione Media", f"R$ {avg_freight:.2f}")
c5.metric("Spesa Media", f"R$ {spesa_media:.2f}")

st.divider()

# ------------------------------------------------------------------
# --- RIGA 1: CATEGORIE E ORDINI ---
# ------------------------------------------------------------------

col_cat, col_ordini = st.columns(2)

# Top 10 Categorie
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


# Distribuzione Geografica Ordini
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
        fig.update_layout(height=450, margin={"r":0,"t":0,"l":0,"b":0}, dragmode=False)
        fig.update_geos(projection_scale=1.5, center={'lat': -15, 'lon': -55}, visible=False)

        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False, 'staticPlot': False})

# ------------------------------------------------------------------
# --- RIGA 2: TEMPI CONSEGNA E COSTI SPEDIZIONE ---
# ------------------------------------------------------------------

col_shipping_time, col_shipping_price = st.columns(2)

# Tempi di Consegna per Stato
with col_shipping_time:
    st.subheader("Tempi di consegna per Stato")
    df_shipping_time = load_shipping_time_data(con, query_where)
    if not df_shipping_time.empty:
        df_shipping_time['Stato Esteso'] = df_shipping_time['Stato'].map(mappa_stati)
        draw_static_bar(df_shipping_time, 'Stato Esteso', 'Tempi_Consegna', color="#FF7F50", orient="h", label_y="Tempi di consegna (gg)")
    else:
        st.info("Dati sui tempi di consegna non disponibili.")

# Costo Medio Spedizione per Stato
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

# Trend Temporale delle Vendite da sttembre 2016 a ottobre (agosto) 2018
with col_trend:
    st.subheader("Trend Temporale Fatturato")
    df_trend = load_trend_data(con, query_where)
    if not df_trend.empty:
        # Debug rapido: stampiamo le ultime 3 righe del dataframe sotto il grafico
        st.write(df_trend.tail(3)) 
        
        fig_trend = px.line(df_trend, x='Periodo', y='Fatturato', markers=True)
        # Forziamo Plotly a non tagliare l'asse X
        fig_trend.update_xaxes(type='category') 
        st.plotly_chart(fig_trend, use_container_width=True)

# Stagionalità Settimanale
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
        
        # --- LOGICA DI MAPPATURA COLONNE ---
        
        #Applico la mappatura dei valori (Stati e Categorie)
        if 'customer_state' in df_ai.columns:
            df_ai['customer_state'] = df_ai['customer_state'].map(mappa_stati).fillna(df_ai['customer_state'])
        
        if 'product_category_name' in df_ai.columns:
            df_ai['product_category_name'] = df_ai['product_category_name'].map(mappa_categorie).fillna(df_ai['product_category_name'])
        
        mappa_colonne_ai = {
            # Nomi reali del database
            'customer_id': 'ID Cliente',
            'order_id': 'ID Ordine',
            'product_id': 'ID Prodotto',
            'customer_city': 'Città',
            'customer_state': 'Stato',
            'product_category_name': 'Categoria Prodotto',
            'order_purchase_timestamp': 'Data Acquisto',
            'price': 'Prezzo',
            'freight_value': 'Costo Spedizione',
            'delivery_time_days': 'Giorni Consegna',
            'year': 'Anno',
            'quarter': 'Trimestre',
            'month': 'Mese',
            'day': 'Giorno',
            'day_of_week': 'Giorno della Settimana',

            # Alias richiesti
            'total_revenue': 'Fatturato Totale',
            'sum(T1.price)': 'Fatturato Totale',
            'sum(price)': 'Fatturato Totale',
            'revenue': 'Fatturato',
            'total_orders': 'Totale Ordini',
            'order_count': 'Numero Ordini',
            'avg_price': 'Prezzo Medio',

            # Ulteriori alias comuni generati dall'AI
            'avg_delivery_time': 'Media Giorni Consegna',
            'max_delivery_time': 'Consegna Massima (gg)',
            'min_delivery_time': 'Consegna Minima (gg)',
            'avg_freight': 'Media Spese Spedizione',
            'total_freight': 'Costo Spedizione Totale',
            'max_price': 'Prezzo Massimo',
            'min_price': 'Prezzo Minimo',
            'orders': 'Numero Ordini',
            'customer_count': 'Numero Clienti',
            'avg_order_value': 'Valore Medio Ordine',
            'city': 'Città',
            'state': 'Stato',
            'category': 'Categoria'
        }

        # Rinomina solo le colonne presenti nel risultato
        df_ai = df_ai.rename(columns=mappa_colonne_ai)
        
        st.subheader("Risultato")
        if not df_ai.empty:
            # Creo un oggetto di formattazione per le colonne monetarie
            format_dict = {}
            if 'Fatturato Totale' in df_ai.columns:
                format_dict['Fatturato Totale'] = "R$ {:,.2f}"
            if 'Prezzo' in df_ai.columns:
                format_dict['Prezzo'] = "R$ {:,.2f}"
            if 'Costo Spedizione' in df_ai.columns:
                format_dict['Costo Spedizione'] = "R$ {:,.2f}"

            # Mostra il dataframe con la formattazione applicata
            st.dataframe(df_ai.style.format(format_dict), width='stretch')
        else:
            st.warning("La query non ha prodotto risultati.")
            
    except Exception as e:
        st.error(f"Errore nell'esecuzione della query: {e}")

con.close()