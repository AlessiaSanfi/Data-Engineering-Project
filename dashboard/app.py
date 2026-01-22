import streamlit as st
import pandas as pd
import os
import plotly.express as px
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

# --- CONFIGURAZIONE PAGINA ---
st.set_page_config(page_title="Olist E-Commerce Dashboard", layout="wide")

st.title("Olist Business Intelligence Dashboard")
st.markdown("Analisi professionale delle vendite e della logistica basata sul dataset Olist.")

# --- CONNESSIONE DATABASE ---
db_path = os.getenv("DB_PATH", "data/warehouse.duckdb")
con = get_connection(db_path)

# --- MAPPE DI DECODIFICA ---
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
    'cama_mesa_banho': 'Casa & Arredo (Biancheria)',
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

# --- FUNZIONI GRAFICHE ---
def draw_static_bar(df, x_col, y_col, color="#4682B4", orient="v", height=300, label_y=None):
    """Renderizza un bar chart statico con tooltip personalizzati."""
    friendly_name = label_y if label_y else y_col
    
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
                {'field': x_col, 'type': 'nominal', 'title': 'Stato'},
                {'field': y_col, 'type': 'quantitative', 'title': friendly_name, 'format': '.2f'}
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

# --- SIDEBAR E FILTRI ---
st.sidebar.header("Filtri")
nomi_selezionati = st.sidebar.multiselect("Seleziona lo Stato", sorted(mappa_stati.values()))

# Logica di filtraggio pulita
mappa_inversa = {v: k for k, v in mappa_stati.items()}
sigle_selezionate = [mappa_inversa[n] for n in nomi_selezionati]

query_where = ""
if sigle_selezionate:
    stati_str = "', '".join(sigle_selezionate)
    query_where = f"WHERE c.customer_state IN ('{stati_str}')"

# --- KPI PRINCIPALI ---
st.subheader("Key Performance Indicators")
total_sales, avg_delivery, total_orders, avg_freight = load_kpis(con, query_where)
spesa_media = total_sales / total_orders if total_orders > 0 else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Fatturato", f"€ {total_sales:,.2f}")
c2.metric("Ordini", f"{total_orders:,}")
c3.metric("Consegna Media", f"{avg_delivery:.1f} gg")
c4.metric("Spedizione Media", f"€ {avg_freight:.2f}")
c5.metric("Spesa Media", f"€ {spesa_media:.2f}")

st.divider()

# --- RIGA 1: CATEGORIE E ORDINI ---
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

# --- RIGA 2: TEMPI CONSEGNA E COSTI SPEDIZIONE ---
col_shipping_time, col_shipping_price = st.columns(2)

# Tempi di Consegna per Stato
with col_shipping_time:
    st.subheader("Tempi di consegna per Stato")
    df_shipping_time = load_shipping_time_data(con, query_where)
    if not df_shipping_time.empty:
        df_shipping_time['Stato Esteso'] = df_shipping_time['Stato'].map(mappa_stati)
        draw_static_bar(df_shipping_time, 'Stato Esteso', 'Media_Consegna', color="#FF7F50", orient="h", label_y="Tempi di consegna")
    else:
        st.info("Dati sui tempi di consegna non disponibili.")

# Costo Medio Spedizione per Stato
with col_shipping_price:
    st.subheader("Costo Medio Spedizione per Stato")
    df_shipping = load_avg_shipping_data(con, query_where)
    if not df_shipping.empty:
        df_shipping['Stato Esteso'] = df_shipping['Stato'].map(mappa_stati) 
        draw_static_bar(df_shipping, 'Stato Esteso', 'Media_Spedizione', color="#9370DB", orient="h", label_y="Costi di spedizione")
    else:
        st.info("Dati sui costi di spedizione non disponibili.")

# --- RIGA 3: TREND TEMPORALE E STAGIONALITÀ SETTIMANALE ---
col_trend, col_weekly = st.columns(2)

# Trend Temporale delle Vendite da sttembre 2016 a ottobre (agosto) 2018
with col_trend:
    st.subheader("Trend Temporale delle Vendite")
    df_trend = load_trend_data(con, query_where)
    if not df_trend.empty:
        draw_static_line(df_trend, 'Periodo', 'Fatturato', color="#4682B4")
    else:
        st.info("Trend temporale non disponibile per la selezione attuale.")

# Stagionalità Settimanale
with col_weekly:
    st.subheader("Stagionalità Settimanale")
    df_weekly = load_weekly_seasonality(con, query_where)
    if not df_weekly.empty:
        ordine_giorni = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_weekly['Giorno della settimana'] = pd.Categorical(df_weekly['Giorno della settimana'], categories=ordine_giorni, ordered=True)
        draw_static_bar(df_weekly.sort_values('Giorno della settimana'), 'Giorno della settimana', 'Fatturato', color="#2E8B57")
    else:
        st.info("Dati di stagionalità settimanale non disponibili.")

con.close()