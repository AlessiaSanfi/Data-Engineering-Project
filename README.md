# BRAZILIAN E-COMMERCE DATA PIPELINE (OLIST)

Questo progetto implementa una pipeline di Data Engineering professionale per l'analisi dei dati di e-commerce brasiliani (dataset Olist). L'obiettivo è trasformare i dati grezzi in un formato ottimizzato per l'analisi di business (KPI su vendite, logistica e performance dei venditori).

## ARCHITETTURA DEL PROGETTO
Il progetto segue l'architettura **Medallion**, che organizza i dati in livelli di qualità crescente:

1.  **Bronze Layer (Raw):** Ingestione dei file CSV originali in un database DuckDB. I dati sono conservati nel loro formato originale per garantire la tracciabilità.
2.  **Silver Layer (Cleaned):** Pulizia e trasformazione. In questa fase i dati vengono tipizzati (conversione stringhe in `TIMESTAMP`, gestione dei prezzi come `DOUBLE`) e filtrati da record inconsistenti.
3.  **Gold Layer (Analytical):** Creazione di uno Star Schema (Fact e Dimension tables) ottimizzato per la visualizzazione dei dati.

## MODELLAZIONE DATI
Il Layer Gold è strutturato secondo un modello a stella (**Star Schema**), progettato per massimizzare le performance delle query analitiche e facilitare la creazione di report.

* **Fact Table:** `fact_sales` (contiene le misure numeriche come prezzo, costi di spedizione e tempi di consegna).
* **Dimension Tables:** `dim_customers`, `dim_products`, `dim_time` (forniscono il contesto descrittivo per filtri e aggregazioni).

Per una descrizione tecnica dettagliata dei campi, delle chiavi primarie/esterne e per visualizzare il diagramma ER completo, consulta la documentazione dedicata:
**[Documentazione Star Schema](models/star_schema.md)**

## DOMANDE DI BUSINESS:
1. **Analisi del fatturato:** Qual è il fatturato totale generato e come si distribuisce tra le diverse categorie? (price e dim_products).
2. **Distribuzione geografica:** Quali sono i primi 5 Stati per volume di ordini e valore delle vendite? (fact_sales e dim_customers).
3. **Performance logistica:** Qual è il tempo medio di consegna (delivery_time_days) per ogni Stato e dove si riscontrano i maggiori ritardi? (dim_customers).
4. **Analisi dei costi:** Quanto incidono le spese di spedizione (freight_value) sul valore totale dell'ordine?
5. **Trend temporali:** Esistono picchi di vendita particolari durante i diversi trimestri (quarter) o giorni della settimana? (dim_time).

## MAPPATURA DASHBOARD
Ogni domanda di business trova una risposta diretta all'interno della dashboard interattiva.

1. **Analisi del fatturato:** = KPI "Fatturato Totatle + grafico "Top 10 Categorie".
2. **Distribuzione geografica:** = KPI "Ordini Totali" + Grafico "Ordini per Stato".
3. **Performance logistica:** = KPI "Consegna Media" + Grafico "Analisi Ritardi per Stato".
4. **Analisi dei costi:** = KPI "Spedizione Media" + Grafico "Prezzo vs Spedizione".
5. **Trend temporali:** = Grafico "Trend Temporale delle Vendite".

**Nota:** La dashboard permette di filtrare tutti i risultati per Stato del cliente tramite la barra laterale, consentendo un'analisi granulare per ogni domanda sopra elencata.

## STACK TECNOLOGICO
- **Linguaggio:** Python 3.x
- **Orchestratore:** [Prefect](https://www.prefect.io/) (per gestione dei flussi, monitoraggio e automazione)
- **Database:** [DuckDB](https://duckdb.org/) (OLAP database in-process)
- **Data Manipulation:** Polars
- **Visualizzazione:** Streamlit

## STRUTTURA DELLE CARTELLE

Data-Engineering-Project/
├── data/
│   ├── raw/               # File CSV originali
│   └── warehouse.duckdb   # Database analitico (creato dalla pipeline)
├── etl/
│   ├── flows/
│   │   └── main_flows.py  # Regista della pipeline (Prefect Flow)
│   ├── tasks/
│   │   ├── bronze.py      # Logica di ingestione
│   │   ├── silver.py      # Logica di pulizia e trasformazione
│   │   └── gold.py        # Logica di modellazione dati (Star Schema)
│   └── utils.py           # Funzioni di utilità (connessione DB)
├── requirements.txt       # Dipendenze del progetto
└── .env                   # Configurazioni locali (percorsi DB)


## COME AVVIARE IL PROGETTO

**Prerequisiti**
Python installato.

**Installazione dipendenze**
Installa le librerie necessarie tramite terminale:
  python -m pip install -r requirements.txt.

**Esecuzione della Pipeline**
Per avviare l'intero processo, esegui:
    
    python -m etl.flows.main_flows


## VISUALIZZAZIONE DATI
La pipeline alimenta una dashboard interattiva sviluppata in Streamlit, che permette di esplorare i KPI definiti nel Gold Layer.
Per avviarla, eseguire il comando:
    
    python -m streamlit run dashboard/app.py
