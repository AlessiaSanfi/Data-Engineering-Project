# Brazilian E-Commerce Data Pipeline (Olist)

Questo progetto implementa una pipeline di Data Engineering professionale per l'analisi dei dati di e-commerce brasiliani (dataset Olist). L'obiettivo è trasformare i dati grezzi in un formato ottimizzato per l'analisi di business (KPI su vendite, logistica e performance dei venditori).

## 🚀 Architettura del Progetto
Il progetto segue l'architettura **Medallion**, che organizza i dati in livelli di qualità crescente:

1.  **Bronze Layer (Raw):** Ingestione dei file CSV originali in un database DuckDB. I dati sono conservati nel loro formato originale per garantire la tracciabilità.
2.  **Silver Layer (Cleaned):** Pulizia e trasformazione. In questa fase i dati vengono tipizzati (conversione stringhe in `TIMESTAMP`, gestione dei prezzi come `DOUBLE`) e filtrati da record inconsistenti.
3.  **Gold Layer (Analytical):** (In fase di sviluppo) Creazione di uno Star Schema (Fact e Dimension tables) ottimizzato per la visualizzazione dei dati.

- **Linguaggio:** Python 3.x
- **Orchestratore:** [Prefect](https://www.prefect.io/) (per la gestione dei flussi e l'automazione)
- **Database:** [DuckDB](https://duckdb.org/) (OLAP database in-process, estremamente veloce per analisi dati)
- **Data Manipulation:** Polars (lettura ultra-rapida dei CSV)

## 📁 Struttura delle Cartelle

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


**COME AVVIARE IL PROGETTO**

**Prerequisiti**
Python installato.

**Installazione dipendenze**
Installa le librerie necessarie tramite terminale:
  python -m pip install -r requirements.txt.

**Esecuzione della Pipeline**
Per avviare l'intero processo, esegui:
  python -m etl.flows.main_flows


**OBIETTIVI FUTURI**
- Completamento del Gold Layer con calcolo dei tempi medi di consegna e fatturato mensile.
- Creazione di una Dashboard interattiva con Streamlit per visualizzare i KPI.
