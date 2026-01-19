# BRAZILIAN E-COMMERCE DATA PIPELINE (OLIST)
Questo progetto implementa una pipeline di Data Engineering professionale per l'analisi dei dati di e-commerce brasiliani (dataset Olist). L'obiettivo è trasformare i dati grezzi in un formato ottimizzato per l'analisi di business (KPI su vendite, logistica e performance dei venditori).


## STACK TECNOLOGICO
- **Linguaggio:** Python 3.x
- **Orchestratore:** [Prefect](https://www.prefect.io/) (per gestione dei flussi, monitoraggio e automazione)
- **Database:** [DuckDB](https://duckdb.org/) (OLAP database in-process)
- **Data Manipulation:** Polars
- **Visualizzazione:** Streamlit
- **Containerizzazione:** Podman


## ARCHITETTURA DEL PROGETTO
Il progetto segue l'architettura **Medallion**, che organizza i dati in livelli di qualità crescente:

1.  **Bronze Layer (raw):** Ingestione dei file CSV originali in un database DuckDB. I dati sono conservati nel loro formato originale per garantire la tracciabilità.
2.  **Silver Layer (cleaned):** Pulizia e trasformazione. In questa fase i dati vengono tipizzati (conversione stringhe in `TIMESTAMP`, gestione dei prezzi come `DOUBLE`) e filtrati da record inconsistenti.
3.  **Gold Layer (analytical):** Creazione dello Star Schema (Fact e Dimension tables) ottimizzato per la visualizzazione dei dati.


## STRUTTURA DEL DATABASE (DuckDB)
Il progetto utilizza **DuckDB** come motore OLAP in-process per gestire l'intero ciclo di vita del dato.
Il database è strutturato in tre schemi logici principali:

### 1. Layer Bronze (raw data)
In questo schema vengono caricate le tabelle grezze direttamente dai file CSV originali senza alcuna trasformazione.
* **Tabelle**: `orders`, `order_items`, `products`, `customers`, `sellers`, ecc.
* **Scopo**: Preservare la fedeltà del dato originale per eventuali ri-elaborazioni.

### 2. Layer Silver (cleaned & validated)
In questo schema i dati vengono puliti, tipizzati e validati tramite **Pandera**.
* **Trasformazioni**: Conversione delle stringhe in `TIMESTAMP`, cast dei prezzi in `DOUBLE` e rimozione di record inconsistenti.
* **Data Quality**: Ogni tabella in questo schema ha superato con successo i controlli di integrità (es. prezzi non negativi, stati ordine validi).

### 3. Layer Gold (analytics ready)
Il layer finale dove i dati vengono modellati secondo uno **Schema a Stella (Star Schema)** per ottimizzare le performance delle query analitiche e della dashboard.
* **Fact Table**: Contiene le metriche quantitative (es. vendite, volumi).
* **Dimension Tables**: Tabelle descrittive (es. `dim_products`, `dim_customers`, `dim_time`) collegate alla Fact Table.
* **Scopo**: Alimentare direttamente la dashboard Streamlit con query ad alte prestazioni.


## DATA QUALITY E VALIDAZIONE (PANDERA)
Per garantire l'integrità del dato durante il passaggio dal layer **Bronze** al layer **Silver**, la pipeline integra dei **Data Quality Gates** sviluppati con la libreria [Pandera](https://pandera.readthedocs.io/).

L'implementazione prevede i seguenti controlli automatizzati:
* **Validazione dei tipi**: Verifica che i dati estratti rispettino rigorosamente la tipizzazione definita (es. `TIMESTAMP` per le date e `DOUBLE` per i prezzi).
* **Integrità degli Identificativi**: Controllo rigoroso sulla presenza obbligatoria del campo `order_id` (not null).
* **Controlli di business**: Applicazione di check sui range numerici, garantendo che colonne critiche come `price` e `freight_value` non contengano mai valori negativi.
* **Integrità categorica**: Validazione della colonna `order_status` tramite una lista chiusa di stati ammessi, filtrando eventuali anomalie nei dati grezzi.

### Fail-Fast architecture:
L'integrazione è progettata per essere **bloccante**: se un test di validazione fallisce, Pandera solleva un'eccezione che interrompe immediatamente il flusso di **Prefect**. Questo approccio impedisce la propagazione di dati corrotti verso il layer **Gold**, garantendo che le analisi di Business Intelligence siano basate su dati certificati.


### MONITORAGGIO DELLA DATA QUALITY (PREFECT)
L'integrazione tra **Pandera** e **Prefect** permette un monitoraggio visivo immediato dello stato della pipeline tramite la dashboard locale (`http://127.0.0.1:4200`):

* **Stato dei Task**: Se i dati non rispettano lo schema definito in `silver.py`, il task `Clean Olist Data (Silver)` viene contrassegnato automaticamente come **Failed**.
* **Log di Validazione**: All'interno della sezione "Logs" del task fallito, è possibile visualizzare il report dettagliato di Pandera che indica esattamente quale colonna e quali righe hanno causato l'errore (es. `Check 'ge(0)' failed for column 'price'`).
* **Blocco Downstream**: Prefect garantisce che il task successivo (`Build Olist Star Schema (Gold)`) rimanga in stato **NotRun** o **Pending**, proteggendo l'integrità del Data Warehouse finale.


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


## CONTAINERIZZAZIONE (PODMAN)
Il progetto è interamente containerizzato per garantire l'isolamento e la portabilità. L'architettura utilizza un'unica immagine unificata per gestire sia l'ETL che la Dashboard.

### Architettura dei Container
- **ETL Service**: Esegue la pipeline orchestrata con Prefect. Si chiude automaticamente al completamento.
- **Dashboard Service**: Avvia l'interfaccia Streamlit (Porta 8501). Parte solo dopo il successo della fase ETL.
- **Persistence**: Il database DuckDB è memorizzato in un volume locale (`./data`) per garantire la persistenza dei dati.

### Come avviare con Podman

1. **Build dell'immagine** (dalla root del progetto):
        
        podman build -t olist_app -f docker/Dockerfile.app .

2. **Avio della Pipeline e Dashboard:** Lanciare il comando e attendere che i log dell'ETL mostrino "PIPELINE COMPLETATA CON SUCCESSO:"
       
        python -m podman_compose -f docker/docker-compose.yml up

3. **Accesso:** Una volta completato l'ETL, la dashboard è disponibile su:
       
        http://localhost:8501


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
