# BRAZILIAN E-COMMERCE DATA PIPELINE (OLIST)
Questo progetto implementa una pipeline di Data Engineering professionale per l'analisi dei dati di e-commerce brasiliani (dataset Olist). L'obiettivo è trasformare i dati grezzi in un formato ottimizzato per l'analisi di business (KPI su vendite, logistica e performance dei venditori).

**Badge di GitHub Actions**:
![Olist Daily Pipeline](https://github.com/AlessiaSanfi/Data-Engineering-Project/actions/workflows/pipeline.yml/badge.svg?branch=feature/fase2)


## STACK TECNOLOGICO
- **Linguaggio:** Python 3.x
- **Orchestratore:** [Prefect](https://www.prefect.io/) e **GitHub Actions**
- **Database:** [DuckDB](https://duckdb.org/) (OLAP database in-process)
- **Data Manipulation:** Polars e Pandas
- **AI Engine:** Google Gemini Flash (Generazione SQL dinamica)
- **Data Format:** Apache Parquet (Storage colonnare)
- **Visualizzazione:** Streamlit
- **Containerizzazione:** Podman


## AUTOMAZIONE
**CI/CD & Scheduling:** GitHub Actions gestisce l'esecuzione automatica della pipeline ogni notte alle 02:00 e convalida ogni nuova modifica al codice tramite test automatizzati.


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
3. **Efficienza logistica:** Quanto tempo impiegano mediamente i prodotti per arrivare a destinazione nei diversi Stati? (delivery_time_days e dim_customers).
4. **Impatto dei costi di spedizione:** Qual è l'incidenza del trasporto sulle vendite e quali Stati presentano le tariffe medie più alte? (freight_value).
5. **Andamento storico:** Come si evolvono le vendite mese dopo mese e quali sono i trend di crescita a lungo termine? (dim_time).
6. **Analisi acquisti settimanali:** In quali giorni della settimana si concentra la maggior parte degli ordini e qual è la spesa media per ogni transazione? (dim_time e fact_sales).

## MAPPATURA DASHBOARD
Ogni domanda di business trova una risposta diretta all'interno della dashboard interattiva.

1. **Analisi del fatturato:** = KPI "Fatturato" + grafico "Top 10 Categorie" (Donut Chart).
2. **Distribuzione geografica:** = KPI "Ordini" + Grafico "Ordini per Stato" (Mappa Coropletica).
3. **Efficienza logistica:** = KPI "Consegna Media" + Grafico "Tempi di consegna per Stato" (Bar Chart orizzontale).
4. **Analisi dei costi:** = KPI "Spedizione Media" + Grafico "Costo Medio Spedizione per Stato" (Bar Chart orizzontale).
5. **Trend temporali:** = Grafico "Trend Temporale delle Vendite" (line chart).
6. **Analisi acquisti settimanali** KPI "Spesa Media" + Grafico "Stagionalità Settimanale" (Bar chart verticale).

**Nota:** La dashboard permette di filtrare tutti i risultati per Stato del cliente tramite la barra laterale, consentendo un'analisi granulare per ogni domanda sopra elencata.


## INTEGRAZIONE GENERATIVE AI (TEXT-TO-SQL)
Il progetto include un modulo avanzato di **Natural Language Processing** che permette agli utenti di interrogare il database utilizzando il linguaggio naturale.

- **Tecnologia:** Integrazione con le API di **Google Gemini** (modello `gemini-flash-lite-latest`).
- **Funzionamento:** Il sistema riceve una domanda in testo libero (es. *"Qual è il fatturato totale per la città di San Paolo?"*), la traduce istantaneamente in una query SQL valida per DuckDB basandosi sulla struttura dello Star Schema Gold, ed esegue l'interrogazione.
- **Sicurezza:** La logica di generazione è blindata da un sistema di *Prompt Engineering* che vincola l'output alle sole tabelle dello Star Schema, prevenendo allucinazioni o accessi a tabelle non autorizzate.

**Esempio:** *"Qual è il fatturato totale per la città di San Paolo nel 2017?"*
- L'AI traduce la domanda in una query DuckDB ottimizzata, recupera i dati dal layer Gold e restituisce il risultato formattato in **Real Brasiliani (R$)**.

## CONTAINERIZZAZIONE (PODMAN)
Il progetto è interamente containerizzato per garantire l'isolamento e la portabilità. L'architettura utilizza un'unica immagine unificata per gestire sia l'ETL che la Dashboard.

### Architettura dei Container
- **ETL Service**: Esegue la pipeline orchestrata con Prefect. Si chiude automaticamente al completamento.
- **Dashboard Service**: Avvia l'interfaccia Streamlit (Porta 8501). Parte solo dopo il successo della fase ETL.
- **Persistence**: Il database DuckDB è memorizzato in un volume locale (`./data`) per garantire la persistenza dei dati.

### Come avviare con Podman

1. **Build dell'immagine** (dalla root del progetto):
        
        podman build -t olist_app -f docker/Dockerfile.app .

2. **Avvio della Pipeline e Dashboard:** Lanciare il comando e attendere che i log dell'ETL mostrino "PIPELINE COMPLETATA CON SUCCESSO:"
       
        python -m podman_compose -f docker/docker-compose.yml up

3. **Accesso:** Una volta completato l'ETL, la dashboard è disponibile su:
       
        http://localhost:8501


## STRUTTURA DELLE CARTELLE
Data-Engineering-Project/
├── .github/
│   └── workflows/
│       └── pipeline.yml            # Automazione GitHub Actions
├── data/
│   ├── lake/                       # Data Lake Medallion (Parquet)
│   │   ├── bronze/
│   │   ├── gold/
│   │   └── silver/
│   └── raw/                        # Dataset originale (CSV)
├── dashboard/                      # Componenti della Dashboard
│   ├── app.py                      # Punto di ingresso Streamlit
│   └── queries.py                  # Query SQL e logica dati
├── docker/                         # Configurazione Container
│   ├── docker-compose.yml
│   └── Dockerfile.app
├── etl/                            # Pipeline di Ingegneria Dati
│   ├── flows/
│   │   └── main_flows.py           # Orchestratore Prefect
│   ├── tasks/
│   │   ├── bronze.py               # Task Ingestione
│   │   ├── gold.py                 # Task Modellazione
│   │   └── silver.py               # Task Validazione (Pandera)
│   └── utils.py                    # Helper e connessioni DB
├── models/                         # Documentazione Modelli
│   └── star_schema.md
├── .env                            # Variabili d'ambiente (API Keys)
├── .gitignore                      # Esclusione file Git
├── ai_utils.py                     # Modulo integrazione Gemini AI
├── duckdb.exe                      # Eseguibile database CLI
├── Fase 2 - diario di bordo.pdf    # Documentazione di progetto
├── README.md                       # Documentazione principale
└── requirements.txt                # Dipendenze Python


## COME AVVIARE IL PROGETTO

**Prerequisiti**
- Python installato.
- Una chiave API di Google Gemini (da inserire nel file .env).

**Installazione dipendenze**
Installa le librerie necessarie tramite terminale:
  python -m pip install -r requirements.txt.

**Esplorazione Dati**
Per ispezionare il Layer Gold da terminale:
  .\duckdb -c "SELECT * FROM 'data/lake/gold/fact_sales.parquet' LIMIT 5;"

**Esecuzione della Pipeline**
Per avviare l'intero processo, esegui:
    
    python -m etl.flows.main_flows


## VISUALIZZAZIONE DATI
La pipeline alimenta una dashboard interattiva sviluppata in Streamlit, che permette di esplorare i KPI definiti nel Gold Layer.
Per avviarla, eseguire il comando:
    
    python -m streamlit run dashboard/app.py
