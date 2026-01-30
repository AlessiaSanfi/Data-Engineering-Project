# BRAZILIAN E-COMMERCE DATA PIPELINE (OLIST) – PHASE 2

La Fase 2 estende la pipeline batch introducendo una **pipeline incrementale, automatizzata e osservabile**, progettata per simulare un contesto reale di produzione.

L’obiettivo è gestire **nuovi dati nel tempo**, garantendo idempotenza, qualità e coerenza dei KPI.

---

## OBIETTIVI DELLA FASE 2
- Ingestione incrementale dei dati
- Tracciamento dello stato della pipeline
- Evitare duplicazioni
- Garantire coerenza storica
- Automazione e scheduling
- Rendere il Gold Layer interrogabile e stabile

---

## STACK TECNOLOGICO (FASE 2)
- **Linguaggio:** Python
- **Database:** DuckDB
- **Formato Storage:** Parquet (Data Lake)
- **Orchestrazione:** Prefect
- **CI / Scheduling:** GitHub Actions
- **Data Quality:** Pandera
- **Dashboard:** Streamlit
- **AI Assistant:** Google Gemini (Text-to-SQL)

---

## ARCHITETTURA FASE 2
La pipeline segue un’architettura **ibrida Data Lake + Data Warehouse**:
CSV Raw
↓
Landing Zone (Parquet + Manifest)
↓
Bronze (DuckDB incrementale)
↓
Silver (pulizia + validazioni)
↓
Gold (Star Schema)
↓
Dashboard / AI Assistant


---

## STEP 1 – LANDING ZONE & MANIFEST
Script: `esplosione_dati.py`

- Conversione CSV → Parquet
- Ordini splittati per mese
- Creazione di `_manifest.json`
- Fingerprint dei file per rilevare modifiche
- Base dell’incrementalità

---

## STEP 2 – BRONZE INCREMENTALE
Script: `bronze_incremental.py`

- Caricamento incrementale nel DB
- Tabelle:
  - **Orders:** insert solo nuovi `order_id`
  - **Order_items:** caricati solo per nuovi ordini
  - **Customers / Products:** full refresh solo se cambia il fingerprint
- Anti-duplicate con `NOT EXISTS`
- Log tecnico in `tech.tech_processed_files`

---

## STEP 3 – SILVER (DATA QUALITY GATE)
Script: `silver.py`

- Pulizia dei dati
- Casting dei tipi
- Validazioni Pandera:
  - prezzi ≥ 0
  - stati ordine validi
  - chiavi non nulle
- Se un controllo fallisce → pipeline bloccata

---

## STEP 4 – GOLD (ANALYTICS READY)
Script: `gold.py`

- Ricostruzione dello Star Schema
- Inclusi solo ordini `delivered`
- Metriche derivate (es. `delivery_time_days`)
- Layer stabile e read-only per BI

---

## ORCHESTRAZIONE & AUTOMAZIONE
- **Prefect**: governa il flusso end-to-end
- **GitHub Actions**:
  - esecuzione schedulata
  - smoke test sul Gold Layer
  - salvataggio DuckDB come artifact

---

## DASHBOARD & TEXT-TO-SQL
- Dashboard Streamlit su Gold Layer
- KPI, grafici e filtri geografici
- Assistant AI:
  - query in linguaggio naturale
  - generazione SQL controllata
  - esecuzione read-only

---

## ASSUNZIONI SUI DATI
- I dati arrivano con cadenza **giornaliera**
- Per semplicità, la simulazione usa file **mensili**
- Ogni periodo rappresenta un batch incrementale

---

## DIFFERENZA CON LA FASE 1
| Fase 1 | Fase 2 |
|------|------|
| Batch completa | Incrementale |
| Stateless | Stateful |
| Full reload | Append + dedup |
| Manuale | Automatizzata |
| Demo | Produzione-like |

---

## DOMANDE

### 1. Come individuate i dati già processati?

Individuo i dati già processati utilizzando **due livelli di controllo**.

Il primo livello è il **manifest nella Landing Zone**, che contiene un fingerprint dei file Parquet:  
se il contenuto del file non cambia, il file viene automaticamente saltato.

Il secondo livello è la tabella tecnica **`tech.tech_processed_files`** nel database, che traccia:
- nome del file
- fingerprint
- stato dell’elaborazione

In questo modo ho sempre visibilità su cosa è già entrato in pipeline e cosa no.


### 2. Come gestite l’arrivo di nuovi dati senza duplicazioni?

Gestisco i nuovi dati con una strategia **append + anti-duplicate**.

Quando arrivano nuovi file:
- **inserisco** solo le righe che **non esistono già** nel database
- **evito duplicati** utilizzando controlli come `NOT EXISTS` sulle chiavi naturali

Questo rende la pipeline **idempotente**: se la rilancio più volte, non vengono create duplicazioni e il database cresce solo con dati realmente nuovi.


### 3. Come scrivete sui vari layer? Perché?

Scrivo in modo diverso su ogni layer perché **ogni layer ha uno scopo specifico**.

- **Landing Zone**  
  Scrivo file Parquet versionati (approccio file-based) per mantenere flessibilità e storicità.

- **Bronze**  
  Inserisco i dati in modo incrementale nel database, mantenendoli il più possibile vicini alla sorgente.

- **Silver**  
  Riscrivo le tabelle applicando pulizia, casting dei tipi e validazioni di qualità.

- **Gold**  
  Ricostruisco sempre lo **star schema**, perché è un layer puramente analitico e deve essere coerente al 100%.


### 4. Come garantite la coerenza dei KPI nel tempo?

La coerenza dei KPI è garantita da tre fattori principali:

- carico solo dati nuovi, evitando duplicazioni
- applico regole di qualità nel Silver layer tramite **Pandera**
- nel Gold includo solo dati **business-validi**, ad esempio solo ordini con stato `delivered`

In questo modo, se ricalcolo un KPI oggi o domani, il risultato cambia solo se arrivano **nuovi dati reali**, non per errori di pipeline.


### 5. Cosa succede se una run schedulata fallisce a metà pipeline?

Se una run fallisce, la pipeline si ferma immediatamente e **non prosegue sugli step successivi**.

I dati già caricati restano consistenti perché:
- gli step sono idempotenti (se eseguiti più volte, producono sempre lo stesso risultato)
- i file già processati vengono riconosciuti e saltati al run successivo

Quando la pipeline riparte, riprende in modo sicuro senza creare duplicazioni o incoerenze.


### 6. Quali assunzioni avete fatto sui dati in ingresso?

Ho assunto che i dati arrivino con una **cadenza quotidiana**, simulando un sistema reale in cui i nuovi ordini vengono estratti a fine giornata.

Per comodità, ho modellato questa logica utilizzando **file mensili**, trattando ogni mese come se rappresentasse un “giorno” di arrivo dati.

Questo mi permette di testare l’incrementalità senza modificare la logica della pipeline.


### 7. Dovete modificare il modello dati con l'integrazione della Fase 2? Perché?

No, non ho modificato lo star schema.

Il modello Gold è rimasto lo stesso della Fase 1 perché era già corretto dal punto di vista analitico.

La Fase 2 cambia come i dati arrivano e vengono aggiornati, ma non come vengono analizzati.  
Questo dimostra una buona separazione tra ingestione e modellazione.


### 8. In cosa la pipeline della Fase 2 differisce concettualmente da quella della Fase 1?

La **Fase 1** è una pipeline batch statica:
- eseguita manualmente
- su un dataset fisso
- ogni run ricostruisce tutto da zero

La **Fase 2** è una pipeline:
- schedulata
- incrementale
- persistente

Non deve più essere avviata dall’utente: si attiva automaticamente ogni notte, individua solo i nuovi dati arrivati e li processa, mentre il database cresce nel tempo.

---

## COME AVVIARE LA FASE 2
```bash
pip install -r requirements.txt
python etl/flows/main_flows_fase2.py

