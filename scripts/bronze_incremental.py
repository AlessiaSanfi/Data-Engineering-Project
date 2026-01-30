#--------------------------------------------------------------
# bronze_incremental.py  (Phase 2 - Step 2)
#
# Legge i parquet in data/lake/landing_zone/ usando _manifest.json,
# processa solo i file nuovi/cambiati e carica nel DB (schema bronze).
#
# - orders: incrementale, anti-duplicate su order_id
# - order_items: incrementale, anti-duplicate su (order_id, order_item_id)
#                + carica solo gli items relativi ai nuovi order_id inseriti
# - customers/products: dump completi (REPLACE) quando cambiano
#
# Log tecnico:
# - tech_processed_files: file_name, fingerprint, processed_at, rows_in, rows_inserted, status, note
#--------------------------------------------------------------

import os
import json
import duckdb
from pathlib import Path
from datetime import datetime, timezone

# Consiglio: DB_PATH da env (perfetto anche per GitHub Actions)
DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")

LANDING_DIR = "data/lake/landing_zone"
MANIFEST_PATH = os.path.join(LANDING_DIR, "_manifest.json")

ORDERS_PREFIX = "orders_"          # orders_YYYY-MM.parquet
ORDERS_SUFFIX = ".parquet"

ORDER_ITEMS_FILE = "olist_order_items_dataset.parquet"
CUSTOMERS_FILE = "olist_customers_dataset.parquet"
PRODUCTS_FILE = "olist_products_dataset.parquet"

# -----------------------------
# Helpers
# -----------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _load_manifest(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Manifest non trovato: {path}. Esegui prima scripts/esplosione_dati.py")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _is_orders_monthly(filename: str) -> bool:
    return filename.startswith(ORDERS_PREFIX) and filename.endswith(ORDERS_SUFFIX)

def _safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
    """, [schema, table]).fetchone()
    return row is not None

# -----------------------------
# DQC base (DuckDB)
# -----------------------------
def _run_dqc_orders(con: duckdb.DuckDBPyConnection, parquet_path: str):
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    if rows == 0:
        raise ValueError(f"DQC FAIL: {os.path.basename(parquet_path)} è vuoto.")

    nulls = con.execute(f"""
        SELECT
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id
        FROM read_parquet('{parquet_path}')
    """).fetchone()
    if nulls[0] > 0:
        raise ValueError(f"DQC FAIL: order_id NULL in {os.path.basename(parquet_path)}")
    if nulls[1] > 0:
        raise ValueError(f"DQC FAIL: customer_id NULL in {os.path.basename(parquet_path)}")

def _run_dqc_order_items(con: duckdb.DuckDBPyConnection, parquet_path: str):
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    if rows == 0:
        raise ValueError(f"DQC FAIL: {os.path.basename(parquet_path)} è vuoto.")

    nulls = con.execute(f"""
        SELECT
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
            SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS null_order_item_id,
            SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id
        FROM read_parquet('{parquet_path}')
    """).fetchone()
    if nulls[0] > 0 or nulls[1] > 0:
        raise ValueError(f"DQC FAIL: chiave (order_id/order_item_id) NULL in {os.path.basename(parquet_path)}")
    if nulls[2] > 0:
        raise ValueError(f"DQC FAIL: product_id NULL in {os.path.basename(parquet_path)}")

    # prezzi non negativi (se presenti)
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()]
    if "price" in cols:
        neg = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') WHERE price < 0").fetchone()[0]
        if neg > 0:
            raise ValueError(f"DQC FAIL: trovati {neg} price negativi in {os.path.basename(parquet_path)}")

# -----------------------------
# Tech table
# -----------------------------
def _ensure_tech_table(con: duckdb.DuckDBPyConnection):
    con.execute("CREATE SCHEMA IF NOT EXISTS tech;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS tech.tech_processed_files (
            file_name       VARCHAR PRIMARY KEY,
            fingerprint     VARCHAR,
            processed_at    TIMESTAMP,
            rows_in         BIGINT,
            rows_inserted   BIGINT,
            status          VARCHAR,   -- OK / SKIP / FAIL
            note            VARCHAR
        );
    """)

def _already_processed_same_fingerprint(con: duckdb.DuckDBPyConnection, file_name: str, fingerprint: str) -> bool:
    row = con.execute("""
        SELECT 1
        FROM tech.tech_processed_files
        WHERE file_name = ? AND fingerprint = ?
          AND status IN ('OK', 'SKIP')
        LIMIT 1
    """, [file_name, fingerprint]).fetchone()
    return row is not None

def _log_processed(
    con: duckdb.DuckDBPyConnection,
    file_name: str,
    fingerprint: str,
    rows_in: int,
    rows_inserted: int,
    status: str,
    note: str = ""
):
    con.execute("""
        INSERT INTO tech.tech_processed_files
            (file_name, fingerprint, processed_at, rows_in, rows_inserted, status, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (file_name) DO UPDATE SET
            fingerprint   = excluded.fingerprint,
            processed_at  = excluded.processed_at,
            rows_in       = excluded.rows_in,
            rows_inserted = excluded.rows_inserted,
            status        = excluded.status,
            note          = excluded.note
    """, [file_name, fingerprint, _utc_now_iso(), rows_in, rows_inserted, status, note])

# -----------------------------
# Bronze schema + tables
# -----------------------------
def _ensure_bronze_schema(con: duckdb.DuckDBPyConnection):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

def _ensure_orders_table(con: duckdb.DuckDBPyConnection, sample_parquet: str):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze.orders AS
        SELECT * FROM read_parquet('{sample_parquet}') LIMIT 0;
    """)

def _ensure_order_items_table(con: duckdb.DuckDBPyConnection, sample_parquet: str):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze.order_items AS
        SELECT * FROM read_parquet('{sample_parquet}') LIMIT 0;
    """)

def _replace_table_from_parquet(con: duckdb.DuckDBPyConnection, table_fqn: str, parquet_path: str):
    con.execute(f"CREATE OR REPLACE TABLE {table_fqn} AS SELECT * FROM read_parquet('{parquet_path}');")

# -----------------------------
# Main ingestion
# -----------------------------
def run_bronze_incremental(db_path: str = DB_PATH):
    Path(LANDING_DIR).mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(MANIFEST_PATH)
    files_meta = manifest.get("files", {})

    if not files_meta:
        print("Manifest vuoto: nessun file da processare.")
        return

    con = duckdb.connect(db_path)
    try:
        _ensure_bronze_schema(con)
        _ensure_tech_table(con)

        # ---------
        # 1) Dimensioni (customers/products): REPLACE se cambiano
        # ---------
        for dim_file, dim_table in [
            (CUSTOMERS_FILE, "bronze.customers"),
            (PRODUCTS_FILE, "bronze.products"),
        ]:
            meta = files_meta.get(dim_file)
            if not meta:
                continue

            fp = meta.get("fingerprint", "")
            parquet_path = os.path.join(LANDING_DIR, dim_file)
            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"Manca {parquet_path} (atteso da manifest).")

            if _already_processed_same_fingerprint(con, dim_file, fp):
                _log_processed(con, dim_file, fp, rows_in=0, rows_inserted=0, status="SKIP", note="immutato (fingerprint)")
                continue

            rows_in = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
            _replace_table_from_parquet(con, dim_table, parquet_path)
            _log_processed(con, dim_file, fp, rows_in=rows_in, rows_inserted=rows_in, status="OK", note=f"REPLACE -> {dim_table}")
            print(f"OK: {dim_file} -> {dim_table} (rows={rows_in})")

        # ---------
        # 2) Order items parquet: lo usiamo come sorgente per filtrare gli items dei nuovi ordini
        #    Nota: non “carichiamo tutto” sempre, ma garantiamo tabella + DQC quando serve.
        # ---------
        oi_meta = files_meta.get(ORDER_ITEMS_FILE)
        if not oi_meta:
            raise FileNotFoundError(f"{ORDER_ITEMS_FILE} non presente in manifest (landing_zone).")

        oi_fp = oi_meta.get("fingerprint", "")
        oi_path = os.path.join(LANDING_DIR, ORDER_ITEMS_FILE)
        if not os.path.exists(oi_path):
            raise FileNotFoundError(f"Manca {oi_path} (atteso da manifest).")

        # Assicura tabella bronze.order_items (schema)
        _ensure_order_items_table(con, oi_path)

        # Se order_items immutato e tabella esiste già, non serve rifare DQC ogni run.
        oi_is_unchanged = _already_processed_same_fingerprint(con, ORDER_ITEMS_FILE, oi_fp)
        if not oi_is_unchanged:
            _run_dqc_order_items(con, oi_path)

        # ---------
        # 3) Orders mensili: incrementale anti-dup su order_id
        # ---------
        monthly_orders_files = sorted([fn for fn in files_meta.keys() if _is_orders_monthly(fn)])
        if not monthly_orders_files:
            print("Nessun file orders_YYYY-MM.parquet nel manifest.")
            return

        first_orders_path = os.path.join(LANDING_DIR, monthly_orders_files[0])
        if not os.path.exists(first_orders_path):
            raise FileNotFoundError(f"Manca {first_orders_path} (atteso da manifest).")
        _ensure_orders_table(con, first_orders_path)

        total_orders_inserted = 0
        total_items_inserted = 0

        for fn in monthly_orders_files:
            meta = files_meta.get(fn, {})
            fp = meta.get("fingerprint", "")
            parquet_path = os.path.join(LANDING_DIR, fn)

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"Manca {parquet_path} (atteso da manifest).")

            if _already_processed_same_fingerprint(con, fn, fp):
                _log_processed(con, fn, fp, rows_in=0, rows_inserted=0, status="SKIP", note="immutato (fingerprint)")
                continue

            _run_dqc_orders(con, parquet_path)
            rows_in = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]

            # 3.1) Trova nuovi orders con NOT EXISTS (DuckDB-safe)
            con.execute("DROP TABLE IF EXISTS _tmp_new_orders;")
            con.execute(f"""
                CREATE TEMP TABLE _tmp_new_orders AS
                SELECT s.*
                FROM read_parquet('{parquet_path}') s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM bronze.orders b
                    WHERE b.order_id = s.order_id
                );
            """)
            new_orders_cnt = con.execute("SELECT COUNT(*) FROM _tmp_new_orders;").fetchone()[0]

            # 3.2) Inserisci solo i nuovi orders
            if new_orders_cnt > 0:
                con.execute("INSERT INTO bronze.orders SELECT * FROM _tmp_new_orders;")

            # 3.3) Carica order_items SOLO per i nuovi order_id (anti-dup composito)
            con.execute("DROP TABLE IF EXISTS _tmp_new_order_ids;")
            con.execute("""
                CREATE TEMP TABLE _tmp_new_order_ids AS
                SELECT DISTINCT order_id FROM _tmp_new_orders;
            """)

            if new_orders_cnt > 0:
                # Se order_items parquet è “immutato”, va bene: lo leggiamo comunque per estrarre SOLO i nuovi items.
                # Se invece era cambiato, abbiamo già eseguito DQC sopra.
                con.execute("DROP TABLE IF EXISTS _tmp_new_items;")
                con.execute(f"""
                    CREATE TEMP TABLE _tmp_new_items AS
                    SELECT oi.*
                    FROM read_parquet('{oi_path}') oi
                    JOIN _tmp_new_order_ids ids
                      ON oi.order_id = ids.order_id
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM bronze.order_items bi
                        WHERE bi.order_id = oi.order_id
                          AND bi.order_item_id = oi.order_item_id
                    );
                """)
                new_items_cnt = con.execute("SELECT COUNT(*) FROM _tmp_new_items;").fetchone()[0]

                if new_items_cnt > 0:
                    con.execute("INSERT INTO bronze.order_items SELECT * FROM _tmp_new_items;")
            else:
                new_items_cnt = 0

            total_orders_inserted += _safe_int(new_orders_cnt)
            total_items_inserted += _safe_int(new_items_cnt)

            _log_processed(
                con, fn, fp,
                rows_in=rows_in,
                rows_inserted=new_orders_cnt,
                status="OK",
                note=f"orders_inserted={new_orders_cnt}; items_inserted={new_items_cnt}"
            )
            print(f"OK: {fn} | rows_in={rows_in} | new_orders={new_orders_cnt} | new_items={new_items_cnt}")

        # ---------
        # 4) Log order_items file come processato (coerente)
        # ---------
        if oi_is_unchanged and total_items_inserted == 0:
            _log_processed(con, ORDER_ITEMS_FILE, oi_fp, rows_in=0, rows_inserted=0, status="SKIP", note="immutato (fingerprint)")
        else:
            oi_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{oi_path}')").fetchone()[0]
            _log_processed(
                con, ORDER_ITEMS_FILE, oi_fp,
                rows_in=oi_rows,
                rows_inserted=total_items_inserted,
                status="OK",
                note="order_items used as source; inserted only for new orders"
            )

        print("\nBronze incremental completato.")
        print(f"- Totale nuovi orders inseriti: {total_orders_inserted}")
        print(f"- Totale nuovi order_items inseriti: {total_items_inserted}")

    except Exception as e:
        print(f"\nERRORE CRITICO: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    run_bronze_incremental()
