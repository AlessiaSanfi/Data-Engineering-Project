# --------------------------------------------------------------
# esplosione_dati.py
#
# Step 1 (Phase 2):
# - Legge i CSV originali (data/raw/)
# - Verifica se ci sono dati "nuovi" (per mese) tramite fingerprint
# - Genera/aggiorna parquet mensili orders_YYYY-MM.parquet
# - Converte anche le anagrafiche in parquet completi
# - Scrive un manifest JSON per tracciare cosa è stato generato
# --------------------------------------------------------------

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

RAW_DATA_PATH = "data/raw/"
LANDING_ZONE = "data/lake/landing_zone/"
MANIFEST_PATH = os.path.join(LANDING_ZONE, "_manifest.json")

ORDERS_CSV = "olist_orders_dataset.csv"
DIM_CSVS = [
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
]

# -----------------------------
# Helpers: manifest
# -----------------------------
def _load_manifest(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"generated_at_utc": None, "files": {}}


def _save_manifest(path: str, manifest: dict) -> None:
    manifest["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


# -----------------------------
# Helpers: fingerprint
# -----------------------------
def _md5_update_str(md5: "hashlib._Hash", s: str) -> None:
    md5.update(s.encode("utf-8"))


def _fingerprint_orders_month(df_month: pd.DataFrame) -> dict:
    ts = pd.to_datetime(df_month["order_purchase_timestamp"], errors="coerce")

    rowcount = int(len(df_month))
    min_ts = "" if ts.isna().all() else str(ts.min())
    max_ts = "" if ts.isna().all() else str(ts.max())

    order_ids = df_month["order_id"].fillna("").astype(str).sort_values(kind="mergesort")
    md5_ids = hashlib.md5()
    for oid in order_ids:
        _md5_update_str(md5_ids, oid)
        md5_ids.update(b"|")

    md5 = hashlib.md5()
    _md5_update_str(md5, f"rows={rowcount};")
    _md5_update_str(md5, f"min_ts={min_ts};")
    _md5_update_str(md5, f"max_ts={max_ts};")
    _md5_update_str(md5, f"order_ids_hash={md5_ids.hexdigest()};")

    return {
        "fingerprint": md5.hexdigest(),
        "rows": rowcount,
        "min_ts": min_ts,
        "max_ts": max_ts,
    }


def _fingerprint_full_df(df: pd.DataFrame) -> dict:
    rowcount = int(len(df))
    cols = [str(c) for c in df.columns]

    md5 = hashlib.md5()
    _md5_update_str(md5, f"rows={rowcount};")
    _md5_update_str(md5, f"cols={','.join(cols)};")

    key_col = next((c for c in ["customer_id", "product_id", "order_id"] if c in df.columns), None)

    if key_col:
        series = df[key_col].fillna("").astype(str).sort_values(kind="mergesort")
        md5_keys = hashlib.md5()
        for v in series:
            _md5_update_str(md5_keys, v)
            md5_keys.update(b"|")
        _md5_update_str(md5, f"keys_hash={md5_keys.hexdigest()};")

    return {
        "fingerprint": md5.hexdigest(),
        "rows": rowcount,
        "columns": cols,
    }


# -----------------------------
# Main
# -----------------------------
def esplodi_dati():
    Path(LANDING_ZONE).mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest(MANIFEST_PATH)

    print("Step 1: Esplosione dati CSV -> Parquet (Landing Zone)")
    print(f"- RAW:     {RAW_DATA_PATH}")
    print(f"- LANDING: {LANDING_ZONE}\n")

    orders_path = os.path.join(RAW_DATA_PATH, ORDERS_CSV)
    if not os.path.exists(orders_path):
        raise FileNotFoundError(f"Non trovo {orders_path}")

    # ✅ FIX BOM + normalizzazione colonne
    df_orders = pd.read_csv(orders_path, encoding="utf-8-sig")
    df_orders.columns = df_orders.columns.str.strip()

    if "order_purchase_timestamp" not in df_orders.columns:
        raise KeyError(
            f"Colonna 'order_purchase_timestamp' non trovata.\n"
            f"Colonne disponibili: {list(df_orders.columns)}"
        )

    df_orders["order_purchase_timestamp"] = pd.to_datetime(
        df_orders["order_purchase_timestamp"], errors="coerce"
    )

    df_orders["periodo"] = df_orders["order_purchase_timestamp"].dt.to_period("M")
    periodi = sorted(df_orders["periodo"].dropna().unique())

    print(f"Ordini: trovati {len(periodi)} mesi\n")

    created = updated = skipped = 0

    for p in periodi:
        df_month = df_orders[df_orders["periodo"] == p].drop(columns="periodo")
        filename = f"orders_{p}.parquet"
        file_path = os.path.join(LANDING_ZONE, filename)

        fp = _fingerprint_orders_month(df_month)
        prev = manifest["files"].get(filename)

        if prev and prev["fingerprint"] == fp["fingerprint"] and os.path.exists(file_path):
            skipped += 1
            continue

        df_month.to_parquet(file_path, index=False)

        action = "CREATO" if not os.path.exists(file_path) else "AGGIORNATO"
        created += action == "CREATO"
        updated += action == "AGGIORNATO"

        manifest["files"][filename] = {
            "type": "orders_monthly",
            "source": orders_path,
            **fp,
            "written_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        print(f"{action}: {filename} (rows={fp['rows']})")

    print(f"\nOrders: created={created}, updated={updated}, skipped={skipped}\n")

    print("Anagrafiche:")
    for csv_name in DIM_CSVS:
        csv_path = os.path.join(RAW_DATA_PATH, csv_name)
        if not os.path.exists(csv_path):
            print(f"SKIP: {csv_name}")
            continue

        df_dim = pd.read_csv(csv_path, encoding="utf-8-sig")
        df_dim.columns = df_dim.columns.str.strip()

        parquet_name = csv_name.replace(".csv", ".parquet")
        parquet_path = os.path.join(LANDING_ZONE, parquet_name)

        fp = _fingerprint_full_df(df_dim)
        prev = manifest["files"].get(parquet_name)

        if prev and prev["fingerprint"] == fp["fingerprint"] and os.path.exists(parquet_path):
            print(f"OK: {parquet_name}")
            continue

        df_dim.to_parquet(parquet_path, index=False)
        print(f"SCRITTO: {parquet_name}")

        manifest["files"][parquet_name] = {
            "type": "dimension_full_dump",
            "source": csv_path,
            **fp,
            "written_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    _save_manifest(MANIFEST_PATH, manifest)
    print("\nStep 1 completato.")


if __name__ == "__main__":
    esplodi_dati()
