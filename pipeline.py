import json
import logging
import os
from pathlib import Path
from urllib.request import Request, urlopen

import luigi
import pandas as pd
import sentry_sdk
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("olist-elt")

SENTRY_DSN = os.getenv("SENTRY_DSN", "")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
ARTIFACT_DIR = Path("artifacts")
EXTRACT_DIR = ARTIFACT_DIR / "extract"
BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "SQL"

# Chunk size for bulk inserts — tuned for balance between memory and throughput
LOAD_CHUNK_SIZE = int(os.getenv("LOAD_CHUNK_SIZE", "5000"))

SOURCE_TABLE_MAP = {
    "orders": "orders",
    "order_items": "order_items",
    "products": "products",
    "customers": "customers",
    "sellers": "sellers",
    "order_reviews": "reviews",
}

DIM_SQL_FILES = [
    "dimensions/01_dim_date.sql",
    "dimensions/02_dim_location.sql",
    "dimensions/03_scd2_customer_close.sql",
    "dimensions/04_scd2_customer_open.sql",
    "dimensions/05_scd2_product_close.sql",
    "dimensions/06_scd2_product_open.sql",
    "dimensions/07_scd2_seller_close.sql",
    "dimensions/08_scd2_seller_open.sql",
]

FACT_SQL_FILES = [
    "facts/01_fact_sales.sql",
    "facts/02_fact_delivery.sql",
    "facts/03_fact_review.sql",
]

# FIX [1]: DQ dipisah dari TransformWarehouse agar bisa di-skip/re-run mandiri
DQ_SQL_FILES = {
    "duplicate current dim_customer": "dq/01_dup_current_customer.sql",
    "duplicate current dim_product": "dq/02_dup_current_product.sql",
    "duplicate current dim_seller": "dq/03_dup_current_seller.sql",
    "null fact_sales dimension keys": "dq/04_null_fact_keys.sql",
    "negative delivery lead time": "dq/05_negative_delivery.sql",
}

# ADD: Mart SQL files untuk serve layer
MART_SQL_FILES = [
    "marts/01_dm_daily_sales.sql",
    "marts/02_dm_customer_summary.sql",
    "marts/03_dm_product_performance.sql",
]

if SENTRY_DSN:
    sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=1.0)


# ─────────────────────────────────────────────
# DB Helpers
# ─────────────────────────────────────────────

def _build_pg_uri(prefix: str) -> str:
    db   = os.getenv(f"{prefix}_POSTGRES_DB")
    host = os.getenv(f"{prefix}_POSTGRES_HOST")
    user = os.getenv(f"{prefix}_POSTGRES_USER")
    pwd  = os.getenv(f"{prefix}_POSTGRES_PASSWORD")
    port = os.getenv(f"{prefix}_POSTGRES_PORT")
    missing = [k for k, v in {
        f"{prefix}_POSTGRES_DB":       db,
        f"{prefix}_POSTGRES_HOST":     host,
        f"{prefix}_POSTGRES_USER":     user,
        f"{prefix}_POSTGRES_PASSWORD": pwd,
        f"{prefix}_POSTGRES_PORT":     port,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def get_source_engine() -> Engine:
    return create_engine(_build_pg_uri("SRC"), pool_pre_ping=True)


def get_dwh_engine() -> Engine:
    return create_engine(_build_pg_uri("DWH"), pool_pre_ping=True)


def run_sql(engine: Engine, sql_text: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql_text))


def fetch_scalar(engine: Engine, sql_text: str) -> int:
    with engine.begin() as conn:
        return conn.execute(text(sql_text)).scalar_one()


def read_sql(relative_path: str) -> str:
    sql_path = SQL_DIR / relative_path
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def run_sql_file(engine: Engine, relative_path: str) -> None:
    run_sql(engine, read_sql(relative_path))


def fetch_scalar_file(engine: Engine, relative_path: str) -> int:
    return fetch_scalar(engine, read_sql(relative_path))


def get_table_columns(engine: Engine, schema_name: str, table_name: str) -> list[str]:
    sql_text = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema_name
      AND table_name   = :table_name
    ORDER BY ordinal_position
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(sql_text),
            {"schema_name": schema_name, "table_name": table_name},
        )
        return [row[0] for row in result.fetchall()]


# FIX [2]: Helper untuk dispose engine setelah digunakan
# Engine yang tidak di-dispose menyebabkan connection pool leak antar task
def dispose_engine(engine: Engine) -> None:
    try:
        engine.dispose()
    except Exception as exc:
        logger.warning("Engine dispose failed: %s", exc)


# ─────────────────────────────────────────────
# Alerting
# ─────────────────────────────────────────────

def send_webhook_alert(message: str) -> None:
    if not ALERT_WEBHOOK_URL:
        return
    payload = json.dumps({"text": message}).encode("utf-8")
    req = Request(
        ALERT_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urlopen(req, timeout=10) as _:
        pass


def notify_error(task_name: str, exc: Exception) -> None:
    msg = f"[ELT FAILED] task={task_name} error={str(exc)}"
    logger.exception(msg)
    if SENTRY_DSN:
        sentry_sdk.capture_exception(exc)
    try:
        send_webhook_alert(msg)
    except Exception as alert_exc:
        logger.error("Webhook alert failed: %s", alert_exc)


def notify_success(message: str) -> None:
    logger.info(message)
    try:
        send_webhook_alert(f"[ELT SUCCESS] {message}")
    except Exception as exc:
        logger.warning("Could not send success alert: %s", exc)


def dq_assert_zero(engine: Engine, sql_file: str, check_name: str) -> None:
    cnt = fetch_scalar_file(engine, sql_file)
    if cnt != 0:
        raise ValueError(f"DQ check failed: '{check_name}' — offending_rows={cnt}")
    logger.info("DQ passed: %s", check_name)


# ─────────────────────────────────────────────
# Luigi Tasks
# ─────────────────────────────────────────────

class ExtractSource(luigi.Task):
    """
    LAYER 1 — EXTRACT
    Ambil seluruh data dari source DB dan simpan sebagai CSV + manifest.
    """
    run_date = luigi.DateParameter()

    def output(self):
        # FIX [3]: ensure_artifact_dir() JANGAN dipanggil di output().
        # output() dipanggil berulang oleh Luigi scheduler hanya untuk cek status.
        # Side-effect (mkdir) di sini menyebabkan dir dibuat bahkan saat task skip.
        # Pindahkan ke run() saja.
        return luigi.LocalTarget(ARTIFACT_DIR / f"extract_source_{self.run_date}.done")

    def run(self):
        # FIX [3] lanjutan: mkdir dilakukan di run(), bukan di output()
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

        src_engine = get_source_engine()
        try:
            manifest: dict[str, int] = {}
            for src_table, dst_table in SOURCE_TABLE_MAP.items():
                logger.info("Extracting public.%s → %s", src_table, dst_table)

                # FIX [4]: SELECT * rentan terhadap perubahan skema source.
                # Gunakan explicit column list dari information_schema agar
                # lebih defensif. Di sini kita tetap dynamic tapi pakai
                # explicit connection untuk kompatibilitas SQLAlchemy 2.x.
                with src_engine.connect() as conn:
                    df = pd.read_sql(
                        text(f"SELECT * FROM public.{src_table}"),
                        conn,
                    )

                extract_file = EXTRACT_DIR / f"{self.run_date}_{dst_table}.csv"
                df.to_csv(extract_file, index=False)
                manifest[dst_table] = len(df)
                logger.info(
                    "Extracted %d rows from public.%s → %s",
                    len(df), src_table, extract_file.name,
                )

            manifest_path = EXTRACT_DIR / f"{self.run_date}_manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            logger.info("Manifest written: %s", manifest_path)

            with self.output().open("w") as f:
                f.write("ok")

        except Exception as e:
            notify_error("ExtractSource", e)
            raise
        finally:
            # FIX [2] terapkan: dispose engine setelah selesai
            dispose_engine(src_engine)


class LoadStaging(luigi.Task):
    """
    LAYER 2 — LOAD (ke Staging)
    Muat CSV hasil extract ke schema staging as-is (tanpa transformasi).
    Staging selalu di-TRUNCATE sebelum load agar idempotent.
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return ExtractSource(run_date=self.run_date)

    def output(self):
        return luigi.LocalTarget(ARTIFACT_DIR / f"load_staging_{self.run_date}.done")

    def run(self):
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        dwh_engine = get_dwh_engine()
        try:
            manifest_path = EXTRACT_DIR / f"{self.run_date}_manifest.json"
            if not manifest_path.exists():
                raise FileNotFoundError(f"Extract manifest not found: {manifest_path}")

            expected_counts: dict[str, int] = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )

            # Truncate semua staging table dalam satu transaksi
            with dwh_engine.begin() as conn:
                for dst in SOURCE_TABLE_MAP.values():
                    conn.execute(text(f"TRUNCATE TABLE staging.{dst}"))
            logger.info("Staging tables truncated.")

            for dst_table in SOURCE_TABLE_MAP.values():
                extract_file = EXTRACT_DIR / f"{self.run_date}_{dst_table}.csv"
                if not extract_file.exists():
                    raise FileNotFoundError(f"Extract file not found: {extract_file}")

                logger.info("Loading %s → staging.%s", extract_file.name, dst_table)
                df = pd.read_csv(extract_file)

                if df.empty:
                    logger.warning("Extract file is empty for staging.%s — skipping load.", dst_table)
                else:
                    staging_columns = get_table_columns(dwh_engine, "staging", dst_table)
                    loadable_columns = [c for c in df.columns if c in staging_columns]
                    ignored_columns  = [c for c in df.columns if c not in staging_columns]

                    if not loadable_columns:
                        raise ValueError(
                            f"No matching columns between extract and staging.{dst_table}"
                        )
                    if ignored_columns:
                        logger.warning(
                            "Ignoring extra extract columns for staging.%s: %s",
                            dst_table, ", ".join(ignored_columns),
                        )

                    # FIX [5]: chunksize dinaikkan + method='multi' untuk performa
                    df[loadable_columns].to_sql(
                        dst_table,
                        dwh_engine,
                        schema="staging",
                        if_exists="append",
                        index=False,
                        chunksize=LOAD_CHUNK_SIZE,
                        method="multi",
                    )

                # Validasi row count
                expected_cnt = int(expected_counts.get(dst_table, -1))
                if expected_cnt < 0:
                    raise ValueError(
                        f"Expected row count for '{dst_table}' not found in manifest"
                    )
                actual_cnt = fetch_scalar(
                    dwh_engine, f"SELECT COUNT(*) FROM staging.{dst_table}"
                )
                if expected_cnt != actual_cnt:
                    raise ValueError(
                        f"Row count mismatch staging.{dst_table}: "
                        f"expected={expected_cnt}, actual={actual_cnt}"
                    )
                logger.info(
                    "staging.%s loaded OK — %d rows", dst_table, actual_cnt
                )

            with self.output().open("w") as f:
                f.write("ok")

        except Exception as e:
            notify_error("LoadStaging", e)
            raise
        finally:
            dispose_engine(dwh_engine)


class TransformWarehouse(luigi.Task):
    """
    LAYER 3 — TRANSFORM (Staging → Warehouse)
    Jalankan SQL dimensi + fakta. Hasilnya masuk ke schema warehouse.
    DQ check dipisah ke task DataQualityCheck agar concern terpisah.
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return LoadStaging(run_date=self.run_date)

    def output(self):
        return luigi.LocalTarget(ARTIFACT_DIR / f"transform_warehouse_{self.run_date}.done")

    def run(self):
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        engine = get_dwh_engine()
        try:
            for sql_file in DIM_SQL_FILES:
                logger.info("Running dimension SQL: %s", sql_file)
                run_sql_file(engine, sql_file)

            for sql_file in FACT_SQL_FILES:
                logger.info("Running fact SQL: %s", sql_file)
                run_sql_file(engine, sql_file)

            with self.output().open("w") as f:
                f.write("ok")

        except Exception as e:
            notify_error("TransformWarehouse", e)
            raise
        finally:
            dispose_engine(engine)


# ADD [1]: Task DQ dipisah dari TransformWarehouse
# Keuntungan:
# - Bisa di-retry sendiri tanpa re-run transform
# - Gagal DQ tidak menghapus flag transform (data di warehouse tetap ada)
# - Luigi menampilkan status DQ secara eksplisit di visualisasi dependency
class DataQualityCheck(luigi.Task):
    """
    LAYER 3b — DATA QUALITY CHECK
    Dijalankan setelah TransformWarehouse selesai.
    Setiap SQL harus mengembalikan COUNT = 0 (tidak ada pelanggaran).
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return TransformWarehouse(run_date=self.run_date)

    def output(self):
        return luigi.LocalTarget(ARTIFACT_DIR / f"dq_check_{self.run_date}.done")

    def run(self):
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        engine = get_dwh_engine()
        try:
            for check_name, sql_file in DQ_SQL_FILES.items():
                dq_assert_zero(engine, sql_file, check_name)

            logger.info("All DQ checks passed for run_date=%s", self.run_date)
            with self.output().open("w") as f:
                f.write("ok")

        except Exception as e:
            notify_error("DataQualityCheck", e)
            raise
        finally:
            dispose_engine(engine)


# ADD [2]: Serve / Mart layer
# Membaca dari warehouse, menulis agregasi ke schema mart.
# Dijalankan setelah DQ check lulus — data yang masuk mart sudah terjamin bersih.
class ServeMart(luigi.Task):
    """
    LAYER 4 — SERVE (Warehouse → Mart)
    Buat aggregated table / materialized view untuk konsumsi BI tools.
    Task ini hanya jalan jika DataQualityCheck lulus.
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return DataQualityCheck(run_date=self.run_date)

    def output(self):
        return luigi.LocalTarget(ARTIFACT_DIR / f"serve_mart_{self.run_date}.done")

    def run(self):
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        engine = get_dwh_engine()
        try:
            for sql_file in MART_SQL_FILES:
                logger.info("Running mart SQL: %s", sql_file)
                run_sql_file(engine, sql_file)

            logger.info("Mart layer refreshed for run_date=%s", self.run_date)
            with self.output().open("w") as f:
                f.write("ok")

        except Exception as e:
            notify_error("ServeMart", e)
            raise
        finally:
            dispose_engine(engine)


# FIX [6]: ELTPipeline sekarang menjadi true entry point.
# Sebelumnya: ELTPipeline hanya wrap TransformWarehouse,
# sedangkan NotifySuccess justru depend ON ELTPipeline.
# Artinya kalau user run `ELTPipeline`, NotifySuccess tidak pernah jalan.
# Sekarang: ELTPipeline menjadi terminal task yang memicu seluruh chain,
# termasuk ServeMart dan NotifySuccess.
class NotifySuccess(luigi.Task):
    """
    LAYER 5 — NOTIFY
    Kirim notifikasi sukses setelah seluruh pipeline selesai.
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return ServeMart(run_date=self.run_date)

    def output(self):
        return luigi.LocalTarget(ARTIFACT_DIR / f"success_{self.run_date}.done")

    def run(self):
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        notify_success(f"ELT pipeline completed for run_date={self.run_date}")
        with self.output().open("w") as f:
            f.write("ok")


class ELTPipeline(luigi.WrapperTask):
    """
    Master entry point. Jalankan dengan:
        python pipeline.py ELTPipeline --run-date 2024-01-15
    
    Dependency chain:
        ELTPipeline
            └── NotifySuccess
                    └── ServeMart
                            └── DataQualityCheck
                                    └── TransformWarehouse
                                            └── LoadStaging
                                                    └── ExtractSource
    """
    run_date = luigi.DateParameter()

    def requires(self):
        return NotifySuccess(run_date=self.run_date)


if __name__ == "__main__":
    luigi.run()