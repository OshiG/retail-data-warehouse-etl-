"""
Retail Data Warehouse ETL Pipeline
==================================

Pipeline flow required by the module guidelines:
    Extract -> Raw Staging -> Validate/Clean/Transform -> Dimensions
    -> Surrogate Keys -> SCD Type 2 -> Fact_Sales -> Audit/Evidence

The original academic group project used a team-owned core warehouse schema.
This repository includes a separately written demo-compatible core schema
for portfolio reproduction; it is not the other team member's original file.

Required before running:
    1. Execute 01_demo_core_schema.sql
    2. Execute 02_etl_support_tables.sql

Dependencies:
    pip install pandas openpyxl mysql-connector-python

Example Run 1:
    python retail_etl.py --source "Online Retail.xlsx" --run-label RUN1 \
        --run-date 2026-09-18 --db-user retail_user

Example Run 2:
    python retail_etl.py --source "demo_run2.csv" --run-label RUN2 \
        --run-date 2026-09-19 --db-user retail_user
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd
try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:  # Allows --help and non-DB checks before dependency installation.
    mysql = None

    class Error(Exception):
        pass


REQUIRED_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]

CORE_TABLES = {
    "stg_online_retail",
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
}

SUPPORT_TABLES = {
    "etl_run_log",
    "stg_online_retail_clean",
    "etl_rejects",
    "etl_fact_registry",
}

MONEY_2DP = Decimal("0.01")
BATCH_SIZE = 5000


def money(value) -> Decimal:
    """Convert a value to DECIMAL(.,2) using conventional half-up rounding."""
    return Decimal(str(value)).quantize(MONEY_2DP, rounding=ROUND_HALF_UP)


def none_if_na(value):
    if pd.isna(value):
        return None
    return value


def text_or_none(value):
    if pd.isna(value):
        return None
    return str(value).strip()


def safe_source_text(value):
    """Text representation used only for reject evidence."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def chunked(items: Sequence[Tuple], size: int = BATCH_SIZE) -> Iterable[Sequence[Tuple]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def extract_source(source_path: Path) -> Tuple[pd.DataFrame, str]:
    suffix = source_path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        df = pd.read_excel(
            source_path,
            sheet_name=0,
            dtype={
                "InvoiceNo": "string",
                "StockCode": "string",
                "Description": "string",
                "Country": "string",
            },
            engine="openpyxl",
        )
        source_type = "EXCEL"
    elif suffix == ".csv":
        df = pd.read_csv(
            source_path,
            dtype={
                "InvoiceNo": "string",
                "StockCode": "string",
                "Description": "string",
                "Country": "string",
            },
        )
        source_type = "CSV"
    else:
        raise ValueError("Supported source formats are .xlsx, .xlsm, and .csv")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Source file is missing required columns: {missing}")

    df = df[REQUIRED_COLUMNS].copy()
    # Excel/CSV row number for evidence. Row 1 is the header, so data starts at 2.
    df.insert(0, "Source_Row_Number", range(2, len(df) + 2))
    return df, source_type


def make_connection(args):
    if "mysql.connector" not in sys.modules:
        raise RuntimeError(
            "mysql-connector-python is not installed. Run: "
            "pip install mysql-connector-python"
        )
    return mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=os.environ.get("RETAIL_DW_PASSWORD", ""),
        database=args.db_name,
        autocommit=False,
    )


def assert_required_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    available = {row[0] for row in cursor.fetchall()}
    cursor.close()

    missing_core = sorted(CORE_TABLES - available)
    missing_support = sorted(SUPPORT_TABLES - available)
    if missing_core or missing_support:
        lines = []
        if missing_core:
            lines.append(
                "Missing core warehouse tables: " + ", ".join(missing_core)
                + ". Run 01_demo_core_schema.sql first."
            )
        if missing_support:
            lines.append(
                "Missing ETL support tables: " + ", ".join(missing_support)
                + ". Run 02_etl_support_tables.sql second."
            )
        raise RuntimeError("\n".join(lines))


def create_run_log(conn, run_label: str, source_file: str, source_type: str, run_date: date) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO etl_run_log
            (Run_Label, Source_File, Source_Type, Run_Date, Status)
        VALUES (%s, %s, %s, %s, 'RUNNING')
        """,
        (run_label, source_file, source_type, run_date),
    )
    run_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    return int(run_id)


def update_run_log(conn, run_id: int, **values):
    if not values:
        return
    allowed = {
        "Rows_Extracted", "Rows_Staged", "Rows_Clean", "Rows_Rejected",
        "Dim_Date_Inserted", "Dim_Product_Inserted", "Dim_Product_Updated",
        "Dim_Customer_Inserted", "Dim_Customer_SCD2_Changed",
        "Fact_Rows_Inserted", "Fact_Rows_Skipped", "Status", "Ended_At", "Notes",
    }
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"Unsupported etl_run_log fields: {sorted(unknown)}")

    assignments = ", ".join(f"{k} = %s" for k in values)
    params = list(values.values()) + [run_id]
    cursor = conn.cursor()
    cursor.execute(f"UPDATE etl_run_log SET {assignments} WHERE Run_ID = %s", params)
    conn.commit()
    cursor.close()


def load_raw_staging(conn, df: pd.DataFrame) -> int:
    """Replace raw staging with the current source data."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE stg_online_retail")

    sql = """
        INSERT INTO stg_online_retail
            (InvoiceNo, StockCode, Description, Quantity, InvoiceDate,
             UnitPrice, CustomerID, Country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Vectorized parsing is much faster than converting 500k+ values one-by-one.
    raw = df.copy()
    raw["Quantity"] = pd.to_numeric(raw["Quantity"], errors="coerce")
    raw["InvoiceDate"] = pd.to_datetime(raw["InvoiceDate"], errors="coerce")
    raw["UnitPrice"] = pd.to_numeric(raw["UnitPrice"], errors="coerce")
    raw["CustomerID"] = pd.to_numeric(raw["CustomerID"], errors="coerce")

    rows: List[Tuple] = []
    for r in raw.itertuples(index=False):
        rows.append((
            text_or_none(r.InvoiceNo),
            text_or_none(r.StockCode),
            text_or_none(r.Description),
            None if pd.isna(r.Quantity) else int(r.Quantity),
            None if pd.isna(r.InvoiceDate) else pd.Timestamp(r.InvoiceDate).to_pydatetime(),
            None if pd.isna(r.UnitPrice) else float(r.UnitPrice),
            None if pd.isna(r.CustomerID) else float(r.CustomerID),
            text_or_none(r.Country),
        ))

    inserted = 0
    for batch in chunked(rows):
        cursor.executemany(sql, batch)
        inserted += len(batch)
        conn.commit()

    cursor.close()
    return inserted


def clean_transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()

    # Normalize string fields without deleting source rows.
    for col in ["InvoiceNo", "StockCode", "Description", "Country"]:
        work[col] = work[col].astype("string").str.strip()

    # Parse typed fields.
    work["Quantity_Num"] = pd.to_numeric(work["Quantity"], errors="coerce")
    work["UnitPrice_Num"] = pd.to_numeric(work["UnitPrice"], errors="coerce")
    work["CustomerID_Num"] = pd.to_numeric(work["CustomerID"], errors="coerce")
    work["InvoiceDate_Parsed"] = pd.to_datetime(work["InvoiceDate"], errors="coerce")

    reject_reasons: List[List[str]] = []
    for r in work.itertuples(index=False):
        reasons: List[str] = []
        invoice_no = None if pd.isna(r.InvoiceNo) else str(r.InvoiceNo).strip()
        stock_code = None if pd.isna(r.StockCode) else str(r.StockCode).strip()

        if not invoice_no:
            reasons.append("Missing InvoiceNo")
        elif invoice_no.upper().startswith("C"):
            reasons.append("Cancelled invoice (InvoiceNo starts with C)")

        if not stock_code:
            reasons.append("Missing StockCode")

        if pd.isna(r.Quantity_Num):
            reasons.append("Invalid Quantity")

        if pd.isna(r.InvoiceDate_Parsed):
            reasons.append("Invalid InvoiceDate")

        if pd.isna(r.UnitPrice_Num):
            reasons.append("Invalid UnitPrice")
        elif float(r.UnitPrice_Num) <= 0:
            reasons.append("Non-positive UnitPrice")

        # CustomerID is nullable by design and maps to Guest Checkout (-1).
        if not pd.isna(r.CustomerID_Num):
            customer_float = float(r.CustomerID_Num)
            if not customer_float.is_integer():
                reasons.append("CustomerID is not a whole number")

        reject_reasons.append(reasons)

    reject_mask = pd.Series([bool(x) for x in reject_reasons], index=work.index)

    rejects = work.loc[reject_mask, ["Source_Row_Number"] + REQUIRED_COLUMNS].copy()
    rejects["Reject_Reason"] = [
        "; ".join(reject_reasons[i])
        for i in range(len(work)) if reject_mask.iloc[i]
    ]

    clean = work.loc[~reject_mask].copy()

    clean["InvoiceNo"] = clean["InvoiceNo"].astype(str).str.strip()
    clean["StockCode"] = clean["StockCode"].astype(str).str.strip()
    clean["Description"] = clean["Description"].fillna("Unknown Product")
    clean.loc[clean["Description"].astype(str).str.strip().eq(""), "Description"] = "Unknown Product"
    clean["Description"] = clean["Description"].astype(str).str.strip().str.slice(0, 255)

    clean["Quantity"] = clean["Quantity_Num"].astype(int)
    clean["InvoiceDate"] = clean["InvoiceDate_Parsed"]
    clean["UnitPrice"] = clean["UnitPrice_Num"].map(money)

    clean["CustomerID"] = clean["CustomerID_Num"].fillna(-1).astype(int)
    clean["Country"] = clean["Country"].fillna("Unknown")
    clean.loc[clean["Country"].astype(str).str.strip().eq(""), "Country"] = "Unknown"
    clean["Country"] = clean["Country"].astype(str).str.strip().str.slice(0, 100)
    clean.loc[clean["CustomerID"].eq(-1), "Country"] = "Unknown"

    clean["Total_Amount"] = [
        (Decimal(int(q)) * p).quantize(MONEY_2DP, rounding=ROUND_HALF_UP)
        for q, p in zip(clean["Quantity"], clean["UnitPrice"])
    ]
    clean["Discount_Amount"] = Decimal("0.00")
    clean["Net_Amount"] = clean["Total_Amount"]

    # A stable source-line key supports idempotent reruns while preserving
    # legitimate duplicate rows that occur at different source row numbers.
    keys = []
    for r in clean.itertuples(index=False):
        canonical = "|".join([
            str(r.Source_Row_Number),
            str(r.InvoiceNo),
            str(r.StockCode),
            pd.Timestamp(r.InvoiceDate).strftime("%Y-%m-%d %H:%M:%S"),
            str(int(r.CustomerID)),
            str(int(r.Quantity)),
            f"{r.UnitPrice:.2f}",
        ])
        keys.append(hashlib.sha256(canonical.encode("utf-8")).hexdigest())
    clean["Fact_Business_Key"] = keys

    keep = [
        "Source_Row_Number", "Fact_Business_Key", "InvoiceNo", "StockCode",
        "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID",
        "Country", "Total_Amount", "Discount_Amount", "Net_Amount",
    ]
    clean = clean[keep]
    return clean, rejects


def load_rejects(conn, run_id: int, rejects: pd.DataFrame) -> int:
    if rejects.empty:
        return 0

    sql = """
        INSERT INTO etl_rejects
            (Run_ID, Source_Row_Number, InvoiceNo, StockCode, Description,
             Quantity, InvoiceDate, UnitPrice, CustomerID, Country, Reject_Reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = []
    for r in rejects.itertuples(index=False):
        rows.append((
            run_id,
            int(r.Source_Row_Number),
            safe_source_text(r.InvoiceNo),
            safe_source_text(r.StockCode),
            safe_source_text(r.Description),
            safe_source_text(r.Quantity),
            safe_source_text(r.InvoiceDate),
            safe_source_text(r.UnitPrice),
            safe_source_text(r.CustomerID),
            safe_source_text(r.Country),
            str(r.Reject_Reason)[:500],
        ))

    cursor = conn.cursor()
    inserted = 0
    for batch in chunked(rows):
        cursor.executemany(sql, batch)
        inserted += len(batch)
        conn.commit()
    cursor.close()
    return inserted


def load_clean_staging(conn, run_id: int, clean: pd.DataFrame) -> int:
    if clean.empty:
        return 0

    sql = """
        INSERT INTO stg_online_retail_clean
            (Run_ID, Source_Row_Number, Fact_Business_Key, InvoiceNo, StockCode,
             Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country,
             Total_Amount, Discount_Amount, Net_Amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = []
    for r in clean.itertuples(index=False):
        rows.append((
            run_id,
            int(r.Source_Row_Number),
            r.Fact_Business_Key,
            r.InvoiceNo,
            r.StockCode,
            r.Description,
            int(r.Quantity),
            pd.Timestamp(r.InvoiceDate).to_pydatetime(),
            r.UnitPrice,
            int(r.CustomerID),
            r.Country,
            r.Total_Amount,
            r.Discount_Amount,
            r.Net_Amount,
        ))

    cursor = conn.cursor()
    inserted = 0
    for batch in chunked(rows):
        cursor.executemany(sql, batch)
        inserted += len(batch)
        conn.commit()
    cursor.close()
    return inserted


def ensure_guest_customer(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT Customer_SK FROM dim_customer WHERE Customer_SK = -1 OR CustomerID = -1 LIMIT 1"
    )
    if cursor.fetchone() is None:
        cursor.execute(
            """
            INSERT INTO dim_customer
                (Customer_SK, CustomerID, Country, Effective_Start_Date,
                 Effective_End_Date, Is_Current)
            VALUES (-1, -1, 'Unknown', '1900-01-01', '9999-12-31', 1)
            """
        )
        conn.commit()
    cursor.close()


def load_dim_date(conn, clean: pd.DataFrame) -> int:
    unique_dates = sorted({pd.Timestamp(x).date() for x in clean["InvoiceDate"]})
    if not unique_dates:
        return 0

    rows = []
    for d in unique_dates:
        date_sk = int(d.strftime("%Y%m%d"))
        quarter = ((d.month - 1) // 3) + 1
        rows.append((date_sk, d, d.day, d.month, quarter, d.year, d.strftime("%A")))

    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT IGNORE INTO dim_date
            (Date_SK, FullDate, Day, Month, Quarter, Year, DayOfWeek)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    return inserted


def latest_rows(clean: pd.DataFrame, natural_key: str) -> pd.DataFrame:
    return (
        clean.sort_values(["InvoiceDate", "Source_Row_Number"])
        .drop_duplicates(subset=[natural_key], keep="last")
        .copy()
    )


def load_dim_product(conn, clean: pd.DataFrame) -> Tuple[int, int]:
    products = latest_rows(clean, "StockCode")

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT Product_SK, StockCode, Description, UnitPrice FROM dim_product")
    existing: Dict[str, dict] = {str(r["StockCode"]): r for r in cursor.fetchall()}

    inserts = []
    updates = []
    for r in products.itertuples(index=False):
        stock = str(r.StockCode)
        desc = str(r.Description)
        price = money(r.UnitPrice)
        old = existing.get(stock)
        if old is None:
            inserts.append((stock, desc, price))
        else:
            old_price = money(old["UnitPrice"])
            old_desc = old["Description"] or ""
            if old_desc != desc or old_price != price:
                updates.append((desc, price, old["Product_SK"]))

    if inserts:
        cursor.executemany(
            "INSERT INTO dim_product (StockCode, Description, UnitPrice) VALUES (%s, %s, %s)",
            inserts,
        )
    inserted = cursor.rowcount if inserts else 0

    updated = 0
    if updates:
        cursor.executemany(
            "UPDATE dim_product SET Description = %s, UnitPrice = %s WHERE Product_SK = %s",
            updates,
        )
        updated = cursor.rowcount

    conn.commit()
    cursor.close()
    return inserted, updated


def validate_customer_current_uniqueness(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT CustomerID, COUNT(*) AS Current_Count
        FROM dim_customer
        WHERE Is_Current = 1
        GROUP BY CustomerID
        HAVING COUNT(*) > 1
        LIMIT 10
        """
    )
    bad = cursor.fetchall()
    cursor.close()
    if bad:
        raise RuntimeError(
            "dim_customer contains more than one current SCD2 row for a CustomerID. "
            f"Fix these records before running ETL: {bad}"
        )


def load_dim_customer_scd2(conn, clean: pd.DataFrame, run_date: date) -> Tuple[int, int]:
    ensure_guest_customer(conn)
    validate_customer_current_uniqueness(conn)

    customers = latest_rows(clean[clean["CustomerID"] != -1], "CustomerID")

    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT Customer_SK, CustomerID, Country, Effective_Start_Date
        FROM dim_customer
        WHERE Is_Current = 1 AND CustomerID <> -1
        """
    )
    existing = {int(r["CustomerID"]): r for r in cursor.fetchall()}

    new_rows = []
    changed_rows = []

    for r in customers.itertuples(index=False):
        customer_id = int(r.CustomerID)
        country = str(r.Country)
        old = existing.get(customer_id)
        if old is None:
            new_rows.append((customer_id, country, run_date, date(9999, 12, 31), 1))
        elif (old["Country"] or "") != country:
            changed_rows.append((old["Customer_SK"], customer_id, country))

    # SCD2: close old row, then insert a new version with a new surrogate key.
    for old_sk, customer_id, new_country in changed_rows:
        cursor.execute(
            """
            UPDATE dim_customer
            SET Effective_End_Date = %s, Is_Current = 0
            WHERE Customer_SK = %s AND Is_Current = 1
            """,
            (run_date, old_sk),
        )
        cursor.execute(
            """
            INSERT INTO dim_customer
                (CustomerID, Country, Effective_Start_Date, Effective_End_Date, Is_Current)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (customer_id, new_country, run_date, date(9999, 12, 31)),
        )

    if new_rows:
        cursor.executemany(
            """
            INSERT INTO dim_customer
                (CustomerID, Country, Effective_Start_Date, Effective_End_Date, Is_Current)
            VALUES (%s, %s, %s, %s, %s)
            """,
            new_rows,
        )

    conn.commit()
    cursor.close()
    validate_customer_current_uniqueness(conn)
    return len(new_rows), len(changed_rows)


def load_fact_sales(conn, run_id: int) -> Tuple[int, int]:
    """
    Load only source lines not seen in a previous ETL execution.

    Surrogate-key resolution happens in SQL AFTER dimensions have been loaded:
      clean.CustomerID -> current dim_customer.Customer_SK
      clean.StockCode  -> dim_product.Product_SK
      clean.InvoiceDate date -> dim_date.Date_SK
    """
    cursor = conn.cursor()

    # Registry insert is in the same transaction as Fact_Sales. If FK/dimension
    # resolution is incomplete, the transaction is rolled back so no source line
    # is incorrectly marked as loaded.
    cursor.execute(
        """
        INSERT IGNORE INTO etl_fact_registry
            (Fact_Business_Key, First_Run_ID, Source_Row_Number, InvoiceNo, StockCode)
        SELECT
            Fact_Business_Key, Run_ID, Source_Row_Number, InvoiceNo, StockCode
        FROM stg_online_retail_clean
        WHERE Run_ID = %s
        """,
        (run_id,),
    )
    newly_registered = cursor.rowcount

    cursor.execute(
        """
        INSERT INTO fact_sales
            (Date_SK, Customer_SK, Product_SK, InvoiceNo, Quantity, UnitPrice,
             Total_Amount, Discount_Amount, Net_Amount)
        SELECT
            d.Date_SK,
            cst.Customer_SK,
            p.Product_SK,
            c.InvoiceNo,
            c.Quantity,
            c.UnitPrice,
            c.Total_Amount,
            c.Discount_Amount,
            c.Net_Amount
        FROM stg_online_retail_clean c
        INNER JOIN etl_fact_registry r
            ON r.Fact_Business_Key = c.Fact_Business_Key
           AND r.First_Run_ID = %s
        INNER JOIN dim_date d
            ON d.FullDate = DATE(c.InvoiceDate)
        INNER JOIN dim_product p
            ON p.StockCode = c.StockCode
        INNER JOIN dim_customer cst
            ON cst.CustomerID = c.CustomerID
           AND cst.Is_Current = 1
        WHERE c.Run_ID = %s
        """,
        (run_id, run_id),
    )
    fact_inserted = cursor.rowcount

    if fact_inserted != newly_registered:
        conn.rollback()
        raise RuntimeError(
            "Fact load validation failed: newly registered clean rows = "
            f"{newly_registered}, but inserted Fact_Sales rows = {fact_inserted}. "
            "This normally indicates a missing dimension surrogate-key lookup."
        )

    cursor.execute(
        "SELECT COUNT(*) FROM stg_online_retail_clean WHERE Run_ID = %s",
        (run_id,),
    )
    clean_count = int(cursor.fetchone()[0])
    skipped = clean_count - newly_registered

    conn.commit()
    cursor.close()
    return fact_inserted, skipped


def run_pipeline(args) -> int:
    source_path = Path(args.source).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else date.today()

    print("=" * 72)
    print("RETAIL DATA WAREHOUSE ETL")
    print("=" * 72)
    print(f"Source    : {source_path.name}")
    print(f"Run label : {args.run_label}")
    print(f"Run date  : {run_date}")

    print("\n[1/8] Extracting source data...")
    source_df, source_type = extract_source(source_path)
    print(f"      Extracted rows: {len(source_df):,}")

    conn = make_connection(args)
    run_id = None
    try:
        assert_required_tables(conn)
        run_id = create_run_log(
            conn,
            args.run_label,
            source_path.name,
            source_type,
            run_date,
        )
        update_run_log(conn, run_id, Rows_Extracted=len(source_df))
        print(f"      ETL Run_ID: {run_id}")

        print("\n[2/8] Loading raw staging table stg_online_retail...")
        staged = load_raw_staging(conn, source_df)
        update_run_log(conn, run_id, Rows_Staged=staged)
        print(f"      Raw staged rows: {staged:,}")

        print("\n[3/8] Cleaning, validating, and transforming...")
        clean_df, reject_df = clean_transform(source_df)
        rejected = load_rejects(conn, run_id, reject_df)
        clean_loaded = load_clean_staging(conn, run_id, clean_df)
        update_run_log(
            conn,
            run_id,
            Rows_Clean=clean_loaded,
            Rows_Rejected=rejected,
        )
        print(f"      Clean rows   : {clean_loaded:,}")
        print(f"      Rejected rows: {rejected:,}")

        print("\n[4/8] Loading Dim_Date...")
        dim_date_inserted = load_dim_date(conn, clean_df)
        update_run_log(conn, run_id, Dim_Date_Inserted=dim_date_inserted)
        print(f"      New dates: {dim_date_inserted:,}")

        print("\n[5/8] Loading Dim_Product (SCD Type 1)...")
        product_inserted, product_updated = load_dim_product(conn, clean_df)
        update_run_log(
            conn,
            run_id,
            Dim_Product_Inserted=product_inserted,
            Dim_Product_Updated=product_updated,
        )
        print(f"      New products    : {product_inserted:,}")
        print(f"      Updated products: {product_updated:,}")

        print("\n[6/8] Loading Dim_Customer (SCD Type 2)...")
        customer_inserted, customer_changed = load_dim_customer_scd2(conn, clean_df, run_date)
        update_run_log(
            conn,
            run_id,
            Dim_Customer_Inserted=customer_inserted,
            Dim_Customer_SCD2_Changed=customer_changed,
        )
        print(f"      New customers     : {customer_inserted:,}")
        print(f"      SCD2 changes      : {customer_changed:,}")

        print("\n[7/8] Resolving surrogate keys and loading Fact_Sales...")
        fact_inserted, fact_skipped = load_fact_sales(conn, run_id)
        update_run_log(
            conn,
            run_id,
            Fact_Rows_Inserted=fact_inserted,
            Fact_Rows_Skipped=fact_skipped,
        )
        print(f"      Fact rows inserted: {fact_inserted:,}")
        print(f"      Duplicate rerun rows skipped: {fact_skipped:,}")

        print("\n[8/8] Finalizing ETL audit log...")
        update_run_log(
            conn,
            run_id,
            Status="SUCCESS",
            Ended_At=datetime.now(),
            Notes="ETL completed successfully.",
        )

        print("\n" + "=" * 72)
        print("ETL COMPLETED SUCCESSFULLY")
        print(f"Run_ID             : {run_id}")
        print(f"Extracted           : {len(source_df):,}")
        print(f"Clean               : {clean_loaded:,}")
        print(f"Rejected            : {rejected:,}")
        print(f"Customer SCD2 change: {customer_changed:,}")
        print(f"Facts inserted      : {fact_inserted:,}")
        print("=" * 72)
        return run_id

    except Exception as exc:
        if conn.is_connected():
            try:
                conn.rollback()
            except Exception:
                pass
            if run_id is not None:
                try:
                    update_run_log(
                        conn,
                        run_id,
                        Status="FAILED",
                        Ended_At=datetime.now(),
                        Notes=str(exc)[:65000],
                    )
                except Exception:
                    pass
        raise
    finally:
        if conn.is_connected():
            conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Retail Data Warehouse ETL: staging, cleaning, dimensions, SCD2 and facts."
    )
    parser.add_argument("--source", required=True, help="Path to Online Retail.xlsx or demo_run2.csv")
    parser.add_argument("--run-label", required=True, help="Example: RUN1 or RUN2")
    parser.add_argument(
        "--run-date",
        default=None,
        help="SCD effective date in YYYY-MM-DD format. Default: today",
    )

    parser.add_argument("--db-host", default=os.getenv("RETAIL_DW_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("RETAIL_DW_PORT", "3306")))
    parser.add_argument("--db-user", default=os.getenv("RETAIL_DW_USER", "retail_user"))
    # Database password is read exclusively from RETAIL_DW_PASSWORD environment variable.
    parser.add_argument("--db-name", default=os.getenv("RETAIL_DW_DATABASE", "retail_dw"))
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run_pipeline(parse_args())
    except (ValueError, FileNotFoundError, RuntimeError, Error) as exc:
        print(f"\nETL FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nETL cancelled by user.", file=sys.stderr)
        sys.exit(130)
