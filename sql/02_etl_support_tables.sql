-- ============================================================
-- Retail Data Warehouse - ETL Support Tables
-- File: 02_etl_support_tables.sql
-- Purpose: Create ONLY the additional operational/support tables
--          required by retail_etl.py.
--
-- IMPORTANT:
--   1) Run 01_demo_core_schema.sql FIRST for the public demo.
--   2) This script creates only ETL-owned support tables.
--   3) Target DB: MySQL 8.x, database: retail_dw
-- ============================================================

USE retail_dw;

-- ------------------------------------------------------------
-- 1. ETL run audit log
--    One row per pipeline execution (Run 1, Run 2, reruns, etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_log (
    Run_ID BIGINT NOT NULL AUTO_INCREMENT,
    Run_Label VARCHAR(50) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Source_Type VARCHAR(20) NOT NULL,
    Run_Date DATE NOT NULL,
    Started_At DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    Ended_At DATETIME NULL,
    Status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',

    Rows_Extracted INT NOT NULL DEFAULT 0,
    Rows_Staged INT NOT NULL DEFAULT 0,
    Rows_Clean INT NOT NULL DEFAULT 0,
    Rows_Rejected INT NOT NULL DEFAULT 0,

    Dim_Date_Inserted INT NOT NULL DEFAULT 0,
    Dim_Product_Inserted INT NOT NULL DEFAULT 0,
    Dim_Product_Updated INT NOT NULL DEFAULT 0,
    Dim_Customer_Inserted INT NOT NULL DEFAULT 0,
    Dim_Customer_SCD2_Changed INT NOT NULL DEFAULT 0,

    Fact_Rows_Inserted INT NOT NULL DEFAULT 0,
    Fact_Rows_Skipped INT NOT NULL DEFAULT 0,
    Notes TEXT NULL,

    PRIMARY KEY (Run_ID),
    KEY idx_etl_run_label (Run_Label),
    KEY idx_etl_run_date (Run_Date),
    KEY idx_etl_run_status (Status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ------------------------------------------------------------
-- 2. Clean / transformed staging table
--    Keeps cleaned rows by Run_ID for validation and evidence.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_online_retail_clean (
    Clean_Row_ID BIGINT NOT NULL AUTO_INCREMENT,
    Run_ID BIGINT NOT NULL,
    Source_Row_Number INT NOT NULL,
    Fact_Business_Key CHAR(64) NOT NULL,

    InvoiceNo VARCHAR(20) NOT NULL,
    StockCode VARCHAR(20) NOT NULL,
    Description VARCHAR(255) NOT NULL,
    Quantity INT NOT NULL,
    InvoiceDate DATETIME NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    CustomerID INT NOT NULL,
    Country VARCHAR(100) NOT NULL,

    Total_Amount DECIMAL(12,2) NOT NULL,
    Discount_Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Net_Amount DECIMAL(12,2) NOT NULL,
    Cleaned_At DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (Clean_Row_ID),
    KEY idx_clean_run (Run_ID),
    KEY idx_clean_invoice (InvoiceNo),
    KEY idx_clean_customer (CustomerID),
    KEY idx_clean_stock (StockCode),
    KEY idx_clean_fact_key (Fact_Business_Key),

    CONSTRAINT fk_clean_run
        FOREIGN KEY (Run_ID) REFERENCES etl_run_log (Run_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ------------------------------------------------------------
-- 3. Rejected / exception records
--    Stores rows excluded during cleaning, together with reasons.
--    Source fields are stored mainly as text so even malformed data
--    can still be captured as evidence.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_rejects (
    Reject_ID BIGINT NOT NULL AUTO_INCREMENT,
    Run_ID BIGINT NOT NULL,
    Source_Row_Number INT NULL,

    InvoiceNo VARCHAR(100) NULL,
    StockCode VARCHAR(100) NULL,
    Description VARCHAR(500) NULL,
    Quantity VARCHAR(100) NULL,
    InvoiceDate VARCHAR(100) NULL,
    UnitPrice VARCHAR(100) NULL,
    CustomerID VARCHAR(100) NULL,
    Country VARCHAR(200) NULL,

    Reject_Reason VARCHAR(500) NOT NULL,
    Rejected_At DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (Reject_ID),
    KEY idx_reject_run (Run_ID),
    KEY idx_reject_reason (Reject_Reason(100)),

    CONSTRAINT fk_reject_run
        FOREIGN KEY (Run_ID) REFERENCES etl_run_log (Run_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ------------------------------------------------------------
-- 4. Fact registry
--    Prevents the same source line from being inserted into
--    Fact_Sales more than once when the same file is rerun.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_fact_registry (
    Fact_Business_Key CHAR(64) NOT NULL,
    First_Run_ID BIGINT NOT NULL,
    Source_Row_Number INT NOT NULL,
    InvoiceNo VARCHAR(20) NOT NULL,
    StockCode VARCHAR(20) NOT NULL,
    Registered_At DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (Fact_Business_Key),
    KEY idx_registry_run (First_Run_ID),
    KEY idx_registry_invoice (InvoiceNo),

    CONSTRAINT fk_registry_run
        FOREIGN KEY (First_Run_ID) REFERENCES etl_run_log (Run_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- End of support tables script.
