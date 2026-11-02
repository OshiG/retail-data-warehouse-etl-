-- ============================================================
-- Retail Data Warehouse - Validation and Evidence
-- File: 03_validation_and_evidence.sql
-- Run AFTER Run 1 and Run 2.
-- MySQL 8.x
-- ============================================================

USE retail_dw;

SET @scd_customer = 17850;

-- ============================================================
-- A. ETL RUN HISTORY - proves at least two pipeline executions
-- ============================================================
SELECT
    Run_ID,
    Run_Label,
    Source_File,
    Source_Type,
    Run_Date,
    Started_At,
    Ended_At,
    Status,
    Rows_Extracted,
    Rows_Staged,
    Rows_Clean,
    Rows_Rejected,
    Dim_Date_Inserted,
    Dim_Product_Inserted,
    Dim_Product_Updated,
    Dim_Customer_Inserted,
    Dim_Customer_SCD2_Changed,
    Fact_Rows_Inserted,
    Fact_Rows_Skipped
FROM etl_run_log
ORDER BY Run_ID;


-- ============================================================
-- B. STAGING / CLEANING / REJECT EVIDENCE
-- ============================================================

-- Current raw staging row count (the latest source state).
SELECT COUNT(*) AS Current_Raw_Staging_Rows
FROM stg_online_retail;

-- Clean rows retained per execution.
SELECT
    Run_ID,
    COUNT(*) AS Clean_Rows
FROM stg_online_retail_clean
GROUP BY Run_ID
ORDER BY Run_ID;

-- Reject rows retained per execution.
SELECT
    Run_ID,
    COUNT(*) AS Rejected_Rows
FROM etl_rejects
GROUP BY Run_ID
ORDER BY Run_ID;

-- Detailed reject reasons - useful screenshot evidence for Run 2.
SELECT
    Run_ID,
    Source_Row_Number,
    InvoiceNo,
    StockCode,
    UnitPrice,
    Reject_Reason
FROM etl_rejects
ORDER BY Run_ID, Reject_ID;


-- ============================================================
-- C. DIMENSION COUNTS AND SURROGATE KEY EVIDENCE
-- ============================================================
SELECT 'dim_date' AS Table_Name, COUNT(*) AS Row_Count FROM dim_date
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Surrogate keys are independent from natural/business keys.
SELECT Customer_SK, CustomerID, Country, Is_Current
FROM dim_customer
ORDER BY CustomerID, Customer_SK
LIMIT 50;

SELECT Product_SK, StockCode, Description, UnitPrice
FROM dim_product
ORDER BY Product_SK
LIMIT 50;


-- ============================================================
-- D. SCD TYPE 2 EVIDENCE - Customer 17850
-- Expected after demo Run 2:
--   old United Kingdom row -> Is_Current = 0
--   new France row         -> Is_Current = 1
--   two different Customer_SK values
-- ============================================================
SELECT
    Customer_SK,
    CustomerID,
    Country,
    Effective_Start_Date,
    Effective_End_Date,
    Is_Current
FROM dim_customer
WHERE CustomerID = @scd_customer
ORDER BY Customer_SK;

-- A valid SCD2 dimension must have no more than one current row
-- for any CustomerID. Expected result: ZERO rows.
SELECT
    CustomerID,
    SUM(CASE WHEN Is_Current = 1 THEN 1 ELSE 0 END) AS Current_Version_Count
FROM dim_customer
GROUP BY CustomerID
HAVING SUM(CASE WHEN Is_Current = 1 THEN 1 ELSE 0 END) > 1;

-- Show all customers with historical versions.
SELECT
    CustomerID,
    COUNT(*) AS Version_Count,
    SUM(CASE WHEN Is_Current = 1 THEN 1 ELSE 0 END) AS Current_Count
FROM dim_customer
GROUP BY CustomerID
HAVING COUNT(*) > 1
ORDER BY Version_Count DESC, CustomerID;


-- ============================================================
-- E. RUN 2 DEMONSTRATION - changed, unchanged, and new customers
-- ============================================================

-- Changed existing customer: 17850 should now have historical + current versions.
SELECT Customer_SK, CustomerID, Country, Is_Current
FROM dim_customer
WHERE CustomerID = 17850
ORDER BY Customer_SK;

-- Unchanged existing customer: 13047 should remain with one current version
-- unless the original data itself already produced another historical version.
SELECT Customer_SK, CustomerID, Country, Is_Current
FROM dim_customer
WHERE CustomerID = 13047
ORDER BY Customer_SK;

-- New Run 2 customer: 19999 should have a new surrogate key.
SELECT Customer_SK, CustomerID, Country, Is_Current
FROM dim_customer
WHERE CustomerID = 19999
ORDER BY Customer_SK;


-- ============================================================
-- F. FACT TABLE FOREIGN-KEY / ORPHAN VALIDATION
-- Each query below should return 0.
-- ============================================================
SELECT COUNT(*) AS Orphan_Date_FKs
FROM fact_sales f
LEFT JOIN dim_date d ON d.Date_SK = f.Date_SK
WHERE d.Date_SK IS NULL;

SELECT COUNT(*) AS Orphan_Customer_FKs
FROM fact_sales f
LEFT JOIN dim_customer c ON c.Customer_SK = f.Customer_SK
WHERE c.Customer_SK IS NULL;

SELECT COUNT(*) AS Orphan_Product_FKs
FROM fact_sales f
LEFT JOIN dim_product p ON p.Product_SK = f.Product_SK
WHERE p.Product_SK IS NULL;

-- No NULL dimension foreign keys are expected.
SELECT COUNT(*) AS Null_Foreign_Keys
FROM fact_sales
WHERE Date_SK IS NULL
   OR Customer_SK IS NULL
   OR Product_SK IS NULL;


-- ============================================================
-- G. CALCULATED MEASURE VALIDATION
-- Both queries should return 0.
-- ============================================================
SELECT COUNT(*) AS Invalid_Total_Amount_Rows
FROM fact_sales
WHERE ABS(Total_Amount - ROUND(Quantity * UnitPrice, 2)) > 0.01;

SELECT COUNT(*) AS Invalid_Net_Amount_Rows
FROM fact_sales
WHERE ABS(Net_Amount - ROUND(Total_Amount - Discount_Amount, 2)) > 0.01;


-- ============================================================
-- H. FACT REGISTRY / IDEMPOTENCY VALIDATION
-- The registry prevents duplicate fact inserts when the same source file
-- is executed again with the same source rows.
-- ============================================================
SELECT
    First_Run_ID,
    COUNT(*) AS Registered_Fact_Source_Rows
FROM etl_fact_registry
GROUP BY First_Run_ID
ORDER BY First_Run_ID;

SELECT
    r.Run_ID,
    r.Run_Label,
    r.Fact_Rows_Inserted,
    r.Fact_Rows_Skipped
FROM etl_run_log r
ORDER BY r.Run_ID;


-- ============================================================
-- I. RUN 2 FACT EVIDENCE
-- R200001 should use the NEW France Customer_SK for Customer 17850.
-- R200002 represents an unchanged customer.
-- R200003 represents a new customer.
-- C200004 and R200005 should NOT appear in Fact_Sales because they are rejects.
-- ============================================================
SELECT
    f.Sales_SK,
    f.InvoiceNo,
    d.FullDate,
    c.Customer_SK,
    c.CustomerID,
    c.Country,
    c.Is_Current,
    p.Product_SK,
    p.StockCode,
    f.Quantity,
    f.UnitPrice,
    f.Total_Amount,
    f.Net_Amount
FROM fact_sales f
JOIN dim_date d ON d.Date_SK = f.Date_SK
JOIN dim_customer c ON c.Customer_SK = f.Customer_SK
JOIN dim_product p ON p.Product_SK = f.Product_SK
WHERE f.InvoiceNo IN ('R200001', 'R200002', 'R200003')
ORDER BY f.InvoiceNo;

-- Expected result: zero fact rows for rejected Run 2 invoices.
SELECT COUNT(*) AS Rejected_Demo_Invoices_In_Fact
FROM fact_sales
WHERE InvoiceNo IN ('C200004', 'R200005');


-- ============================================================
-- J. HISTORY PRESERVATION EVIDENCE
-- Old Run 1 facts for Customer 17850 should remain linked to the old
-- surrogate key even after Run 2 creates the new France version.
-- ============================================================
SELECT
    c.Customer_SK,
    c.CustomerID,
    c.Country,
    c.Is_Current,
    COUNT(*) AS Fact_Row_Count,
    ROUND(SUM(f.Net_Amount), 2) AS Net_Revenue
FROM fact_sales f
JOIN dim_customer c ON c.Customer_SK = f.Customer_SK
WHERE c.CustomerID = @scd_customer
GROUP BY c.Customer_SK, c.CustomerID, c.Country, c.Is_Current
ORDER BY c.Customer_SK;


-- End of ETL validation script.
