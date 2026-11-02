-- Portfolio demonstration only: independently created compatible schema.
-- This is NOT the original group project's Member 1 SQL file.
-- For a fresh local MySQL 8+ database. No DROP statements are used.
CREATE DATABASE IF NOT EXISTS retail_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE retail_dw;

CREATE TABLE IF NOT EXISTS stg_online_retail (
    InvoiceNo VARCHAR(20) NULL,
    StockCode VARCHAR(20) NULL,
    Description VARCHAR(255) NULL,
    Quantity INT NULL,
    InvoiceDate DATETIME NULL,
    UnitPrice DECIMAL(10,2) NULL,
    CustomerID FLOAT NULL,
    Country VARCHAR(100) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_date (
    Date_SK INT NOT NULL PRIMARY KEY,
    FullDate DATE NULL,
    `Day` INT NULL,
    `Month` INT NULL,
    Quarter INT NULL,
    `Year` INT NULL,
    DayOfWeek VARCHAR(15) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_product (
    Product_SK INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    StockCode VARCHAR(20) NULL,
    Description VARCHAR(255) NULL,
    UnitPrice DECIMAL(10,2) NULL,
    UNIQUE KEY uq_dim_product_stock (StockCode)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_customer (
    Customer_SK INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT NULL,
    Country VARCHAR(100) NULL,
    Effective_Start_Date DATE NULL,
    Effective_End_Date DATE NULL,
    Is_Current TINYINT DEFAULT 1,
    KEY idx_customer_natural (CustomerID)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fact_sales (
    Sales_SK INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Date_SK INT NULL,
    Customer_SK INT NULL,
    Product_SK INT NULL,
    InvoiceNo VARCHAR(20) NULL,
    Quantity INT NULL,
    UnitPrice DECIMAL(10,2) NULL,
    Total_Amount DECIMAL(12,2) NULL,
    Discount_Amount DECIMAL(12,2) NULL DEFAULT 0.00,
    Net_Amount DECIMAL(12,2) NULL,
    KEY idx_fact_date (Date_SK),
    KEY idx_fact_customer (Customer_SK),
    KEY idx_fact_product (Product_SK),
    CONSTRAINT fk_fact_date FOREIGN KEY (Date_SK) REFERENCES dim_date(Date_SK),
    CONSTRAINT fk_fact_customer FOREIGN KEY (Customer_SK) REFERENCES dim_customer(Customer_SK),
    CONSTRAINT fk_fact_product FOREIGN KEY (Product_SK) REFERENCES dim_product(Product_SK)
) ENGINE=InnoDB;
