# Retail Data Warehouse — Individual ETL Contribution

**Project type:** University group project (3 members)  
**My role:** Member 2 — ETL & Data Engineering  
**Focus:** Python, Pandas, MySQL, data quality checks, surrogate-key mapping and SCD Type 2.

## What this repository demonstrates

My ETL and data-engineering work from a group retail data-warehouse assignment. The pipeline reads CSV/Excel records, loads raw staging, validates and separates rejected rows, transforms sales amounts, maintains date/product/customer dimensions, applies SCD Type 2 tracking for customer-country changes, and loads the sales fact table with run auditing and duplicate prevention.

**This is not a claim to have built the entire group project.** Other team members' original code and original warehouse schema are intentionally omitted.

## Repository layout

- `src/retail_etl.py` — ETL implementation for my project role.
- `sql/02_etl_support_tables.sql` — ETL staging, audit, rejection and registry tables.
- `sql/03_etl_validation.sql` — ETL data-quality, SCD2 and fact-load checks.
- `sql/01_demo_core_schema.sql` — **separately created, simplified example** schema only for reproducing the portfolio demonstration. **Not the original team schema and not presented as my original project deliverable.**
- `sample_data/demo_run1.csv`, `sample_data/demo_run2.csv` — small illustrative demonstration inputs, **not the original complete source dataset**.
- `.env.example`, `requirements.txt`, `.gitignore` — configuration/dependencies/ignore rules.

## Run a small demonstration (MySQL 8+)

1. Install Python and MySQL; run `pip install -r requirements.txt`.
2. Create a **local** MySQL user and grant the necessary permissions to `retail_dw` (use a non-production environment). Run `sql/01_demo_core_schema.sql` then `sql/02_etl_support_tables.sql` against your local MySQL server.
3. Set `RETAIL_DW_HOST`, `RETAIL_DW_PORT`, `RETAIL_DW_USER`, `RETAIL_DW_PASSWORD`, and `RETAIL_DW_DATABASE` as **environment variables**. `.env.example` is only a template; Python does not load it automatically. **Never commit your actual password.**
4. Run initial data: `python src/retail_etl.py --source sample_data/demo_run1.csv --run-label RUN1 --run-date 2026-09-18`.
5. Run the incremental example: `python src/retail_etl.py --source sample_data/demo_run2.csv --run-label RUN2 --run-date 2026-09-19`.
6. Review `sql/03_etl_validation.sql`. The demo changes customer `17850` from United Kingdom to France; cancelled and zero-priced invoices should be rejected.

The full original `Online Retail.xlsx` dataset is **not distributed**. Use a properly licensed source independently if you want to reproduce the academic-scale ETL.

## Notes on evidence and limitations

- The academic project's original run evidence came from the team's environment; **this portfolio package has not been tested end-to-end against a running MySQL database**.
- The sample inputs are demonstrations and **do not reproduce the original reported row counts or performance**.
- The schema supplied here is a compatible illustration for standalone practice, not the group-owned schema.
- Do not upload real customer-level data, secrets or collaborator-owned files without permission. Follow any applicable university/project sharing rules.
