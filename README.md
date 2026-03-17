# Olist Brazilian E-Commerce Data Warehouse

A end-to-end data warehouse project built on the [Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), implementing the **Medallion Architecture** (Bronze → Silver → Gold) on SQL Server.

---

## Project Overview

Olist is a Brazilian marketplace platform (similar to Trendyol) that connects small businesses to customers across multiple sales channels. This project transforms raw Olist transactional data into a clean, analytics-ready Star Schema data warehouse.

**Dataset:** 100,000 orders from 2016–2018, covering customers, sellers, products, payments, and reviews.

---

## Architecture

```
Raw CSV Files
      ↓
 Bronze Layer     → Raw data, no transformations. Loaded as-is from source files.
      ↓
 Silver Layer     → Cleaned, typed, and standardized. Business rules applied.
      ↓
  Gold Layer      → Star Schema. Analytics-ready dimensional model.
```

### Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Extraction | Full Load | Static dataset, no incremental updates needed |
| Load Strategy | TRUNCATE + INSERT | Stateless pipeline, safe to re-run |
| SCD Type | Type 0 | Static dataset — no change tracking required |
| Fact Grain | One row per order item | Lowest granularity for flexible aggregation |
| Surrogate Keys | IDENTITY(1,1) | Integer keys for join performance |

---

## Star Schema

> _Diagram: [View on dbdiagram.io](https://dbdiagram.io/d/olist-69b0d53dcf54053b6f5703e2)_

```
                    dim_date
                       |
dim_customers ── fact_orders ── dim_products
                       |
                   dim_sellers
```

### Tables

**fact_orders** — One row per order item. Contains surrogate keys to all dimensions plus measures (total_order_value, delivery_days, is_late).

**fact_order_reviews** — One row per customer review. Contains review score, comment, and a FK to dim_date via review_creation_date.

**dim_customers** — One row per unique customer (grain: customer_unique_id). Includes location data.

**dim_products** — One row per product. Includes category name in English.

**dim_sellers** — One row per seller. Includes location data.

**dim_date** — Physical date dimension table covering the full range of order dates.

---

## Folder Structure

```
olist-dwh/
├── bronze/
│   ├── ddl_bronze.sql
│   └── proc_load_bronze.sql
├── silver/
│   ├── ddl_silver.sql
│   └── proc_load_silver.sql
├── gold/
│   ├── ddl_gold.sql
│   ├── load_gold.sql
│   ├── indexes_gold.sql
│   └── quality_checks_gold.sql
├── docs/
│   ├── data_dictionary.md
│   ├── data_flow.md
│   ├── known_issues.md
│   └── design_decisions.md
└── README.md
```

---

## Gold Layer — Quality Check Findings

All quality checks passed. One known source data characteristic was identified:

**NULL product_key and seller_key in fact_orders (~775–778 rows)**
These rows belong to orders with status `canceled` or `unavailable`. These orders have no corresponding records in `orders_order_items` in the source data — no product or seller was ever assigned before the order closed. This is expected behavior from the source system, not a pipeline error.

---

## Silver Layer — Known Characteristics

- **3 orphan sellers** in `catalog_sellers` have no matching orders. Retained in Silver intentionally — filtering at Gold layer would hide a source data characteristic.
- **orders_order_reviews** was loaded via SSMS Flat File Import Wizard instead of BULK INSERT due to embedded commas and newlines in review text fields.
- `orders_orders` and `orders_order_reviews`: `order_id` contains embedded quotes in source — cleaned with `REPLACE(order_id, '"', '')` in Silver.
- `orders_order_payments`: 2 records with `payment_installments = 0` and valid `payment_value` — retained in Silver, handled at Gold with `NULLIF(payment_installments, 0)`.
- 13 products have no English category mapping — filled with `'unknown'` via `ISNULL()`.

---

## Tech Stack

- **Database:** SQL Server (SSMS)
- **Language:** T-SQL
- **Architecture:** Medallion (Bronze / Silver / Gold)
- **Schema:** Star Schema
- **Dataset:** [Olist Brazilian E-Commerce — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
