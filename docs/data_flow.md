# Data Flow — Olist DWH

This document describes how data moves from raw CSV files through each layer of the Medallion Architecture.

---

## Overview

```
Raw CSV Files (9 files)
        ↓
   Bronze Layer        → Load as-is, no transformation
        ↓
   Silver Layer        → Clean, type-cast, standardize
        ↓
    Gold Layer         → Star Schema, surrogate keys, derived columns
```

---

## Source Files

| File | Loaded Into |
|---|---|
| olist_customers_dataset.csv | bronze.orders_customers |
| olist_orders_dataset.csv | bronze.orders_orders |
| olist_order_items_dataset.csv | bronze.orders_order_items |
| olist_order_payments_dataset.csv | bronze.orders_order_payments |
| olist_order_reviews_dataset.csv | bronze.orders_order_reviews |
| olist_products_dataset.csv | bronze.catalog_products |
| olist_sellers_dataset.csv | bronze.catalog_sellers |
| product_category_name_translation.csv | bronze.catalog_product_category_name_translation |

> Note: `olist_order_reviews_dataset.csv` was loaded via SSMS Flat File Import Wizard due to embedded commas and newlines in review text fields.

---

## Bronze Layer

**Purpose:** Land raw data exactly as it exists in source files. No transformations applied.

**Load strategy:** Full Load — TRUNCATE + INSERT on every run.

**What happens here:**
- All columns stored as NVARCHAR to avoid type errors on load
- No NULL handling, no cleaning
- Serves as a permanent raw data archive

---

## Silver Layer

**Purpose:** Clean and standardize the data. Make it queryable and reliable.

**Load strategy:** Full Load — TRUNCATE + INSERT via `proc_load_silver`.

**Transformations applied:**

| Transformation | Detail |
|---|---|
| Type casting | All columns cast to proper types: `DATETIME2(0)`, `DATE`, `DECIMAL(10,2)`, `INT` |
| Safe casting | `TRY_CAST` used instead of `CAST` to avoid errors on dirty data |
| NULL handling | `NULLIF` applied before type casts to convert empty strings to NULL |
| String cleaning | `REPLACE` before `TRIM` to handle embedded quotes and whitespace |
| Embedded quotes | `order_id` in orders and reviews contained embedded `"` → removed with `REPLACE(order_id, '"', '')` |
| Category translation | `product_category_name_english` joined from translation table via LEFT JOIN. 13 unmapped categories filled with `'unknown'` |
| Review score | Some rows contained non-numeric values → handled with `CASE WHEN ISNUMERIC() THEN CAST(...) ELSE NULL END` |
| Payment installments | 2 records with `payment_installments = 0` and valid `payment_value` → retained in Silver, handled at Gold |

**Known source data characteristics retained in Silver:**
- 3 orphan sellers in `catalog_sellers` have no matching orders — retained intentionally, documented
- Some orders have no corresponding `order_items` records (canceled/unavailable orders)

---

## Gold Layer

**Purpose:** Build the Star Schema. Analytics-ready dimensional model.

**Load strategy:** Full Load — TRUNCATE + INSERT via `load_gold.sql`.

**How each table is built:**

### dim_customers
- Source: `silver.orders_customers`
- Deduplication: `ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp DESC)` — keeps the most recent record per unique customer
- Grain: `customer_unique_id`

### dim_products
- Source: `silver.catalog_products`
- No deduplication needed — one row per product in source

### dim_sellers
- Source: `silver.catalog_sellers`
- No deduplication needed — one row per seller in source

### dim_date
- Source: Generated programmatically using a date range loop
- Covers full range of order dates in the dataset
- Physical table (not a VIEW)

### fact_orders
- Source: `silver.orders_orders` + `silver.orders_order_items` + `silver.orders_order_payments`
- Surrogate keys resolved via JOIN to each dim table on business key
- Derived columns calculated at load time:
  - `total_order_value` = `price + freight_value`
  - `delivery_days` = `DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date)`
  - `is_late` = `CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END`
- `payment_type` aggregated with `STRING_AGG` — orders with multiple payment methods stored as concatenated string

### fact_order_reviews
- Source: `silver.orders_order_reviews`
- Joined to `dim_date` on `review_creation_date`

---

## SCD Strategy

**Type 0 — No change tracking.**

The Olist dataset is static (2016–2018 snapshot). No updates or deletes occur after initial load. All dimension tables are fully refreshed on each pipeline run via TRUNCATE + INSERT. Historical change tracking is not required.

---

## Pipeline Execution Order

```
1. EXEC bronze.proc_load_bronze
2. EXEC silver.proc_load_silver
3. EXEC gold.load_gold          ← runs all dim loads then fact loads
4. Run indexes_gold.sql
5. Run quality_checks_gold.sql
```

> Dims must be loaded before facts — fact table resolves surrogate keys via JOIN to dims at load time.
