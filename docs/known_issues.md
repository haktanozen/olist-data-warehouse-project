# Known Issues & Source Data Characteristics

This document lists all known data quality issues, source data characteristics, and deliberate design decisions made during the pipeline build. These are not bugs — they are documented behaviors.

---

## Gold Layer

### NULL product_key and seller_key in fact_orders

| | |
|---|---|
| **Affected rows** | ~775 rows (product_key), ~778 rows (seller_key) |
| **Affected table** | gold.fact_orders |
| **Root cause** | These orders have status `canceled` or `unavailable` and have no corresponding records in `silver.orders_order_items`. No product or seller was ever assigned to these orders in the source system before they were closed. |
| **Verified** | Silver.orders_order_items contains 0 rows for these order_ids. |
| **Action** | No fix required. Retained as-is. Filter with `WHERE product_key IS NOT NULL` in analytical queries if needed. |

---

## Silver Layer

### 3 Orphan Sellers in catalog_sellers

| | |
|---|---|
| **Affected rows** | 3 rows |
| **Affected table** | silver.catalog_sellers |
| **Root cause** | 3 seller_ids exist in the sellers table but have no matching records in order_items — they never fulfilled an order in the dataset period. |
| **Action** | Retained in Silver intentionally. Filtering them out would hide a source data characteristic. They will appear in dim_sellers with no linked fact rows. |

### orders_order_reviews — Flat File Import

| | |
|---|---|
| **Affected table** | bronze.orders_order_reviews |
| **Root cause** | The source CSV contains embedded commas and newlines inside review comment fields, making standard BULK INSERT fail. |
| **Action** | Loaded via SSMS Flat File Import Wizard which handles irregular delimiters correctly. Documented here for reproducibility. |

### Embedded Quotes in order_id

| | |
|---|---|
| **Affected tables** | silver.orders_orders, silver.orders_order_reviews |
| **Root cause** | Some `order_id` values in the source CSV contain embedded double-quote characters. |
| **Action** | Cleaned in Silver with `REPLACE(order_id, '"', '')` before any other transformations. |

### payment_installments = 0

| | |
|---|---|
| **Affected rows** | 2 rows |
| **Affected table** | silver.orders_order_payments |
| **Root cause** | 2 records have `payment_installments = 0` with a valid `payment_value`. Logically inconsistent — a payment with 0 installments should not have a value. |
| **Action** | Retained in Silver as-is. Handled at Gold layer with `NULLIF(payment_installments, 0)`. |

### Non-numeric review_score Values

| | |
|---|---|
| **Affected table** | silver.orders_order_reviews |
| **Root cause** | Some rows in the source CSV contain dates or text strings in the `review_score` column instead of numeric scores (1–5). |
| **Action** | Handled in Silver with `CASE WHEN ISNUMERIC(review_score) THEN CAST(...) ELSE NULL END`. Non-numeric values stored as NULL. |

### 13 Products with No English Category Translation

| | |
|---|---|
| **Affected rows** | 13 rows |
| **Affected table** | silver.catalog_products |
| **Root cause** | 13 product category names in Portuguese have no corresponding entry in the translation table. |
| **Action** | `product_category_name_english` filled with `'unknown'` via `ISNULL()`. Original Portuguese name retained in `product_category_name`. |
