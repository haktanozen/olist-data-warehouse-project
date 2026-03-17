# Design Decisions

This document explains the key architectural and technical decisions made during the build of the Olist DWH, and the reasoning behind each choice.

---

## Architecture

### Medallion Architecture (Bronze / Silver / Gold)

**Decision:** Implement three distinct layers instead of loading directly into a Star Schema.

**Reason:** Each layer serves a specific purpose:
- Bronze preserves the raw source data exactly as received — acts as an audit trail and allows reprocessing from scratch at any time
- Silver isolates all cleaning and type-casting logic in one place — if a cleaning rule changes, only Silver needs to be updated
- Gold focuses purely on analytical modeling — no cleaning logic lives here

This separation makes the pipeline easier to debug, maintain, and extend.

---

## Load Strategy

### Full Load — TRUNCATE + INSERT

**Decision:** Every layer uses TRUNCATE + INSERT on each pipeline run, not MERGE or incremental load.

**Reason:** The Olist dataset is a static historical snapshot. There are no new records arriving after the initial load. A full reload is safe, simple, and easy to reason about. MERGE logic adds complexity with no benefit for a static dataset.

**Trade-off:** Not suitable for large production datasets with frequent updates — in that case, incremental or MERGE-based loading would be required.

---

## Slowly Changing Dimensions

### SCD Type 0 — No Change Tracking

**Decision:** All dimension tables use SCD Type 0 — the first loaded value is kept forever, no historical tracking.

**Reason:** The dataset is a static 2016–2018 snapshot. Customer cities, product categories, and seller locations do not change within this dataset. Building SCD Type 2 logic (new rows, effective dates, is_active flags) would add significant complexity with no analytical value here.

**Trade-off:** In a live production system where customer addresses or seller information changes over time, SCD Type 2 would be needed to correctly answer "what was the customer's city at the time of this order?"

---

## Dimensional Model

### Star Schema over Snowflake Schema

**Decision:** Use a flat Star Schema — no normalization of dimension tables.

**Reason:** Star Schema is simpler to query. Analytical tools and BI platforms work better with wide, flat dimension tables. The slight storage overhead from denormalization is negligible at this data volume.

### dim_customers Grain — customer_unique_id

**Decision:** One row per `customer_unique_id`, not per `customer_id`.

**Reason:** In the Olist source system, the same physical customer receives a new `customer_id` for every order. Using `customer_id` as the grain would result in one row per order instead of one row per customer — making customer-level analysis (repeat purchase rate, RFM segmentation) impossible.

### dim_date as a Physical Table

**Decision:** Build `dim_date` as a physical table, not a VIEW or computed expression.

**Reason:** Physical tables are faster to query — the date attributes are pre-computed and stored. A VIEW would recompute date parts on every query.

---

## Surrogate Keys

### IDENTITY(1,1) Integer Keys

**Decision:** All surrogate keys use `INT IDENTITY(1,1)`.

**Reason:** Integer keys are smaller (4 bytes vs. 36 bytes for a UUID string), faster to JOIN, and easier to index. The DWH owns these keys — they have no meaning in the source system and are never exposed to end users.

**Behavior with TRUNCATE:** `TRUNCATE` resets the IDENTITY counter to 1. Since we use TRUNCATE + INSERT, surrogate keys are deterministic across pipeline runs — the same source record always receives the same surrogate key.

---

## Indexes

### Fact Table

| Index | Reason |
|---|---|
| order_id | Business key — used in external lookups and joins from reporting tools |
| customer_key | Most frequent JOIN column in customer-level analysis |
| date_key | Most frequent WHERE filter column in time-based analysis |

### Dimension Tables

| Index | Reason |
|---|---|
| dim_customers(customer_unique_id) | Used in ETL JOIN from Silver and in customer-level reporting |
| dim_products(product_id) | Used in ETL JOIN from silver.orders_order_items |
| dim_sellers(seller_id) | Used in ETL JOIN from silver.orders_order_items |
| dim_date(full_date) | Used in ETL JOIN when mapping order dates to date_key |

**Note:** Surrogate key columns (customer_key, product_key, etc.) on dimension tables are PRIMARY KEYs — SQL Server automatically creates a clustered index on them. No additional index is needed.

---

## Known Deliberate Retentions

### Orphan Sellers in Silver

3 sellers in `catalog_sellers` have no matching orders. Retained in Silver because filtering them out would silently hide a source data characteristic. They appear in `dim_sellers` with no linked fact rows — this is expected and harmless for analysis.

### NULL product_key / seller_key in fact_orders

~775–778 rows in `fact_orders` have NULL foreign keys. These belong to canceled or unavailable orders with no `order_items` record in the source. Retained as-is — filtering them out would remove valid order records from the pipeline. Analysts should apply `WHERE product_key IS NOT NULL` when product-level analysis is required.
