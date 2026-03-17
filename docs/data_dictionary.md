# Data Dictionary — Gold Layer

All tables are in the `gold` schema. Surrogate keys are generated via `IDENTITY(1,1)` and are internal to the DWH — they have no meaning in the source system.

---

## dim_customers

One row per unique customer. Grain: `customer_unique_id`.

> Note: In the source system, the same physical customer receives a different `customer_id` for each order. `customer_unique_id` is used to identify the same customer across multiple orders.

| Column | Type | Nullable | Description |
|---|---|---|---|
| customer_key | INT | NO | Surrogate key (PK) |
| customer_unique_id | NVARCHAR(100) | NO | Business key — unique identifier for a customer across all orders |
| customer_zip_code_prefix | NVARCHAR(20) | YES | First 5 digits of customer zip code |
| customer_city | NVARCHAR(100) | YES | Customer city |
| customer_state | NVARCHAR(20) | YES | Customer state (e.g. SP, RJ) |
| customer_region | NVARCHAR(40) | YES | Derived region grouping based on state (e.g. Southeast, Northeast) |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |

---

## dim_products

One row per product.

> Note: 13 products have no English category mapping in the source translation table. These are filled with `'unknown'` via `ISNULL()`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| product_key | INT | NO | Surrogate key (PK) |
| product_id | NVARCHAR(100) | NO | Business key — unique product identifier from source |
| product_category_name | NVARCHAR(200) | YES | Original Portuguese category name |
| product_category_name_english | NVARCHAR(200) | YES | Translated English category name. 'unknown' if no translation exists |
| product_name_length | INT | YES | Character count of product name in source listing |
| product_description_length | INT | YES | Character count of product description in source listing |
| product_photos_qty | INT | YES | Number of product photos in source listing |
| product_weight_g | INT | YES | Product weight in grams |
| product_length_cm | INT | YES | Product length in centimeters |
| product_height_cm | INT | YES | Product height in centimeters |
| product_width_cm | INT | YES | Product width in centimeters |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |

---

## dim_sellers

One row per seller.

> Note: 3 orphan sellers exist in the source data with no matching orders. Retained intentionally — see Known Issues.

| Column | Type | Nullable | Description |
|---|---|---|---|
| seller_key | INT | NO | Surrogate key (PK) |
| seller_id | NVARCHAR(100) | NO | Business key — unique seller identifier from source |
| seller_zip_code_prefix | NVARCHAR(20) | YES | First 5 digits of seller zip code |
| seller_city | NVARCHAR(100) | YES | Seller city |
| seller_state | NVARCHAR(20) | YES | Seller state (e.g. SP, RJ) |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |

---

## dim_date

Physical date dimension table. One row per calendar date, covering the full range of order dates in the dataset.

| Column | Type | Nullable | Description |
|---|---|---|---|
| date_key | INT | NO | Surrogate key (PK). Format: YYYYMMDD (e.g. 20171025) |
| full_date | DATE | NO | Calendar date (e.g. 2017-10-25) |
| year | INT | NO | Year (e.g. 2017) |
| quarter | INT | NO | Quarter number (1–4) |
| month | INT | NO | Month number (1–12) |
| month_name | NVARCHAR(40) | NO | Month name (e.g. October) |
| week | INT | NO | ISO week number (1–53) |
| day_of_month | INT | NO | Day of month (1–31) |
| day_of_week | INT | NO | Day of week (1=Sunday, 7=Saturday) |
| day_name | NVARCHAR(40) | NO | Day name (e.g. Wednesday) |
| is_weekend | BIT | NO | 1 if Saturday or Sunday, 0 otherwise |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |

---

## fact_orders

One row per order item. Central fact table of the Star Schema.

> Note: ~775 rows have NULL `product_key` and ~778 rows have NULL `seller_key`. These belong to canceled or unavailable orders that have no corresponding records in `order_items` in the source system — no product or seller was ever assigned. This is expected source data behavior, not a pipeline error.

> Note: `payment_type` can contain multiple values for a single order (e.g. a customer paid with both credit card and voucher). These are stored as a concatenated string using `STRING_AGG`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| order_key | INT | NO | Surrogate key (PK) |
| order_id | NVARCHAR(100) | NO | Business key — unique order identifier from source |
| order_item_id | INT | YES | Item sequence number within an order (1, 2, 3...) |
| customer_key | INT | YES | FK → dim_customers |
| product_key | INT | YES | FK → dim_products. NULL for canceled/unavailable orders |
| seller_key | INT | YES | FK → dim_sellers. NULL for canceled/unavailable orders |
| date_key | INT | YES | FK → dim_date. Based on order_purchase_timestamp |
| order_status | NVARCHAR(100) | YES | Order status from source (delivered, canceled, unavailable, etc.) |
| order_purchase_timestamp | DATETIME2 | YES | When the customer placed the order |
| order_approved_at | DATETIME2 | YES | When the payment was approved |
| order_delivered_carrier_date | DATETIME2 | YES | When the order was handed to the carrier |
| order_delivered_customer_date | DATETIME2 | YES | When the order was delivered to the customer |
| order_estimated_delivery_date | DATE | YES | Estimated delivery date from source |
| payment_type | NVARCHAR(MAX) | YES | Payment method(s) used. May be concatenated if multiple methods used |
| payment_installments | INT | YES | Number of installments. Source records with 0 installments handled via NULLIF |
| payment_value | DECIMAL | YES | Total payment amount in BRL |
| price | DECIMAL | YES | Item price in BRL |
| freight_value | DECIMAL | YES | Freight cost in BRL |
| total_order_value | DECIMAL | YES | Derived: price + freight_value |
| delivery_days | INT | YES | Derived: days between order_purchase_timestamp and order_delivered_customer_date |
| is_late | BIT | YES | Derived: 1 if order_delivered_customer_date > order_estimated_delivery_date |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |

---

## fact_order_reviews

One row per review. Contains customer satisfaction scores and comments.

> Note: Loaded via SSMS Flat File Import Wizard due to embedded commas and newlines in review text fields — BULK INSERT was not viable for this table.

| Column | Type | Nullable | Description |
|---|---|---|---|
| review_key | INT | NO | Surrogate key (PK) |
| review_id | NVARCHAR(100) | NO | Business key — unique review identifier from source |
| order_id | NVARCHAR(100) | NO | Reference to the reviewed order |
| date_key | INT | YES | FK → dim_date. Based on review_creation_date |
| review_score | INT | YES | Customer satisfaction score (1–5). Some source rows contained non-numeric values — handled with CASE WHEN ISNUMERIC() |
| review_comment_title | NVARCHAR(200) | YES | Short title of the review comment |
| review_comment_message | NVARCHAR(MAX) | YES | Full review comment text |
| review_creation_date | DATE | YES | Date the review survey was sent to the customer |
| review_answer_timestamp | DATETIME2 | YES | Timestamp when the customer submitted the review |
| dwh_create_date | DATETIME2 | YES | Timestamp when the row was loaded into the DWH |
