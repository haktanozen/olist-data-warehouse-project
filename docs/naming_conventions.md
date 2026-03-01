---

# **Naming Conventions (Olist Project)**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the **Olist Data Warehouse**.

## **Table of Contents**

1. [General Principles](https://www.google.com/search?q=%23general-principles)
2. [Table Naming Conventions](https://www.google.com/search?q=%23table-naming-conventions)
3. [Column Naming Conventions](https://www.google.com/search?q=%23column-naming-conventions)
4. [Stored Procedure](https://www.google.com/search?q=%23stored-procedure-naming-conventions)

---

## **General Principles**

* **Naming Conventions**: Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.
* **Language**: Use English for all names.
* **Avoid Reserved Words**: Do not use SQL reserved words as object names.

## **Table Naming Conventions**

### **Bronze Rules**

* All names must start with the source system name (`orders` or `catalog`).
* **`<sourcesystem>_<entity>`**
* `<sourcesystem>`: `orders` (Transactions/Customers) or `catalog` (Products/Sellers).
* `<entity>`: Original table name from the Olist dataset.
* Example: `orders_customers` → Raw customer data.



### **Silver Rules**

* Names follow the source system prefix to maintain lineage.
* **`<sourcesystem>_<entity>`**
* Example: `catalog_products` → Cleaned and standardized product data.



### **Gold Rules**

* Use business-aligned names with role-based prefixes.
* **`<category>_<entity>`**
* `dim_`: Dimension table (e.g., `dim_customers`).
* `fact_`: Fact table (e.g., `fact_sales`).



| Pattern | Meaning | Example |
| --- | --- | --- |
| `dim_` | Dimension | `dim_products` |
| `fact_` | Fact | `fact_sales` |

## **Column Naming Conventions**

### **Surrogate Keys**

* All primary keys in Gold dimension tables must use the suffix `_key`.
* **`<table_name>_key`** (Example: `product_key`).

### **Technical Columns**

* All system-generated columns must start with `dwh_`.
* **`dwh_<column_name>`** (Example: `dwh_load_date`).

## **Stored Procedure**

* Loading procedures must follow: **`load_<layer>`**.
* Example: `load_bronze` → Stored procedure for loading data into the Bronze layer.

---

