# Velora Analytics — Production-Grade E-Commerce Analytics Platform

## Project Overview
End-to-end Microsoft Fabric analytics platform built on real-world 
e-commerce data (Olist Brazilian E-Commerce dataset — 99,441 orders, 
500K+ rows across 9 source tables).

## Architecture
Source CSVs → Bronze Layer → Silver Layer → Gold Layer → Power BI Dashboards

## Tech Stack
- Microsoft Fabric (Lakehouse, Notebooks, Semantic Model)
- PySpark / Python
- Delta Lake
- Power BI

## Layers Built

### Bronze Layer (velora_bronze)
- Raw data landed as-is from 9 source CSV files
- Append-only, zero transformations
- Load log table tracking batch_id, row counts, timestamps

### Silver Layer (velora_silver)
- Deduplication and null handling
- Business flags added:
  - is_late_delivery
  - delivery_delay_days
  - is_repeat_customer
  - is_installment
- Portuguese product categories translated to English
- Audit columns: ingested_at, source_layer

### Gold Layer (velora_gold)
- Star schema with grain discipline
- 4 Dimension tables: dim_date, dim_customer, dim_product, dim_seller
- 3 Fact tables: fact_orders, fact_order_items, fact_payments
- 635,000+ rows across Gold layer

## Data Model
| Table | Rows | Description |
|---|---|---|
| dim_date | 1,461 | Full date spine 2016-2019 |
| dim_customer | 96,096 | Unique customers |
| dim_product | 32,951 | Products with English categories |
| dim_seller | 3,095 | Seller locations |
| fact_orders | 99,441 | One row per order |
| fact_order_items | 112,650 | One row per order line |
| fact_payments | 103,886 | One row per payment |

## Key Business Insights Discovered
- 8% late delivery rate (7,827 orders delivered late)
- Only 3.12% repeat customer rate — retention opportunity
- SP state dominates with 42% of all customers
- Credit card = 74% of all payments
- Nearly 50% of orders paid in installments
- Orders peaked November 2017 (Black Friday effect)

## Dashboards Built
1. Executive Dashboard — GMV, total orders, delivery KPIs, category revenue
2. Operations Dashboard — Late deliveries, state-wise delays, order status
3. Growth Dashboard — Repeat customers, payment methods, monthly trends

## Data Quality Findings
- 610 products with missing category (handled as unknown)
- 160 orders never approved (payment failed)
- 2,965 orders undelivered (cancelled or in transit)
- 3 payments with undefined payment type (handled as unknown)

