# Naming Conventions — Homologation Master Reference

**Status:** DRAFT — must be approved by business stakeholders before Phase 3 begins.  
**Owner:** Data Steward  
**Last updated:** [DATE]

---

## 1. General Rules

- All names use UPPER_SNAKE_CASE in database tables and columns.
- All names use lowercase-with-hyphens in file names.
- No accented characters in technical names (use ASCII equivalents in code; store display names with accents in a `_display_name` column).
- Language: English for all technical names; Spanish display names stored separately.

---

## 2. Brand Names

| Raw name (source) | Clean name | Brand key | Notes |
|-------------------|-----------|-----------|-------|
| [TO BE COMPLETED IN PHASE 1] | | | |

---

## 3. Category Names

| Raw name | Clean name | Category key | Notes |
|----------|-----------|-------------|-------|
| [TO BE COMPLETED IN PHASE 1] | | | |

---

## 4. Channel Names

| Raw name | Clean name | Channel type | Channel key |
|----------|-----------|-------------|------------|
| [TO BE COMPLETED IN PHASE 1] | | | |

Channel types (controlled vocabulary):
- `MODERN_TRADE` — supermarkets, hypermarkets, club stores
- `TRADITIONAL_TRADE` — small independent stores, tiendas
- `ECOMMERCE` — online platforms
- `FOODSERVICE` — restaurants, institutions

---

## 5. Customer Names

| Raw name | Clean name | Customer key | Notes |
|----------|-----------|-------------|-------|
| [TO BE COMPLETED IN PHASE 1] | | | |

---

## 6. Column Naming Standards

| Field type | Convention | Example |
|-----------|-----------|---------|
| Surrogate key | `[entity]_key` | `brand_key`, `product_key` |
| Natural key | `[entity]_id` | `brand_id`, `sku_id` |
| Date field | `[context]_date` or `[context]_week` | `ship_date`, `sell_out_week` |
| Measure (units) | `[metric]_units` | `sell_in_units`, `waste_units` |
| Measure (value) | `[metric]_value` | `sell_out_value`, `waste_value` |
| Rate / ratio | `[metric]_rate` or `[metric]_ratio` | `waste_rate`, `sell_out_sell_in_ratio` |
| Score | `[metric]_score` | `growth_quality_score`, `waste_risk_score` |
| Flag | `is_[condition]` | `is_holiday`, `is_promotion_active` |
| Audit | `ingested_at`, `updated_at`, `batch_id` | |

---

## 7. File Naming Standards

| Artifact | Convention | Example |
|---------|-----------|---------|
| Databricks notebook | `[phase]_[description].py` | `bronze_sell_in.py` |
| SQL DDL | `[entity_type]_[name].sql` | `dim_brand.sql`, `fact_sell_in.sql` |
| SQL function | `fn_[kpi_name]_v[n].sql` | `fn_waste_rate_v1.sql` |
| Config file | `[scope]_config.yaml` | `pipeline_config.yaml` |
| Test file | `test_[what_is_tested].py` | `test_kpi_waste_rate.py` |
