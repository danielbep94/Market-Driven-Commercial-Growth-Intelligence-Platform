# Data Contract — Sell-in

**Source system:** [ERP / SAP / Other — TBD]  
**Business owner:** [TBD]  
**Technical owner:** [TBD]  
**Status:** DRAFT

---

## Expected Schema

| Column | Type | Nullable | Description | Example |
|--------|------|---------|-------------|---------|
| sku_id | STRING | No | Source SKU identifier | "SKU-001234" |
| customer_id | STRING | No | Source customer identifier | "CUST-5678" |
| ship_date | DATE | No | Date of shipment | 2025-03-15 |
| ship_week | INT | No | ISO week number | 11 |
| ship_year | INT | No | Year | 2025 |
| units_shipped | DECIMAL(18,4) | No | Units shipped to customer | 1200.0 |
| net_revenue | DECIMAL(18,2) | Yes | Net revenue after discounts | 45600.00 |
| list_price | DECIMAL(18,4) | Yes | List price per unit | 42.00 |
| net_price | DECIMAL(18,4) | Yes | Net price per unit after discounts | 38.00 |

## Expected Delivery

| Attribute | Value |
|-----------|-------|
| Frequency | Weekly |
| Expected delivery day/time | [TBD — e.g., Monday by 6:00 AM] |
| Delivery method | [TBD — SFTP / API / direct DB connection] |
| File format | [TBD — CSV / Parquet / Snowflake table] |
| Expected row count range | [TBD — e.g., 50,000–200,000 rows per week] |
| Historical depth available | [TBD — e.g., 3 years] |

## Known Quality Risks

- Missing weeks for some customers during holidays
- Duplicate shipment records if a shipment is amended post-close
- Inconsistent customer codes between ERP versions

## SLA Breach Escalation

If data is not delivered by [TBD]:
1. Automated alert sent to [Data Engineer]
2. If not resolved within 2 hours: escalate to [Technical Owner]
3. If not resolved within 4 hours: escalate to [Business Owner]
4. Pipeline holds on Silver/Gold processing until source is resolved

## Acceptance Criteria

- Row count within expected range (alert if < 80% of prior week)
- No NULL values in sku_id, customer_id, ship_date, units_shipped
- All sku_ids must exist in DIM_PRODUCT (unresolved SKUs → FACT_SELL_IN_UNRESOLVED)
- No duplicate records (sku_id + customer_id + ship_date must be unique, or deduplication rules applied)
