# Access Control Matrix

## Roles

| Role | Databricks | Snowflake | Power BI |
|------|-----------|-----------|---------|
| Data Engineer | Full workspace access (DEV + STAGING) | Read/Write on Bronze, Silver, Gold (DEV + STAGING); Read-only PROD | No access |
| Data Scientist | Read/Write on notebooks; Read on all layers | Read on Silver + Gold + MART; Write on FEATURE_STORE | No access |
| BI Developer | Read on Gold notebooks | Read on Gold + MART + VIEWS | Full authoring access |
| Commercial Leader | No access | No access | Read-only (all brands, all regions) |
| Brand Manager | No access | No access | Read-only (own brands only — RLS) |
| Regional Manager | No access | No access | Read-only (own region only — RLS) |
| Data Steward | Read on all layers | Read/Write on HOMOLOGATION tables | No access |

## Power BI Row-Level Security (RLS)

| User group | Filter applied |
|-----------|---------------|
| Brand Manager | `brand_key IN (brands assigned to user)` |
| Regional Manager | `region_key IN (regions assigned to user)` |
| Commercial Leader | No filter (sees all) |
| Executive | No filter (sees all) |
