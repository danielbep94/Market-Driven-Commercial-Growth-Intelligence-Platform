# SLA Registry

## Pipeline SLAs

| Stage | Expected completion | Alert if not done by | Notification recipient |
|-------|--------------------|--------------------|----------------------|
| Bronze ingestion (all sources) | Sunday 11:59 PM | Monday 2:00 AM | Data Engineer |
| Silver processing | Monday 3:00 AM | Monday 5:00 AM | Data Engineer |
| Gold + Snowflake write | Monday 5:00 AM | Monday 6:00 AM | Data Engineer + BI Developer |
| Power BI refresh | Monday 8:00 AM | Monday 9:00 AM | BI Developer |
| Executive dashboard available | Monday 8:00 AM | Monday 10:00 AM | PM |

## Source Data SLAs

| Source | Expected delivery | Grace period | Action if breached |
|--------|------------------|--------------|--------------------|
| [All sources — TO BE COMPLETED IN PHASE 0] | | | |
