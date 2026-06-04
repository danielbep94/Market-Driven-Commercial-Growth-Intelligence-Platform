#!/bin/bash
# setup_snowflake_schemas.sh
# Creates the MGI_DEV / MGI_STAGING / MGI_PROD database structure in Snowflake.
# Run once per environment.
# Requires: snowsql installed and authenticated, or paste the SQL into a Snowflake worksheet.

set -euo pipefail

ENVIRONMENT=${1:-dev}
DB="MGI_$(echo $ENVIRONMENT | tr '[:lower:]' '[:upper:]')"

echo "Creating Snowflake schema structure for: $DB"

snowsql -q "
-- Market Growth Intelligence — Schema Setup
-- Environment: $ENVIRONMENT

CREATE DATABASE IF NOT EXISTS $DB COMMENT = 'Market Growth Intelligence — $ENVIRONMENT';

-- Bronze: raw, append-only, never modified
CREATE SCHEMA IF NOT EXISTS $DB.BRONZE COMMENT = 'Raw ingested data — append only, never modified';

-- Silver: homologated, enriched
CREATE SCHEMA IF NOT EXISTS $DB.SILVER COMMENT = 'Cleaned and homologated data';

-- Gold: dimensional model
CREATE SCHEMA IF NOT EXISTS $DB.GOLD COMMENT = 'Star schema — dimensions and facts';

-- MART: aggregated intelligence tables (Power BI reads here)
CREATE SCHEMA IF NOT EXISTS $DB.MART COMMENT = 'Aggregated MART tables — Power BI source';

-- MONITORING: data quality logs, model performance, orphan tracking
CREATE SCHEMA IF NOT EXISTS $DB.MONITORING COMMENT = 'DQ logs, model performance, homologation exceptions';

-- FEATURE_STORE: ML features
CREATE SCHEMA IF NOT EXISTS $DB.FEATURE_STORE COMMENT = 'Weekly ML feature store';

SHOW SCHEMAS IN DATABASE $DB;
"

echo "✅ Schema structure created in $DB"
echo ""
echo "Next step: run the DDL scripts in sql/ddl/ to create individual tables:"
echo "  snowsql -f sql/ddl/dimensions/dim_date.sql"
echo "  snowsql -f sql/ddl/facts/fact_sell_in.sql"
echo "  (etc.)"
