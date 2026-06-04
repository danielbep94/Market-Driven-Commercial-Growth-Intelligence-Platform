#!/bin/bash
# setup_databricks_secrets.sh
# Sets up Databricks Secret Scope for Snowflake credentials.
# Run once from your work computer.
# Requires: Databricks CLI installed and authenticated (databricks configure)
#
# Usage:
#   chmod +x scripts/setup_databricks_secrets.sh
#   ./scripts/setup_databricks_secrets.sh

set -euo pipefail

SCOPE_NAME="MGI_SECRETS"

echo "=== Market Growth Intelligence — Secret Setup ==="
echo "Scope: $SCOPE_NAME"
echo ""

# ── Step 1: Create scope if it doesn't exist ─────────────────────────────────
echo "Step 1: Creating secret scope..."
databricks secrets create-scope "$SCOPE_NAME" \
  --initial-manage-principal "users" 2>/dev/null || \
  echo "  Scope already exists — continuing"

# ── Step 2: Snowflake credentials ────────────────────────────────────────────
echo ""
echo "Step 2: Snowflake credentials"
echo "  Enter values when prompted. These are stored encrypted in Databricks."
echo "  They are NEVER written to disk or to this repo."
echo ""

read -rp "Snowflake account (e.g. abc12345.us-east-1): " SF_ACCOUNT
read -rp "Snowflake username: " SF_USER
read -rp "Snowflake warehouse: " SF_WAREHOUSE
read -rp "Snowflake role: " SF_ROLE
read -rp "Snowflake private key file path (e.g. ~/snowflake_rsa_key.p8): " SF_KEY_PATH

databricks secrets put-secret "$SCOPE_NAME" SNOWFLAKE_ACCOUNT   --string-value "$SF_ACCOUNT"
databricks secrets put-secret "$SCOPE_NAME" SNOWFLAKE_USER      --string-value "$SF_USER"
databricks secrets put-secret "$SCOPE_NAME" SNOWFLAKE_WAREHOUSE --string-value "$SF_WAREHOUSE"
databricks secrets put-secret "$SCOPE_NAME" SNOWFLAKE_ROLE      --string-value "$SF_ROLE"
databricks secrets put-secret "$SCOPE_NAME" SNOWFLAKE_PRIVATE_KEY \
  --string-value "$(cat "${SF_KEY_PATH/#\~/$HOME}")"

echo ""
echo "✅ Snowflake secrets stored in scope: $SCOPE_NAME"

# ── Step 3: Verify ────────────────────────────────────────────────────────────
echo ""
echo "Step 3: Verifying secrets (keys only — values are hidden):"
databricks secrets list-secrets "$SCOPE_NAME" --output table

echo ""
echo "=== Setup complete ==="
echo ""
echo "In Databricks notebooks, access secrets like this:"
echo '  account     = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_ACCOUNT")'
echo '  user        = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_USER")'
echo '  private_key = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_PRIVATE_KEY")'
echo ""
echo "Next step: open notebooks/phase1_assessment/01_data_profiling_sell_in.py in Databricks"
