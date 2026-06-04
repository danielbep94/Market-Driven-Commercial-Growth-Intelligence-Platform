# setup_databricks_secrets_windows.ps1
# Run this in PowerShell on your work computer (Windows).
# Requires: Databricks CLI installed (pip install databricks-cli)
# and authenticated (databricks configure --token)
#
# Usage:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\setup_databricks_secrets_windows.ps1

$SCOPE = "MGI_SECRETS"

Write-Host "=== Market Growth Intelligence — Secret Setup (Windows) ===" -ForegroundColor Cyan
Write-Host "Scope: $SCOPE"
Write-Host ""

# ── Step 1: Create secret scope ──────────────────────────────────────────────
Write-Host "Step 1: Creating secret scope..." -ForegroundColor Yellow
try {
    databricks secrets create-scope --scope $SCOPE --initial-manage-principal users 2>$null
    Write-Host "  Scope created: $SCOPE" -ForegroundColor Green
} catch {
    Write-Host "  Scope already exists — continuing" -ForegroundColor Gray
}

# ── Step 2: Collect Snowflake credentials ────────────────────────────────────
Write-Host ""
Write-Host "Step 2: Snowflake credentials" -ForegroundColor Yellow
Write-Host "  Values are stored encrypted in Databricks. Never written to disk." -ForegroundColor Gray
Write-Host ""

$SF_ACCOUNT   = Read-Host "Snowflake account (e.g. abc12345.us-east-1)"
$SF_USER      = Read-Host "Snowflake username"
$SF_WAREHOUSE = Read-Host "Snowflake warehouse"
$SF_ROLE      = Read-Host "Snowflake role (e.g. SYSADMIN or your analyst role)"
$SF_DATABASE  = Read-Host "Snowflake database prefix (e.g. MGI — we store without env suffix)"

Write-Host ""
Write-Host "Storing secrets..." -ForegroundColor Yellow

databricks secrets put --scope $SCOPE --key SNOWFLAKE_ACCOUNT   --string-value $SF_ACCOUNT
databricks secrets put --scope $SCOPE --key SNOWFLAKE_USER      --string-value $SF_USER
databricks secrets put --scope $SCOPE --key SNOWFLAKE_WAREHOUSE --string-value $SF_WAREHOUSE
databricks secrets put --scope $SCOPE --key SNOWFLAKE_ROLE      --string-value $SF_ROLE
databricks secrets put --scope $SCOPE --key SNOWFLAKE_DB_PREFIX --string-value $SF_DATABASE

# ── Step 3: Private key (if using key-pair auth) ─────────────────────────────
Write-Host ""
$USE_KEY = Read-Host "Do you use private key authentication? (y/n)"
if ($USE_KEY -eq "y") {
    $KEY_PATH = Read-Host "Path to private key file (e.g. C:\Users\HERN1124\snowflake_rsa_key.p8)"
    $KEY_CONTENT = Get-Content $KEY_PATH -Raw
    databricks secrets put --scope $SCOPE --key SNOWFLAKE_PRIVATE_KEY --string-value $KEY_CONTENT
    Write-Host "  Private key stored." -ForegroundColor Green
} else {
    $SF_PASSWORD = Read-Host "Snowflake password (stored encrypted)" -AsSecureString
    $PLAIN_PWD = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SF_PASSWORD))
    databricks secrets put --scope $SCOPE --key SNOWFLAKE_PASSWORD --string-value $PLAIN_PWD
    Write-Host "  Password stored." -ForegroundColor Green
}

# ── Step 4: Verify ────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Step 4: Verifying (keys only — values hidden):" -ForegroundColor Yellow
databricks secrets list --scope $SCOPE

Write-Host ""
Write-Host "=== Setup complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "In Databricks notebooks, access your credentials like this:" -ForegroundColor Cyan
Write-Host '  account   = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_ACCOUNT")'
Write-Host '  user      = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_USER")'
Write-Host '  warehouse = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_WAREHOUSE")'
Write-Host ""
Write-Host "Next: open notebooks/phase1_assessment/01_data_profiling_sell_in.py in Databricks"
