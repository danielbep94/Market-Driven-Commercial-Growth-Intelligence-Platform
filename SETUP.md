# Setup Guide — Clone on Work Computer & Databricks

This guide gets you from zero to running notebooks in under 10 minutes.

---

## A · Clone on Your Work Computer

Open a terminal on your work computer:

```bash
git clone https://github.com/danielbep94/Market-Driven-Commercial-Growth-Intelligence-Platform.git
cd Market-Driven-Commercial-Growth-Intelligence-Platform
```

That's it. The repo is now on your work computer.

To keep it in sync with the agent's changes (do this every session):

```bash
git pull origin main
```

---

## B · Connect to Databricks — Repos (Recommended, 3 clicks)

Databricks Repos syncs directly with GitHub. You run notebooks from within Databricks — no local Python environment needed.

### Step 1 — Add the repo in Databricks

1. Open your Databricks workspace
2. Click **Repos** in the left sidebar (or **Workspace → Repos**)
3. Click **Add Repo** (top-right)
4. Paste the GitHub URL:
   ```
   https://github.com/danielbep94/Market-Driven-Commercial-Growth-Intelligence-Platform.git
   ```
5. Leave **Git provider** as GitHub. Click **Create Repo**.

Databricks clones the repo into your workspace at:
```
/Repos/<your-email>/Market-Driven-Commercial-Growth-Intelligence-Platform/
```

### Step 2 — Connect GitHub (one-time, if not already done)

If Databricks asks for GitHub credentials:
1. Go to **Settings → User Settings → Git Integration**
2. Set **Git provider:** GitHub
3. Set **Personal access token:** create one at https://github.com/settings/tokens
   - Scopes needed: `repo` (read + write)
4. Save. Go back to Repos → Add Repo and retry.

### Step 3 — Pull latest changes (every session)

Inside Databricks Repos:
1. Click the **branch name** at the top of the repo (e.g., `main`)
2. Click **Pull**

Or from the Git dialog: **Repos → [repo name] → Git… → Pull**

---

## C · Run Your First Notebook

1. In Databricks Repos, navigate to:
   `notebooks/phase1_assessment/01_data_profiling_sell_in.py`
2. Click **Open** — Databricks recognizes it as a notebook (`.py` with Databricks source header)
3. Attach to your cluster
4. Update `SOURCE_TABLE` to your actual Snowflake Bronze table name
5. Click **Run All**

The notebook writes results to:
`docs/phase_outputs/phase1_data_inventory.md`

---

## D · Push Results Back (every time you run a notebook)

From the Databricks Repos Git dialog, or from your work computer terminal:

```bash
cd Market-Driven-Commercial-Growth-Intelligence-Platform
git add docs/phase_outputs/
git commit -m "data: phase1 sell_in profiling results"
git push origin main
```

Then on your personal laptop, tell the agent:
> "sell_in profiling done, results committed"

---

## E · Snowflake Connection in Databricks

Databricks connects to Snowflake via the native connector. Add these to your cluster's **Environment Variables** (never hardcode in notebooks):

| Variable | Value |
|----------|-------|
| `SNOWFLAKE_ACCOUNT` | `<your-account>.snowflakecomputing.com` |
| `SNOWFLAKE_USER` | `<your-username>` |
| `SNOWFLAKE_WAREHOUSE` | `<your-warehouse>` |
| `SNOWFLAKE_ROLE` | `<your-role>` |

The private key (for key-pair authentication) goes in a **Databricks Secret Scope**:

```bash
# Run once from your work computer terminal (Databricks CLI must be installed)
databricks secrets create-scope MGI_SECRETS
databricks secrets put-secret MGI_SECRETS SNOWFLAKE_PRIVATE_KEY --string-value "$(cat snowflake_rsa_key.p8)"
```

In notebooks, access secrets like this:
```python
private_key = dbutils.secrets.get(scope="MGI_SECRETS", key="SNOWFLAKE_PRIVATE_KEY")
```

The setup script (`scripts/setup_databricks_secrets.sh`) walks through this step by step.

---

## F · Verify Everything Is Working

Run this quick check in a Databricks notebook:

```python
# Paste into a new cell — should all print OK
import subprocess, sys

checks = {
    "PySpark": lambda: __import__('pyspark').__version__,
    "MLflow":  lambda: __import__('mlflow').__version__,
    "LightGBM": lambda: __import__('lightgbm').__version__,
    "SHAP":    lambda: __import__('shap').__version__,
    "Snowflake connector": lambda: __import__('snowflake.connector').__version__,
}

for name, fn in checks.items():
    try:
        print(f"  ✅ {name}: {fn()}")
    except Exception as e:
        print(f"  ❌ {name}: {e}")
```

---

## Quick Reference

| Task | Command / Location |
|------|--------------------|
| Clone repo (work computer) | `git clone https://github.com/danielbep94/Market-Driven-Commercial-Growth-Intelligence-Platform.git` |
| Add to Databricks | Repos → Add Repo → paste GitHub URL |
| Pull latest changes (Databricks) | Repos → repo name → Git… → Pull |
| Pull latest changes (work computer) | `git pull origin main` |
| Push results back | `git add docs/phase_outputs/ && git commit -m "..." && git push` |
| First notebook to run | `notebooks/phase1_assessment/01_data_profiling_sell_in.py` |

