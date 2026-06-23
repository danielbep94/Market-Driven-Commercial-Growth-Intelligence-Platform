# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE: Copy this file to configs/snowflake_creds.py and fill in values.
# configs/snowflake_creds.py is GITIGNORED — never committed to GitHub.
# ─────────────────────────────────────────────────────────────────────────────

SF_MEX_USER     = "YOUR_MEX_USER"
SF_MEX_PASSWORD = "YOUR_MEX_PASSWORD"
SF_MEX_ROLE     = "PRD_MEX_READER"
SF_MEX_WH       = "PRD_MEX_ANL_WH"

SF_MDP_USER     = None   # leave None → resolved from KV scope DAN-AM-P-KVT800-R-MDP-DB
SF_MDP_PASSWORD = None   # leave None → resolved from KV
SF_MDP_ROLE     = "PRD_MDP"
SF_MDP_WH       = "PRD_MDP_ANL_WH"
