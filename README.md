# Agentic Fraud Triage (Banking) – brick-con-3

End-to-end fraud triage: mock data → Unity Catalog → Silver (wire+IP rule) → Reasoning Agent (Vector Search + LLM) → Lakebase triage → Live Fraud Queue app. Genie for conversational KPIs; impossible-travel monitor.

---

## Run order (Databricks workspace)

| Step | File | What it does |
|------|------|--------------|
| 1 | `01_unity_catalog_setup.py` | Create catalog **Financial_Security**, schemas raw/silver/gold, volume **mock_source_data**, ABAC/tag placeholder |
| 2 | `02_mock_data_generator.py` | **Run locally**: generate `mock_data/transactions.csv` and `login_logs.csv` |
| 3 | Upload CSVs to volume | In Databricks: Data → Volumes → `Financial_Security.raw.mock_source_data` → Upload `transactions.csv`, `login_logs.csv` |
| 4 | `03_ingest_from_volume.py` | Ingest from volume into **bronze_transactions**, **bronze_login_logs** |
| 5 | `04_silver_lakeflow_pipeline.py` | Build **silver_user_activity** (wire >$10k within 1h of IP change, MFA flag, etc.) |
| 6 | `05_lakebase_schema.sql` | Run in **Lakebase** (Postgres): create **real_time_fraud_triage** table |
| 7 | `06_lakebase_triage_service.py` | Triage upsert (risk_score → Lakebase) for &lt;200ms path; set PGHOST/PGUSER/PGPASSWORD |
| 8 | `07_fraud_signatures_table.py` | Create **fraud_signatures** Delta table (CDF on) |
| 9 | `08_vector_search_setup.py` | Create Vector Search endpoint + index on fraud_signatures (sync until Ready) |
| 10 | `09_reasoning_agent.py` | **Reasoning Agent**: transaction → Vector Search → LLM → **plain-English reason** + risk_score 0–100 |
| 11 | `10_impossible_travel_monitor.py` | Flag users with **&gt;500 miles in 10 minutes** → **impossible_travel_flags** |
| 12 | `11a_gold_triage_table.py` | Create **gold** Delta table `Financial_Security.gold.real_time_fraud_triage` in UC (used by FPR view) — [see below](#step-12-how-to-create-the-gold-triage-table-on-databricks) |
| 13 | `11_genie_certified_sql.sql` | Create Genie **certified** views: FPR, Account Takeover Rate, wire $10k + MFA 24h |
| 14 | `13_batch_risk_to_lakebase.py` | Optional: batch run Reasoning Agent → upsert to Lakebase + gold Delta (so FPR view has data) |
| 15 | Live Fraud Queue app | Deploy `12_live_fraud_queue_app/` as Databricks App; add Lakebase DB resource — [see below](#step-15-how-to-create-and-deploy-the-live-fraud-queue-app) |

---

## Step 15: How to create and deploy the Live Fraud Queue app

The **Live Fraud Queue** is a Databricks App that lets analysts open a web UI, see yellow-flagged transactions (CHALLENGE / MONITOR) from Lakebase, and choose **Allow**, **Block**, or **Escalate**. The app reads from and updates the `real_time_fraud_triage` table in your Lakebase database.

### Prerequisites

- **Databricks Apps** enabled in your workspace (admin may need to turn this on).
- **Lakebase** instance created, with the `real_time_fraud_triage` table (from Step 6).
- **Databricks CLI** installed and logged in (for deploy from your machine), or use the workspace UI only.

---

### Option A: Create and deploy the app from the Databricks UI

1. **Upload the app folder to Workspace**
   - In the left sidebar, go to **Workspace**.
   - Navigate to your user folder (e.g. `/Users/your.email@company.com/`).
   - Create a folder, e.g. `live-fraud-queue`.
   - Upload these files into that folder (drag-and-drop or **⋮** → **Import**):
     - `app.yaml`
     - `app.py`
     - `requirements.txt`
     - `static/index.html` (inside a `static` folder)

   Your structure should look like:
   ```
   /Users/your.email@company.com/live-fraud-queue/
   ├── app.yaml
   ├── app.py
   ├── requirements.txt
   └── static/
       └── index.html
   ```

2. **Create the App**
   - In the left sidebar, click **Compute** → **Apps** (or **Apps** under your profile).
   - Click **Create app**.
   - **Name**: e.g. `Live Fraud Queue`.
   - **Source**: Choose **Workspace** and browse to the folder you created (e.g. `/Users/your.email@company.com/live-fraud-queue`).
   - Click **Create**.

3. **Add the Lakebase database resource**
   - Open your app → **Settings** or **Edit**.
   - Find **Resources** (or **App resources**).
   - Click **Add resource** → **Database** (or **Lakebase**).
   - Select your Lakebase instance (the one where you ran `05_lakebase_schema.sql`).
   - Set permission to **Can connect** (or equivalent so the app can read/write).
   - Save.

   The app’s `app.yaml` uses `valueFrom: database` for `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`. Databricks will inject these (and usually the password) when the Database resource is attached. If your Lakebase uses a separate password secret, add an env var (see Option B) for `PGPASSWORD`.

4. **Deploy**
   - Click **Deploy** (or **Save and deploy**).
   - Wait until the app status is **Running**.

5. **Open the app**
   - Click the app URL (e.g. `https://live-fraud-queue-xxxxx.cloud.databricks.com` or similar).
   - You should see the Live Fraud Queue page; it loads pending items from `/api/queue` and you can click **Allow** / **Block** / **Escalate** per row.

---

### Option B: Create and deploy the app using the Databricks CLI

1. **Install and log in to the CLI**
   ```bash
   # Install: https://docs.databricks.com/dev-tools/cli.html
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```

2. **Create the app (first time only)**
   ```bash
   cd /path/to/brick-con-3
   databricks apps create live-fraud-queue --description "Live Fraud Queue for triage review"
   ```

3. **Sync the app folder from your machine to Workspace**
   ```bash
   # Replace <your-username> with your Databricks username (e.g. your.email@company.com)
   databricks sync 12_live_fraud_queue_app/ /Users/<your-username>/live-fraud-queue
   ```
   Or with profile:
   ```bash
   databricks sync 12_live_fraud_queue_app/ /Users/<your-username>/live-fraud-queue -p <your-cli-profile>
   ```
   Ensure `app.yaml`, `app.py`, `requirements.txt`, and `static/index.html` are under the synced path.

4. **Deploy the app**
   ```bash
   databricks apps deploy live-fraud-queue --source-code-path /Users/<your-username>/live-fraud-queue
   ```

5. **Add the Lakebase resource (in the UI)**
   - Go to **Compute** → **Apps** → **live-fraud-queue** → **Edit**.
   - Add **Database** resource and select your Lakebase instance (as in Option A, step 3).
   - Redeploy if needed so the app picks up the new env vars.

6. **Get the app URL**
   ```bash
   databricks apps get live-fraud-queue
   ```
   Open the returned URL in a browser to use the queue.

---

### Troubleshooting

- **“No pending items”**  
  Run Step 14 (`13_batch_risk_to_lakebase.py`) so Lakebase has rows with `review_status = 'PENDING'` and `automated_action IN ('CHALLENGE', 'MONITOR')`.

- **Connection / 500 errors**  
  Confirm the Lakebase resource is attached and that the table `real_time_fraud_triage` exists in that database. If Lakebase requires a password not auto-injected, add an environment variable (e.g. from a secret) for `PGPASSWORD` in the app configuration.

- **App logs**  
  Open the app URL and add `/logz` (e.g. `https://your-app-url.../logz`) to see application logs, or use **Compute** → **Apps** → your app → **Logs**.

---

## Step 12: How to create the gold triage table on Databricks

**What Step 12 does**  
Step 12 creates a **Delta table in Unity Catalog** named `Financial_Security.gold.real_time_fraud_triage`. That table holds the same triage data (transaction_id, risk_score, automated_action, review_status, etc.) so that Genie views like **False Positive Ratio** (`v_false_positive_ratio`) can query it. Without this table, the view has no source and will fail.

**Why it’s in UC**  
Operational triage lives in **Lakebase** (Postgres) for low-latency blocking. Analytics and Genie use **Unity Catalog**; the gold Delta table is the UC copy that those views read from.

### How to create it on Databricks

**Option A: Run the notebook (recommended)**

1. **Open your workspace** and go to **Workspace** (or **Repos** if you use Git).
2. **Create a notebook**  
   - Click **Create** → **Notebook**.  
   - Name it e.g. `11a_gold_triage_table`.  
   - Set language to **Python** and attach a cluster (or create one).
3. **Put the notebook code in the notebook**  
   - Copy the contents of `brick-con-3/11a_gold_triage_table.py` (all cells) into this notebook.  
   - Or: upload the file from your machine (drag-and-drop or **File** → **Import**), then open it.
4. **Run the notebook**  
   - Run all cells (e.g. **Run all** in the toolbar, or run each cell with Shift+Enter).  
   - You should see: `Created: Financial_Security.gold.real_time_fraud_triage`.
5. **Confirm in Catalog**  
   - Go to **Catalog** → **Financial_Security** → **gold** → you should see table **real_time_fraud_triage**.

**Option B: Run equivalent SQL in a notebook or SQL warehouse**

In a **SQL** notebook or a **SQL warehouse** query, run:

```sql
CREATE CATALOG IF NOT EXISTS Financial_Security;
CREATE SCHEMA IF NOT EXISTS Financial_Security.gold;

CREATE TABLE IF NOT EXISTS Financial_Security.gold.real_time_fraud_triage (
  transaction_id   STRING NOT NULL,
  user_id          STRING NOT NULL,
  risk_score       DOUBLE NOT NULL,
  risk_level       STRING NOT NULL,
  automated_action STRING NOT NULL,
  action_reason    STRING,
  action_timestamp TIMESTAMP,
  review_status    STRING,
  reviewed_by      STRING,
  review_timestamp TIMESTAMP,
  created_at       TIMESTAMP
) USING DELTA
COMMENT 'Gold triage table for Genie FPR view';
```

Then run **Step 13** (`11_genie_certified_sql.sql`) to create the views. After that, **Step 14** (`13_batch_risk_to_lakebase.py`) will populate this gold table (and Lakebase) when you run the batch job.

---

## Requirements

- **Unity Catalog** enabled; create catalog permission.
- **Volume** for mock data; upload CSVs after running `02_mock_data_generator.py` locally.
- **Lakebase** instance; run `05_lakebase_schema.sql` and set env (or secrets) for `06_lakebase_triage_service.py` and the app.
- **Vector Search**: serverless, embedding endpoint (e.g. `databricks-bge-large-en`). In `08_vector_search_setup.py` set `EMBEDDING_MODEL` if different.
- **Foundation Model** for Reasoning Agent (e.g. `databricks-claude-sonnet-4-5`); set `SERVING_ENDPOINT` if needed.
- **Genie**: run `11a_gold_triage_table.py` first to create the gold Delta table; then run `11_genie_certified_sql.sql`. The batch job `13_batch_risk_to_lakebase.py` writes to both Lakebase and this gold table so `v_false_positive_ratio` has data.

---

## Customer requirements

- **Explainability**: Reasoning Agent returns a **plain-English `reason`** for each risk score (GDPR/CCPA).
- **&lt;200 ms**: Triage path = agent/model → `06_lakebase_triage_service.upsert_triage()` → Lakebase; keep networking and DB fast.
- **PII**: Use Unity Catalog masking and ABAC (tags) so fraud agents see only necessary data; mask card/identifiers as required.

---

## File layout (all under brick-con-3)

```
brick-con-3/
├── 01_unity_catalog_setup.py
├── 02_mock_data_generator.py
├── 03_ingest_from_volume.py
├── 04_silver_lakeflow_pipeline.py
├── 05_lakebase_schema.sql
├── 06_lakebase_triage_service.py
├── 07_fraud_signatures_table.py
├── 08_vector_search_setup.py
├── 09_reasoning_agent.py
├── 10_impossible_travel_monitor.py
├── 11a_gold_triage_table.py
├── 11_genie_certified_sql.sql
├── 12_live_fraud_queue_app/
│   ├── app.yaml
│   ├── app.py
│   ├── requirements.txt
│   └── static/
│       └── index.html
├── 13_batch_risk_to_lakebase.py
├── README.md
└── mock_data/                    (created by 02, then upload to volume)
    ├── transactions.csv
    └── login_logs.csv
```

---

## Quick prompts (for extension)

- **Mock data**: “Generate more mock transactions with wire and MFA patterns.”
- **Lakeflow**: “Add a continuous pipeline that reads from the volume and writes to silver.”
- **Genie**: “Show me all wire transfers over $10k where the user changed MFA in the last 24 hours.”
- **Cursor/Vibe + Databricks Connect**: “Using Databricks Connect, flag users whose geolocation jumps more than 500 miles in 10 minutes.” → implemented in `10_impossible_travel_monitor.py`.
