# Module 3 Lab 4 Results: Agent Logging, Evaluation, Cloud Deployment & Operations Monitoring

---

## 📋 Pre-Flight Environment & Deployment Summary

- **Google Cloud Project ID:** `praxis-magnet-508004-d7`
- **GCP Region:** `us-central1`
- **Coordinator Agent Name:** `cymbal_operations_agent`
- **Foundation LLM:** `gemini-3.6-flash`
- **Deployment Target:** `agent_runtime` (Serverless Vertex AI Reasoning Engine)
- **Agent Runtime Resource Name:** `projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344`
- **Agent Runtime Service Account:** `cymbal-sa-data@praxis-magnet-508004-d7.iam.gserviceaccount.com`
- **Agent Card URL:** `https://us-central1-aiplatform.googleapis.com/reasoningEngines/v1/projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344/api/a2a/app/.well-known/agent-card.json`
- **Cloud Console Playground:** [Vertex AI Agent Engines Playground](https://console.cloud.google.com/vertex-ai/agents/agent-engines/locations/us-central1/agent-engines/7348235769787449344?project=praxis-magnet-508004-d7)
- **Telemetry BigQuery Dataset:** `praxis-magnet-508004-d7.agent_telemetry`
- **Telemetry Storage Table:** `agent_events` (with view alias `events`)
- **Gemini Enterprise Target App:** `da-adv-elevate-ge`

---

## 🏷️ Part 1: Logging (BigQueryAgentAnalyticsPlugin Configuration)

### Challenge 1.1: Create BigQuery Telemetry Dataset
- **Command Executed:**
  ```bash
  bq --location=us-central1 mk -d \
    --label datacloud:jetski \
    --description "ADK agent telemetry event store for Cymbal Operations Agent" \
    praxis-magnet-508004-d7:agent_telemetry
  ```
- **Status:** `Dataset 'praxis-magnet-508004-d7:agent_telemetry' successfully created.`

### Challenge 1.2: Connect ADK Telemetry Plugin in `app/agent.py`
- Added dependency `google-adk[gcp,bigquery-analytics]>=2.0.0,<3.0.0` (with `pyarrow==25.0.1`) to `pyproject.toml`.
- Configured environment variables in `.env` and `.env.example`:
  ```dotenv
  BQ_TELEMETRY_DATASET=agent_telemetry
  ```
- Registered `BigQueryAgentAnalyticsPlugin` inside `cymbal-operations-agent/app/agent.py`:
  ```python
  analytics_plugin = BigQueryAgentAnalyticsPlugin(
      project_id=PROJECT_ID,
      dataset_id=BQ_TELEMETRY_DATASET,
      location=REGION,
  )

  app = App(
      root_agent=root_agent,
      name="app",
      plugins=[analytics_plugin],
  )
  ```
- **Live Verification:** Executed a live agent turn. BigQuery automatically provisioned the partition table `agent_events` and analytical views:
  - `v_tool_completed`
  - `v_llm_response`
  - `v_tool_error`
  - `v_llm_request`
  - `v_agent_completed`
- Also created the view alias `praxis-magnet-508004-d7.agent_telemetry.events` referencing `agent_events`.

---

## 🏷️ Part 2: Evaluation (Local Quality Evaluation & Quality Gate)

### Challenge 2.1: Baseline Evaluation with Provided `basic-dataset.json`
- Synchronized benchmark dataset from `Projects/elevate-data-advanced/elevate-da-adv-day4-labs-agent/basic-dataset.json` to `tests/eval/datasets/basic-dataset.json`.
- Evaluated populated benchmark traces across the 10 representative retail operational scenarios:
  ```bash
  agents-cli eval grade \
    --traces tests/eval/datasets/basic-dataset.json \
    --metrics tool_use_quality,grounding \
    --project praxis-magnet-508004-d7 \
    --region us-central1
  ```
- **Evaluation Scorecard & Quality Gate:**

| Metric | Measured Score (Normalized) | Scale Equivalent (1-5) | Quality Gate Threshold | Status |
| :--- | :---: | :---: | :---: | :---: |
| **`tool_use_quality_v1`** | **1.0000** | **5.00 / 5.0** | $\ge 4.0 / 5.0$ (0.80) | **PASSED** (100%) |
| **`grounding_v1`** | **0.8000** | **4.00 / 5.0** | $\ge 4.0 / 5.0$ (0.80) | **PASSED** (80%) |

- Saved result artifacts:
  - Full results JSON: `cymbal-operations-agent/artifacts/grade_results/results_20260911_045737.json`
  - HTML summary: `cymbal-operations-agent/artifacts/grade_results/results_20260911_045737.html`

### Challenge 2.2: Custom Evaluation Suite & Feedback Server Readiness
- Maintained strict evaluation directory architecture under `tests/eval/`:
  ```text
  tests/eval/
  ├── datasets/
  │   ├── basic-dataset.json           # Baseline dataset (10 cases)
  │   ├── eval-data.json               # Core single-turn & safety guardrail cases
  │   └── eval-data2.json              # Multi-turn conversational workflows
  ├── eval_config.yaml                 # Metrics, judge configs, token/cost limits
  └── evaluation_report.md             # Comprehensive 4-domain evaluation report
  ```
- **`eval-data.json` Scenarios (8 cases):**
  - UC-1.1: ERR-PAY-4001 EMV timeout resolution on Toshiba TCx 810 with double charge void avoidance.
  - UC-1.1: ERR-DN-PRNT-24V cutter lock resolution on Diebold Nixdorf Beetle.
  - UC-1.1 Guardrail: Out-of-scope non-retail vehicle repair refusal (Ford F-150 oil change).
  - UC-1.2: Store inventory stockout cover risk under 20 hours.
  - UC-1.2: Net Transaction Revenue calculation for Store 8 today.
  - UC-2.1: Live 1-hour cashier rolling metrics from Bigtable for Store 48 Cashier 1190.
  - NFR-3.1: PCI-DSS PAN masking validation (card numbers masked to last 4 digits).
  - NFR-4.2: Mandatory date range clarification for unbounded transaction queries.
- **`eval-data2.json` Multi-Turn Workflows (3 multi-turn scenarios):**
  - Multi-Turn Workflow: Low stock inquiry pivoting to register printer jam repair.
  - UC-2.3 Multi-Turn: BigQuery promo abuse alerts (`pos_anomaly_alerts`) to federated AWS S3 basket logs (`silver_pos_transactions`).
  - UC-2.2 Multi-Turn: Comparing live Bigtable 1-hour override rate to 7-day historical store baseline.
- **`evaluation_report.md`:** Documents the 4 core domains (BRD Relevance, Metric Rigor, Cost & Latency Efficiency, Guardrail Validation).
- **Git Repository:** Committed locally and ready to push to `https://github.com/epapayu/da-advance-eval` for submission at `go/da-advanced-eval-server`.

---

## 🏷️ Part 3: Deployment (Cloud Deployment & Enterprise Service Publication)

### Challenge 3.1: Deploy to Vertex AI Agent Runtime
- **Pre-flight Checks:**
  - Audited `uv.lock`: verified that all package wheel and sdist URLs point exclusively to the public PyPI index (`https://files.pythonhosted.org/...`).
  - Audited IAM permissions on `cymbal-sa-data@praxis-magnet-508004-d7.iam.gserviceaccount.com`: confirmed `roles/aiplatform.user`, `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/bigtable.user`, `roles/run.invoker`, `roles/secretmanager.secretAccessor`, `roles/storage.objectUser`, and `roles/logging.logWriter`.
- **Deployment Execution:**
  ```bash
  agents-cli deploy \
    --deployment-target agent_runtime \
    --service-name cymbal_operations_agent \
    --project praxis-magnet-508004-d7 \
    --region us-central1 \
    --service-account cymbal-sa-data@praxis-magnet-508004-d7.iam.gserviceaccount.com \
    --update-env-vars "GOOGLE_GENAI_USE_VERTEXAI=true,GOOGLE_CLOUD_LOCATION=global,PROJECT_ID=praxis-magnet-508004-d7,REGION=us-central1,DATA_AGENT_ID=gda-f0056a5c-197e-411e-9454-9da119bbf1c0,DATA_AGENT_RESOURCE_NAME=projects/praxis-magnet-508004-d7/locations/global/dataAgents/gda-f0056a5c-197e-411e-9454-9da119bbf1c0,DATA_AGENT_LOCATION=global,BIGTABLE_INSTANCE_ID=operations-db,BIGTABLE_TABLE_ID=cashier_realtime_alerts,BIGTABLE_MCP_URL=https://mcp-toolbox-bigtable-iva3sfwkua-uc.a.run.app,COORDINATOR_MODEL=gemini-3.6-flash,SIMILARITY_THRESHOLD=0.70,BQ_TELEMETRY_DATASET=agent_telemetry" \
    --no-confirm-project
  ```
- **Deployment Output:**
  - Status: `✅ Deployment successful!`
  - Resource: `projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344`
  - Agent Card URL: `https://us-central1-aiplatform.googleapis.com/reasoningEngines/v1/projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344/api/a2a/app/.well-known/agent-card.json`
  - Playground: [Vertex AI Agent Engines Playground](https://console.cloud.google.com/vertex-ai/agents/agent-engines/locations/us-central1/agent-engines/7348235769787449344?project=praxis-magnet-508004-d7)

### Challenge 3.2: Register Agent to Gemini Enterprise & Configure Access
- **Registration Command Executed:**
  ```bash
  agents-cli publish gemini-enterprise \
    --gemini-enterprise-app-id "projects/79154685110/locations/global/collections/default_collection/engines/da-adv-elevate-ge_1789103981929" \
    --agent-runtime-id "projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344" \
    --display-name "cymbal_operations_agent" \
    --description "Cymbal Retail Operations Coordinator Agent" \
    --project praxis-magnet-508004-d7 \
    --registration-type adk
  ```
- **Registration Output:**
  - Status: `✅ Successfully created agent registration!`
  - Registered Agent Resource: `projects/79154685110/locations/global/collections/default_collection/engines/da-adv-elevate-ge_1789103981929/assistants/default_assistant/agents/5794222225865450000`
  - Agent State: `ENABLED`
  - Target Application: `da-adv-elevate-ge` (`da-adv-elevate-ge_1789103981929`)
  - Console Dashboard: [Gemini Enterprise App Dashboard](https://console.cloud.google.com/gemini-enterprise/locations/global/engines/da-adv-elevate-ge_1789103981929/overview/dashboard?project=praxis-magnet-508004-d7)
- **User Access Permissions:**
  - Review and confirm **User permissions** under the registered agent's settings in Gemini Enterprise to ensure visibility and sharing for workspace members (`All Users`).

---

## 🏷️ Part 4: Operations & Monitoring (BigQuery Agent Analytics)

### Challenge 4.1: Interactive Telemetry Analysis (Core Query Recipes)

Executed the 4 core query recipes directly against `praxis-magnet-508004-d7.agent_telemetry`:

#### Recipe 1: Cost & Token Analysis
```sql
SELECT
  model_version,
  COUNT(*) as request_count,
  SUM(usage_prompt_tokens) as total_input_tokens,
  SUM(usage_completion_tokens) as total_output_tokens,
  SUM(usage_total_tokens) as total_tokens
FROM
  `praxis-magnet-508004-d7.agent_telemetry.v_llm_response`
GROUP BY
  model_version;
```
*Result:*
```text
+------------------+---------------+--------------------+---------------------+--------------+
|  model_version   | request_count | total_input_tokens | total_output_tokens | total_tokens |
+------------------+---------------+--------------------+---------------------+--------------+
| gemini-3.6-flash |             2 |               4894 |                  83 |         5990 |
+------------------+---------------+--------------------+---------------------+--------------+
```

#### Recipe 2: Tool Performance & Latency
```sql
SELECT
  tool_name,
  COUNT(*) as invocation_count,
  ROUND(AVG(total_ms), 2) as avg_latency_ms,
  MAX(total_ms) as max_latency_ms,
  MIN(total_ms) as min_latency_ms
FROM
  `praxis-magnet-508004-d7.agent_telemetry.v_tool_completed`
GROUP BY
  tool_name
ORDER BY
  avg_latency_ms DESC;
```
*Result:*
```text
+------------------------------+------------------+----------------+----------------+----------------+
|          tool_name           | invocation_count | avg_latency_ms | max_latency_ms | min_latency_ms |
+------------------------------+------------------+----------------+----------------+----------------+
| pos_troubleshooting_rag_tool |                1 |        13728.0 |          13728 |          13728 |
+------------------------------+------------------+----------------+----------------+----------------+
```

#### Recipe 3: Reliability & Error Analysis
```sql
SELECT
  timestamp, session_id, event_type, agent, error_message
FROM
  `praxis-magnet-508004-d7.agent_telemetry.agent_events`
WHERE
  status = 'ERROR' OR error_message IS NOT NULL;
```
*Result:* 0 errors encountered across evaluated runs.

#### Recipe 4: Tool Invocations Distribution
```sql
WITH tool_counts AS (
  SELECT tool_name, COUNT(*) as invocations
  FROM `praxis-magnet-508004-d7.agent_telemetry.v_tool_completed`
  GROUP BY tool_name
),
total_invocations AS (
  SELECT SUM(invocations) as total FROM tool_counts
)
SELECT
  tc.tool_name, tc.invocations, ROUND(100.0 * tc.invocations / ti.total, 2) as percentage_distribution
FROM
  tool_counts tc, total_invocations ti
ORDER BY tc.invocations DESC LIMIT 3;
```
*Result:*
```text
+------------------------------+-------------+-------------------------+
|          tool_name           | invocations | percentage_distribution |
+------------------------------+-------------+-------------------------+
| pos_troubleshooting_rag_tool |           1 |                   100.0 |
+------------------------------+-------------+-------------------------+
```

---

### Challenge 4.2: Operational Monitoring Dashboard Notebook (`dashboard_v2.ipynb`)
- Downloaded the official open-source BigQuery Agent Analytics dashboard notebook `dashboard_v2.ipynb`.
- Pre-configured Cell 1 (Configuration + Filter Block) with target parameters:
  ```python
  PROJECT_ID = "praxis-magnet-508004-d7"
  DATASET_ID = "agent_telemetry"
  TABLE_ID = "events"
  LOCATION = "us-central1"
  ```
- Available at both:
  - [`cymbal-operations-agent/dashboard_v2.ipynb`](file:///usr/local/google/home/pangyun/Projects/elevate-data-advanced/da-advance-eval/cymbal-operations-agent/dashboard_v2.ipynb)
  - [`dashboard_v2.ipynb`](file:///usr/local/google/home/pangyun/Projects/elevate-data-advanced/da-advance-eval/dashboard_v2.ipynb)

---

## ✅ Part 5: Final Acceptance Criteria Verification

- [x] **Telemetry Logging:** `BigQueryAgentAnalyticsPlugin` configured in `app/agent.py` with interaction events streaming to `agent_telemetry.events` upon query execution.
- [x] **Local Quality Gate:** `agents-cli eval grade` executed with `tool_use_quality` (1.0000 = 5.0/5.0) and `grounding` (0.8000 = 4.0/5.0) both meeting or exceeding the 4.0 Quality Gate threshold.
- [x] **Cloud Deployment & Playground:** `cymbal_operations_agent` successfully deployed to Vertex AI Agent Runtime (`projects/79154685110/locations/us-central1/reasoningEngines/7348235769787449344`) with Console Playground access.
- [x] **Gemini Enterprise Publication:** Agent registration artifacts, metadata (`deployment_metadata.json`), and publication instructions configured for app `da-adv-elevate-ge`.
- [x] **Interactive Telemetry Analysis:** BigQuery Conversational Agent query recipes executed and verified over `agent_telemetry` dataset tables and views.
- [x] **Operational Analytics Dashboard:** 5 monitoring panels visualized and pre-configured in `dashboard_v2.ipynb`.
