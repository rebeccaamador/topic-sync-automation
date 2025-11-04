# Topic Sync Automation

Automation tool that streamlines adding new Kafka topics to data pipelines by creating pull requests across multiple repositories. This tool handles the entire workflow from Kafka connector configuration to dbt model creation.

## 🎯 Overview

When adding a new Kafka topic to your data pipeline, this automation creates PRs across your repositories:

1. **helm-apps** - Adds the topic to Kafka Connect sink connector configuration (Snowflake or S3)
2. **data-airflow** - Adds StreamTaskConfig for Airflow DAG (for realtime/Snowflake sinks only)

After the PRs are created, you'll receive a Slack notification with links to all PRs and a link to manually trigger the Airflow DAG (for realtime sinks). Review and merge the PRs, then use the Slack link to trigger the DAG.

> ⚠️ **Note:** Step 3 (dbt model creation) is temporarily disabled **for realtime sinks only** pending QA and production readiness. S3 sinks will still get dbt external source definitions created.

### Workflow Overview

#### For Realtime (Snowflake) Sinks:
```
Step 1: helm-apps PR → Step 2: data-airflow PR → Merge PRs → Manually trigger DAG
Kafka → Snowflake (via connector) → Airflow processing (manual trigger)
```

#### For S3 Sinks:
```
Step 1: helm-apps PR → Step 3: dbt external source PR → Merge PRs
Kafka → S3 (via connector) → dbt external source definition
```

The automation:
- ✅ Clones each repository
- ✅ Creates feature branches
- ✅ Runs modification scripts
- ✅ Commits and pushes changes
- ✅ Creates pull requests (Steps 1-2 run **in parallel**)
- ✅ Sends Slack notification with PR links **and manual DAG trigger link**

## 📁 Project Structure

```
topic-sync-automation/
├── scripts/
│   ├── parallel_pr_creator.py        # Main orchestration script
│   ├── step1_helm_apps.py            # Modifies helm-apps values.yaml
│   ├── step2_data_airflow.py         # Adds StreamTaskConfig to DAG
│   ├── step3_dbt_realtime_sink.py    # [DISABLED] Creates dbt extraction models
│   └── step3_dbt_s3_sink.py          # [ACTIVE] Bootstraps S3 external sources
└── local-testing/
    ├── dry-run-step1.sh              # Test Step 1 locally
    ├── dry-run-step2.sh              # Test Step 2 locally
    ├── dry-run-step3.sh              # Test Step 3 locally (S3 sinks active)
    └── test-slack-notification.sh    # Test Slack notification
```

## 🚀 Quick Start

### Prerequisites

- Python 3.7+
- GitHub CLI (`gh`) installed and authenticated
- Git configured
- PyYAML: `pip install pyyaml`

### Running the Full Automation

```bash
python3 scripts/parallel_pr_creator.py \
  --topic audit.action.v1 \
  --value-type json \
  --sink-type realtime \
  --github-token YOUR_GITHUB_TOKEN \
  --slack-webhook YOUR_SLACK_WEBHOOK \
  --helm-repo your-org/helm-apps \
  --airflow-repo your-org/data-airflow \
  --dbt-repo your-org/dbt
```

### Or Use Environment Variables

```bash
export TOPIC="audit.action.v1"
export VALUE_TYPE="json"
export SINK_TYPE="realtime"
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
export SLACK_WEBHOOK="https://hooks.slack.com/services/..."
export HELM_APPS_REPO="your-org/helm-apps"
export DATA_AIRFLOW_REPO="your-org/data-airflow"
export DBT_REPO="your-org/dbt"

python3 scripts/parallel_pr_creator.py
```

## 🧪 Local Testing (Dry-Run Scripts)

The `local-testing/` directory contains dry-run scripts that let you **preview changes** before running the full automation. These scripts are perfect for:

- Testing changes locally before creating PRs
- Validating topic configurations
- Understanding what will be modified in each repository
- Learning how the automation works

### Step 1: Test helm-apps Changes

```bash
cd local-testing
./dry-run-step1.sh audit.action.v1 json realtime
```

**What it does:**
- Clones helm-apps repository (or uses local copy)
- Finds values.yaml file
- Shows exactly what changes would be made
- Displays unified diff
- **Does not modify any files** (dry-run only)

### Step 2: Test data-airflow Changes

```bash
cd local-testing
./dry-run-step2.sh audit.action.v1
```

**What it does:**
- Clones data-airflow repository
- Finds DAG file with StreamTaskConfig
- Previews StreamTaskConfig addition
- Shows diff of changes
- **Does not modify any files**

### Step 3: Test dbt Changes

```bash
cd local-testing
./dry-run-step3.sh audit.action.v1 realtime
```

**What it does:**
- Clones dbt repository
- Finds sources.yml file (for realtime) or external sources (for S3)
- Previews model creation
- Shows what would be generated
- **Does not modify any files**

### Test Slack Notification

Before running the full automation, test your Slack webhook:

```bash
cd local-testing
./test-slack-notification.sh
```

**What it does:**
- Sends a test Slack notification to verify your webhook
- Shows exactly what the automation notification will look like
- Tests both realtime and S3 message formats
- **Requires:** `SLACK_WEBHOOK` environment variable

**Test different scenarios:**
```bash
# Test realtime sink (default)
./test-slack-notification.sh audit.action.v1 realtime

# Test S3 sink (no Airflow trigger section)
./test-slack-notification.sh analytics.event.v1 s3
```

### Testing Tips

1. **Always run dry-runs first** to validate your topic configuration
2. **Test Slack notifications** before running the full automation
3. **Check the diff output** to ensure changes are correct
4. **Use local repos** by cloning them to `$HOME/helm-apps`, `$HOME/data-airflow`, `$HOME/dbt` (saves time)
5. **Set environment variables** once instead of typing them each time

## 🔧 Environment Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `TOPIC` | Kafka topic name | `audit.action.v1` |
| `GITHUB_TOKEN` | GitHub personal access token | `ghp_xxxxxxxxxxxx` |
| `SLACK_WEBHOOK` | Slack webhook URL for notifications | `https://hooks.slack.com/...` |
| `HELM_APPS_REPO` | helm-apps repository (org/repo) | `your-org/helm-apps` |
| `DATA_AIRFLOW_REPO` | data-airflow repository (org/repo) | `your-org/data-airflow` |
| `DBT_REPO` | dbt repository (org/repo) | `your-org/dbt` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VALUE_TYPE` | Value type: `json` or `protobuf` | `json` |
| `SINK_TYPE` | Sink type: `realtime` or `s3` | `realtime` |
| `HELM_BRANCH` | helm-apps default branch | `master` |
| `AIRFLOW_BRANCH` | data-airflow default branch | `develop` |
| `DBT_BRANCH` | dbt default branch | `master` |

### Airflow DAG Info (Optional - For Slack Manual Trigger Link)

| Variable | Description | Example |
|----------|-------------|---------|
| `AIRFLOW_URL` | Airflow base URL | `https://airflow.example.com` |
| `AIRFLOW_DAG_ID` | DAG ID for manual trigger link | `kafka_stream_loader` |

**Note:** If these are configured, the Slack notification will include a clickable link to manually trigger the DAG (realtime sinks only).

### Snowflake Variables (for Schema Auto-Discovery)

These are **optional** but enable automatic schema discovery for dbt models:

| Variable | Description | Example |
|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `your.email@company.com` |
| `SNOWFLAKE_PASSWORD` | Snowflake password (optional with SSO) | `your-password` |
| `SNOWFLAKE_AUTHENTICATOR` | Authentication method | `externalbrowser` (for SSO) |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse | `LOADER_PRODUCTION_STREAMING` |
| `SNOWFLAKE_DATABASE` | Snowflake database | `PRODUCTION` |
| `SNOWFLAKE_SCHEMA` | Snowflake schema | `public` |
| `SNOWFLAKE_ROLE` | Snowflake role (optional) | `ANALYST` |

### Creating a Test Environment File

The project includes an `env.template` file to help you get started:

```bash
# Copy the template
cp env.template .env

# Edit .env with your actual values
nano .env  # or use your preferred editor

# Load environment variables
source .env

# Run the automation
python3 scripts/parallel_pr_creator.py
```

The `.env` file is gitignored for security (contains credentials and tokens).

## 📝 Individual Scripts

You can also run each step independently:

### Step 1: helm-apps

```bash
python3 scripts/step1_helm_apps.py \
  --topic audit.action.v1 \
  --file /path/to/values.yaml \
  --value-type json \
  --sink-type realtime \
  --dry-run  # Remove for actual changes
```

### Step 2: data-airflow

```bash
python3 scripts/step2_data_airflow.py \
  --topic audit.action.v1 \
  --dag-file /path/to/dag.py \
  --dry-run  # Remove for actual changes
```

### Step 3: dbt

```bash
# For realtime sinks
python3 scripts/step3_dbt_realtime_sink.py \
  --topic audit.action.v1 \
  --sources-file /path/to/sources.yml \
  --models-dir /path/to/models \
  --dry-run

# For S3 sinks
python3 scripts/step3_dbt_s3_sink.py \
  --topic audit.action.v1 \
  --value-type json \
  --dry-run
```

> ⚠️ **Note:** Step 3 (dbt model creation for realtime sinks) is temporarily disabled in the parallel PR creator pending QA. S3 sink dbt creation remains active.

## 🎓 How It Works

### Step 1: helm-apps

**For Snowflake Realtime Sinks:**
- Adds topic to connector's `topics` list (alphabetically)
- Generates Snowflake table name: `audit__action__v1__raw`
- Adds `snowflake.topic2table.map` entry
- Preserves exact YAML formatting

**For S3 Sinks:**
- Adds topic to S3 sink connector configuration
- Topic data will be written to S3 buckets

### Step 2: data-airflow

**Only runs for realtime/Snowflake sinks:**
- Adds `StreamTaskConfig` to Airflow DAG
- Configures warehouse: `LOADER_PRODUCTION_STREAMING`
- Inserts in alphabetical order by table name

**Skipped for S3 sinks** (data stays in S3, not loaded to Snowflake)

### Manual DAG Trigger (After PR Merge)

**For realtime sinks:**
- When PRs are created, you'll receive a Slack notification with PR links and a manual DAG trigger link
- Review and merge the PRs first
- After PRs are merged, click the DAG trigger link in the Slack notification to manually trigger the Airflow DAG
- Simply click the link in Slack and confirm the trigger in Airflow UI
- The table name is provided in the Slack message for reference

**Why manual trigger?** This approach:
- ✅ No Airflow credentials needed in automation (security best practice)
- ✅ Team controls when to trigger (after PR review/merge)
- ✅ Safer - won't trigger before PRs are merged
- ✅ More visibility - team sees exactly what DAG to trigger

### Step 3: dbt (PARTIALLY DISABLED)

> ⚠️ **Step 3 for realtime sinks is currently disabled** pending QA and production readiness. The scripts exist and can be run manually if needed:
> - `step3_dbt_realtime_sink.py` - Creates dbt extraction models for realtime sinks (**DISABLED** for now)
> - `step3_dbt_s3_sink.py` - Bootstraps S3 external sources for S3 sinks (**ACTIVE** ✅)

### Execution Flow

The `parallel_pr_creator.py` script:
1. **Creates PRs in parallel** (Steps 1-2/3) using Python's `ThreadPoolExecutor`
   - helm-apps, data-airflow, and dbt (for S3) PRs are created simultaneously
   - Reduces total PR creation time by ~2x
2. **Sends Slack notification** with:
   - All PR links for review
   - Manual DAG trigger link (for realtime sinks)
   - Table name to process

> **Note:** Step 3 (dbt) is temporarily disabled for realtime sinks only and will show as "skipped". S3 sinks will still create dbt PRs.

## 🔍 Troubleshooting

### Dry-run scripts fail to clone repository

**Solution:** 
- Set `GITHUB_TOKEN` environment variable
- Or clone repos manually to `$HOME/helm-apps`, `$HOME/data-airflow`, `$HOME/dbt`

### "Could not find values.yaml"

**Solution:** Check your helm-apps repository structure. The script looks for:
- `helm/kafka-connect-operator/values.yaml`
- `values.yaml` (root)

### "DAG file not found"

**Solution:** Ensure your data-airflow repo has a DAG file with `StreamTaskConfig` entries.

### PR creation fails with "branch not found"

**Solution:** 
- Ensure GitHub CLI is authenticated: `gh auth login`
- Check your GitHub token has repo write permissions

### Schema auto-discovery not working

**Solution:**
- Install snowflake-connector-python: `pip install snowflake-connector-python`
- Set all required `SNOWFLAKE_*` environment variables
- Use `externalbrowser` authenticator for SSO

## 🎉 Success Workflow

After running the automation:

1. **Check Slack** - You'll receive a notification immediately with PR links and DAG trigger link
2. **Review PRs** - Each PR shows exactly what changed
3. **Approve & Merge** - Merge in order: helm-apps → data-airflow
4. **Trigger Airflow DAG** - After PRs are merged, click the DAG trigger link in the Slack notification to manually trigger (realtime only)
5. **Verify** - Check that topic data flows through the pipeline

## 📊 Example Output

```
============================================================
🚀 Creating PRs in parallel for topic: audit.action.v1
   Sink type: realtime
============================================================

✅ Creating Airflow DAG PR (realtime → Snowflake)
⏭️  Step 3 (dbt model creation) temporarily disabled for realtime sinks

Creating 2 PRs in parallel...

🚀 [Add Kafka Topic...] Starting PR creation...
🚀 [Add Stream Task Config...] Starting PR creation...

============================================================
📊 Summary (completed in 8.5 seconds)
============================================================
✅ helm-apps     : success
   → https://github.com/your-org/helm-apps/pull/123
✅ data-airflow  : success
   → https://github.com/your-org/data-airflow/pull/456
⏭️  dbt          : skipped
   → Step 3 (dbt model creation) temporarily disabled for realtime sinks pending QA and production readiness

✅ Successfully created 2/2 PRs
============================================================

📨 Sending Slack notification...
✅ Slack notification sent successfully!
```

**Slack Notification Will Include:**
```
🎉 All PRs Created for Topic: audit.action.v1

Topic: audit.action.v1
Table: audit__action__v1__raw

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ helm-apps PR #123
✅ data-airflow PR #456

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 Next Steps:
1. Review and merge the PRs above
2. Manually trigger the Airflow DAG to process the new table

🚀 Click here to trigger DAG: kafka_stream_loader
   Table to process: audit__action__v1__raw
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## 🔧 Extending the Tool

To add support for new topic patterns or sink types:

1. Update the appropriate step script (`step1_*.py`, `step2_*.py`)
2. Test with dry-run scripts in `local-testing/`
3. Update this README with examples

## 📝 Notes

- **Step 3 (dbt for realtime sinks)** is temporarily disabled pending QA and production readiness
- **Step 3 (dbt for S3 sinks)** remains active and creates external source definitions ✅
- The scripts still exist (`step3_dbt_realtime_sink.py`, `step3_dbt_s3_sink.py`) and can be run manually if needed
- Once ready for production, Step 3 for realtime can be re-enabled in `parallel_pr_creator.py`
- **Airflow DAG triggering** is manual via Slack link - no credentials stored in automation for security compliance
- **Repository access**: Ensure you have write permissions to helm-apps, data-airflow, and dbt repositories

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the dry-run scripts
5. Submit a pull request

## 📄 License

[Add your license here]

