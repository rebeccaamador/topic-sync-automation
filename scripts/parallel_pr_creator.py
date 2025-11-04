#!/usr/bin/env python3
"""
Pure Python Parallel PR Creator
Creates PRs for all 3 repos in parallel, then sends Slack notification.

Can run locally or in GitHub Actions.
"""

import argparse
import subprocess
import tempfile
import shutil
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple
import time
import requests


class ParallelPRCreator:
    """Creates PRs in parallel for all 3 repositories."""
    
    def __init__(self, topic: str, value_type: str, sink_type: str,
                 github_token: str, slack_webhook: str,
                 helm_repo: str, airflow_repo: str, dbt_repo: str,
                 helm_branch: str = "master",
                 airflow_branch: str = "develop",
                 dbt_branch: str = "master",
                 airflow_url: str = None,
                 airflow_dag_id: str = None):
        self.topic = topic
        self.value_type = value_type
        self.sink_type = sink_type
        self.github_token = github_token
        self.slack_webhook = slack_webhook
        
        self.helm_repo = helm_repo
        self.airflow_repo = airflow_repo
        self.dbt_repo = dbt_repo
        
        self.helm_branch = helm_branch
        self.airflow_branch = airflow_branch
        self.dbt_branch = dbt_branch
        
        # Airflow DAG configuration (for manual trigger link in Slack)
        self.airflow_url = airflow_url
        self.airflow_dag_id = airflow_dag_id
        
        # Generate table name
        self.table_name = topic.replace('.', '__').replace('-', '_') + '__raw'
        
        # Store results
        self.results = {}
    
    def run_command(self, cmd: list, cwd: str = None, env: dict = None) -> Tuple[int, str, str]:
        """Run a shell command and return exit code, stdout, stderr."""
        full_env = os.environ.copy()
        if env:
            full_env.update(env)
        
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                env=full_env,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return 1, "", "Command timed out"
        except Exception as e:
            return 1, "", str(e)
    
    def create_helm_apps_pr(self) -> Dict:
        """Create PR for helm-apps repository."""
        step_name = "Add Kafka Topic to Sink Connector Configuration in helm-apps"
        print(f"🚀 [{step_name}] Starting PR creation...")
        
        try:
            # Create temp directory
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_dir = os.path.join(tmpdir, "helm-apps")
                
                # Clone repository
                print(f"📥 [{step_name}] Cloning repository...")
                clone_url = f"https://x-access-token:{self.github_token}@github.com/{self.helm_repo}.git"
                ret, out, err = self.run_command(['git', 'clone', clone_url, repo_dir])
                if ret != 0:
                    return {'status': 'failed', 'error': f'Clone failed: {err}', 'pr_url': None}
                
                # Configure git
                self.run_command(['git', 'config', 'user.name', 'github-actions[bot]'], cwd=repo_dir)
                self.run_command(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'], cwd=repo_dir)
                
                # Checkout default branch
                self.run_command(['git', 'checkout', self.helm_branch], cwd=repo_dir)
                
                # Create feature branch
                branch_name = f"add-snowflake-sink-{self.topic.replace('.', '-')}"
                ret, out, err = self.run_command(['git', 'checkout', '-b', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Branch creation failed: {err}', 'pr_url': None}
                
                # Run step1 script
                print(f"🔧 [{step_name}] Running modification script...")
                script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'step1_helm_apps.py')
                
                # Find values.yaml in helm-apps repo structure
                values_file = os.path.join(repo_dir, 'helm', 'kafka-connect-operator', 'values.yaml')
                if not os.path.exists(values_file):
                    # Try alternate location
                    values_file = os.path.join(repo_dir, 'values.yaml')
                
                if not os.path.exists(values_file):
                    return {'status': 'failed', 'error': 'Could not find values.yaml', 'pr_url': None}
                
                print(f"📄 [{step_name}] Using values file: {values_file.replace(repo_dir, '.')}")
                
                ret, out, err = self.run_command([
                    'python3', script_path,
                    '--topic', self.topic,
                    '--file', values_file,
                    '--value-type', self.value_type,
                    '--sink-type', self.sink_type
                ], cwd=repo_dir)
                
                if ret != 0:
                    print(f"⚠️  [{step_name}] Script failed: {err}")
                    # Continue anyway for testing
                
                # Check for changes
                ret, out, err = self.run_command(['git', 'status', '--porcelain'], cwd=repo_dir)
                if not out.strip():
                    print(f"ℹ️  [{step_name}] No changes detected. This might mean the topic is already sinked for this sink type.")
                    return {'status': 'no_changes', 'error': None, 'pr_url': None}
                else:
                    print(f"📝 [{step_name}] Changes detected:")
                    for line in out.strip().split('\n'):
                        print(f"   {line}")
                
                # Commit changes
                print(f"💾 [{step_name}] Committing changes...")
                self.run_command(['git', 'add', '.'], cwd=repo_dir)
                commit_msg = f"""Add Kafka topic: {self.topic}

- Topic: {self.topic}
- Value Type: {self.value_type}
- Sink Type: {self.sink_type}

Auto-generated by parallel PR creator"""
                
                ret, out, err = self.run_command(['git', 'commit', '-m', commit_msg], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Commit failed: {err}', 'pr_url': None}
                
                # Push branch
                print(f"📤 [{step_name}] Pushing branch...")
                ret, out, err = self.run_command(['git', 'push', 'origin', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Push failed: {err}', 'pr_url': None}
                
                # Verify the push worked
                print(f"🔍 [{step_name}] Verifying branch exists on remote...")
                ret, out, err = self.run_command(['git', 'ls-remote', '--heads', 'origin', branch_name], cwd=repo_dir)
                if not out.strip():
                    print(f"⚠️  [{step_name}] Warning: Branch not found on remote after push")
                else:
                    print(f"✅ [{step_name}] Branch verified on remote")
                
                # Small delay to ensure GitHub has processed the push
                import time
                time.sleep(2)
                
                # Create PR using GitHub CLI
                print(f"📝 [{step_name}] Creating pull request...")
                pr_body = f"""## Kafka Topic Configuration

**Topic:** `{self.topic}`
**Value Type:** {self.value_type}
**Sink Type:** {self.sink_type}

### Changes
- Added topic to Kafka Connect Snowflake sink configuration

---
*Auto-generated by Python Parallel PR Creator*"""
                
                env = {'GH_TOKEN': self.github_token}
                ret, out, err = self.run_command([
                    'gh', 'pr', 'create',
                    '--repo', self.helm_repo,
                    '--title', f'Add Kafka topic: {self.topic}',
                    '--body', pr_body,
                    '--base', self.helm_branch
                ], cwd=repo_dir, env=env)
                
                # Debug gh pr create output
                print(f"   DEBUG: gh exit code: {ret}")
                print(f"   DEBUG: gh stdout: '{out}'")
                print(f"   DEBUG: gh stderr: '{err}'")
                
                if ret != 0:
                    print(f"❌ [{step_name}] PR creation failed: {err}")
                    return {'status': 'failed', 'error': f'PR creation failed: {err}', 'pr_url': None}
                
                pr_url = out.strip()
                print(f"✅ [{step_name}] PR created: {pr_url}")
                
                return {'status': 'success', 'error': None, 'pr_url': pr_url}
                
        except Exception as e:
            print(f"❌ [{step_name}] Exception: {str(e)}")
            return {'status': 'failed', 'error': str(e), 'pr_url': None}
    
    def create_data_airflow_pr(self) -> Dict:
        """Create PR for data-airflow repository."""
        step_name = "Add Stream Task Config to Airflow DAG in data-airflow"
        print(f"🚀 [{step_name}] Starting PR creation...")
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_dir = os.path.join(tmpdir, "data-airflow")
                
                # Clone
                print(f"📥 [{step_name}] Cloning repository...")
                clone_url = f"https://x-access-token:{self.github_token}@github.com/{self.airflow_repo}.git"
                ret, out, err = self.run_command(['git', 'clone', clone_url, repo_dir])
                if ret != 0:
                    return {'status': 'failed', 'error': f'Clone failed: {err}', 'pr_url': None}
                
                # Configure git
                self.run_command(['git', 'config', 'user.name', 'github-actions[bot]'], cwd=repo_dir)
                self.run_command(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'], cwd=repo_dir)
                
                # Checkout default branch
                self.run_command(['git', 'checkout', self.airflow_branch], cwd=repo_dir)
                
                # Find DAG file
                ret, out, err = self.run_command([
                    'grep', '-r', 'StreamTaskConfig', 'dags/',
                    '--include=*.py'
                ], cwd=repo_dir)
                
                dag_file = None
                if ret == 0 and out:
                    dag_file = out.split(':')[0]
                    dag_file = os.path.join(repo_dir, dag_file)
                
                # Create feature branch
                branch_name = f"add-stream-config-{self.topic.replace('.', '-')}"
                ret, out, err = self.run_command(['git', 'checkout', '-b', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Branch creation failed: {err}', 'pr_url': None}
                
                # Run step2 script
                print(f"🔧 [{step_name}] Running modification script...")
                script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'step2_data_airflow.py')
                
                if dag_file:
                    ret, out, err = self.run_command([
                        'python3', script_path,
                        '--topic', self.topic,
                        '--dag-file', dag_file
                    ], cwd=repo_dir)
                    if ret != 0:
                        print(f"⚠️  [{step_name}] Script failed: {err}")
                else:
                    print(f"⚠️  [{step_name}] Could not find DAG file with StreamTaskConfig")
                    return {'status': 'failed', 'error': 'DAG file not found', 'pr_url': None}
                
                # Check for changes
                ret, out, err = self.run_command(['git', 'status', '--porcelain'], cwd=repo_dir)
                if not out.strip():
                    print(f"ℹ️  [{step_name}] No changes detected. This might mean the stream config already exists for this topic.")
                    return {'status': 'no_changes', 'error': None, 'pr_url': None}
                else:
                    print(f"📝 [{step_name}] Changes detected:")
                    for line in out.strip().split('\n'):
                        print(f"   {line}")
                
                # Commit
                print(f"💾 [{step_name}] Committing changes...")
                self.run_command(['git', 'add', '.'], cwd=repo_dir)
                commit_msg = f"""Add stream config: {self.table_name}

- Topic: {self.topic}
- Table: {self.table_name}

Auto-generated by parallel PR creator"""
                
                ret, out, err = self.run_command(['git', 'commit', '-m', commit_msg], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Commit failed: {err}', 'pr_url': None}
                
                # Push
                print(f"📤 [{step_name}] Pushing branch...")
                ret, out, err = self.run_command(['git', 'push', 'origin', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Push failed: {err}', 'pr_url': None}
                
                # Verify the push worked
                print(f"🔍 [{step_name}] Verifying branch exists on remote...")
                ret, out, err = self.run_command(['git', 'ls-remote', '--heads', 'origin', branch_name], cwd=repo_dir)
                if not out.strip():
                    print(f"⚠️  [{step_name}] Warning: Branch not found on remote after push")
                else:
                    print(f"✅ [{step_name}] Branch verified on remote")
                
                # Create PR
                print(f"📝 [{step_name}] Creating pull request...")
                pr_body = f"""## Stream Configuration

**Topic:** `{self.topic}`
**Table:** `{self.table_name}`

### Changes
- Added StreamTaskConfig to Airflow DAG
- Configured warehouse: LOADER_PRODUCTION_STREAMING

---
*Auto-generated by Python Parallel PR Creator*"""
                
                env = {'GH_TOKEN': self.github_token}
                ret, out, err = self.run_command([
                    'gh', 'pr', 'create',
                    '--repo', self.airflow_repo,
                    '--title', f'Add stream config: {self.table_name}',
                    '--body', pr_body,
                    '--base', self.airflow_branch
                ], cwd=repo_dir, env=env)
                
                if ret != 0:
                    print(f"❌ [{step_name}] PR creation failed: {err}")
                    return {'status': 'failed', 'error': f'PR creation failed: {err}', 'pr_url': None}
                
                pr_url = out.strip()
                print(f"✅ [{step_name}] PR created: {pr_url}")
                
                return {'status': 'success', 'error': None, 'pr_url': pr_url}
                
        except Exception as e:
            print(f"❌ [{step_name}] Exception: {str(e)}")
            return {'status': 'failed', 'error': str(e), 'pr_url': None}
    
    def create_dbt_pr(self) -> Dict:
        """Create PR for dbt repository."""
        step_name = "Create Materialized Model for Kafka Topic in dbt"
        print(f"🚀 [{step_name}] Starting PR creation...")
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_dir = os.path.join(tmpdir, "dbt")
                
                # Clone
                print(f"📥 [{step_name}] Cloning repository...")
                clone_url = f"https://x-access-token:{self.github_token}@github.com/{self.dbt_repo}.git"
                ret, out, err = self.run_command(['git', 'clone', clone_url, repo_dir])
                if ret != 0:
                    return {'status': 'failed', 'error': f'Clone failed: {err}', 'pr_url': None}
                
                # Configure git
                self.run_command(['git', 'config', 'user.name', 'github-actions[bot]'], cwd=repo_dir)
                self.run_command(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'], cwd=repo_dir)
                
                # Checkout default branch
                self.run_command(['git', 'checkout', self.dbt_branch], cwd=repo_dir)
                
                # Create feature branch
                branch_name = f"add-extraction-{self.topic.replace('.', '-')}"
                ret, out, err = self.run_command(['git', 'checkout', '-b', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Branch creation failed: {err}', 'pr_url': None}
                
                # Run step3 script
                print(f"🔧 [{step_name}] Running modification script...")
                script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'step3_dbt_realtime_sink.py')
                
                # Find sources file and models directory
                sources_file = os.path.join(repo_dir, 'models', 'staging', 'kafka_realtime', '_kafka_connect__sources.yml')
                models_dir = os.path.join(repo_dir, 'models')
                
                if not os.path.exists(sources_file):
                    return {'status': 'failed', 'error': 'Could not find _kafka_connect__sources.yml', 'pr_url': None}
                
                print(f"📄 [{step_name}] Using sources file: {sources_file.replace(repo_dir, '.')}")
                
                ret, out, err = self.run_command([
                    'python3', script_path,
                    '--topic', self.topic,
                    '--sources-file', sources_file,
                    '--models-dir', models_dir
                ], cwd=repo_dir)
                if ret != 0:
                    print(f"⚠️  [{step_name}] Script failed: {err}")
                
                # Check for changes
                ret, out, err = self.run_command(['git', 'status', '--porcelain'], cwd=repo_dir)
                if not out.strip():
                    print(f"ℹ️  [{step_name}] No changes detected. This might mean the materialized model already exists for this topic.")
                    return {'status': 'no_changes', 'error': None, 'pr_url': None}
                else:
                    print(f"📝 [{step_name}] Changes detected:")
                    for line in out.strip().split('\n'):
                        print(f"   {line}")
                
                # Commit
                print(f"💾 [{step_name}] Committing changes...")
                self.run_command(['git', 'add', '.'], cwd=repo_dir)
                commit_msg = f"""Add dbt extraction layer: {self.table_name}

- Topic: {self.topic}
- Table: {self.table_name}

Auto-generated by parallel PR creator"""
                
                ret, out, err = self.run_command(['git', 'commit', '-m', commit_msg], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Commit failed: {err}', 'pr_url': None}
                
                # Push
                print(f"📤 [{step_name}] Pushing branch...")
                ret, out, err = self.run_command(['git', 'push', 'origin', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Push failed: {err}', 'pr_url': None}
                
                # Verify the push worked
                print(f"🔍 [{step_name}] Verifying branch exists on remote...")
                ret, out, err = self.run_command(['git', 'ls-remote', '--heads', 'origin', branch_name], cwd=repo_dir)
                if not out.strip():
                    print(f"⚠️  [{step_name}] Warning: Branch not found on remote after push")
                else:
                    print(f"✅ [{step_name}] Branch verified on remote")
                
                # Create PR
                print(f"📝 [{step_name}] Creating pull request...")
                pr_body = f"""## dbt Extraction Layer

**Topic:** `{self.topic}`
**Table:** `{self.table_name}`

### Changes
- Added source definition
- Created staging model
- Added tests

---
*Auto-generated by Python Parallel PR Creator*"""
                
                env = {'GH_TOKEN': self.github_token}
                ret, out, err = self.run_command([
                    'gh', 'pr', 'create',
                    '--repo', self.dbt_repo,
                    '--title', f'Add extraction layer: {self.table_name}',
                    '--body', pr_body,
                    '--base', self.dbt_branch
                ], cwd=repo_dir, env=env)
                
                if ret != 0:
                    print(f"❌ [{step_name}] PR creation failed: {err}")
                    return {'status': 'failed', 'error': f'PR creation failed: {err}', 'pr_url': None}
                
                pr_url = out.strip()
                print(f"✅ [{step_name}] PR created: {pr_url}")
                
                return {'status': 'success', 'error': None, 'pr_url': pr_url}
                
        except Exception as e:
            print(f"❌ [{step_name}] Exception: {str(e)}")
            return {'status': 'failed', 'error': str(e), 'pr_url': None}
    
    def create_dbt_bootstrap_pr(self) -> Dict:
        """Create PR for dbt repository with S3 external source bootstrap."""
        step_name = "dbt-bootstrap"
        print(f"🚀 [{step_name}] Starting PR creation (S3 external sources)...")
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_dir = os.path.join(tmpdir, "dbt")
                
                # Clone
                print(f"📥 [{step_name}] Cloning repository...")
                clone_url = f"https://x-access-token:{self.github_token}@github.com/{self.dbt_repo}.git"
                ret, out, err = self.run_command(['git', 'clone', clone_url, repo_dir])
                if ret != 0:
                    return {'status': 'failed', 'error': f'Clone failed: {err}', 'pr_url': None}
                
                # Configure git
                self.run_command(['git', 'config', 'user.name', 'github-actions[bot]'], cwd=repo_dir)
                self.run_command(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'], cwd=repo_dir)
                
                # Checkout default branch
                self.run_command(['git', 'checkout', self.dbt_branch], cwd=repo_dir)
                
                # Create feature branch
                branch_name = f"add-s3-external-{self.topic.replace('.', '-')}"
                ret, out, err = self.run_command(['git', 'checkout', '-b', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Branch creation failed: {err}', 'pr_url': None}
                
                # Copy our bootstrap script to the repo (ensures we use the latest version)
                print(f"🔧 [{step_name}] Setting up bootstrap script...")
                automation_script = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'step3_dbt_s3_sink.py')
                repo_scripts_dir = os.path.join(repo_dir, 'scripts')
                bootstrap_script = os.path.join(repo_scripts_dir, 'step3_dbt_s3_sink.py')
                
                # Ensure scripts directory exists
                os.makedirs(repo_scripts_dir, exist_ok=True)
                
                # Copy our version of the script
                import shutil
                if os.path.exists(automation_script):
                    shutil.copy(automation_script, bootstrap_script)
                    print(f"   ✅ Copied bootstrap script from automation repo (ensures latest version)")
                elif not os.path.exists(bootstrap_script):
                    return {'status': 'failed', 'error': 'Bootstrap script not found in automation repo', 'pr_url': None}
                
                # Run the bootstrap script with topic and value-type arguments
                print(f"   Command: python3 step3_dbt_s3_sink.py --topic {self.topic} --value-type {self.value_type}")
                ret, out, err = self.run_command([
                    'python3', bootstrap_script,
                    '--topic', self.topic,
                    '--value-type', self.value_type
                ], cwd=repo_dir)
                
                # Show bootstrap script output for debugging
                if out.strip():
                    print(f"   Bootstrap output: {out.strip()}")
                if err.strip():
                    print(f"   Bootstrap stderr: {err.strip()}")
                
                if ret != 0:
                    print(f"⚠️  [{step_name}] Bootstrap script failed: {err}")
                    return {'status': 'failed', 'error': f'Bootstrap failed: {err}', 'pr_url': None}
                
                # Check for changes
                ret, out, err = self.run_command(['git', 'status', '--porcelain'], cwd=repo_dir)
                if not out.strip():
                    print(f"ℹ️  [{step_name}] No changes detected after running bootstrap script.")
                    print(f"   This means one of two things:")
                    print(f"   1. The topic '{self.topic}' is already in the remote repository's bootstrap script")
                    print(f"   2. The bootstrap script ran but generated the same files that already exist")
                    
                    # Check if topic is in the remote bootstrap script
                    bootstrap_content_check = self.run_command(['grep', '-c', self.topic, bootstrap_script], cwd=repo_dir)
                    if bootstrap_content_check[0] == 0 and int(bootstrap_content_check[1].strip() or '0') > 0:
                        print(f"   ✓ Confirmed: Topic found {bootstrap_content_check[1].strip()} time(s) in remote bootstrap script")
                    
                    return {'status': 'no_changes', 'error': f'Topic already exists in {self.dbt_repo} external sources', 'pr_url': None}
                else:
                    print(f"📝 [{step_name}] Changes detected:")
                    for line in out.strip().split('\n'):
                        print(f"   {line}")
                
                # Commit
                print(f"💾 [{step_name}] Committing changes...")
                self.run_command(['git', 'add', '.'], cwd=repo_dir)
                commit_msg = f"""Add S3 external source: {self.topic}

- Topic: {self.topic}
- Value type: {self.value_type}
- Table: {self.table_name}

Auto-generated by parallel PR creator"""
                
                ret, out, err = self.run_command(['git', 'commit', '-m', commit_msg], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Commit failed: {err}', 'pr_url': None}
                
                # Push
                print(f"📤 [{step_name}] Pushing branch...")
                ret, out, err = self.run_command(['git', 'push', 'origin', branch_name], cwd=repo_dir)
                if ret != 0:
                    return {'status': 'failed', 'error': f'Push failed: {err}', 'pr_url': None}
                
                # Verify the push worked
                print(f"🔍 [{step_name}] Verifying branch exists on remote...")
                ret, out, err = self.run_command(['git', 'ls-remote', '--heads', 'origin', branch_name], cwd=repo_dir)
                if not out.strip():
                    print(f"⚠️  [{step_name}] Warning: Branch not found on remote after push")
                else:
                    print(f"✅ [{step_name}] Branch verified on remote")
                
                # Create PR
                print(f"📝 [{step_name}] Creating pull request...")
                pr_body = f"""## S3 External Source Bootstrap

**Topic:** `{self.topic}`
**Value Type:** `{self.value_type}`
**Table:** `{self.table_name}`

### Changes
- Generated external source definition for S3 data
- Created base model: `stg_kafka__{self.table_name}__external.sql`
- Created typecast model: `stg_kafka__{self.table_name}.sql`

### How to Use
After merge, the S3 data will be available as an external source in Snowflake.

---
*Auto-generated by Python Parallel PR Creator*"""
                
                env = {'GH_TOKEN': self.github_token}
                ret, out, err = self.run_command([
                    'gh', 'pr', 'create',
                    '--repo', self.dbt_repo,
                    '--title', f'Add S3 external source: {self.topic}',
                    '--body', pr_body,
                    '--base', self.dbt_branch
                ], cwd=repo_dir, env=env)
                
                # Debug gh pr create output
                print(f"   DEBUG: gh exit code: {ret}")
                print(f"   DEBUG: gh stdout: '{out}'")
                print(f"   DEBUG: gh stderr: '{err}'")
                
                if ret != 0:
                    print(f"❌ [{step_name}] PR creation failed: {err}")
                    return {'status': 'failed', 'error': f'PR creation failed: {err}', 'pr_url': None}
                
                pr_url = out.strip()
                print(f"✅ [{step_name}] PR created: {pr_url}")
                return {'status': 'success', 'error': None, 'pr_url': pr_url}
                
        except Exception as e:
            print(f"❌ [{step_name}] Exception: {str(e)}")
            return {'status': 'failed', 'error': str(e), 'pr_url': None}
    
    def send_slack_notification(self, results: Dict) -> bool:
        """Send Slack notification with PR links and next steps."""
        print("\n📨 Sending Slack notification...")
        
        # Build Slack message
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🎉 All PRs Created for Topic: {self.topic}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Topic:*\n`{self.topic}`"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Table:*\n`{self.table_name}`"
                    }
                ]
            },
            {"type": "divider"}
        ]
        
        # Define step order for display (customize this order as needed)
        step_order = ['helm-apps', 'data-airflow', 'dbt', 'dbt-bootstrap']
        
        # Add each PR result in the defined order
        for step in step_order:
            if step not in results:
                continue
            result = results[step]
            if result['status'] == 'success' and result.get('pr_url'):
                pr_number = result['pr_url'].split('/')[-1]
                
                # Customize message based on step
                if step == 'helm-apps':
                    message = f"*Add Kafka Topic to Sink Connector Configuration in helm-apps*\n<{result['pr_url']}|View PR #{pr_number}> ✅"
                elif step == 'data-airflow':
                    message = f"*Add Stream Task Config to Airflow DAG in data-airflow*\n<{result['pr_url']}|View PR #{pr_number}> ✅"
                elif step == 'dbt':
                    message = f"*Create Materialized Model for Kafka Topic in dbt*\n<{result['pr_url']}|View PR #{pr_number}> ✅"
                elif step == 'dbt-bootstrap':
                    message = f"*Add S3 External Source Bootstrap in dbt*\n<{result['pr_url']}|View PR #{pr_number}> ✅"
                else:
                    message = f"*Step: {step}*\n<{result['pr_url']}|View PR #{pr_number}> ✅"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                })
            else:
                # Handle non-success cases
                if result['status'] == 'skipped':
                    status_icon = "⏭️"
                    error_msg = result['error'] or 'Skipped'
                elif result['status'] == 'no_changes':
                    status_icon = "⚠️"
                    if step == 'helm-apps':
                        error_msg = 'No changes detected. Topic might already be configured in sink.'
                    elif step == 'data-airflow':
                        error_msg = 'No changes detected. Stream config might already exist.'
                    elif step == 'dbt' or step == 'dbt-bootstrap':
                        error_msg = 'No changes detected. Model or external source might already exist.'
                    else:
                        error_msg = 'No changes detected'
                else:
                    status_icon = "❌"
                    error_msg = result['error'] or 'Failed'
                
                # Customize step title
                if step == 'helm-apps':
                    step_title = "Add Kafka Topic to Sink Connector Configuration in helm-apps"
                elif step == 'data-airflow':
                    step_title = "Add Stream Task Config to Airflow DAG in data-airflow"
                elif step == 'dbt':
                    step_title = "Create Materialized Model for Kafka Topic in dbt"
                elif step == 'dbt-bootstrap':
                    step_title = "Add S3 External Source Bootstrap in dbt"
                else:
                    step_title = f"Step: {step}"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{step_title}*\n{status_icon} {error_msg}"
                    }
                })
        
        blocks.append({"type": "divider"})
        
        # Add "Next Steps" section for realtime sinks with Airflow DAG info
        if self.sink_type == 'realtime' and self.airflow_url and self.airflow_dag_id:
            dag_trigger_url = f"{self.airflow_url}/dags/{self.airflow_dag_id}/trigger"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📋 Next Steps:*\n1. Review and merge the PRs above\n2. Manually trigger the Airflow DAG to process the new table"
                }
            })
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚀 *<{dag_trigger_url}|Click here to trigger DAG: {self.airflow_dag_id}>*\n\n*Table to process:* `{self.table_name}`"
                }
            })
            blocks.append({"type": "divider"})
        
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "👆 Click the links above to review and approve each PR in GitHub"
            }]
        })
        
        # Send to Slack
        payload = {
            "text": f"PRs created for topic: {self.topic}",
            "blocks": blocks
        }
        
        try:
            response = requests.post(self.slack_webhook, json=payload, timeout=10)
            response.raise_for_status()
            print("✅ Slack notification sent successfully!")
            return True
        except Exception as e:
            print(f"❌ Failed to send Slack notification: {e}")
            return False
    
    def create_all_prs_parallel(self) -> Dict:
        """Create all PRs in parallel using ThreadPoolExecutor."""
        print(f"\n{'='*60}")
        print(f"🚀 Creating PRs in parallel for topic: {self.topic}")
        print(f"   Sink type: {self.sink_type}")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        
        # Build futures dict based on sink type
        futures = {}
        
        # Step 1: Always create Helm Apps PR (connector configuration)
        futures_to_create = [
            (self.create_helm_apps_pr, 'helm-apps')
        ]
        
        # Different workflow for realtime vs S3
        if self.sink_type == 'realtime':
            # Realtime: Data goes to Snowflake → Need Airflow processing
            # NOTE: Step 3 (dbt) is temporarily disabled pending QA
            futures_to_create.append((self.create_data_airflow_pr, 'data-airflow'))
            print("✅ Creating Airflow DAG PR (realtime → Snowflake)")
            print("⏭️  Step 3 (dbt model creation) temporarily disabled for realtime sinks")
        else:
            # S3: Data goes to S3 → Need external source bootstrap in dbt
            futures_to_create.append((self.create_dbt_bootstrap_pr, 'dbt'))
            print(f"✅ Creating dbt external source PR (S3 → Snowflake)")
            print(f"⏭️  Skipping Airflow DAG PR (not needed for {self.sink_type})")
        
        print(f"\nCreating {len(futures_to_create)} PRs in parallel...\n")
        
        # Create thread pool
        max_workers = len(futures_to_create)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            for func, name in futures_to_create:
                futures[executor.submit(func)] = name
            
            # Collect results as they complete
            results = {}
            for future in as_completed(futures):
                step_name = futures[future]
                try:
                    result = future.result()
                    results[step_name] = result
                except Exception as e:
                    print(f"❌ [{step_name}] Failed with exception: {e}")
                    results[step_name] = {
                        'status': 'failed',
                        'error': str(e),
                        'pr_url': None
                    }
        
        # Add skipped steps to results
        if self.sink_type != 'realtime':
            results['data-airflow'] = {
                'status': 'skipped',
                'error': f'Airflow DAG not needed for {self.sink_type} sinks (data in S3, not Snowflake)',
                'pr_url': None
            }
        
        # Step 3 (dbt) is temporarily disabled for realtime sinks only
        if self.sink_type == 'realtime':
            results['dbt'] = {
                'status': 'skipped',
                'error': 'Step 3 (dbt model creation) temporarily disabled for realtime sinks pending QA and production readiness',
                'pr_url': None
            }
        
        elapsed = time.time() - start_time
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Summary (completed in {elapsed:.1f} seconds)")
        print(f"{'='*60}")
        
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        total_expected = len(futures_to_create)
        
        for step, result in results.items():
            status_icon = {
                'success': '✅',
                'no_changes': 'ℹ️ ',
                'skipped': '⏭️ ',
                'failed': '❌'
            }.get(result['status'], '❓')
            
            print(f"{status_icon} {step:15s}: {result['status']}")
            if result['pr_url']:
                print(f"   → {result['pr_url']}")
            elif result['status'] == 'skipped':
                print(f"   → {result.get('error', 'Skipped')}")
        
        print(f"\n✅ Successfully created {success_count}/{total_expected} PRs")
        if self.sink_type != 'realtime':
            print(f"⏭️  Skipped Airflow DAG (not needed for {self.sink_type} → S3)")
            # Only show dbt bootstrap success if it actually succeeded
            dbt_bootstrap_status = results.get('dbt-bootstrap', {}).get('status')
            if dbt_bootstrap_status == 'success':
                print(f"✅ Created dbt bootstrap PR (S3 external sources)")
            elif dbt_bootstrap_status == 'no_changes':
                print(f"ℹ️  No dbt changes (topic may already exist in external sources)")
        print(f"{'='*60}\n")
        
        # Send Slack notification
        self.send_slack_notification(results)
        
        return results


def main():
    parser = argparse.ArgumentParser(
        description='Create PRs in parallel for all 3 repos (Pure Python, no Codefresh/Node.js)',
        epilog='Arguments can be provided via CLI or environment variables. CLI args take precedence.'
    )
    
    # All arguments are now optional if provided via env vars
    parser.add_argument('--topic', help='Kafka topic name (e.g., audit.action.v1) [env: TOPIC]')
    parser.add_argument('--github-token', help='GitHub Personal Access Token [env: GITHUB_TOKEN]')
    parser.add_argument('--slack-webhook', help='Slack webhook URL [env: SLACK_WEBHOOK]')
    
    # Repository arguments
    parser.add_argument('--helm-repo', help='helm-apps repo (org/repo) [env: HELM_APPS_REPO]')
    parser.add_argument('--airflow-repo', help='data-airflow repo (org/repo) [env: DATA_AIRFLOW_REPO]')
    parser.add_argument('--dbt-repo', help='dbt repo (org/repo) [env: DBT_REPO]')
    
    # Optional arguments
    parser.add_argument('--value-type', choices=['json', 'protobuf'], 
                       help='Value type [env: VALUE_TYPE] (default: json)')
    parser.add_argument('--sink-type', choices=['realtime', 's3'],
                       help='Sink type [env: SINK_TYPE] (default: realtime)')
    parser.add_argument('--helm-branch', help='helm-apps default branch [env: HELM_BRANCH] (default: master)')
    parser.add_argument('--airflow-branch', help='data-airflow default branch [env: AIRFLOW_BRANCH] (default: develop)')
    parser.add_argument('--dbt-branch', help='dbt default branch [env: DBT_BRANCH] (default: master)')
    
    # Airflow DAG info (for Slack notification manual trigger link)
    parser.add_argument('--airflow-url', help='Airflow base URL for manual trigger link [env: AIRFLOW_URL]')
    parser.add_argument('--airflow-dag-id', help='Airflow DAG ID for manual trigger link [env: AIRFLOW_DAG_ID]')
    
    args = parser.parse_args()
    
    # Helper function to get value from CLI args or env vars
    def get_value(arg_value, env_var, default=None, required=True):
        value = arg_value or os.getenv(env_var, default)
        if required and not value:
            parser.error(f"Missing required argument: --{env_var.lower().replace('_', '-')} or env var {env_var}")
        return value
    
    # Get all values, prioritizing CLI args over env vars
    topic = get_value(args.topic, 'TOPIC')
    github_token = get_value(args.github_token, 'GITHUB_TOKEN')
    slack_webhook = get_value(args.slack_webhook, 'SLACK_WEBHOOK')
    helm_repo = get_value(args.helm_repo, 'HELM_APPS_REPO')
    airflow_repo = get_value(args.airflow_repo, 'DATA_AIRFLOW_REPO')
    dbt_repo = get_value(args.dbt_repo, 'DBT_REPO')
    
    # Optional with defaults
    value_type = get_value(args.value_type, 'VALUE_TYPE', default='json', required=False)
    sink_type = get_value(args.sink_type, 'SINK_TYPE', default='realtime', required=False)
    helm_branch = get_value(args.helm_branch, 'HELM_BRANCH', default='master', required=False)
    airflow_branch = get_value(args.airflow_branch, 'AIRFLOW_BRANCH', default='develop', required=False)
    dbt_branch = get_value(args.dbt_branch, 'DBT_BRANCH', default='master', required=False)
    
    # Airflow DAG info (for Slack notification) - optional
    airflow_url = get_value(args.airflow_url, 'AIRFLOW_URL', default=None, required=False)
    airflow_dag_id = get_value(args.airflow_dag_id, 'AIRFLOW_DAG_ID', default=None, required=False)
    
    # Create PR creator
    creator = ParallelPRCreator(
        topic=topic,
        value_type=value_type,
        sink_type=sink_type,
        github_token=github_token,
        slack_webhook=slack_webhook,
        helm_repo=helm_repo,
        airflow_repo=airflow_repo,
        dbt_repo=dbt_repo,
        helm_branch=helm_branch,
        airflow_branch=airflow_branch,
        dbt_branch=dbt_branch,
        airflow_url=airflow_url,
        airflow_dag_id=airflow_dag_id
    )
    
    # Run parallel PR creation
    results = creator.create_all_prs_parallel()
    
    # Exit with appropriate code
    failed_count = sum(1 for r in results.values() if r['status'] == 'failed')
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == '__main__':
    main()

