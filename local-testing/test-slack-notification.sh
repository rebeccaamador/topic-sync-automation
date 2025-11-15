#!/bin/bash
# Local Testing Script for Slack Notifications
# This script sends a test Slack notification to verify your webhook and see what the message looks like

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Slack Notification Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Configuration
# ============================================

# Check if we have the required variables
if [ -z "$SLACK_WEBHOOK" ]; then
    echo -e "${YELLOW}⚠️  SLACK_WEBHOOK not set${NC}"
    echo -e "Please enter your Slack webhook URL:"
    read -p "Webhook URL: " SLACK_WEBHOOK
fi

echo -e "${GREEN}✅ Slack webhook configured${NC}"
echo ""

# Get test parameters (or use defaults)
TOPIC="${1:-customer.action.v1}"
TABLE_NAME=$(echo "$TOPIC" | sed 's/\./__/g' | sed 's/-/_/g')__raw
SINK_TYPE="${2:-realtime}"
AIRFLOW_URL="${AIRFLOW_URL:-https://airflow.company.com}"
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-kafka_stream_loader}"

echo -e "${BLUE}📋 Test Parameters:${NC}"
echo -e "  Topic:       ${GREEN}$TOPIC${NC}"
echo -e "  Table:       ${GREEN}$TABLE_NAME${NC}"
echo -e "  Sink Type:   ${GREEN}$SINK_TYPE${NC}"
echo -e "  Airflow URL: ${GREEN}$AIRFLOW_URL${NC}"
echo -e "  DAG ID:      ${GREEN}$AIRFLOW_DAG_ID${NC}"
echo ""

# ============================================
# Build Slack Message
# ============================================

echo -e "${BLUE}🔨 Building Slack message...${NC}"

# Base blocks with PR results
BLOCKS='[
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "🎉 All PRs Created for Topic: '"$TOPIC"'",
            "emoji": true
        }
    },
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*Topic:*\n`'"$TOPIC"'`"
            },
            {
                "type": "mrkdwn",
                "text": "*Table:*\n`'"$TABLE_NAME"'`"
            }
        ]
    },
    {"type": "divider"},
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Add Kafka Topic to Sink Connector Configuration in helm-apps*\n<https://github.com/org/helm-apps/pull/123|View PR #123> ✅"
        }
    },
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Add Stream Task Config to Airflow DAG in data-airflow*\n<https://github.com/org/data-airflow/pull/456|View PR #456> ✅"
        }
    },
    {"type": "divider"}'

# Add Step 3 (dbt) status - different for realtime vs S3
if [ "$SINK_TYPE" = "realtime" ]; then
    # Realtime: dbt is active and creates extraction models
    BLOCKS="$BLOCKS"',
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Create Materialized Model for Kafka Topic in dbt*\n<https://github.com/org/dbt/pull/789|View PR #789> ✅"
        }
    },
    {"type": "divider"}'
else
    # S3: dbt is active and creates external sources
    BLOCKS="$BLOCKS"',
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Add S3 External Source Bootstrap in dbt*\n<https://github.com/org/dbt/pull/789|View PR #789> ✅"
        }
    },
    {"type": "divider"}'
fi

# Add "Next Steps" section for realtime sinks
if [ "$SINK_TYPE" = "realtime" ]; then
    DAG_TRIGGER_URL="$AIRFLOW_URL/dags/$AIRFLOW_DAG_ID/trigger"
    
    BLOCKS="$BLOCKS"',
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*📋 Next Steps:*\n1. Review and merge the PRs above\n2. Manually trigger the Airflow DAG to process the new table"
        }
    },
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "🚀 *<'"$DAG_TRIGGER_URL"'|Click here to trigger DAG: '"$AIRFLOW_DAG_ID"'>*\n\n*Table to process:* `'"$TABLE_NAME"'`"
        }
    },
    {"type": "divider"}'
fi

# Add footer
BLOCKS="$BLOCKS"',
    {
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "👆 Click the links above to review and approve each PR in GitHub"
        }]
    }
]'

# Build the full payload
PAYLOAD='{
    "text": "PRs created for topic: '"$TOPIC"'",
    "blocks": '"$BLOCKS"'
}'

echo -e "${GREEN}✅ Message built${NC}"
echo ""

# ============================================
# Show Preview
# ============================================

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Message Preview${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Subject:${NC} PRs created for topic: $TOPIC"
echo ""
echo -e "${GREEN}Content:${NC}"
echo -e "  🎉 All PRs Created for Topic: $TOPIC"
echo -e "  Topic: $TOPIC"
echo -e "  Table: $TABLE_NAME"
echo -e "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  ✅ helm-apps PR #123"

if [ "$SINK_TYPE" = "realtime" ]; then
    echo -e "  ✅ data-airflow PR #456"
    echo -e "  ✅ dbt PR #789"
    echo -e "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  📋 Next Steps:"
    echo -e "  1. Review and merge the PRs above"
    echo -e "  2. Manually trigger the Airflow DAG"
    echo -e "  🚀 Click to trigger: $AIRFLOW_DAG_ID"
    echo -e "  Table: $TABLE_NAME"
else
    echo -e "  ✅ dbt (S3 external source) PR #789"
    echo -e "  ⏭️  data-airflow (not needed for S3)"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Send to Slack
# ============================================

read -p "Send this test message to Slack? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}❌ Cancelled${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}📤 Sending to Slack...${NC}"

# Send the request
HTTP_CODE=$(curl -s -w "%{http_code}" -o /tmp/slack_response.txt -X POST "$SLACK_WEBHOOK" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")

# Read response body from temp file
RESPONSE_BODY=$(cat /tmp/slack_response.txt 2>/dev/null || echo "")

# Check response
if [ "$HTTP_CODE" = "200" ]; then
    echo -e "${GREEN}✅ Message sent successfully!${NC}"
    echo -e "${GREEN}✅ Check your Slack channel for the message${NC}"
else
    echo -e "${RED}❌ Failed to send message${NC}"
    echo -e "${RED}   HTTP Status: $HTTP_CODE${NC}"
    echo -e "${RED}   Response: $RESPONSE_BODY${NC}"
    exit 1
fi

# Cleanup temp file
rm -f /tmp/slack_response.txt

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Complete${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}💡 Tips:${NC}"
echo -e "  • Customize the message by editing the script"
echo -e "  • Test different sink types: ./test-slack-notification.sh <topic> realtime"
echo -e "  • Test with S3: ./test-slack-notification.sh <topic> s3"
echo ""

