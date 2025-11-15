#!/bin/bash
# Dry Run Script for Step 2 - Preview data-airflow changes
# This script simulates Step 2 workflow without making any changes

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(dirname "$SCRIPT_DIR")/scripts"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 2 Dry Run - data-airflow Preview${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Configuration
# ============================================

# Check if we have the required variables
if [ -z "$DATA_AIRFLOW_REPO" ]; then
    echo -e "${YELLOW}⚠️  DATA_AIRFLOW_REPO not set${NC}"
    echo -e "Please enter the data-airflow repository (format: owner/repo)"
    read -p "Repository: " DATA_AIRFLOW_REPO
fi

# Parse owner and repo name
REPO_OWNER=$(echo "$DATA_AIRFLOW_REPO" | cut -d'/' -f1)
REPO_NAME=$(echo "$DATA_AIRFLOW_REPO" | cut -d'/' -f2)

echo -e "\n${GREEN}Repository:${NC} $DATA_AIRFLOW_REPO"

# ============================================
# Get Input Parameters
# ============================================

# Get topic name
if [ -z "$1" ]; then
    echo -e "\n${YELLOW}Enter Kafka topic name:${NC}"
    echo -e "Example: ${GREEN}customer.action.v1${NC}"
    read -p "Topic: " TOPIC
else
    TOPIC="$1"
fi

echo -e "\n${BLUE}Configuration:${NC}"
echo -e "  Topic: ${GREEN}$TOPIC${NC}"
echo ""

# ============================================
# Setup Temporary Workspace
# ============================================

TEMP_DIR=$(mktemp -d)
DATA_AIRFLOW_DIR="$TEMP_DIR/data-airflow"

echo -e "${BLUE}🔧 Setting up workspace...${NC}"

# Cleanup on exit
cleanup() {
    echo -e "\n${BLUE}🧹 Cleaning up...${NC}"
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# ============================================
# Clone or Copy Repository
# ============================================

echo -e "${BLUE}📥 Fetching data-airflow repository...${NC}"

# Check if GITHUB_TOKEN is available for private repos
if [ -n "$GITHUB_TOKEN" ]; then
    CLONE_URL="https://${GITHUB_TOKEN}@github.com/${DATA_AIRFLOW_REPO}.git"
else
    CLONE_URL="https://github.com/${DATA_AIRFLOW_REPO}.git"
    echo -e "${YELLOW}⚠️  No GITHUB_TOKEN set - cloning as public repo${NC}"
fi

# Try to clone
if git clone --depth 1 "$CLONE_URL" "$DATA_AIRFLOW_DIR" 2>/dev/null; then
    echo -e "${GREEN}✅ Repository cloned${NC}"
else
    echo -e "${RED}❌ Failed to clone repository${NC}"
    echo -e "${YELLOW}💡 Trying to find local copy...${NC}"
    
    # Try to find local data-airflow repo
    POSSIBLE_PATHS=(
        "$HOME/data-airflow"
        "$(dirname "$(dirname "$SCRIPT_DIR")")/data-airflow"
    )
    
    FOUND=false
    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -d "$path" ]; then
            echo -e "${GREEN}✅ Found local copy: $path${NC}"
            cp -r "$path" "$DATA_AIRFLOW_DIR"
            FOUND=true
            break
        fi
    done
    
    if [ "$FOUND" = false ]; then
        echo -e "${RED}❌ Could not find data-airflow repository${NC}"
        echo -e "${YELLOW}Please either:${NC}"
        echo -e "  1. Set GITHUB_TOKEN environment variable"
        echo -e "  2. Clone the repo manually to one of these locations:"
        for path in "${POSSIBLE_PATHS[@]}"; do
            echo -e "     - $path"
        done
        exit 1
    fi
fi

# ============================================
# Find DAG file with StreamTaskConfig
# ============================================

echo -e "\n${BLUE}🔍 Searching for DAG file with StreamTaskConfig...${NC}"

DAG_FILE=$(grep -r "StreamTaskConfig" "$DATA_AIRFLOW_DIR/dags/" --include="*.py" 2>/dev/null | head -1 | cut -d: -f1)

if [ -z "$DAG_FILE" ]; then
    echo -e "${RED}❌ Could not find DAG file with StreamTaskConfig${NC}"
    echo -e "${YELLOW}Searching for any DAG files:${NC}"
    find "$DATA_AIRFLOW_DIR/dags/" -name "*.py" -type f | head -5
    exit 1
fi

echo -e "${GREEN}✅ Found: $DAG_FILE${NC}"

# Show file info
FILE_SIZE=$(wc -l < "$DAG_FILE" | tr -d ' ')
echo -e "${BLUE}ℹ️  File info:${NC}"
echo -e "   Path: $(echo "$DAG_FILE" | sed "s|$DATA_AIRFLOW_DIR/||")"
echo -e "   Lines: $FILE_SIZE"

# Count existing StreamTaskConfig entries
STREAM_COUNT=$(grep -c "StreamTaskConfig" "$DAG_FILE" || echo "0")
echo -e "   Existing StreamTaskConfig entries: $STREAM_COUNT"

# ============================================
# Check Python Dependencies
# ============================================

echo -e "\n${BLUE}🔍 Checking dependencies...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ python3 not found${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Dependencies OK${NC}"

# ============================================
# Run Dry Run
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Running Dry Run Analysis${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Run the step2 script in dry-run mode
python3 "$SCRIPTS_DIR/step2_data_airflow.py" \
    --topic "$TOPIC" \
    --dag-file "$DAG_FILE" \
    --dry-run

# ============================================
# Show Next Steps
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Next Steps${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${GREEN}To apply these changes:${NC}"
echo -e "1. Run the GitHub Actions workflow:"
echo -e "   ${BLUE}https://github.com/$(git -C "$(dirname "$SCRIPT_DIR")" remote get-url origin 2>/dev/null | sed 's/.*github.com[:/]\(.*\)\.git/\1/' || echo 'topic-sync-automation')/actions${NC}"
echo -e ""
echo -e "2. Or manually trigger via GitHub CLI:"
echo -e "   ${YELLOW}gh workflow run step2-data-airflow.yml \\${NC}"
echo -e "   ${YELLOW}  --field topic=\"$TOPIC\"${NC}"
echo -e ""
echo -e "3. Or run locally (not recommended):"
echo -e "   ${YELLOW}# Clone data-airflow repo${NC}"
echo -e "   ${YELLOW}cd path/to/data-airflow${NC}"
echo -e "   ${YELLOW}python3 $(dirname "$SCRIPT_DIR")/scripts/step2_data_airflow.py \\${NC}"
echo -e "   ${YELLOW}  --topic \"$TOPIC\" \\${NC}"
echo -e "   ${YELLOW}  --dag-file dags/your-dag-file.py${NC}"

echo -e "\n${GREEN}✅ Dry run complete!${NC}\n"
