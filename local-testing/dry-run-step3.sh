#!/bin/bash
# Dry Run Script for Step 3 - Preview dbt changes
# This script simulates Step 3 workflow without making any changes

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Step 3 Dry Run - dbt Preview${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Configuration
# ============================================

# Check if we have the required variables
if [ -z "$DBT_REPO" ]; then
    echo -e "${YELLOW}⚠️  DBT_REPO not set${NC}"
    echo -e "Please enter the dbt repository (format: owner/repo)"
    read -p "Repository: " DBT_REPO
fi

# Parse owner and repo name
REPO_OWNER=$(echo "$DBT_REPO" | cut -d'/' -f1)
REPO_NAME=$(echo "$DBT_REPO" | cut -d'/' -f2)

echo -e "\n${GREEN}Repository:${NC} $DBT_REPO"

# ============================================
# Get Input Parameters
# ============================================

# Get topic name
if [ -z "$1" ]; then
    echo -e "\n${YELLOW}Enter Kafka topic name:${NC}"
    echo -e "Example: ${GREEN}audit.action.v1${NC}"
    read -p "Topic: " TOPIC
else
    TOPIC="$1"
fi

# Get sink type to determine which workflow to use
if [ -z "$2" ]; then
    echo -e "\n${YELLOW}Select sink type:${NC}"
    echo -e "  1) realtime (default) - Creates dbt extraction models"
    echo -e "  2) s3 - Runs bootstrap for external sources"
    read -p "Choice [1]: " SINK_TYPE_CHOICE
    SINK_TYPE_CHOICE=${SINK_TYPE_CHOICE:-1}
    
    if [ "$SINK_TYPE_CHOICE" = "2" ]; then
        SINK_TYPE="s3"
    else
        SINK_TYPE="realtime"
    fi
else
    SINK_TYPE="$2"
fi

echo -e "\n${BLUE}Configuration:${NC}"
echo -e "  Topic:      ${GREEN}$TOPIC${NC}"
echo -e "  Sink Type:  ${GREEN}$SINK_TYPE${NC}"
echo ""

# ============================================
# Setup Temporary Workspace
# ============================================

TEMP_DIR=$(mktemp -d)
DBT_DIR="$TEMP_DIR/dbt"

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

echo -e "${BLUE}📥 Fetching dbt repository...${NC}"

# Check if GITHUB_TOKEN is available for private repos
if [ -n "$GITHUB_TOKEN" ]; then
    CLONE_URL="https://${GITHUB_TOKEN}@github.com/${DBT_REPO}.git"
else
    CLONE_URL="https://github.com/${DBT_REPO}.git"
    echo -e "${YELLOW}⚠️  No GITHUB_TOKEN set - cloning as public repo${NC}"
fi

# Try to clone
if git clone --depth 1 "$CLONE_URL" "$DBT_DIR" 2>/dev/null; then
    echo -e "${GREEN}✅ Repository cloned${NC}"
else
    echo -e "${RED}❌ Failed to clone repository${NC}"
    echo -e "${YELLOW}💡 Trying to find local copy...${NC}"
    
    # Try to find local dbt repo
    POSSIBLE_PATHS=(
        "$HOME/dbt"
        "$(dirname "$SCRIPT_DIR")/dbt"
        "$(dirname "$SCRIPT_DIR")/test-repos/dbt"
    )
    
    FOUND=false
    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -d "$path" ]; then
            echo -e "${GREEN}✅ Found local copy: $path${NC}"
            cp -r "$path" "$DBT_DIR"
            FOUND=true
            break
        fi
    done
    
    if [ "$FOUND" = false ]; then
        echo -e "${RED}❌ Could not find dbt repository${NC}"
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
# Check Python Dependencies
# ============================================

echo -e "\n${BLUE}🔍 Checking dependencies...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ python3 not found${NC}"
    exit 1
fi

if ! python3 -c "import yaml" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  PyYAML not installed${NC}"
    echo -e "${BLUE}Installing PyYAML...${NC}"
    pip3 install pyyaml --quiet
fi

echo -e "${GREEN}✅ Dependencies OK${NC}"

# ============================================
# Run Dry Run Based on Sink Type
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Running Dry Run Analysis${NC}"
echo -e "${BLUE}========================================${NC}\n"

if [ "$SINK_TYPE" = "s3" ]; then
    # S3 workflow - Bootstrap external sources
    echo -e "${GREEN}S3 Sink Workflow - Bootstrap External Sources${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
    
    BOOTSTRAP_SCRIPT="$DBT_DIR/scripts/step3_dbt_s3_sink.py"
    
    if [ ! -f "$BOOTSTRAP_SCRIPT" ]; then
        echo -e "${RED}❌ Error: Bootstrap script not found at $BOOTSTRAP_SCRIPT${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}Current bootstrap script location:${NC}"
    echo -e "  $(echo "$BOOTSTRAP_SCRIPT" | sed "s|$DBT_DIR/||")"
    echo ""
    
    # Determine value type (default to json for dry-run)
    VALUE_TYPE="${3:-json}"
    
    # Show what would be added
    echo -e "${BLUE}Would run bootstrap script with:${NC}"
    echo -e "  Topic: ${GREEN}$TOPIC${NC}"
    echo -e "  Value Type: ${GREEN}$VALUE_TYPE${NC}"
    echo -e "  Table: ${GREEN}$(echo "$TOPIC" | sed 's/\./__/g' | sed 's/-/_/g')${NC}"
    echo ""
    
    # Run the bootstrap script in a safe way (to a temp output directory)
    echo -e "${BLUE}Running bootstrap preview...${NC}"
    TEMP_OUTPUT="$TEMP_DIR/preview"
    mkdir -p "$TEMP_OUTPUT"
    
    # Copy the bootstrap script to temp and run it there
    cp -r "$DBT_DIR/scripts" "$TEMP_OUTPUT/"
    cp -r "$DBT_DIR/models" "$TEMP_OUTPUT/" 2>/dev/null || mkdir -p "$TEMP_OUTPUT/models/staging/kafka/external"
    
    cd "$TEMP_OUTPUT"
    
    if python3 scripts/step3_dbt_s3_sink.py --topic "$TOPIC" --value-type "$VALUE_TYPE" 2>&1; then
        echo -e "\n${GREEN}✅ Bootstrap would succeed${NC}"
        echo -e "${BLUE}ℹ️  Files that would be generated:${NC}"
        
        # Show what files were created
        if [ -f "models/staging/kafka/_kafka_external__sources.yml" ]; then
            echo -e "  ${GREEN}✓${NC} External source definition in _kafka_external__sources.yml"
        fi
        
        TABLE_NAME=$(echo "$TOPIC" | sed 's/\./__/g' | sed 's/-/_/g')
        if [ -f "models/staging/kafka/external/stg_kafka__${TABLE_NAME}__external.sql" ]; then
            echo -e "  ${GREEN}✓${NC} Base model: stg_kafka__${TABLE_NAME}__external.sql"
        fi
        
        if [ -f "models/staging/kafka/stg_kafka__${TABLE_NAME}.sql" ]; then
            echo -e "  ${GREEN}✓${NC} Typecast model: stg_kafka__${TABLE_NAME}.sql"
        fi
    else
        echo -e "\n${YELLOW}⚠️  Bootstrap script encountered an issue${NC}"
        echo -e "${BLUE}ℹ️  This might mean the topic already exists or there's a configuration issue${NC}"
    fi
    
else
    # Realtime workflow - Run step3_dbt.py
    echo -e "${GREEN}Realtime Sink Workflow - dbt Extraction Models${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
    
    # Find sources file
    SOURCES_FILE=$(find "$DBT_DIR" -name "_kafka_connect__sources.yml" -o -name "*kafka*sources*.yml" | head -1)
    
    if [ -z "$SOURCES_FILE" ]; then
        echo -e "${RED}❌ Error: Could not find kafka sources file${NC}"
        echo -e "${YELLOW}Searching for any sources.yml files:${NC}"
        find "$DBT_DIR" -name "*sources.yml" -type f | head -5
        exit 1
    fi
    
    echo -e "${GREEN}✅ Found sources file${NC}"
    echo -e "   Path: $(echo "$SOURCES_FILE" | sed "s|$DBT_DIR/||")"
    echo ""
    
    MODELS_DIR="$DBT_DIR/models"
    
    # Check for Snowflake credentials
    if [ -n "$SNOWFLAKE_ACCOUNT" ]; then
        echo -e "${GREEN}✅ Snowflake credentials detected - using auto-discovery${NC}"
        AUTO_DISCOVER="--auto-discover"
    else
        echo -e "${YELLOW}ℹ️  No Snowflake credentials - using default fields${NC}"
        echo -e "   ${BLUE}Tip: Set SNOWFLAKE_* env vars for schema auto-discovery${NC}"
        AUTO_DISCOVER=""
    fi
    echo ""
    
    # Run the step3 script in dry-run mode
    python3 "$SCRIPT_DIR/step3_dbt.py" \
        --topic "$TOPIC" \
        --sources-file "$SOURCES_FILE" \
        --models-dir "$MODELS_DIR" \
        $AUTO_DISCOVER \
        --dry-run
fi

# ============================================
# Show Next Steps
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Next Steps${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${GREEN}To apply these changes:${NC}"
echo -e "1. Run the GitHub Actions workflow:"
echo -e "   ${BLUE}https://github.com/$(git -C "$SCRIPT_DIR" remote get-url origin 2>/dev/null | sed 's/.*github.com[:/]\(.*\)\.git/\1/' || echo 'YOUR-ORG/YOUR-REPO')/actions${NC}"
echo -e ""
echo -e "2. Or manually trigger via GitHub CLI:"
if [ "$SINK_TYPE" = "s3" ]; then
    echo -e "   ${YELLOW}gh workflow run parallel-pr-creator.yml \\${NC}"
    echo -e "   ${YELLOW}  --field topic=\"$TOPIC\" \\${NC}"
    echo -e "   ${YELLOW}  --field sink_type=\"s3\"${NC}"
else
    echo -e "   ${YELLOW}gh workflow run parallel-pr-creator.yml \\${NC}"
    echo -e "   ${YELLOW}  --field topic=\"$TOPIC\" \\${NC}"
    echo -e "   ${YELLOW}  --field sink_type=\"realtime\"${NC}"
fi
echo -e ""
echo -e "3. Or run locally (not recommended):"
echo -e "   ${YELLOW}cd path/to/dbt${NC}"
if [ "$SINK_TYPE" = "s3" ]; then
    echo -e "   ${YELLOW}# Edit scripts/step3_dbt_s3_sink.py to add topic${NC}"
    echo -e "   ${YELLOW}python3 scripts/step3_dbt_s3_sink.py${NC}"
else
    echo -e "   ${YELLOW}python3 ../scripts/step3_dbt.py \\${NC}"
    echo -e "   ${YELLOW}  --topic \"$TOPIC\" \\${NC}"
    echo -e "   ${YELLOW}  --sources-file models/.../sources.yml \\${NC}"
    echo -e "   ${YELLOW}  --models-dir models${NC}"
fi

echo -e "\n${GREEN}✅ Dry run complete!${NC}\n"

