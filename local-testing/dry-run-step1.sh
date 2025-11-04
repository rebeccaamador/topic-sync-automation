#!/bin/bash
# Dry Run Script for Step 1 - Preview helm-apps changes
# This script simulates Step 1 workflow without making any changes

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
echo -e "${BLUE}  Step 1 Dry Run - helm-apps Preview${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Configuration
# ============================================

# Check if we have the required variables
if [ -z "$HELM_APPS_REPO" ]; then
    echo -e "${YELLOW}⚠️  HELM_APPS_REPO not set${NC}"
    echo -e "Please enter the helm-apps repository (format: owner/repo)"
    echo -e "Example: ${GREEN}rebeccaamador/helm-apps${NC}"
    read -p "Repository: " HELM_APPS_REPO
fi

# Parse owner and repo name
REPO_OWNER=$(echo "$HELM_APPS_REPO" | cut -d'/' -f1)
REPO_NAME=$(echo "$HELM_APPS_REPO" | cut -d'/' -f2)

echo -e "\n${GREEN}Repository:${NC} $HELM_APPS_REPO"

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

# Get value type
if [ -z "$2" ]; then
    echo -e "\n${YELLOW}Select value type:${NC}"
    echo -e "  1) json (default)"
    echo -e "  2) protobuf"
    read -p "Choice [1]: " VALUE_TYPE_CHOICE
    VALUE_TYPE_CHOICE=${VALUE_TYPE_CHOICE:-1}
    
    if [ "$VALUE_TYPE_CHOICE" = "2" ]; then
        VALUE_TYPE="protobuf"
    else
        VALUE_TYPE="json"
    fi
else
    VALUE_TYPE="$2"
fi

# Get sink type
if [ -z "$3" ]; then
    echo -e "\n${YELLOW}Select sink type:${NC}"
    echo -e "  1) realtime (default)"
    echo -e "  2) s3"
    read -p "Choice [1]: " SINK_TYPE_CHOICE
    SINK_TYPE_CHOICE=${SINK_TYPE_CHOICE:-1}
    
    if [ "$SINK_TYPE_CHOICE" = "2" ]; then
        SINK_TYPE="s3"
    else
        SINK_TYPE="realtime"
    fi
else
    SINK_TYPE="$3"
fi

echo -e "\n${BLUE}Configuration:${NC}"
echo -e "  Topic:      ${GREEN}$TOPIC${NC}"
echo -e "  Value Type: ${GREEN}$VALUE_TYPE${NC}"
echo -e "  Sink Type:  ${GREEN}$SINK_TYPE${NC}"
echo ""

# ============================================
# Setup Temporary Workspace
# ============================================

TEMP_DIR=$(mktemp -d)
HELM_APPS_DIR="$TEMP_DIR/helm-apps"

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

echo -e "${BLUE}📥 Fetching helm-apps repository...${NC}"

# Check if GITHUB_TOKEN is available for private repos
if [ -n "$GITHUB_TOKEN" ]; then
    CLONE_URL="https://${GITHUB_TOKEN}@github.com/${HELM_APPS_REPO}.git"
else
    CLONE_URL="https://github.com/${HELM_APPS_REPO}.git"
    echo -e "${YELLOW}⚠️  No GITHUB_TOKEN set - cloning as public repo${NC}"
fi

# Try to clone
if git clone --depth 1 "$CLONE_URL" "$HELM_APPS_DIR" 2>/dev/null; then
    echo -e "${GREEN}✅ Repository cloned${NC}"
else
    echo -e "${RED}❌ Failed to clone repository${NC}"
    echo -e "${YELLOW}💡 Trying to find local copy...${NC}"
    
    # Try to find local helm-apps repo
    POSSIBLE_PATHS=(
        "$HOME/helm-apps"
        "$(dirname "$SCRIPT_DIR")/helm-apps"
    )
    
    FOUND=false
    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -d "$path" ]; then
            echo -e "${GREEN}✅ Found local copy: $path${NC}"
            cp -r "$path" "$HELM_APPS_DIR"
            FOUND=true
            break
        fi
    done
    
    if [ "$FOUND" = false ]; then
        echo -e "${RED}❌ Could not find helm-apps repository${NC}"
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
# Find values.yaml
# ============================================

echo -e "\n${BLUE}🔍 Searching for values.yaml...${NC}"

VALUES_FILE=$(find "$HELM_APPS_DIR" -name "values.yaml" -path "*/kafka-connect-operator/*" | head -1)

if [ -z "$VALUES_FILE" ]; then
    echo -e "${RED}❌ Could not find values.yaml in kafka-connect-operator${NC}"
    echo -e "${YELLOW}Searching for any values.yaml files:${NC}"
    find "$HELM_APPS_DIR" -name "values.yaml" -type f
    exit 1
fi

echo -e "${GREEN}✅ Found: $VALUES_FILE${NC}"

# Show file info
FILE_SIZE=$(wc -l < "$VALUES_FILE" | tr -d ' ')
echo -e "${BLUE}ℹ️  File info:${NC}"
echo -e "   Path: $(echo "$VALUES_FILE" | sed "s|$HELM_APPS_DIR/||")"
echo -e "   Lines: $FILE_SIZE"

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
# Run Dry Run
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Running Dry Run Analysis${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Run the step1 script in dry-run mode
python3 "$SCRIPTS_DIR/step1_helm_apps.py" \
    --topic "$TOPIC" \
    --file "$VALUES_FILE" \
    --value-type "$VALUE_TYPE" \
    --sink-type "$SINK_TYPE" \
    --dry-run

# ============================================
# Show Next Steps
# ============================================

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Next Steps${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${GREEN}To apply these changes:${NC}"
echo -e "1. Run the GitHub Actions workflow:"
echo -e "   ${BLUE}https://github.com/$(git -C "$SCRIPT_DIR" remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions${NC}"
echo -e ""
echo -e "2. Or manually trigger via GitHub CLI:"
echo -e "   ${YELLOW}gh workflow run step1-helm-apps.yml \\${NC}"
echo -e "   ${YELLOW}  --field topic=\"$TOPIC\" \\${NC}"
echo -e "   ${YELLOW}  --field value_type=\"$VALUE_TYPE\" \\${NC}"
echo -e "   ${YELLOW}  --field sink_type=\"$SINK_TYPE\"${NC}"
echo -e ""
echo -e "3. Or run locally (not recommended):"
echo -e "   ${YELLOW}# Clone helm-apps repo${NC}"
echo -e "   ${YELLOW}cd path/to/helm-apps${NC}"
echo -e "   ${YELLOW}python3 $(dirname "$SCRIPT_DIR")/scripts/step1_helm_apps.py \\${NC}"
echo -e "   ${YELLOW}  --topic \"$TOPIC\" \\${NC}"
echo -e "   ${YELLOW}  --file helm/kafka-connect-operator/values.yaml \\${NC}"
echo -e "   ${YELLOW}  --value-type \"$VALUE_TYPE\" \\${NC}"
echo -e "   ${YELLOW}  --sink-type \"$SINK_TYPE\"${NC}"

echo -e "\n${GREEN}✅ Dry run complete!${NC}\n"
