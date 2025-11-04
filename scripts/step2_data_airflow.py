#!/usr/bin/env python3
"""
Step 2: Add StreamTaskConfig to data-airflow DAG
Adds a new stream configuration for loading Kafka topic data to Snowflake.
"""

import argparse
import sys
import re
from pathlib import Path
from difflib import unified_diff


def generate_table_name(topic):
    """Convert topic name to Snowflake table name."""
    # audit.action.v1 -> audit__action__v1__raw
    table_name = topic.replace('.', '__').replace('-', '_') + '__raw'
    return table_name


def check_config_exists(content, table_name):
    """Check if the stream config already exists."""
    pattern = f'table="{table_name}"'
    return pattern in content


def add_stream_config_to_file(content, table_name):
    """Add StreamTaskConfig by modifying the file content directly (preserving formatting)."""
    
    # Find the configs = [ ... ] section with StreamTaskConfig entries
    pattern = r'(configs\s*=\s*\[)(.*?)(\n\s*\])'
    match = re.search(pattern, content, re.DOTALL)
    
    if not match:
        print("❌ Error: Could not find 'configs = [...]' section in DAG file")
        sys.exit(1)
    
    prefix = match.group(1)
    configs_content = match.group(2)
    suffix = match.group(3)
    
    # Parse existing StreamTaskConfig entries to extract table names
    table_pattern = r'table="([^"]+)"'
    existing_tables = re.findall(table_pattern, configs_content)
    
    # Find where to insert (alphabetically by table name)
    lines = configs_content.split('\n')
    insert_line_index = None
    
    for i, line in enumerate(lines):
        table_match = re.search(table_pattern, line)
        if table_match:
            existing_table = table_match.group(1)
            if table_name < existing_table:
                # Find the start of this StreamTaskConfig block
                for j in range(i, -1, -1):
                    if 'StreamTaskConfig(' in lines[j]:
                        insert_line_index = j
                        # Don't move back - we want to insert right at this position
                        # The blank line (if any) will be handled separately
                        break
                break
    
    # Detect indentation from existing entries
    indent = '        '  # Default 8 spaces
    for line in lines:
        if 'StreamTaskConfig(' in line:
            indent_match = re.match(r'^(\s+)', line)
            if indent_match:
                indent = indent_match.group(1)
            break
    
    # Create new StreamTaskConfig entry as separate lines
    new_entry_lines = [
        f'{indent}StreamTaskConfig(',
        f'{indent}    table="{table_name}",',
        f'{indent}    warehouse="LOADER_PRODUCTION_STREAMING",',
        f'{indent}),'
    ]
    
    # Insert the new entry
    if insert_line_index is not None:
        # Check if there's a blank line right before the insertion point
        has_blank_before = (insert_line_index > 0 and 
                           lines[insert_line_index - 1].strip() == '')
        
        # If there's a blank line before, remove it first to avoid double spacing
        if has_blank_before:
            lines.pop(insert_line_index - 1)
            insert_line_index -= 1
        
        # Insert our new entry
        for idx, entry_line in enumerate(new_entry_lines):
            lines.insert(insert_line_index + idx, entry_line)
    else:
        # Add to the end (before the closing ])
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].strip().endswith('),'):
                # Remove ALL blank lines after this last entry
                while (i + 1 < len(lines) and lines[i + 1].strip() == ''):
                    lines.pop(i + 1)
                
                # Insert after the last entry (at position i+1)
                for idx, entry_line in enumerate(new_entry_lines):
                    lines.insert(i + 1 + idx, entry_line)
                break
    
    # Reconstruct the content
    new_configs_content = '\n'.join(lines)
    new_content = content[:match.start()] + prefix + new_configs_content + suffix + content[match.end():]
    
    return new_content, len(existing_tables)


def preview_changes(table_name, existing_count):
    """Print a preview of what changes will be made."""
    print("\n" + "="*70)
    print("📋 PREVIEW OF CHANGES")
    print("="*70)
    print(f"\n📊 Existing StreamTaskConfig entries: {existing_count}")
    print(f"📊 After change: {existing_count + 1}")
    print(f"\n➕ Adding new StreamTaskConfig:")
    print("-" * 70)
    print(f"    StreamTaskConfig(")
    print(f"        table=\"{table_name}\",")
    print(f"        warehouse=\"LOADER_PRODUCTION_STREAMING\",")
    print(f"    ),")
    print("-" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Add StreamTaskConfig to data-airflow DAG'
    )
    parser.add_argument(
        '--topic',
        required=True,
        help='Kafka topic name (e.g., audit.action.v1)'
    )
    parser.add_argument(
        '--dag-file',
        required=True,
        help='Path to the DAG Python file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without modifying the file'
    )
    
    args = parser.parse_args()
    
    # Validate file exists
    dag_file = Path(args.dag_file)
    if not dag_file.exists():
        print(f"❌ Error: File not found: {args.dag_file}")
        sys.exit(1)
    
    print(f"📂 Reading: {args.dag_file}")
    
    # Read the DAG file
    try:
        with open(dag_file, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)
    
    # Generate table name
    table_name = generate_table_name(args.topic)
    print(f"📊 Generated table name: {table_name}")
    
    # Check if config already exists
    if check_config_exists(content, table_name):
        print(f"\n⚠️  Stream config for '{table_name}' already exists in DAG")
        print("ℹ️  No changes needed")
        sys.exit(0)
    
    # Count existing configs
    table_pattern = r'table="([^"]+)"'
    existing_tables = re.findall(table_pattern, content)
    existing_count = len(existing_tables)
    print(f"📊 Found {existing_count} existing StreamTaskConfig entries")
    
    # Preview the changes
    preview_changes(table_name, existing_count)
    
    # Generate the new content to show diff
    print(f"\n💾 Generating changes (preserving original formatting)...")
    try:
        new_content, _ = add_stream_config_to_file(content, table_name)
    except Exception as e:
        print(f"❌ Error generating changes: {e}")
        sys.exit(1)
    
    # Show the diff
    print("\n" + "="*70)
    print("📝 EXACT FILE CHANGES (unified diff)")
    print("="*70)
    
    original_lines = content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    
    diff = unified_diff(
        original_lines,
        new_lines,
        fromfile=f'{args.dag_file} (original)',
        tofile=f'{args.dag_file} (modified)',
        lineterm=''
    )
    
    diff_lines = list(diff)
    if diff_lines:
        for line in diff_lines:
            # Color code the diff output
            if line.startswith('+++') or line.startswith('---'):
                print(f"\033[1m{line}\033[0m")  # Bold
            elif line.startswith('+'):
                print(f"\033[32m{line}\033[0m")  # Green
            elif line.startswith('-'):
                print(f"\033[31m{line}\033[0m")  # Red
            elif line.startswith('@@'):
                print(f"\033[36m{line}\033[0m")  # Cyan
            else:
                print(line)
    else:
        print("(No changes detected)")
    
    print("="*70)
    
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No changes written to file")
        print("✅ Run without --dry-run to apply changes")
        sys.exit(0)
    
    # Write the changes
    print(f"\n💾 Writing changes to file...")
    try:
        with open(dag_file, 'w') as f:
            f.write(new_content)
        
        print("✅ Successfully updated DAG file")
        print("ℹ️  Only the configs list was modified, all other formatting preserved")
    except Exception as e:
        print(f"❌ Error writing file: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
