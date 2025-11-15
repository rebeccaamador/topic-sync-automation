#!/usr/bin/env python3
"""
Step 1: Add Kafka topic to helm-apps values.yaml
Adds a new topic to the appropriate Snowflake sink connector configuration.
"""

import argparse
import sys
import yaml
import re
from pathlib import Path
from difflib import unified_diff


def get_connector_name(value_type, sink_type):
    """Get the appropriate connector name based on sink type and value type."""
    if sink_type == 'realtime':
        if value_type == 'json':
            return 'snowflake-realtime-sink-json-v2'
        else:
            return 'snowflake-realtime-sink-protobuf-v2'
    else:  # s3
        if value_type == 'json':
            return 's3-sink-json-to-json'
        else:
            return 's3-sink-protobuf-to-json'


def generate_table_name(topic):
    """Generate Snowflake table name from topic."""
    table_name = topic.replace('.', '__').replace('-', '_') + '__raw'
    return table_name


def check_topic_exists(file_content, connector_name, topic):
    """Check if topic already exists in the connector configuration."""
    # Find the topics section for this connector
    pattern = rf'{re.escape(connector_name)}:.*?topics:\s*>-?\s*\n((?:[ \t]{{8,}}[^\n]+\n)+)'
    match = re.search(pattern, file_content, re.DOTALL)
    
    if not match:
        return False
    
    topics_section = match.group(1)
    topic_list = [t.strip().rstrip(',') for t in topics_section.strip().split('\n') if t.strip()]
    
    return topic in topic_list


def add_topic_to_file(file_content, connector_name, topic, sink_type):
    """Add topic by modifying the file content directly (preserving formatting)."""
    
    # Pattern: Match topics: >- through all indented lines until next YAML key
    pattern = rf'({re.escape(connector_name)}:.*?topics:\s*>-?\s*\n)((?:[ \t]{{8,}}[^\n]+\n)+)(?=[ \t]{{0,7}}\S|\Z)'
    
    match = re.search(pattern, file_content, re.DOTALL)
    
    if not match:
        print(f"❌ Error: Could not find topics section for {connector_name}")
        sys.exit(1)
    
    prefix = match.group(1)
    topics_section = match.group(2)
    
    # Parse existing topics (preserving exact order)
    topic_lines = topics_section.strip().split('\n')
    topic_list = []
    for line in topic_lines:
        if line.strip():
            topic_list.append(line.strip().rstrip(','))
    
    # Find alphabetical insertion position WITHOUT changing existing order
    insert_index = len(topic_list)  # Default: append at end
    for i, existing_topic in enumerate(topic_list):
        if topic < existing_topic:
            insert_index = i
            break
    
    # Insert new topic at the correct position
    topic_list.insert(insert_index, topic)
    
    # Format topics with proper indentation (8 spaces) - preserving order
    formatted_topics = '        ' + ',\n        '.join(topic_list)
    
    # Replace in content
    new_content, replacements = re.subn(
        pattern,
        lambda m: f"{m.group(1)}{formatted_topics}\n",
        file_content,
        flags=re.DOTALL
    )
    
    if replacements == 0:
        print("❌ Error: Could not update topics section")
        sys.exit(1)
    
    # For Snowflake realtime connectors, also update snowflake.topic2table.map
    if sink_type == 'realtime':
        table_name = generate_table_name(topic)
        new_mapping = f"{topic}:{table_name}"
        
        # Pattern for topic2table.map
        map_pattern = rf'({re.escape(connector_name)}:.*?snowflake\.topic2table\.map:\s*>-?\s*\n)((?:[ \t]{{8,}}[^\n]+\n)+)(?=[ \t]{{0,7}}\S|\Z)'
        map_match = re.search(map_pattern, new_content, re.DOTALL)
        
        if map_match:
            # Parse existing mappings (preserving exact order)
            mappings_section = map_match.group(2)
            mapping_lines = mappings_section.strip().split('\n')
            mapping_list = []
            for line in mapping_lines:
                if line.strip():
                    mapping_list.append(line.strip().rstrip(','))
            
            # Check if mapping already exists
            if not any(m.startswith(f"{topic}:") for m in mapping_list):
                # Find alphabetical insertion position WITHOUT changing existing order
                map_insert_index = len(mapping_list)  # Default: append at end
                for i, existing_mapping in enumerate(mapping_list):
                    existing_topic = existing_mapping.split(':')[0]
                    if topic < existing_topic:
                        map_insert_index = i
                        break
                
                # Insert new mapping at the correct position
                mapping_list.insert(map_insert_index, new_mapping)
                
                # Format mappings with proper indentation (preserving order)
                formatted_mappings = '        ' + ',\n        '.join(mapping_list)
                
                # Replace in content
                new_content, map_replacements = re.subn(
                    map_pattern,
                    lambda m: f"{m.group(1)}{formatted_mappings}\n",
                    new_content,
                    flags=re.DOTALL
                )
                
                if map_replacements > 0:
                    print(f"   ✅ Added topic2table mapping: {new_mapping}")
    
    return new_content


def preview_changes(connector_name, topic, existing_count, sink_type):
    """Print a preview of what changes will be made."""
    table_name = generate_table_name(topic)
    
    print("\n" + "="*70)
    print("📋 PREVIEW OF CHANGES")
    print("="*70)
    print(f"\n✅ Connector: {connector_name}")
    print(f"📊 Current topics: {existing_count}")
    print(f"📊 After change: {existing_count + 1}")
    print(f"\n➕ Adding topic: {topic}")
    print(f"📊 Snowflake table: {table_name}")
    
    if sink_type == 'realtime':
        print(f"📊 Topic2table mapping: {topic}:{table_name}")
    
    print("-" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Add Kafka topic to helm-apps values.yaml'
    )
    parser.add_argument(
        '--topic',
        required=True,
        help='Kafka topic name (e.g., customer.action.v1)'
    )
    parser.add_argument(
        '--file',
        required=True,
        help='Path to values.yaml file'
    )
    parser.add_argument(
        '--value-type',
        choices=['json', 'protobuf'],
        default='json',
        help='Value type (default: json)'
    )
    parser.add_argument(
        '--sink-type',
        choices=['realtime', 's3'],
        default='realtime',
        help='Sink type (default: realtime)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without modifying the file'
    )
    
    args = parser.parse_args()
    
    # Validate file exists
    values_file = Path(args.file)
    if not values_file.exists():
        print(f"❌ Error: File not found: {args.file}")
        sys.exit(1)
    
    print(f"📂 Reading: {args.file}")
    
    # Read file content
    with open(values_file, 'r') as f:
        file_content = f.read()
    
    # Get connector name
    connector_name = get_connector_name(args.value_type, args.sink_type)
    print(f"🔍 Looking for connector: {connector_name}")
    
    # Check if connector exists in file
    if connector_name not in file_content:
        print(f"❌ Error: Connector '{connector_name}' not found in values.yaml")
        sys.exit(1)
    
    print(f"✅ Found connector: {connector_name}")
    
    # Check if topic already exists
    if check_topic_exists(file_content, connector_name, args.topic):
        print(f"\n⚠️  Topic '{args.topic}' already exists in {connector_name}")
        print("ℹ️  No changes needed")
        sys.exit(0)
    
    # Count existing topics
    pattern = rf'{re.escape(connector_name)}:.*?topics:\s*>-?\s*\n((?:[ \t]{{8,}}[^\n]+\n)+)'
    match = re.search(pattern, file_content, re.DOTALL)
    
    if match:
        topics_section = match.group(1)
        topic_list = [t.strip().rstrip(',') for t in topics_section.strip().split('\n') if t.strip()]
        existing_count = len(topic_list)
    else:
        existing_count = 0
    
    print(f"📊 Found {existing_count} existing topics")
    
    # Preview the changes
    preview_changes(connector_name, args.topic, existing_count, args.sink_type)
    
    # Generate the new content to show diff
    print(f"\n💾 Generating changes (preserving original formatting)...")
    try:
        new_content = add_topic_to_file(file_content, connector_name, args.topic, args.sink_type)
    except Exception as e:
        print(f"❌ Error generating changes: {e}")
        sys.exit(1)
    
    # Show the diff
    print("\n" + "="*70)
    print("📝 EXACT FILE CHANGES (unified diff)")
    print("="*70)
    
    original_lines = file_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    
    diff = unified_diff(
        original_lines,
        new_lines,
        fromfile=f'{args.file} (original)',
        tofile=f'{args.file} (modified)',
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
        with open(values_file, 'w') as f:
            f.write(new_content)
        
        print("✅ Successfully updated values.yaml")
        print("ℹ️  Only the topics section was modified, all other formatting preserved")
    except Exception as e:
        print(f"❌ Error writing file: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
