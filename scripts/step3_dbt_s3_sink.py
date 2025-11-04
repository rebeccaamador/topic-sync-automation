#!/usr/bin/env python3
"""
Bootstrap Kafka S3 External Sources in dbt

Generates external source definitions and staging models for a Kafka topic
that lands in S3.
"""

import argparse
import subprocess
from pathlib import Path


def generate_base_model_sql(topic_table):
    """Generate the base external model SQL."""
    return (
        "{{\n"
        "    config(\n"
        f"        pre_hook='{{{{ stage_external_source_prehook(external_table=\"kafka_s3.{topic_table}\") }}}}',\n"
        "    )\n"
        "}}\n"
        "\n"
        f"{{{{ kafka_base('s3', '{topic_table}') }}}}\n"
        "{{ dev_limit() }}\n"
    )


def generate_typecast_model_sql(topic_table, base_model):
    """Generate the typecast model SQL."""
    # Generate clean alias without stg_kafka__ prefix
    alias = topic_table
    
    return (
        f"{{{{ config(alias='{alias}') }}}}\n"
        "\n"
        f"with raw as (select * from {{{{ ref('{base_model}') }}}})\n"
        "\n"
        "select *\n"
        "\n"
        "from raw\n"
    )


def run_sqlfluff(filepath):
    """Run sqlfluff fix on a SQL file."""
    try:
        # Check if sqlfluff is available
        check_result = subprocess.run(
            ['sqlfluff', '--version'],
            capture_output=True,
            text=True
        )
        
        if check_result.returncode != 0:
            print(f"   ⚠️  sqlfluff not found - skipping formatting")
            return False
        
        # Run sqlfluff fix
        result = subprocess.run(
            ['sqlfluff', 'fix', '--dialect', 'snowflake', str(filepath)],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"   ✅ Formatted with sqlfluff: {filepath.name}")
            return True
        else:
            print(f"   ⚠️  sqlfluff had issues (non-critical): {filepath.name}")
            if result.stderr:
                print(f"      {result.stderr.strip()}")
            return False
            
    except FileNotFoundError:
        print(f"   ⚠️  sqlfluff not installed - skipping formatting")
        return False
    except Exception as e:
        print(f"   ⚠️  Error running sqlfluff: {e}")
        return False


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Bootstrap Kafka external sources in dbt for a single topic'
    )
    parser.add_argument(
        '--topic',
        required=True,
        help='Kafka topic name (e.g., audit.action.v1)'
    )
    parser.add_argument(
        '--value-type',
        required=True,
        choices=['json', 'protobuf'],
        help='Value type for the topic'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without modifying files'
    )
    args = parser.parse_args()

    print(f"🎯 Processing S3 external source for: {args.topic} ({args.value_type})")

    # Convert topic name to table name
    # Example: threat.virustotal_raw.v1 -> threat__virustotal_raw__v1
    topic_table = args.topic.replace(".", "__").replace("-", "_")
    
    print(f"📊 Table name: {topic_table}")

    # Output paths
    output_path = Path("models/staging/kafka")
    external_path = output_path / "external"
    sources_file = external_path / "_kafka_external__sources.yml"

    # Ensure directories exist
    external_path.mkdir(parents=True, exist_ok=True)

    # Read existing sources file or create new one
    if sources_file.exists():
        print(f"\n📄 Reading existing sources file: {sources_file}")
        with open(sources_file, 'r') as f:
            existing_content = f.read()
        
        # Check if topic already exists
        if topic_table in existing_content:
            print(f"⚠️  Topic '{args.topic}' (table: {topic_table}) already exists in sources file")
            print("   No changes needed")
            return
    else:
        print(f"\n📄 Creating new sources file: {sources_file}")
        existing_content = None

    # YAML header for sources file
    header = """version: 2

sources:
  - name: kafka_s3
    database: raw_kafka_s3_production
    schema: public
    tables:
"""

    # Source definition template
    source_definition = f"""      - name: {topic_table}
        description: This is an external table, mounted on an S3 bucket correlating to Kafka topic {args.topic}
        external:
          location: "@raw_kafka_s3_production.public.stage_kafka_s3_production/{args.topic}/"
          file_format: "( type = json )"
          partitions:
            - name: dt
              data_type: date
              expression: >-
                to_date(split_part(split_part(metadata$filename, '/', 3), '=', 2)
                        || '-' ||
                        split_part(split_part(metadata$filename, '/', 4), '=', 2)
                        || '-' ||
                        split_part(split_part(metadata$filename, '/', 5), '=', 2))
"""

    # Write the updated sources file
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No files will be modified")
        print(f"\n📝 Would update sources file: {sources_file}")
        print(f"   Would add table: {topic_table}")
        
        # Show what would be added
        print(f"\n📝 Would generate model files:")
        base_model = f"stg_kafka__{topic_table}__external"
        typecast_model = f"stg_kafka__{topic_table}"
        print(f"   - {base_model}.sql (in models/staging/kafka/external/)")
        print(f"   - {typecast_model}.sql (in models/staging/kafka/)")
        
        print(f"\n{'='*70}")
        print(f"🔍 DRY RUN COMPLETE - No changes written")
        print(f"{'='*70}\n")
        return
    
    if existing_content:
        # Insert alphabetically without moving existing entries
        import re
        
        lines = existing_content.splitlines(keepends=True)
        
        # Find all existing table entries and their line numbers
        table_pattern = re.compile(r'^(\s+)-\s+name:\s+(.+)$')
        table_entries = []
        in_tables_section = False
        
        for i, line in enumerate(lines):
            # Detect when we enter the tables section
            if 'tables:' in line:
                in_tables_section = True
                continue
            
            # Stop if we hit another top-level key (less indentation than tables)
            if in_tables_section and line.strip() and not line.startswith(' ' * 4):
                # If line doesn't start with at least 4 spaces, we've left the tables section
                if not line.startswith(' '):
                    break
            
            if in_tables_section:
                match = table_pattern.match(line)
                if match:
                    indent = match.group(1)
                    tname = match.group(2).strip()
                    table_entries.append((i, indent, tname))
        
        if not table_entries:
            # No tables found, append at the end
            with open(sources_file, 'a') as outfile:
                outfile.write(source_definition)
            print(f"   ✅ Added external source definition for {topic_table}")
        else:
            # Find alphabetical insert position
            insert_line = None
            indent_to_use = table_entries[0][1]  # Use same indentation as existing entries
            
            for line_num, indent, tname in table_entries:
                if topic_table < tname:
                    # Insert before this entry
                    insert_line = line_num
                    break
            
            if insert_line is None:
                # Append after last table
                last_table_line = table_entries[-1][0]
                # Find the end of the last table entry (look for next table or end of section)
                insert_line = last_table_line + 1
                # Skip any lines that are part of the last table entry
                while insert_line < len(lines) and lines[insert_line].startswith(' ' * 8):
                    insert_line += 1
            
            # Insert the new lines at the correct position
            new_lines = lines.copy()
            # Split source_definition into lines while preserving line endings
            source_lines = [line + '\n' if not line.endswith('\n') else line 
                           for line in source_definition.rstrip().split('\n')]
            
            new_lines[insert_line:insert_line] = source_lines
            
            # Write back to file
            with open(sources_file, 'w') as f:
                f.write(''.join(new_lines))
            print(f"   ✅ Added external source definition for {topic_table} (alphabetically)")
    else:
        # Create new file with header
        with open(sources_file, 'w') as outfile:
            outfile.write(header)
            outfile.write(source_definition)
        print(f"   ✅ Created sources file with definition for {topic_table}")

    # Generate dbt model files
    print(f"\n📄 Generating dbt models...")
    
    base_model = f"stg_kafka__{topic_table}__external"
    typecast_model = f"stg_kafka__{topic_table}"

    typecast_view_filename = f"{typecast_model}.sql"
    base_view_filename = f"{base_model}.sql"

    typecast_model_filepath = output_path / typecast_view_filename
    base_model_filepath = external_path / base_view_filename

    # Create base external view (always overwrite - it's generated)
    base_sql = generate_base_model_sql(topic_table)
    with open(base_model_filepath, "w") as outfile:
        outfile.write(base_sql)
    print(f"   ✅ Created {base_view_filename}")

    # Create typecast view (only if it doesn't exist - may have customizations)
    typecast_created = False
    if typecast_model_filepath.exists():
        print(f"   ⏭️  Skipped {typecast_view_filename} (already exists, preserving customizations)")
    else:
        typecast_sql = generate_typecast_model_sql(topic_table, base_model)
        with open(typecast_model_filepath, "w") as outfile:
            outfile.write(typecast_sql)
        print(f"   ✅ Created {typecast_view_filename}")
        typecast_created = True

    # Run sqlfluff on generated models
    print(f"\n🔧 Running sqlfluff...")
    run_sqlfluff(base_model_filepath)
    if typecast_created:
        run_sqlfluff(typecast_model_filepath)

    # Summary
    print(f"\n{'='*70}")
    print(f"✅ Bootstrap complete!")
    print(f"{'='*70}")
    print(f"Topic:       {args.topic}")
    print(f"Value Type:  {args.value_type}")
    print(f"Table:       {topic_table}")
    print(f"\nFiles modified:")
    print(f"   - {sources_file}")
    print(f"   - {base_model_filepath}")
    if not typecast_model_filepath.exists():
        print(f"   - {typecast_model_filepath} (new)")
    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
