#!/usr/bin/env python3
"""
Step 3: Create dbt extraction layer
Adds source definition and creates staging model for Kafka topic data.
"""

import argparse
import sys
import yaml
import json
import os
from pathlib import Path
from difflib import unified_diff
from collections import defaultdict


def generate_table_name(topic):
    """Convert topic name to Snowflake table name."""
    # Note: We add __raw here because the _processed tables have it
    table_name = topic.replace('.', '__').replace('-', '_') + '__raw'
    return table_name


def generate_model_name(topic):
    """Generate dbt model name from topic (no prefix, but with __extracted suffix for realtime sink)."""
    # Convert topic directly to model name (no stg_kafka__ prefix, but keep __extracted suffix)
    model_name = topic.replace('.', '__').replace('-', '_') + '__extracted'
    return model_name


def generate_source_config(table_name):
    """Generate the source configuration for sources.yml."""
    # Add _processed suffix for the source table name
    source_table_name = f"{table_name}_processed"
    config = {
        'name': source_table_name
    }
    return config


def discover_schema_from_snowflake(table_name, snowflake_config, sample_size=100):
    """
    Connect to Snowflake and discover the schema of record_content by sampling records.
    
    Args:
        table_name: The base table name (without _processed suffix)
        snowflake_config: Dict with Snowflake connection parameters
        sample_size: Number of records to sample for schema discovery
        
    Returns:
        List of field names discovered, or None if discovery fails
    """
    try:
        import snowflake.connector
    except ImportError:
        print("⚠️  snowflake-connector-python not installed. Install with:")
        print("   pip install snowflake-connector-python")
        return None
    
    source_table = f"{table_name}_processed"
    
    print(f"\n🔍 Discovering schema from Snowflake table: {source_table}")
    print(f"   Sampling {sample_size} records...")
    
    try:
        # Build connection parameters
        conn_params = {
            'account': snowflake_config.get('account'),
            'user': snowflake_config.get('user'),
            'warehouse': snowflake_config.get('warehouse'),
            'database': snowflake_config.get('database'),
            'schema': snowflake_config.get('schema'),
        }
        
        # Add optional parameters
        if snowflake_config.get('role'):
            conn_params['role'] = snowflake_config.get('role')
        
        # Handle authentication
        authenticator = snowflake_config.get('authenticator')
        if authenticator:
            conn_params['authenticator'] = authenticator
            if authenticator.lower() == 'externalbrowser':
                print("   Using external browser authentication (SSO)...")
        elif snowflake_config.get('password'):
            conn_params['password'] = snowflake_config.get('password')
        else:
            # Default to externalbrowser if no password provided
            conn_params['authenticator'] = 'externalbrowser'
            print("   Using external browser authentication (SSO)...")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**conn_params)
        
        cursor = conn.cursor()
        
        # Query sample records
        query = f"""
        SELECT record_content
        FROM {source_table}
        WHERE record_content IS NOT NULL
        LIMIT {sample_size}
        """
        
        print(f"   Executing: {query}")
        cursor.execute(query)
        
        # Collect all field names from sampled records
        field_counts = defaultdict(int)
        field_types = defaultdict(set)
        nested_field_counts = defaultdict(lambda: defaultdict(int))
        nested_field_types = defaultdict(lambda: defaultdict(set))
        array_fields = set()
        total_records = 0
        
        for row in cursor:
            record_content = row[0]
            if record_content:
                try:
                    # Parse JSON if it's a string
                    if isinstance(record_content, str):
                        data = json.loads(record_content)
                    else:
                        data = record_content
                    
                    # Collect field names and types
                    if isinstance(data, dict):
                        for field_name, field_value in data.items():
                            field_counts[field_name] += 1
                            field_types[field_name].add(type(field_value).__name__)
                            
                            # If this is a list/array, also inspect its nested structure
                            if isinstance(field_value, list) and len(field_value) > 0:
                                array_fields.add(field_name)
                                # Sample first item in the array to discover nested fields
                                first_item = field_value[0]
                                if isinstance(first_item, dict):
                                    # These are the fields within the array that we'd flatten
                                    for nested_field, nested_value in first_item.items():
                                        nested_field_counts[field_name][nested_field] += 1
                                        nested_field_types[field_name][nested_field].add(type(nested_value).__name__)
                    
                    total_records += 1
                except json.JSONDecodeError:
                    continue
        
        cursor.close()
        conn.close()
        
        if total_records == 0:
            print("⚠️  No records found in table")
            return None
        
        # Determine which array to use (prioritize common array names)
        primary_array = None
        priority_arrays = ['applications', 'events', 'items', 'records', 'data', 'results']
        
        for priority in priority_arrays:
            if priority in array_fields:
                primary_array = priority
                break
        
        if primary_array is None and array_fields:
            primary_array = list(array_fields)[0]
        
        # If we detected an array with nested fields, return those nested fields
        if primary_array and primary_array in nested_field_counts:
            discovered_fields = sorted(
                nested_field_counts[primary_array].keys(),
                key=lambda x: nested_field_counts[primary_array][x],
                reverse=True
            )
            discovered_types = nested_field_types[primary_array]
            
            print(f"\n✅ Discovered array field '{primary_array}' with {len(discovered_fields)} nested fields from {total_records} records:")
            print("─" * 70)
            for field in discovered_fields:
                frequency = nested_field_counts[primary_array][field]
                percentage = (frequency / total_records) * 100
                types = ', '.join(sorted(discovered_types[field]))
                print(f"   • {field:30s} ({frequency:3d}/{total_records} = {percentage:5.1f}%) [{types}]")
            print("─" * 70)
            
            # Return: (fields, field_types, array_field_name)
            return discovered_fields, discovered_types, primary_array
        
        # Otherwise, return top-level fields
        discovered_fields = sorted(field_counts.keys(), key=lambda x: field_counts[x], reverse=True)
        
        print(f"\n✅ Discovered {len(discovered_fields)} top-level fields from {total_records} records:")
        print("─" * 70)
        for field in discovered_fields:
            frequency = field_counts[field]
            percentage = (frequency / total_records) * 100
            types = ', '.join(sorted(field_types[field]))
            marker = " [array]" if field in array_fields else ""
            print(f"   • {field:30s} ({frequency:3d}/{total_records} = {percentage:5.1f}%) [{types}]{marker}")
        print("─" * 70)
        
        # Return: (fields, field_types, array_field_name=None)
        return discovered_fields, field_types, None
        
    except Exception as e:
        print(f"⚠️  Schema discovery failed: {e}")
        print(f"   Will use default or provided fields instead")
        return None


def infer_unique_key_from_fields(field_list):
    """
    Infer the appropriate unique_key based on discovered field names.
    Only detects fields that are truly unique record identifiers, not foreign keys.
    
    Args:
        field_list: List of field names from the schema
        
    Returns:
        Tuple of (unique_key_name, base_field_name, needs_surrogate_key) or None if no ID field found
    """
    if not field_list:
        return None
    
    # Convert to lowercase for case-insensitive matching
    fields_lower = [f.lower() for f in field_list]
    
    # Conservative list: IDs that should be part of the unique key
    # NOT foreign keys like cve_id, tenant_id, device_id (those aren't unique per record)
    #
    # These IDs need a surrogate key combining ce_id + field_id for uniqueness
    #
    # To add a new auto-detected ID:
    # 1. Verify the field contributes to uniqueness (may need ce_id combo)
    # 2. Add it to this list
    # 3. Test with actual data to confirm uniqueness
    #
    # Example: If notification_id is verified as unique with ce_id:
    #   unique_id_patterns = ['override_id', 'notification_id']
    #
    unique_id_patterns = [
        'override_id',      # Needs surrogate: ce_id + override_id = override_id__sk
    ]
    
    for pattern in unique_id_patterns:
        if pattern in fields_lower:
            # Found a matching unique ID field - generate surrogate key with ce_id
            return f"{pattern}__sk", pattern, True  # True = generate surrogate key
    
    return None


def infer_unique_key_from_topic(topic):
    """
    Infer the appropriate unique_key based on topic naming patterns.
    This is a fallback when no ID field is found in the data.
    
    Args:
        topic: The Kafka topic name
        
    Returns:
        Tuple of (unique_key_name, needs_surrogate_key)
    """
    topic_lower = topic.lower()
    
    # Define patterns and their corresponding unique keys
    patterns = [
        ('device', 'device_id__sk', True),
        ('feedback', 'feedback_id__sk', True),
        ('audit', 'audit_id__sk', True),
        ('compliance', 'compliance_id__sk', True),
        ('vulnerability', 'vulnerability_id__sk', True),
        ('threat', 'threat_id__sk', True),
        ('agent', 'agent_id__sk', True),
        ('experience', 'experience_id__sk', True),
    ]
    
    for pattern, key_name, needs_surrogate in patterns:
        if pattern in topic_lower:
            return key_name, needs_surrogate
    
    # Default: use generic event_id__sk
    return 'event_id__sk', True


def infer_snowflake_type(field_name, python_types):
    """
    Infer appropriate Snowflake type based on field name and observed Python types.
    
    Args:
        field_name: Name of the field
        python_types: Set of Python type names observed
        
    Returns:
        Snowflake cast function (e.g., 'to_varchar', 'to_number')
    """
    field_lower = field_name.lower()
    
    # Timestamp/date fields
    if any(x in field_lower for x in ['timestamp', 'time', 'date', 'created', 'updated', 'modified']):
        return 'to_timestamp_ntz'
    
    # Boolean fields
    if any(x in field_lower for x in ['is_', 'has_', 'enabled', 'disabled', 'active']):
        return 'to_boolean'
    
    # Numeric fields
    if any(x in field_lower for x in ['count', 'amount', 'total', 'score', 'rating', 'version']):
        if 'int' in python_types:
            return 'to_number'
        elif 'float' in python_types:
            return 'to_double'
    
    # ID fields - keep as varchar
    if field_lower.endswith('_id') or field_lower == 'id':
        return 'to_varchar'
    
    # Default to varchar for strings, variant for complex types
    if 'dict' in python_types or 'list' in python_types:
        return 'variant'
    
    return 'to_varchar'


def detect_array_field(field_types):
    """
    Detect if there's an array/list field in the schema that should be flattened.
    
    Returns:
        Tuple of (array_field_name, has_array) or (None, False)
    """
    if not field_types:
        return None, False
    
    # Common array field names to check first
    priority_arrays = ['applications', 'events', 'items', 'records', 'data', 'results']
    
    # Check priority arrays first
    for field_name in priority_arrays:
        if field_name in field_types and 'list' in field_types[field_name]:
            return field_name, True
    
    # Check any field with list type
    for field_name, types in field_types.items():
        if 'list' in types:
            return field_name, True
    
    return None, False


def generate_dbt_model(table_name, model_name, fields, topic='', field_types=None, flatten_array=None):
    """
    Generate the dbt SQL model with incremental materialization and dynamic unique_key.
    
    Args:
        table_name: Snowflake table name
        model_name: dbt model name
        fields: List or comma-separated string of field names
        topic: Original Kafka topic name (for unique_key inference)
        field_types: Optional dict mapping field names to observed Python types
        flatten_array: Optional array field name to flatten (auto-detected if None)
    """
    # Handle both string (comma-separated) and list inputs
    if isinstance(fields, str):
        field_list = [f.strip() for f in fields.split(',') if f.strip()]
    elif isinstance(fields, list):
        field_list = fields
    else:
        field_list = []
    
    # Check for override_id in fields - auto-detect and use it in surrogate key
    base_id_field = None
    fields_lower = [f.lower() for f in field_list]
    
    if 'override_id' in fields_lower:
        # Found override_id in the data - use it in surrogate key
        base_id_field = 'override_id'
        unique_key = 'override_id__sk'
        needs_surrogate = True
    else:
        # Fallback to topic-based inference
        unique_key, needs_surrogate = infer_unique_key_from_topic(topic)
    
    # Auto-detect array field if not specified
    array_field = flatten_array
    uses_flatten = False
    if array_field is None and field_types:
        array_field, uses_flatten = detect_array_field(field_types)
    elif array_field:
        uses_flatten = True
    
    # Determine extraction source (app.value for flattened arrays, record_content otherwise)
    extraction_source = "app.value" if uses_flatten else "record_content"
    
    # Generate surrogate key if needed
    surrogate_key_sql = ""
    
    if needs_surrogate:
        # Build surrogate key - if we have a base_id_field, include it
        if base_id_field:
            # Special case: combine ce_id + field_id for uniqueness
            surrogate_key_sql = f"""    nvl2(
        meta__kafka_headers:ce_id,
        {{{{ dbt_utils.generate_surrogate_key([
            'meta__kafka_headers:ce_id',
            '{extraction_source}:{base_id_field}'
        ]) }}}},
        null
    ) as {unique_key},
"""
        else:
            # Standard surrogate key with ce_id + partition offset
            surrogate_key_sql = f"""    nvl2(
        meta__kafka_headers:ce_id,
        {{{{ dbt_utils.generate_surrogate_key([
            'meta__kafka_headers:ce_id',
            'meta__partition_offset_location__sk'
        ]) }}}},
        null
    ) as {unique_key},
"""
    
    # Generate field extractions with proper type casting
    field_extractions = []
    
    # If we have a base_id_field, extract it first (right after surrogate key)
    if base_id_field:
        if field_types and base_id_field in field_types:
            cast_func = infer_snowflake_type(base_id_field, field_types[base_id_field])
        else:
            cast_func = 'to_varchar'
        
        if cast_func == 'variant':
            field_extractions.append(f"    {extraction_source}:{base_id_field}::variant as {base_id_field},")
        else:
            field_extractions.append(f"    {cast_func}({extraction_source}:{base_id_field}) as {base_id_field},")
    
    for field in field_list:
        # Skip the array field itself if we're flattening it
        if uses_flatten and field == array_field:
            continue
        
        # Skip the base ID field if we already added it at the top
        if base_id_field and field.lower() == base_id_field.lower():
            continue
        
        # Determine the appropriate Snowflake type
        if field_types and field in field_types:
            cast_func = infer_snowflake_type(field, field_types[field])
        else:
            cast_func = 'to_varchar'
        
        # Handle field names that might be SQL reserved words
        field_alias = f'"{field}"' if field.lower() in ['name', 'comment', 'order', 'group', 'user', 'version'] else field
        
        if cast_func == 'variant':
            field_extractions.append(f"    {extraction_source}:{field}::variant as {field_alias},")
        else:
            field_extractions.append(f"    {cast_func}({extraction_source}:{field}) as {field_alias},")
    
    # Build FROM clause
    if uses_flatten:
        from_clause = f"""from {{{{ source('kafka_realtime', '{table_name}_processed') }}}},
    lateral flatten(input => record_content:{array_field}) as app"""
    else:
        from_clause = f"""from {{{{ source('kafka_realtime', '{table_name}_processed') }}}}"""
    
    # Determine appropriate tag based on topic
    topic_lower = topic.lower()
    if 'vulnerability' in topic_lower or 'vulnerability_management' in topic_lower:
        tag = 'vulnerability'
    else:
        tag = 'kafka_extracted'
    
    # Build the model SQL
    # Note: materialized='incremental' is set at project level in dbt_project.yml
    model_sql = f"""{{{{
  config(
        unique_key='{unique_key}',
        tags=["{tag}"]
  )
}}}}

select
{surrogate_key_sql}{chr(10).join(field_extractions)}
    meta__kafka_topic,
    meta__partition,
    meta__offset,
    meta__kafka_location,
    meta__partition_offset_location__sk,
    meta__kafka_timestamp,
    meta__kafka_headers,
    meta__kafka_key,
    meta__kafka_key__sk,
    meta__is_tombstone,
    meta__materialized_at
{from_clause}

{{% if is_incremental() %}}
    WHERE meta__materialized_at > (
        SELECT max(meta__materialized_at)
        FROM {{{{ this }}}}
    )
{{% endif %}}
"""
    
    return model_sql


def add_source_to_yaml(sources_file, table_name, dry_run=False):
    """
    Add the source to sources.yml with minimal changes.
    Only adds the table name entry in alphabetical order, preserving all formatting.
    """
    import re
    
    print(f"\n📂 Processing sources file: {sources_file}")
    
    # Read existing sources.yml
    try:
        with open(sources_file, 'r') as f:
            original_content = f.read()
            lines = original_content.splitlines(keepends=True)
    except Exception as e:
        print(f"❌ Error reading sources file: {e}")
        sys.exit(1)
    
    # Parse to check if entry already exists
    sources_data = yaml.safe_load(original_content)
    sources_list = sources_data.get('sources', [])
    raw_kafka_source = None
    
    for source in sources_list:
        if source.get('name') == 'kafka_realtime':
            raw_kafka_source = source
            break
    
    if not raw_kafka_source:
        print("❌ Error: 'kafka_realtime' source not found in file")
        return False, None, None
    
    # Check if table already exists
    tables = raw_kafka_source.get('tables', [])
    existing_table_names = [t.get('name') for t in tables]
    
    new_table = generate_source_config(table_name)
    new_table_name = new_table['name']
    
    if new_table_name in existing_table_names:
        print(f"⚠️  Table '{new_table_name}' already exists in sources")
        return False, None, None
    
    print(f"➕ Adding table '{new_table_name}' to sources in alphabetical order")
    
    # Find all existing table entries and their line numbers
    # We need to find entries under the 'tables:' section, not the source name
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
        print("❌ Error: Could not find any table entries in sources file")
        return False, None, None
    
    # Find alphabetical insert position
    insert_line = None
    indent_to_use = table_entries[0][1]  # Use same indentation as existing entries
    
    for line_num, indent, tname in table_entries:
        if new_table_name < tname:
            # Insert before this entry
            insert_line = line_num
            break
    
    if insert_line is None:
        # Append after last table
        last_table_line = table_entries[-1][0]
        insert_line = last_table_line + 1
    
    # Create new entry with minimal formatting (name only, no description)
    new_lines = [
        f"{indent_to_use}- name: {new_table['name']}\n"
    ]
    
    # Insert the new lines
    new_lines_copy = lines.copy()
    new_lines_copy[insert_line:insert_line] = new_lines
    new_content = ''.join(new_lines_copy)
    
    if not dry_run:
        # Write back to file
        with open(sources_file, 'w') as f:
            f.write(new_content)
        print(f"✅ Updated {sources_file} (added 1 line only)")
    
    return True, original_content, new_content


def create_dbt_model_file(models_dir, model_name, table_name, fields, topic='', field_types=None, flatten_array=None, dry_run=False):
    """Create the dbt model SQL file."""
    model_path = Path(models_dir) / 'staging' / 'kafka_realtime' / f'{model_name}.sql'
    
    print(f"\n📝 Creating model file: {model_path}")
    
    if model_path.exists():
        print(f"⚠️  Model file already exists: {model_path}")
        return False, None, None
    
    # Generate model SQL
    model_sql = generate_dbt_model(table_name, model_name, fields, topic, field_types, flatten_array)
    
    print("➕ Creating new model file")
    
    if not dry_run:
        # Ensure directory exists
        model_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write model file
        with open(model_path, 'w') as f:
            f.write(model_sql)
        print(f"✅ Created {model_path}")
    
    return True, str(model_path), model_sql


def create_typecast_model_file(models_dir, raw_model_name, topic='', dry_run=False):
    """
    Create a typecast model that aliases the raw staging model.
    This model simply selects * from the raw model and uses an alias to rename it.
    
    Args:
        models_dir: Directory containing dbt models
        raw_model_name: Name of the raw staging model (e.g., 'stg_kafka__topic_name')
        topic: Original Kafka topic name
        dry_run: If True, don't write files
    
    Returns:
        Tuple of (success, model_path, model_sql)
    """
    # Generate the alias name by removing 'stg_kafka__' prefix
    if raw_model_name.startswith('stg_kafka__'):
        alias_name = raw_model_name.replace('stg_kafka__', '', 1)
    else:
        alias_name = raw_model_name
    
    # The typecast model filename matches the alias (without stg_kafka__ prefix)
    typecast_model_name = alias_name
    model_path = Path(models_dir) / 'staging' / 'kafka_realtime' / f'{typecast_model_name}.sql'
    
    print(f"\n📝 Creating typecast model file: {model_path}")
    
    if model_path.exists():
        print(f"⚠️  Typecast model file already exists: {model_path}")
        return False, None, None
    
    # Generate simple typecast model SQL
    model_sql = f"""{{{{
  config(
        alias='{alias_name}'
  )
}}}}

select *
from {{{{ ref('{raw_model_name}') }}}}
"""
    
    print("➕ Creating new typecast model file")
    
    if not dry_run:
        # Ensure directory exists
        model_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write model file
        with open(model_path, 'w') as f:
            f.write(model_sql)
        print(f"✅ Created {model_path} (aliased as '{alias_name}')")
    
    return True, str(model_path), model_sql


def main():
    parser = argparse.ArgumentParser(
        description='Create dbt extraction layer for Kafka topic'
    )
    parser.add_argument(
        '--topic',
        required=True,
        help='Kafka topic name (e.g., audit.action.v1)'
    )
    parser.add_argument(
        '--sources-file',
        required=True,
        help='Path to sources.yml file'
    )
    parser.add_argument(
        '--models-dir',
        required=True,
        help='Path to dbt models directory'
    )
    parser.add_argument(
        '--fields',
        default=None,
        help='Comma-separated fields to extract (auto-discovered if not provided)'
    )
    parser.add_argument(
        '--auto-discover',
        action='store_true',
        help='Auto-discover schema from Snowflake (requires Snowflake credentials)'
    )
    parser.add_argument(
        '--snowflake-account',
        help='Snowflake account (or set SNOWFLAKE_ACCOUNT env var)'
    )
    parser.add_argument(
        '--snowflake-user',
        help='Snowflake user (or set SNOWFLAKE_USER env var)'
    )
    parser.add_argument(
        '--snowflake-password',
        help='Snowflake password (or set SNOWFLAKE_PASSWORD env var). Not needed if using --snowflake-authenticator=externalbrowser'
    )
    parser.add_argument(
        '--snowflake-authenticator',
        help='Snowflake authenticator (e.g., "externalbrowser" for SSO, or set SNOWFLAKE_AUTHENTICATOR env var)'
    )
    parser.add_argument(
        '--snowflake-warehouse',
        help='Snowflake warehouse (or set SNOWFLAKE_WAREHOUSE env var)'
    )
    parser.add_argument(
        '--snowflake-database',
        help='Snowflake database (or set SNOWFLAKE_DATABASE env var)'
    )
    parser.add_argument(
        '--snowflake-schema',
        default='public',
        help='Snowflake schema (or set SNOWFLAKE_SCHEMA env var)'
    )
    parser.add_argument(
        '--snowflake-role',
        help='Snowflake role (or set SNOWFLAKE_ROLE env var)'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=100,
        help='Number of records to sample for schema discovery (default: 100)'
    )
    parser.add_argument(
        '--flatten-array',
        help='Array field name to flatten with LATERAL FLATTEN (auto-detected if not specified)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without modifying files'
    )
    
    args = parser.parse_args()
    
    # Validate paths exist
    sources_file = Path(args.sources_file)
    models_dir = Path(args.models_dir)
    
    if not sources_file.exists():
        print(f"❌ Error: Sources file not found: {args.sources_file}")
        sys.exit(1)
    
    if not models_dir.exists():
        print(f"❌ Error: Models directory not found: {args.models_dir}")
        sys.exit(1)
    
    # Generate names
    table_name = generate_table_name(args.topic)
    model_name = generate_model_name(args.topic)  # Pass topic directly, not table_name
    
    print("="*70)
    print("📋 Step 3 - dbt Extraction Layer")
    print("="*70)
    print(f"\n✅ Topic: {args.topic}")
    print(f"✅ Table: {table_name}")
    print(f"✅ Model: {model_name}")
    
    # Determine fields to use
    fields_to_use = args.fields
    field_types_discovered = None
    flatten_array = args.flatten_array
    
    if args.auto_discover or (args.fields is None and any([
        args.snowflake_account,
        os.getenv('SNOWFLAKE_ACCOUNT')
    ])):
        # Build Snowflake config from args or environment variables
        snowflake_config = {
            'account': args.snowflake_account or os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': args.snowflake_user or os.getenv('SNOWFLAKE_USER'),
            'password': args.snowflake_password or os.getenv('SNOWFLAKE_PASSWORD'),
            'authenticator': args.snowflake_authenticator or os.getenv('SNOWFLAKE_AUTHENTICATOR'),
            'warehouse': args.snowflake_warehouse or os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': args.snowflake_database or os.getenv('SNOWFLAKE_DATABASE'),
            'schema': args.snowflake_schema or os.getenv('SNOWFLAKE_SCHEMA', 'public'),
            'role': args.snowflake_role or os.getenv('SNOWFLAKE_ROLE'),
        }
        
        # Auto-default to externalbrowser if no password or authenticator provided
        if not snowflake_config.get('password') and not snowflake_config.get('authenticator'):
            snowflake_config['authenticator'] = 'externalbrowser'
            print("ℹ️  No password provided - defaulting to external browser authentication (SSO)")
        
        # Check if we have required config
        required_fields = ['account', 'user', 'warehouse', 'database', 'schema']
        missing_config = [k for k in required_fields if not snowflake_config.get(k)]
        if missing_config:
            print(f"⚠️  Missing Snowflake config: {', '.join(missing_config)}")
            print("   Provide via arguments or environment variables")
            print("   Falling back to default fields")
            fields_to_use = 'id,timestamp,user_id,event_type'
        else:
            # Try to discover schema
            discovery_result = discover_schema_from_snowflake(
                table_name, 
                snowflake_config, 
                args.sample_size
            )
            
            if discovery_result and isinstance(discovery_result, tuple):
                if len(discovery_result) == 3:
                    # New format: (fields, types, array_field)
                    discovered_fields, field_types_discovered, detected_array = discovery_result
                    fields_to_use = discovered_fields
                    if detected_array:
                        flatten_array = detected_array
                    print(f"\n✅ Using {len(discovered_fields)} discovered fields")
                elif len(discovery_result) == 2:
                    # Old format: (fields, types)
                    discovered_fields, field_types_discovered = discovery_result
                    fields_to_use = discovered_fields
                    print(f"\n✅ Using {len(discovered_fields)} discovered fields")
            elif discovery_result:
                # Single value
                fields_to_use = discovery_result
                print(f"\n✅ Using {len(discovery_result)} discovered fields")
            else:
                print("⚠️  Schema discovery failed, using default fields")
                fields_to_use = 'id,timestamp,user_id,event_type'
    
    # If still None, use defaults
    if fields_to_use is None:
        fields_to_use = 'id,timestamp,user_id,event_type'
        print(f"✅ Using default fields: {fields_to_use}")
    else:
        if isinstance(fields_to_use, list):
            print(f"✅ Using fields: {', '.join(fields_to_use[:5])}{'...' if len(fields_to_use) > 5 else ''} ({len(fields_to_use)} total)")
        else:
            print(f"✅ Using fields: {fields_to_use}")
    
    # Determine unique key (priority: field-based > topic-based)
    detected_unique_key = None
    detected_base_field = None
    unique_key_source = None
    
    if fields_to_use:
        # Try to detect from actual fields in the data
        field_list = fields_to_use if isinstance(fields_to_use, list) else [f.strip() for f in fields_to_use.split(',')]
        inferred_from_fields = infer_unique_key_from_fields(field_list)
        if inferred_from_fields:
            detected_unique_key, detected_base_field, _ = inferred_from_fields
            unique_key_source = "detected from data fields (surrogate with ce_id)"
    
    if not detected_unique_key:
        # Fallback to topic-based inference
        detected_unique_key, _ = infer_unique_key_from_topic(args.topic)
        unique_key_source = "inferred from topic pattern"
    
    print(f"✅ Unique_key: {detected_unique_key} ({unique_key_source})")
    if detected_base_field:
        print(f"   Base field: {detected_base_field}")
    
    # Display array flattening info (may have been set during discovery)
    if flatten_array:
        if args.flatten_array:
            print(f"✅ Array flattening: {flatten_array} (manually specified)")
        else:
            print(f"✅ Array flattening: {flatten_array} (auto-detected from schema)")
    
    if args.dry_run:
        print(f"\n🔍 DRY RUN MODE - No files will be modified")
    
    # Add source to sources.yml
    source_added, sources_original, sources_new = add_source_to_yaml(sources_file, table_name, args.dry_run)
    
    # Create dbt model file (detects override_id automatically inside the function)
    model_created, model_path, model_content = create_dbt_model_file(
        models_dir, 
        model_name, 
        table_name, 
        fields_to_use, 
        args.topic, 
        field_types_discovered,
        flatten_array,
        args.dry_run
    )
    
    # No typecast model for realtime sink - only one model is created
    typecast_created = False
    typecast_path = None
    typecast_content = None
    
    # Show exact file changes (what WOULD be created in dry-run, or what WAS created)
    if source_added or model_created:
        print("\n" + "="*70)
        if args.dry_run:
            print("📝 EXACT FILE CHANGES (DRY RUN - These files would be created/modified)")
        else:
            print("📝 EXACT FILE CHANGES")
        print("="*70)
        
        # Show sources.yml diff
        if source_added and sources_original and sources_new:
            if args.dry_run:
                print(f"\n1. {sources_file} (WOULD MODIFY)")
            else:
                print(f"\n1. {sources_file}")
            print("-" * 70)
            
            original_lines = sources_original.splitlines(keepends=True)
            new_lines = sources_new.splitlines(keepends=True)
            
            diff = unified_diff(
                original_lines,
                new_lines,
                fromfile=f'{sources_file} (original)',
                tofile=f'{sources_file} (modified)',
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
        
        # Show new model file
        if model_created and model_path and model_content:
            if args.dry_run:
                print(f"\n2. {model_path} (WOULD CREATE)")
            else:
                print(f"\n2. {model_path} (NEW FILE)")
            print("-" * 70)
            print(f"\033[32m{model_content}\033[0m")  # Green for new file
            print("-" * 70)
        
        print("="*70)
    
    # Summary
    print("\n" + "="*70)
    
    if args.dry_run:
        print("🔍 DRY RUN COMPLETE - No changes written")
        print("="*70)
        if source_added or model_created:
            print("\n✅ Would create:")
            if source_added:
                print(f"   - Source entry in {sources_file}")
            if model_created:
                print(f"   - Model file: models/staging/kafka_realtime/{model_name}.sql")
        else:
            print("\nℹ️  No changes needed (files already exist)")
    else:
        print("✅ COMPLETE")
        print("="*70)
        if source_added or model_created:
            print("\n✅ Created:")
            if source_added:
                print(f"   - Source entry in {sources_file}")
            if model_created:
                print(f"   - Model file: models/staging/kafka_realtime/{model_name}.sql")
        else:
            print("\nℹ️  No changes needed (files already exist)")


if __name__ == '__main__':
    main()
