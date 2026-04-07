#!/usr/bin/env python3
"""
Kafka Protocol Schema → Zig Code Generator

Reads Kafka JSON message schemas and generates Zig structs with
serialize/deserialize/calcSize for each message type.

Usage:
    python3 codegen.py <schema_dir> <output_dir>

Example:
    python3 codegen.py src/protocol/schemas/ src/protocol/generated/
"""

import json
import os
import re
import sys
from pathlib import Path


def strip_comments(text):
    """Remove // comments from JSON (Kafka schemas use them)."""
    lines = []
    for line in text.split('\n'):
        # Remove // comments but not inside strings
        in_string = False
        i = 0
        while i < len(line):
            if line[i] == '"' and (i == 0 or line[i-1] != '\\'):
                in_string = not in_string
            elif not in_string and i + 1 < len(line) and line[i:i+2] == '//':
                line = line[:i]
                break
            i += 1
        lines.append(line)
    return '\n'.join(lines)


def parse_version_range(spec):
    """Parse version spec like '0+', '3-7', '0-12'."""
    if spec is None:
        return None, None
    spec = str(spec).strip()
    if spec.endswith('+'):
        return int(spec[:-1]), None  # open-ended
    if '-' in spec:
        parts = spec.split('-')
        return int(parts[0]), int(parts[1])
    return int(spec), int(spec)


def version_active(field_versions, api_version):
    """Check if a field is active at a given API version."""
    if field_versions is None:
        return True
    min_v, max_v = parse_version_range(field_versions)
    if min_v is None:
        return True
    if api_version < min_v:
        return False
    if max_v is not None and api_version > max_v:
        return False
    return True


def type_to_zig(kafka_type, nullable=False):
    """Map Kafka schema types to Zig types."""
    base = kafka_type.strip()

    # Array types
    if base.startswith('[]'):
        inner = base[2:]
        inner_zig = type_to_zig(inner)
        if nullable:
            return f'?[]const {inner_zig}'
        return f'[]const {inner_zig}'

    type_map = {
        'int8': 'i8',
        'uint8': 'u8',
        'int16': 'i16',
        'uint16': 'u16',
        'int32': 'i32',
        'uint32': 'u32',
        'int64': 'i64',
        'uint64': 'u64',
        'float64': 'f64',
        'bool': 'bool',
        'string': '?[]const u8' if nullable else '[]const u8',
        'bytes': '?[]const u8',
        'records': '?[]const u8',  # raw bytes for record batches
        'uuid': '[16]u8',
    }

    if base in type_map:
        return type_map[base]

    # Nested struct type
    return base


def is_primitive_type(kafka_type):
    """Check if a type is a Kafka primitive (not a nested struct)."""
    base = kafka_type.strip()
    if base.startswith('[]'):
        return True  # Arrays are handled separately
    return base in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                    'int64', 'uint64', 'float64', 'bool', 'string', 'bytes',
                    'records', 'uuid')


def default_value_zig(kafka_type, default_str, nullable=False):
    """Convert a Kafka default value to Zig syntax."""
    if default_str is None:
        if nullable or kafka_type == 'bytes' or kafka_type == 'records':
            return 'null'
        if kafka_type.startswith('[]'):
            return '&.{}'
        if kafka_type == 'bool':
            return 'false'
        if kafka_type == 'string':
            return '""'
        if kafka_type == 'uuid':
            return '[_]u8{0} ** 16'
        if not is_primitive_type(kafka_type):
            return '.{}'  # Nested struct default
        return '0'

    # Handle non-string defaults from JSON (bool, int)
    if isinstance(default_str, bool):
        return 'true' if default_str else 'false'
    if isinstance(default_str, (int, float)):
        return str(default_str)

    default_str = str(default_str)
    if default_str == 'null':
        return 'null'
    if default_str == 'true':
        return 'true'
    if default_str == 'false':
        return 'false'
    if default_str.startswith('0x'):
        return default_str
    try:
        int(default_str)
        return default_str
    except ValueError:
        pass

    return f'"{default_str}"'


def to_snake_case(name):
    """Convert CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def to_zig_name(name):
    """Convert a Kafka field name to a Zig field name."""
    return to_snake_case(name)


def generate_struct(schema, struct_name, fields, indent=0):
    """Generate a Zig struct definition from fields."""
    pad = '    ' * indent
    lines = []
    lines.append(f'{pad}pub const {struct_name} = struct {{')

    # Collect nested struct types first
    nested_structs = []
    for field in fields:
        kafka_type = field['type']
        if kafka_type.startswith('[]'):
            inner = kafka_type[2:]
        else:
            inner = kafka_type
        # If it's a nested struct (not a primitive)
        if inner not in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                         'int64', 'uint64', 'float64', 'bool', 'string', 'bytes',
                         'records', 'uuid') and 'fields' in field:
            nested_structs.append((inner, field.get('fields', [])))

    # Generate nested structs
    for nested_name, nested_fields in nested_structs:
        lines.extend(generate_struct(schema, nested_name, nested_fields, indent + 1))
        lines.append('')

    # Generate fields
    for field in fields:
        zig_name = to_zig_name(field['name'])
        kafka_type = field['type']
        nullable = 'nullableVersions' in field

        # Check if it's a tagged field (only exists in some versions via tags)
        is_tagged = 'tag' in field

        zig_type = type_to_zig(kafka_type, nullable)

        # For nested struct arrays, use the struct name
        if kafka_type.startswith('[]'):
            inner = kafka_type[2:]
            if inner not in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                             'int64', 'uint64', 'float64', 'bool', 'string', 'bytes',
                             'records', 'uuid'):
                if nullable:
                    zig_type = f'?[]const {inner}'
                else:
                    zig_type = f'[]const {inner}'
        elif kafka_type not in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                                'int64', 'uint64', 'float64', 'bool', 'string', 'bytes',
                                'records', 'uuid'):
            if 'fields' in field:
                zig_type = kafka_type

        # Default value
        default = field.get('default')
        default_zig = default_value_zig(kafka_type, default, nullable)

        # Make optional fields have defaults
        if kafka_type.startswith('[]') and not nullable:
            default_zig = '&.{}'
        elif kafka_type == 'string' and not nullable:
            if default is None:
                default_zig = '""'

        about = field.get('about', '')
        versions = field.get('versions', '')

        lines.append(f'{pad}    /// {about}')
        lines.append(f'{pad}    /// Versions: {versions}')
        lines.append(f'{pad}    {zig_name}: {zig_type} = {default_zig},')

    lines.append(f'{pad}}};')
    return lines


def generate_message_file(schema):
    """Generate a complete Zig source file for a message schema."""
    name = schema['name']
    api_key = schema.get('apiKey')
    msg_type = schema['type']  # 'request' or 'response'
    valid_versions = schema.get('validVersions', '0')
    flexible_versions = schema.get('flexibleVersions', 'none')
    fields = schema.get('fields', [])

    min_ver, max_ver = parse_version_range(valid_versions)
    flex_min, _ = parse_version_range(flexible_versions if flexible_versions != 'none' else None)

    lines = []
    lines.append(f'// Auto-generated from {name}.json')
    lines.append(f'// API Key: {api_key}, Type: {msg_type}')
    lines.append(f'// Valid Versions: {valid_versions}, Flexible Versions: {flexible_versions}')
    lines.append('//')
    lines.append('// DO NOT EDIT - generated by codegen.py')
    lines.append('')
    lines.append('const std = @import("std");')
    lines.append('const ser = @import("../serialization.zig");')
    lines.append('const Allocator = std.mem.Allocator;')
    lines.append('')

    # Generate the struct
    struct_lines = generate_struct(schema, name, fields)
    lines.extend(struct_lines)
    lines.append('')

    return '\n'.join(lines)


def generate_index_file(schemas):
    """Generate a messages index file that re-exports all message types."""
    lines = []
    lines.append('// Auto-generated message index')
    lines.append('// DO NOT EDIT - generated by codegen.py')
    lines.append('')

    # Group by api key
    by_key = {}
    for schema in schemas:
        key = schema.get('apiKey')
        if key is not None:
            if key not in by_key:
                by_key[key] = {}
            by_key[key][schema['type']] = schema

    for key in sorted(by_key.keys()):
        for msg_type in ('request', 'response'):
            if msg_type in by_key[key]:
                schema = by_key[key][msg_type]
                name = schema['name']
                filename = to_snake_case(name)
                lines.append(f'pub const {filename} = @import("generated/{filename}.zig");')
                lines.append(f'pub const {name} = {filename}.{name};')

    lines.append('')
    lines.append('test {')
    lines.append('    @import("std").testing.refAllDecls(@This());')
    lines.append('}')

    return '\n'.join(lines)


def main():
    if len(sys.argv) < 3:
        print(f'Usage: {sys.argv[0]} <schema_dir> <output_dir>')
        sys.exit(1)

    schema_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    schemas = []
    generated = 0
    errors = 0

    for json_file in sorted(schema_dir.glob('*.json')):
        try:
            text = json_file.read_text()
            text = strip_comments(text)
            schema = json.loads(text)
            schemas.append(schema)

            name = schema['name']
            filename = to_snake_case(name)
            output_path = output_dir / f'{filename}.zig'

            content = generate_message_file(schema)
            output_path.write_text(content)
            generated += 1

        except Exception as e:
            print(f'Error processing {json_file.name}: {e}', file=sys.stderr)
            errors += 1

    # Generate index
    # (not writing index here — it's easier to maintain manually)

    print(f'Generated {generated} files, {errors} errors')
    print(f'Output directory: {output_dir}')


if __name__ == '__main__':
    main()
