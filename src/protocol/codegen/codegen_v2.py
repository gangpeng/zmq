#!/usr/bin/env python3
"""
Kafka Protocol Schema → Zig Code Generator (v2)

Enhanced generator that produces Zig structs with:
- serialize(buf, pos, version)
- deserialize(alloc, buf, pos, version)
- calcSize(version)

Handles: version-conditional fields, flexible versions, nullable types,
arrays (compact and non-compact), nested structs, tagged fields.

Usage:
    python3 codegen_v2.py <schema_dir> <output_dir>
"""

import json
import os
import re
import sys
from pathlib import Path


def strip_comments(text):
    """Remove // comments from JSON."""
    lines = []
    for line in text.split('\n'):
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
    """Parse version spec like '0+', '3-7', 'none'."""
    if spec is None or spec == 'none':
        return None, None
    spec = str(spec).strip()
    if spec.endswith('+'):
        return int(spec[:-1]), None
    if '-' in spec:
        parts = spec.split('-')
        return int(parts[0]), int(parts[1])
    return int(spec), int(spec)


def to_snake_case(name):
    """Convert CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def type_to_zig(kafka_type, nullable=False):
    """Map Kafka schema types to Zig types."""
    base = kafka_type.strip()
    if base.startswith('[]'):
        inner = base[2:]
        inner_zig = type_to_zig(inner)
        # Arrays are always non-nullable in Zig — null arrays become empty slices
        return f'[]const {inner_zig}'

    type_map = {
        'int8': 'i8', 'uint8': 'u8',
        'int16': 'i16', 'uint16': 'u16',
        'int32': 'i32', 'uint32': 'u32',
        'int64': 'i64', 'uint64': 'u64',
        'float64': 'f64', 'bool': 'bool',
        # All strings are nullable since readString/readCompactString return ?[]const u8
        'string': '?[]const u8',
        'bytes': '?[]const u8',
        'records': '?[]const u8',
        'uuid': '[16]u8',
    }
    if base in type_map:
        return type_map[base]
    return f'?{base}' if nullable else base


def is_primitive(kafka_type):
    """Check if a type is a Kafka primitive."""
    base = kafka_type.strip()
    if base.startswith('[]'):
        base = base[2:]
    return base in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                    'int64', 'uint64', 'float64', 'bool', 'string', 'bytes',
                    'records', 'uuid')


def default_value_zig(kafka_type, default, nullable=False):
    """Convert a Kafka default value to Zig syntax."""
    if default is None:
        if nullable or kafka_type in ('string', 'bytes', 'records'):
            return 'null'
        if kafka_type.startswith('[]'):
            return '&.{}'
        if kafka_type == 'bool':
            return 'false'
        if kafka_type == 'uuid':
            return '.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }'
        if not is_primitive(kafka_type):
            return '.{}'
        return '0'

    if isinstance(default, bool):
        return 'true' if default else 'false'
    if isinstance(default, (int, float)):
        return str(default)

    default = str(default)
    if default == 'null':
        return 'null'
    if default in ('true', 'false'):
        return default
    try:
        int(default)
        return default
    except ValueError:
        return f'"{default}"'


# Type classification helpers

def is_fixed_size_type(kafka_type):
    """Check if a type has a fixed wire size (no length prefix)."""
    return kafka_type in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                          'int64', 'uint64', 'float64', 'bool', 'uuid')


def get_fixed_size(kafka_type):
    """Get fixed wire size for a primitive type."""
    sizes = {
        'int8': 1, 'uint8': 1, 'int16': 2, 'uint16': 2,
        'int32': 4, 'uint32': 4, 'int64': 8, 'uint64': 8,
        'float64': 8, 'bool': 1, 'uuid': 16,
    }
    return sizes.get(kafka_type)


def get_read_expr(kafka_type, flexible, nullable=False):
    """Get the read expression for a type (no 'try' prefix — caller adds if needed)."""
    base = kafka_type.strip()

    # Fixed-size types that don't return errors
    noerror_readers = {
        'int8': 'ser.readI8(buf, pos)',
        'uint8': 'ser.readU8(buf, pos)',
        'int16': 'ser.readI16(buf, pos)',
        'uint16': 'ser.readU16(buf, pos)',
        'int32': 'ser.readI32(buf, pos)',
        'uint32': 'ser.readU32(buf, pos)',
        'int64': 'ser.readI64(buf, pos)',
        'uint64': 'ser.readU64(buf, pos)',
        'float64': 'ser.readF64(buf, pos)',
    }

    if base in noerror_readers:
        return noerror_readers[base], False  # (expr, needs_try)

    if base == 'bool':
        return 'ser.readBool(buf, pos)', True  # readBool calls readU8 then compare

    if base == 'string':
        if flexible:
            return 'ser.readCompactString(buf, pos)', True
        return 'ser.readString(buf, pos)', True

    if base in ('bytes', 'records'):
        if flexible:
            return 'ser.readCompactBytes(buf, pos)', True
        return 'ser.readBytes(buf, pos)', True

    if base == 'uuid':
        return 'ser.readUuid(buf, pos)', True

    return None, False


def get_write_fn(kafka_type, flexible, field_expr):
    """Get the serialization write function call for a type."""
    base = kafka_type.strip()

    type_writers = {
        'int8': 'writeI8', 'uint8': 'writeU8',
        'int16': 'writeI16', 'uint16': 'writeU16',
        'int32': 'writeI32', 'uint32': 'writeU32',
        'int64': 'writeI64', 'uint64': 'writeU64',
        'float64': 'writeF64', 'bool': 'writeBool',
    }

    if base in type_writers:
        return f'ser.{type_writers[base]}(buf, pos, {field_expr})'

    if base == 'string':
        if flexible:
            return f'ser.writeCompactString(buf, pos, {field_expr})'
        return f'ser.writeString(buf, pos, {field_expr})'

    if base in ('bytes', 'records'):
        if flexible:
            return f'ser.writeCompactBytes(buf, pos, {field_expr})'
        return f'ser.writeBytesBuf(buf, pos, {field_expr})'

    if base == 'uuid':
        return f'ser.writeUuid(buf, pos, {field_expr})'

    return None


def get_size_expr(kafka_type, flexible, field_expr):
    """Get the size computation expression for a type."""
    base = kafka_type.strip()

    fixed_sizes = {
        'int8': '1', 'uint8': '1',
        'int16': '2', 'uint16': '2',
        'int32': '4', 'uint32': '4',
        'int64': '8', 'uint64': '8',
        'float64': '8', 'bool': '1',
        'uuid': '16',
    }

    if base in fixed_sizes:
        return fixed_sizes[base]

    if base == 'string':
        if flexible:
            return f'ser.compactStringSize({field_expr})'
        return f'ser.stringSize({field_expr})'

    if base in ('bytes', 'records'):
        if flexible:
            return f'ser.compactBytesSize({field_expr})'
        return f'ser.bytesSize({field_expr})'

    return None


def has_any_non_tagged_fields(fields):
    """Check if there are any non-tagged fields."""
    return any('tag' not in f for f in fields)


def needs_allocator(fields):
    """Check if deserialization needs an allocator.
    True if any field is an array (needs alloc for slice allocation) or a
    nested/common struct (even non-array, since the nested struct's deserialize
    takes an allocator parameter)."""
    for f in fields:
        if 'tag' in f:
            continue
        kafka_type = f['type']
        inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
        # Arrays are materialized instead of skipped so deserialized messages
        # round-trip and tests catch bad array handling.
        if kafka_type.startswith('[]'):
            return True
        # Non-array nested/common struct — needs to forward alloc
        if not kafka_type.startswith('[]') and not is_primitive(inner):
            return True
    return False


def has_variable_fields(fields):
    """Check if any non-tagged field uses self (strings, bytes, arrays, nested structs)."""
    for f in fields:
        if 'tag' in f:
            continue
        kafka_type = f['type']
        if kafka_type.startswith('[]'):
            return True
        if kafka_type in ('string', 'bytes', 'records'):
            return True
        inner = kafka_type
        if not is_primitive(inner) and 'fields' in f:
            return True
    return False


class CodeGen:
    """Enhanced code generator with serde methods."""

    def __init__(self, schema):
        self.schema = schema
        self.name = schema['name']
        self.api_key = schema.get('apiKey')
        self.msg_type = schema['type']
        self.valid_versions = schema.get('validVersions', '0')
        self.flexible_versions = schema.get('flexibleVersions', 'none')
        self.fields = schema.get('fields', [])
        self.common_structs = schema.get('commonStructs', [])

        self.min_ver, self.max_ver = parse_version_range(self.valid_versions)
        self.flex_min, _ = parse_version_range(self.flexible_versions)

    def is_flexible_at(self, check_version=None):
        """Check if the message uses flexible versions."""
        if self.flex_min is None:
            return False
        if check_version is not None:
            return check_version >= self.flex_min
        return True  # Has flexible versions at some point

    def generate(self):
        """Generate complete Zig source file."""
        lines = []
        lines.append(f'// Auto-generated from {self.name}.json')
        lines.append(f'// API Key: {self.api_key}, Type: {self.msg_type}')
        lines.append(f'// Valid Versions: {self.valid_versions}, Flexible Versions: {self.flexible_versions}')
        lines.append('//')
        lines.append('// DO NOT EDIT - generated by codegen_v2.py')
        lines.append('')
        lines.append('const std = @import("std");')
        lines.append('const ser = @import("../serialization.zig");')
        lines.append('const Allocator = std.mem.Allocator;')
        lines.append('')

        for common in self.common_structs:
            lines.extend(self._gen_struct(common['name'], common.get('fields', []), 0))
            lines.append('')

        lines.extend(self._gen_struct(self.name, self.fields, 0))
        lines.append('')
        return '\n'.join(lines)

    def _gen_struct(self, struct_name, fields, indent):
        """Generate a struct with nested structs, fields, and serde methods."""
        pad = '    ' * indent
        lines = []
        lines.append(f'{pad}pub const {struct_name} = struct {{')

        # Generate nested structs first (Zig requires declarations before fields or after)
        # Actually in Zig, nested type declarations can go anywhere in a struct.
        # But we'll put them first for clarity.
        nested = []
        for field in fields:
            kafka_type = field['type']
            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
            if not is_primitive(inner) and 'fields' in field:
                nested.append((inner, field.get('fields', [])))

        for nested_name, nested_fields in nested:
            lines.extend(self._gen_struct(nested_name, nested_fields, indent + 1))
            lines.append('')

        # Generate fields
        for field in fields:
            if 'tag' in field:
                continue  # Skip tagged fields in struct definition for now

            zig_name = to_snake_case(field['name'])
            kafka_type = field['type']
            nullable = 'nullableVersions' in field
            versions = field.get('versions', '0+')
            about = field.get('about', '')

            zig_type = type_to_zig(kafka_type, nullable)
            # Handle nested struct type references
            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
            if not is_primitive(inner) and 'fields' in field:
                if kafka_type.startswith('[]'):
                    zig_type = f'[]const {inner}'  # always non-nullable arrays
                else:
                    zig_type = f'?{inner}' if nullable else inner

            default = default_value_zig(kafka_type, field.get('default'), nullable)
            # All arrays default to empty slice
            if kafka_type.startswith('[]'):
                default = '&.{}'

            lines.append(f'{pad}    /// {about}' if about else f'{pad}    ///')
            lines.append(f'{pad}    /// Versions: {versions}')
            lines.append(f'{pad}    {zig_name}: {zig_type} = {default},')

        # Generate serialize method
        lines.append('')
        lines.extend(self._gen_serialize(struct_name, fields, indent + 1))

        # Generate deserialize method
        lines.append('')
        lines.extend(self._gen_deserialize(struct_name, fields, indent + 1))

        # Generate calcSize method
        lines.append('')
        lines.extend(self._gen_calc_size(struct_name, fields, indent + 1))

        lines.append(f'{pad}}};')
        return lines

    def _flex_check(self):
        """Return the version check expression for flexible versions.
        Returns None if never flexible, or 'version >= N' for N > 0.
        Use _is_always_flexible() and _has_flex_versions() for the edge cases."""
        if self.flex_min is None:
            return None
        if self.flex_min == 0:
            return None  # Always flexible — use _is_always_flexible() check instead
        return f'version >= {self.flex_min}'

    def _is_always_flexible(self):
        """Check if this message is always flexible (flex_min=0)."""
        return self.flex_min is not None and self.flex_min == 0

    def _has_flex_versions(self):
        """Check if this message has flexible versions at all."""
        return self.flex_min is not None

    def _gen_flex_branch(self, lines, pad, flex_code, nonflex_code):
        """Generate flex/nonflex branching code.
        flex_code: list of lines for flexible path
        nonflex_code: list of lines for non-flexible path
        """
        fc = self._flex_check()
        if fc:
            # Conditional: some versions flexible, some not
            lines.append(f'{pad}if ({fc}) {{')
            for line in flex_code:
                lines.append(f'{pad}    {line}')
            lines.append(f'{pad}}} else {{')
            for line in nonflex_code:
                lines.append(f'{pad}    {line}')
            lines.append(f'{pad}}}')
        elif self._is_always_flexible():
            # Always flexible
            for line in flex_code:
                lines.append(f'{pad}{line}')
        else:
            # Never flexible
            for line in nonflex_code:
                lines.append(f'{pad}{line}')

    def _gen_flex_tagged_fields_write(self, lines, pad):
        """Generate tagged fields write for flexible versions."""
        fc = self._flex_check()
        if fc:
            lines.append(f'{pad}if ({fc}) ser.writeEmptyTaggedFields(buf, pos);')
        elif self._is_always_flexible():
            lines.append(f'{pad}ser.writeEmptyTaggedFields(buf, pos);')
        # else: never flexible, no tagged fields

    def _gen_flex_tagged_fields_skip(self, lines, pad):
        """Generate tagged fields skip for flexible versions (deserialize)."""
        fc = self._flex_check()
        if fc:
            lines.append(f'{pad}if ({fc}) try ser.skipTaggedFields(buf, pos);')
        elif self._is_always_flexible():
            lines.append(f'{pad}try ser.skipTaggedFields(buf, pos);')

    def _gen_flex_tagged_fields_size(self, lines, pad):
        """Generate tagged fields size for flexible versions."""
        fc = self._flex_check()
        if fc:
            lines.append(f'{pad}if ({fc}) size += 1;')
        elif self._is_always_flexible():
            lines.append(f'{pad}size += 1;')

    def _gen_version_guard(self, pad, versions):
        """Generate version guard if needed. Returns (needs_close, extra_pad, lines)."""
        v_min, v_max = parse_version_range(versions)
        lines = []
        needs_close = False

        if v_min is not None and v_min > 0:
            if v_max is not None:
                lines.append(f'{pad}if (version >= {v_min} and version <= {v_max}) {{')
            else:
                lines.append(f'{pad}if (version >= {v_min}) {{')
            needs_close = True
            extra_pad = pad + '    '
        else:
            extra_pad = pad
        return needs_close, extra_pad, lines

    def _gen_serialize(self, struct_name, fields, indent):
        """Generate serialize method."""
        pad = '    ' * indent
        lines = []

        # Check if version parameter is used
        non_tagged = [f for f in fields if 'tag' not in f]
        uses_version = self._flex_check() is not None  # conditional flex check uses version
        for field in non_tagged:
            v_min, v_max = parse_version_range(field.get('versions', '0+'))
            if v_min is not None and v_min > 0:
                uses_version = True
            kafka_type = field['type']
            # String/bytes with conditional flex versions use version
            if kafka_type in ('string', 'bytes', 'records') and self._flex_check() is not None:
                uses_version = True
            if kafka_type.startswith('[]') and self._flex_check() is not None:
                uses_version = True
            # Nested structs or struct arrays forward version
            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
            if not is_primitive(inner):
                uses_version = True  # Forward version to inner struct's serialize
            if kafka_type.startswith('[]') and not is_primitive(inner):
                uses_version = True  # Forward version to array element serialize

        version_param = 'version: i16' if uses_version else '_: i16'
        uses_self = len(non_tagged) > 0
        self_param = f'self: *const {struct_name}' if uses_self else f'_: *const {struct_name}'
        # Check if buf/pos are used (only when there are fields or tagged fields)
        uses_buf = len(non_tagged) > 0 or self._has_flex_versions()
        buf_param = 'buf: []u8' if uses_buf else '_: []u8'
        pos_param = 'pos: *usize' if uses_buf else '_: *usize'
        lines.append(f'{pad}pub fn serialize({self_param}, {buf_param}, {pos_param}, {version_param}) void {{')

        for field in non_tagged:
            zig_name = to_snake_case(field['name'])
            kafka_type = field['type']
            versions = field.get('versions', '0+')
            field_expr = f'self.{zig_name}'

            needs_close, extra_pad, guard_lines = self._gen_version_guard(pad + '    ', versions)
            lines.extend(guard_lines)

            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type

            if kafka_type.startswith('[]') and not is_primitive(inner):
                # Array of structs
                self._gen_write_array_of_structs(lines, extra_pad, field_expr)
            elif kafka_type.startswith('[]'):
                # Array of primitives
                prim_type = kafka_type[2:]
                self._gen_write_array_of_primitives(lines, extra_pad, field_expr, prim_type)
            elif not is_primitive(kafka_type):
                # Nested/common struct (non-array)
                nullable = 'nullableVersions' in field
                if nullable:
                    lines.append(f'{extra_pad}if ({field_expr}) |value| {{')
                    lines.append(f'{extra_pad}    ser.writeUnsignedVarint(buf, pos, 1);')
                    lines.append(f'{extra_pad}    value.serialize(buf, pos, version);')
                    lines.append(f'{extra_pad}}} else {{')
                    lines.append(f'{extra_pad}    ser.writeUnsignedVarint(buf, pos, 0);')
                    lines.append(f'{extra_pad}}}')
                else:
                    lines.append(f'{extra_pad}{field_expr}.serialize(buf, pos, version);')
            else:
                # Primitive / string / bytes
                self._gen_write_primitive(lines, extra_pad, field_expr, kafka_type)

            if needs_close:
                lines.append(f'{pad}    }}')

        # Tagged fields at end for flexible versions
        self._gen_flex_tagged_fields_write(lines, pad + '    ')

        lines.append(f'{pad}}}')
        return lines

    def _gen_write_array_of_structs(self, lines, pad, field_expr):
        self._gen_flex_branch(lines, pad,
            [f'ser.writeCompactArrayLen(buf, pos, {field_expr}.len);'],
            [f'ser.writeArrayLen(buf, pos, {field_expr}.len);'])
        lines.append(f'{pad}for ({field_expr}) |item| {{')
        lines.append(f'{pad}    item.serialize(buf, pos, version);')
        lines.append(f'{pad}}}')

    def _gen_write_array_of_primitives(self, lines, pad, field_expr, prim_type):
        self._gen_flex_branch(lines, pad,
            [f'ser.writeCompactArrayLen(buf, pos, {field_expr}.len);'],
            [f'ser.writeArrayLen(buf, pos, {field_expr}.len);'])
        # For variable-length element types (string/bytes), use flex check per element
        if prim_type in ('string', 'bytes', 'records') and self._has_flex_versions():
            write_flex = get_write_fn(prim_type, True, 'item')
            write_nonflex = get_write_fn(prim_type, False, 'item')
            lines.append(f'{pad}for ({field_expr}) |item| {{')
            self._gen_flex_branch(lines, pad + '    ',
                [f'{write_flex};'],
                [f'{write_nonflex};'])
            lines.append(f'{pad}}}')
        else:
            write_fn = get_write_fn(prim_type, False, 'item')
            if write_fn:
                lines.append(f'{pad}for ({field_expr}) |item| {{')
                lines.append(f'{pad}    {write_fn};')
                lines.append(f'{pad}}}')

    def _gen_write_primitive(self, lines, pad, field_expr, kafka_type):
        """Generate write for a primitive/string/bytes field."""
        if kafka_type in ('string', 'bytes', 'records') and self._has_flex_versions():
            write_flex = get_write_fn(kafka_type, True, field_expr)
            write_nonflex = get_write_fn(kafka_type, False, field_expr)
            self._gen_flex_branch(lines, pad,
                [f'{write_flex};'],
                [f'{write_nonflex};'])
        else:
            use_flex = self._is_always_flexible() and kafka_type in ('string', 'bytes', 'records')
            write_fn = get_write_fn(kafka_type, use_flex, field_expr)
            if write_fn:
                lines.append(f'{pad}{write_fn};')

    def _gen_deserialize(self, struct_name, fields, indent):
        """Generate deserialize method."""
        pad = '    ' * indent
        lines = []

        non_tagged = [f for f in fields if 'tag' not in f]
        needs_alloc = needs_allocator(non_tagged)

        # Check if version parameter is used
        uses_version = self._flex_check() is not None  # conditional flex check uses version
        for field in non_tagged:
            v_min, v_max = parse_version_range(field.get('versions', '0+'))
            if v_min is not None and v_min > 0:
                uses_version = True
            kafka_type = field['type']
            if kafka_type in ('string', 'bytes', 'records') and self._flex_check() is not None:
                uses_version = True
            if kafka_type.startswith('[]') and self._flex_check() is not None:
                uses_version = True
            # Nested structs or struct arrays forward version
            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
            if not is_primitive(inner):
                uses_version = True

        version_param = 'version: i16' if uses_version else '_: i16'

        if needs_alloc:
            alloc_param = 'alloc: Allocator'
        else:
            alloc_param = '_: Allocator'

        uses_buf = len(non_tagged) > 0 or self._has_flex_versions()
        buf_param = 'buf: []const u8' if uses_buf else '_: []const u8'
        pos_param = 'pos: *usize' if uses_buf else '_: *usize'

        lines.append(f'{pad}pub fn deserialize({alloc_param}, {buf_param}, {pos_param}, {version_param}) !{struct_name} {{')
        if len(non_tagged) > 0:
            lines.append(f'{pad}    var result = {struct_name}{{}};')
        else:
            lines.append(f'{pad}    const result = {struct_name}{{}};')

        for field in non_tagged:
            zig_name = to_snake_case(field['name'])
            kafka_type = field['type']
            nullable = 'nullableVersions' in field
            versions = field.get('versions', '0+')

            needs_close, extra_pad, guard_lines = self._gen_version_guard(pad + '    ', versions)
            lines.extend(guard_lines)

            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type

            if kafka_type.startswith('[]'):
                self._gen_read_array(lines, extra_pad, zig_name, inner, field, nullable)
            elif not is_primitive(kafka_type):
                # Nested/common struct (non-array)
                if nullable:
                    lines.append(f'{extra_pad}if ((try ser.readUnsignedVarint(buf, pos)) != 0) {{')
                    lines.append(f'{extra_pad}    result.{zig_name} = try {inner}.deserialize(alloc, buf, pos, version);')
                    lines.append(f'{extra_pad}}}')
                else:
                    lines.append(f'{extra_pad}result.{zig_name} = try {inner}.deserialize(alloc, buf, pos, version);')
            elif kafka_type in ('string', 'bytes', 'records') and self._has_flex_versions():
                fc = self._flex_check()
                read_flex, _ = get_read_expr(kafka_type, True, nullable)
                read_nonflex, _ = get_read_expr(kafka_type, False, nullable)
                if fc:
                    lines.append(f'{extra_pad}result.{zig_name} = if ({fc})')
                    lines.append(f'{extra_pad}    try {read_flex}')
                    lines.append(f'{extra_pad}else')
                    lines.append(f'{extra_pad}    try {read_nonflex};')
                else:
                    # Always flexible
                    lines.append(f'{extra_pad}result.{zig_name} = try {read_flex};')
            else:
                read_expr, needs_try = get_read_expr(kafka_type, False, nullable)
                if read_expr:
                    prefix = 'try ' if needs_try else ''
                    lines.append(f'{extra_pad}result.{zig_name} = {prefix}{read_expr};')

            if needs_close:
                lines.append(f'{pad}    }}')

        # Skip tagged fields
        self._gen_flex_tagged_fields_skip(lines, pad + '    ')

        lines.append(f'{pad}    return result;')
        lines.append(f'{pad}}}')
        return lines

    def _gen_read_array(self, lines, pad, zig_name, inner, field, nullable):
        """Generate array deserialization with proper allocation."""
        kafka_type = field['type']
        is_struct = not is_primitive(inner)
        fc = self._flex_check()
        item_type = type_to_zig(inner)

        # Read array length
        if fc:
            lines.append(f'{pad}const {zig_name}_len: usize = if ({fc})')
            lines.append(f'{pad}    (try ser.readCompactArrayLen(buf, pos)) orelse 0')
            lines.append(f'{pad}else')
            lines.append(f'{pad}    (try ser.readArrayLen(buf, pos)) orelse 0;')
        elif self._is_always_flexible():
            lines.append(f'{pad}const {zig_name}_len: usize = (try ser.readCompactArrayLen(buf, pos)) orelse 0;')
        else:
            lines.append(f'{pad}const {zig_name}_len: usize = (try ser.readArrayLen(buf, pos)) orelse 0;')

        if is_struct:
            # Allocate and read array of structs
            lines.append(f'{pad}if ({zig_name}_len > 0) {{')
            lines.append(f'{pad}    const {zig_name}_items = try alloc.alloc({inner}, {zig_name}_len);')
            lines.append(f'{pad}    for ({zig_name}_items) |*item| {{')
            lines.append(f'{pad}        item.* = try {inner}.deserialize(alloc, buf, pos, version);')
            lines.append(f'{pad}    }}')
            lines.append(f'{pad}    result.{zig_name} = {zig_name}_items;')
            lines.append(f'{pad}}}')
        else:
            # Array of primitives — allocate and read elements.
            read_flex, flex_needs_try = get_read_expr(inner, True)
            read_nonflex, nonflex_needs_try = get_read_expr(inner, False)
            lines.append(f'{pad}if ({zig_name}_len > 0) {{')
            lines.append(f'{pad}    const {zig_name}_items = try alloc.alloc({item_type}, {zig_name}_len);')
            lines.append(f'{pad}    for ({zig_name}_items) |*item| {{')
            if inner in ('string', 'bytes', 'records'):
                if fc:
                    lines.append(f'{pad}        item.* = if ({fc})')
                    lines.append(f'{pad}            try {read_flex}')
                    lines.append(f'{pad}        else')
                    lines.append(f'{pad}            try {read_nonflex};')
                elif self._is_always_flexible():
                    lines.append(f'{pad}        item.* = try {read_flex};')
                else:
                    lines.append(f'{pad}        item.* = try {read_nonflex};')
            else:
                if read_nonflex:
                    prefix = 'try ' if nonflex_needs_try else ''
                    lines.append(f'{pad}        item.* = {prefix}{read_nonflex};')
                elif read_flex:
                    prefix = 'try ' if flex_needs_try else ''
                    lines.append(f'{pad}        item.* = {prefix}{read_flex};')
            lines.append(f'{pad}    }}')
            lines.append(f'{pad}    result.{zig_name} = {zig_name}_items;')
            lines.append(f'{pad}}}')

    def _gen_calc_size(self, struct_name, fields, indent):
        """Generate calcSize method."""
        pad = '    ' * indent
        lines = []

        non_tagged = [f for f in fields if 'tag' not in f]

        # Check if version / self are used
        uses_version = self._flex_check() is not None  # conditional flex check uses version
        uses_self = False
        for field in non_tagged:
            v_min, v_max = parse_version_range(field.get('versions', '0+'))
            if v_min is not None and v_min > 0:
                uses_version = True
            kafka_type = field['type']
            if kafka_type in ('string', 'bytes', 'records'):
                uses_self = True
                if self._flex_check() is not None:
                    uses_version = True
            if kafka_type.startswith('[]'):
                uses_self = True
                if self._flex_check() is not None:
                    uses_version = True
            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type
            if not is_primitive(inner):
                uses_self = True
                uses_version = True  # Forward version to inner struct's calcSize

        version_param = 'version: i16' if uses_version else '_: i16'
        self_param = 'self: *const ' + struct_name if uses_self else '_: *const ' + struct_name

        lines.append(f'{pad}pub fn calcSize({self_param}, {version_param}) usize {{')
        lines.append(f'{pad}    var size: usize = 0;')

        for field in non_tagged:
            zig_name = to_snake_case(field['name'])
            kafka_type = field['type']
            versions = field.get('versions', '0+')
            field_expr = f'self.{zig_name}'

            needs_close, extra_pad, guard_lines = self._gen_version_guard(pad + '    ', versions)
            lines.extend(guard_lines)

            inner = kafka_type[2:] if kafka_type.startswith('[]') else kafka_type

            if kafka_type.startswith('[]'):
                self._gen_size_array(lines, extra_pad, field_expr, inner, field)
            elif not is_primitive(kafka_type):
                # Nested/common struct
                nullable = 'nullableVersions' in field
                if nullable:
                    lines.append(f'{extra_pad}if ({field_expr}) |value| {{')
                    lines.append(f'{extra_pad}    size += 1 + value.calcSize(version);')
                    lines.append(f'{extra_pad}}} else {{')
                    lines.append(f'{extra_pad}    size += 1;')
                    lines.append(f'{extra_pad}}}')
                else:
                    lines.append(f'{extra_pad}size += {field_expr}.calcSize(version);')
            elif kafka_type in ('string', 'bytes', 'records') and self._has_flex_versions():
                sz_flex = get_size_expr(kafka_type, True, field_expr)
                sz_nonflex = get_size_expr(kafka_type, False, field_expr)
                self._gen_flex_branch(lines, extra_pad,
                    [f'size += {sz_flex};'],
                    [f'size += {sz_nonflex};'])
            else:
                use_flex = self._is_always_flexible() and kafka_type in ('string', 'bytes', 'records')
                sz = get_size_expr(kafka_type, use_flex, field_expr)
                if sz:
                    lines.append(f'{extra_pad}size += {sz};')

            if needs_close:
                lines.append(f'{pad}    }}')

        self._gen_flex_tagged_fields_size(lines, pad + '    ')

        lines.append(f'{pad}    return size;')
        lines.append(f'{pad}}}')
        return lines

    def _gen_size_array(self, lines, pad, field_expr, inner, field):
        """Generate size computation for an array field."""
        is_struct = not is_primitive(inner)

        # Array length encoding size
        self._gen_flex_branch(lines, pad,
            [f'size += ser.unsignedVarintSize({field_expr}.len + 1);'],
            [f'size += 4;'])

        # Element sizes
        if is_struct:
            lines.append(f'{pad}for ({field_expr}) |item| {{')
            lines.append(f'{pad}    size += item.calcSize(version);')
            lines.append(f'{pad}}}')
        else:
            prim_size = get_fixed_size(inner)
            if prim_size:
                lines.append(f'{pad}size += {field_expr}.len * {prim_size};')
            else:
                # Variable-size primitives (string, bytes)
                sz_nonflex = get_size_expr(inner, False, 'item')
                sz_flex = get_size_expr(inner, True, 'item')
                if sz_nonflex:
                    lines.append(f'{pad}for ({field_expr}) |item| {{')
                    self._gen_flex_branch(lines, pad + '    ',
                        [f'size += {sz_flex};'],
                        [f'size += {sz_nonflex};'])
                    lines.append(f'{pad}}}')


def main():
    if len(sys.argv) < 3:
        print(f'Usage: {sys.argv[0]} <schema_dir> <output_dir>')
        sys.exit(1)

    schema_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    errors = 0

    for json_file in sorted(schema_dir.glob('*.json')):
        try:
            text = json_file.read_text()
            text = strip_comments(text)
            schema = json.loads(text)

            name = schema['name']
            filename = to_snake_case(name)
            output_path = output_dir / f'{filename}.zig'

            gen = CodeGen(schema)
            content = gen.generate()
            output_path.write_text(content)
            generated += 1

        except Exception as e:
            import traceback
            print(f'Error processing {json_file.name}: {e}', file=sys.stderr)
            traceback.print_exc()
            errors += 1

    print(f'Generated {generated} files ({errors} errors)')


if __name__ == '__main__':
    main()
