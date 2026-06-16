"""
Type Mapper — Automatic column type casting between source PG and target DSQL
==============================================================================
Handles common type mismatches between PostgreSQL source and Aurora DSQL target:
  - integer/bigint → uuid (generates UUID v5 from integer for deterministic mapping)
  - serial/bigserial → uuid
  - text → uuid (if value looks like a UUID, pass through; otherwise generate)
  - integer → text (cast to string)
  - timestamp → timestamptz (add UTC if no timezone)
  - Custom column-level overrides via config

Configuration via environment variables:
  TYPE_MAPPING_FILE    - Path to JSON mapping file (optional)
  TYPE_MAPPING         - Inline JSON mapping (optional, overrides file)

Mapping JSON format:
  {
    "public.customers": {
      "customer_id": {"source_type": "integer", "target_type": "uuid", "strategy": "uuid_v5"},
      "status": {"source_type": "integer", "target_type": "text", "strategy": "cast"}
    },
    "__global__": {
      "_serial_to_uuid": true,
      "_int_pk_to_uuid": true
    }
  }

Strategies:
  - "uuid_v5"    : Generate deterministic UUID from integer (same input = same UUID)
  - "uuid_random": Generate random UUID (not recommended for PKs)
  - "cast"       : Simple Python str()/int()/float() cast
  - "expression" : Custom SQL CAST expression (injected into query)
  - "auto"       : Detect source type and auto-pick strategy
"""

import json
import os
import re
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("pg_dsql_cdc.type_mapper")

# Namespace UUID for deterministic UUID v5 generation (fixed, change if you want different UUIDs)
CDC_UUID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # URL namespace


class TypeMapper:
    """
    Maps column values between source types and target types.
    Sits between the WAL parser output and the DSQL writer.
    """

    def __init__(self, config=None):
        """
        Initialize with optional config. If no explicit mapping is provided,
        auto-discovers type mismatches on first write failure.
        
        Args:
            config: CDCConfig instance (reads TYPE_MAPPING_FILE/TYPE_MAPPING env vars)
        """
        self._column_mappings: Dict[str, Dict[str, ColumnMapping]] = {}  # table -> col -> mapping
        self._target_schema_cache: Dict[str, Dict[str, str]] = {}  # table -> col -> type
        self._source_schema_cache: Dict[str, Dict[str, str]] = {}
        self._auto_discover = True
        self._target_dsn = None
        self._source_dsn = None

        if config:
            self._target_dsn = config.target_dsn
            self._source_dsn = config.source_dsn

        # Load explicit mappings from env/file
        self._load_mappings()

    def _load_mappings(self):
        """Load type mappings from environment variable or file."""
        mapping_json = os.environ.get("TYPE_MAPPING", "")
        mapping_file = os.environ.get("TYPE_MAPPING_FILE", "")

        if mapping_json:
            self._parse_mapping(json.loads(mapping_json))
        elif mapping_file and os.path.exists(mapping_file):
            with open(mapping_file) as f:
                self._parse_mapping(json.load(f))

    def _parse_mapping(self, mapping: Dict):
        """Parse the mapping JSON into internal structure."""
        for table, columns in mapping.items():
            if table == "__global__":
                continue  # Global settings handled separately
            self._column_mappings[table] = {}
            for col_name, col_config in columns.items():
                self._column_mappings[table][col_name] = ColumnMapping(
                    source_type=col_config.get("source_type", ""),
                    target_type=col_config.get("target_type", ""),
                    strategy=col_config.get("strategy", "auto"),
                )

    def transform_values(
        self,
        schema: str,
        table: str,
        values: Optional[Dict[str, Any]],
        columns: List[Dict],
    ) -> Optional[Dict[str, Any]]:
        """
        Transform column values to match target types.
        
        Args:
            schema: Table schema (e.g., "public")
            table: Table name
            values: Column name → value dict (from WAL event)
            columns: Column metadata list [{name, type, pk}]
        
        Returns:
            Transformed values dict, or None if input was None
        """
        if values is None:
            return None

        fqtn = f"{schema}.{table}"
        table_mappings = self._column_mappings.get(fqtn, {})
        
        # Also check without schema prefix
        if not table_mappings:
            table_mappings = self._column_mappings.get(table, {})

        # If no explicit mapping, try auto-discovery from target schema
        if not table_mappings and self._auto_discover and self._target_dsn:
            table_mappings = self._discover_mappings(schema, table, columns)

        if not table_mappings:
            return values  # No mappings needed

        transformed = {}
        for col_name, value in values.items():
            if col_name in table_mappings and value is not None:
                mapping = table_mappings[col_name]
                transformed[col_name] = self._apply_mapping(value, mapping, col_name, fqtn)
            else:
                transformed[col_name] = value

        return transformed

    def _apply_mapping(self, value: Any, mapping: "ColumnMapping", col_name: str, table: str) -> Any:
        """Apply a single column type mapping."""
        strategy = mapping.strategy

        if strategy == "auto":
            strategy = self._pick_strategy(value, mapping.source_type, mapping.target_type)

        if strategy == "uuid_v5":
            return self._to_uuid_v5(value, table, col_name)
        elif strategy == "uuid_random":
            return str(uuid.uuid4())
        elif strategy == "cast":
            return self._cast_value(value, mapping.target_type)
        elif strategy == "expression":
            # For SQL expression strategy, we return the value as-is
            # and let the SQL generation handle the CAST
            return value
        elif strategy == "passthrough":
            return value
        else:
            return value

    def _to_uuid_v5(self, value: Any, table: str, col_name: str) -> str:
        """
        Generate a deterministic UUID from an integer/string value.
        Same input always produces the same UUID (important for FK relationships).
        
        The namespace is: table_name + column_name, so the same ID in different
        tables produces different UUIDs (avoiding collisions).
        """
        # Create a table-specific namespace
        table_namespace = uuid.uuid5(CDC_UUID_NAMESPACE, f"{table}.{col_name}")
        # Generate UUID from the value
        return str(uuid.uuid5(table_namespace, str(value)))

    def _cast_value(self, value: Any, target_type: str) -> Any:
        """Simple type casting."""
        target_lower = target_type.lower()
        
        if target_lower in ("text", "varchar", "character varying"):
            return str(value)
        elif target_lower in ("integer", "int4", "int"):
            return int(value)
        elif target_lower in ("bigint", "int8"):
            return int(value)
        elif target_lower in ("numeric", "decimal", "float8", "double precision"):
            return float(value)
        elif target_lower == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "t", "1", "yes")
            return bool(value)
        elif target_lower == "uuid":
            # If it already looks like a UUID, pass through
            if isinstance(value, str) and self._is_uuid(value):
                return value
            # Otherwise generate deterministic UUID
            return str(uuid.uuid5(CDC_UUID_NAMESPACE, str(value)))
        elif target_lower in ("timestamptz", "timestamp with time zone"):
            # Add UTC if no timezone
            s = str(value)
            if "+" not in s and "Z" not in s and "-" not in s[10:]:
                return s + "+00:00"
            return s
        else:
            return value

    def _pick_strategy(self, value: Any, source_type: str, target_type: str) -> str:
        """Auto-pick the best strategy based on source/target types."""
        source_lower = source_type.lower() if source_type else ""
        target_lower = target_type.lower() if target_type else ""

        # integer → uuid: use deterministic UUID
        if source_lower in ("integer", "bigint", "int4", "int8", "serial", "bigserial", "smallint"):
            if target_lower == "uuid":
                return "uuid_v5"
            else:
                return "cast"

        # text → uuid: check if already UUID format
        if source_lower in ("text", "varchar", "character varying"):
            if target_lower == "uuid":
                if isinstance(value, str) and self._is_uuid(value):
                    return "passthrough"
                return "uuid_v5"

        return "cast"

    def _is_uuid(self, value: str) -> bool:
        """Check if a string is a valid UUID format."""
        try:
            uuid.UUID(value)
            return True
        except (ValueError, AttributeError):
            return False

    def _discover_mappings(self, schema: str, table: str, source_columns: List[Dict]) -> Dict[str, "ColumnMapping"]:
        """
        Auto-discover type mismatches by comparing source column types
        with target schema. Caches results.
        """
        fqtn = f"{schema}.{table}"
        
        if fqtn in self._column_mappings:
            return self._column_mappings[fqtn]

        # Fetch target column types
        target_types = self._get_target_column_types(schema, table)
        if not target_types:
            return {}

        mappings = {}
        for col in source_columns:
            col_name = col["name"]
            source_type = col.get("type", "").lower()
            target_type = target_types.get(col_name, "").lower()

            if not target_type or not source_type:
                continue

            # Check if types are incompatible
            if self._types_need_mapping(source_type, target_type):
                mappings[col_name] = ColumnMapping(
                    source_type=source_type,
                    target_type=target_type,
                    strategy="auto",
                )
                logger.info(f"Auto-discovered mapping: {fqtn}.{col_name}: {source_type} → {target_type}")

        # Cache for future events
        self._column_mappings[fqtn] = mappings
        return mappings

    def _types_need_mapping(self, source: str, target: str) -> bool:
        """Determine if two types are incompatible and need mapping."""
        # Normalize type names
        int_types = {"integer", "int4", "int8", "bigint", "smallint", "int2", "serial", "bigserial"}
        text_types = {"text", "varchar", "character varying", "char"}
        uuid_types = {"uuid"}
        
        source_family = "int" if source in int_types else "text" if source in text_types else "uuid" if source in uuid_types else source
        target_family = "int" if target in int_types else "text" if target in text_types else "uuid" if target in uuid_types else target

        # Same family = no mapping needed
        if source_family == target_family:
            return False

        # Different families = mapping needed
        return source_family != target_family

    def _get_target_column_types(self, schema: str, table: str) -> Dict[str, str]:
        """Fetch column types from the target DSQL database."""
        fqtn = f"{schema}.{table}"
        if fqtn in self._target_schema_cache:
            return self._target_schema_cache[fqtn]

        if not self._target_dsn:
            return {}

        try:
            import psycopg2
            with psycopg2.connect(self._target_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema, table))
                    types = {row[0]: row[1] for row in cur.fetchall()}
                    self._target_schema_cache[fqtn] = types
                    return types
        except Exception as e:
            logger.warning(f"Failed to fetch target schema for {fqtn}: {e}")
            return {}

    def get_mapping_report(self) -> Dict:
        """Return a report of all discovered/configured mappings."""
        report = {}
        for table, cols in self._column_mappings.items():
            report[table] = {
                col: {"source": m.source_type, "target": m.target_type, "strategy": m.strategy}
                for col, m in cols.items()
            }
        return report


class ColumnMapping:
    """Configuration for a single column type mapping."""

    def __init__(self, source_type: str, target_type: str, strategy: str = "auto"):
        self.source_type = source_type
        self.target_type = target_type
        self.strategy = strategy

    def __repr__(self):
        return f"ColumnMapping({self.source_type} → {self.target_type}, strategy={self.strategy})"
