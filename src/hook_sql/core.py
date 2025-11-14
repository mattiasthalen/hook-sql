from . import hook, uss
from sqlglot import exp
from pathlib import Path


def write_query_file(path: Path, content: str) -> None:
    """Write a SQL query to a file.
    
    Args:
        path: Target file path
        content: SQL query content
    
    Example:
        >>> from pathlib import Path
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     path = Path(tmpdir) / "test.sql"
        ...     write_query_file(path, "SELECT 1")
        ...     path.read_text()
        'SELECT 1'
    """
    path.write_text(content)


def create_export_directories(base_path: Path) -> dict[str, Path]:
    """Create export directory structure.
    
    Args:
        base_path: Base export directory
    
    Returns:
        Dictionary mapping query types to their directory paths
    
    Example:
        >>> from pathlib import Path
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     base = Path(tmpdir)
        ...     dirs = create_export_directories(base)
        ...     all(d.exists() for d in dirs.values())
        True
    """
    directories = {
        "hook": base_path / "hook",
        "uss_bridge": base_path / "uss_bridge",
        "uss_peripheral": base_path / "uss_peripheral",
    }
    
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    
    return directories


def export_queries(
    queries: dict[str, dict],
    export_path: Path,
    dialect: str | None = None,
    identify: bool = True
) -> None:
    """Export queries to SQL files in organized directories.
    
    Args:
        queries: Dictionary of queries by table and query type
        export_path: Base directory for export
        dialect: SQL dialect for converting expressions to SQL
    
    Example:
        >>> from pathlib import Path
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     queries = {
        ...         "test_table": {
        ...             "hook": {"query": "SELECT * FROM source"},
        ...             "uss_bridge": {"query": "SELECT * FROM hook"},
        ...             "uss_peripheral": {"query": "SELECT * FROM bridge"}
        ...         }
        ...     }
        ...     export_queries(queries, Path(tmpdir))
        ...     (Path(tmpdir) / "hook" / "test_table.sql").exists()
        True
    """
    directories = create_export_directories(export_path)
    
    for table, query_types in queries.items():
        for query_type, query_info in query_types.items():
            query = query_info.get("query")
            if query is not None:
                # Convert to SQL string if it's an expression
                if isinstance(query, exp.Expression):
                    query = query.sql(dialect=dialect, pretty=True, identify=identify)
                target_dir = directories[query_type]
                file_path = target_dir / f"{table}.sql"
                write_query_file(file_path, query)


def build_queries(
    *,
    manifest: dict[str, dict],
    hook_target_db: str = "silver",
    hook_target_schema: str = "hook",
    uss_target_db: str = "gold",
    uss_target_schema: str = "uss",
    as_sql: bool = True,
    dialect: str | None = None,
    export_path: str | Path | None = None,
    identify: bool = True
) -> dict[str, dict]:
    """
    Example:
        >>> import json
        >>> from hook_sql.manifest import define_table_spec
        >>> manifest = {
        ...     "northwind__orders": define_table_spec(
        ...         database="bronze",
        ...         schema="northwind",
        ...         table="orders",
        ...         grain=["_HK__order"],
        ...         columns={
        ...             "id": "int",
        ...             "customer_id": "int",
        ...             "order_date": "datetime"
        ...         },
        ...         hooks=[
        ...             {
        ...                 "name": "_HK__order",
        ...                 "concept": "order",
        ...                 "keyset": "northwind:order",
        ...                 "expression": "id",
        ...             },
        ...             {
        ...                 "name": "_HK__customer",
        ...                 "concept": "customer",
        ...                 "keyset": "northwind:customer",
        ...                 "expression": "customer_id",
        ...             }
        ...         ],
        ...         invalidate_hard_deletes=True,
        ...         managed=True
        ...     ),
        ...     "northwind__customers": define_table_spec(
        ...         database="bronze",
        ...         schema="northwind",
        ...         table="customers",
        ...         grain=["_HK__customer"],
        ...         columns={
        ...             "id": "int",
        ...             "name": "string"
        ...         },
        ...         hooks=[
        ...             {
        ...                 "name": "_HK__customer",
        ...                 "concept": "customer",
        ...                 "keyset": "northwind:customer",
        ...                 "expression": "id",
        ...             },
        ...             {
        ...                 "name": "_HK__region",
        ...                 "concept": "region",
        ...                 "keyset": "northwind:region",
        ...                 "expression": "region_id",
        ...             }
        ...         ],
        ...         invalidate_hard_deletes=True,
        ...         managed=True
        ...     ),
        ...     "northwind__regions": define_table_spec(
        ...         database="bronze",
        ...         schema="northwind",
        ...         table="regions",
        ...         grain=["_HK__region"],
        ...         columns={
        ...             "id": "int",
        ...             "name": "string"
        ...         },
        ...         hooks=[
        ...             {
        ...                 "name": "_HK__region",
        ...                 "concept": "region",
        ...                 "keyset": "northwind:region",
        ...                 "expression": "id",
        ...             }
        ...         ],
        ...         invalidate_hard_deletes=True,
        ...         managed=True
        ...     )
        ... }
        >>> queries = build_queries(manifest=manifest)
        >>> print(json.dumps(queries, indent=2))
        {
          "northwind__orders": {
            "hook": {
              "target_database": "silver",
              "target_schema": "hook",
              "target_table": "northwind__orders",
              "query": "..."
            },
            "uss_bridge": {
              "target_database": "gold",
              "target_schema": "uss",
              "target_table": "_bridge__northwind__orders",
              "query": "..."
            },
            "uss_peripheral": {
              "target_database": "gold",
              "target_schema": "uss",
              "target_table": "northwind__orders",
              "query": "..."
            }
          },
          "northwind__customers": {
            "hook": {
              "target_database": "silver",
              "target_schema": "hook",
              "target_table": "northwind__customers",
              "query": "..."
            },
            "uss_bridge": {
              "target_database": "gold",
              "target_schema": "uss",
              "target_table": "_bridge__northwind__customers",
              "query": "..."
            },
            "uss_peripheral": {
              "target_database": "gold",
              "target_schema": "uss",
              "target_table": "northwind__customers",
              "query": "..."
            }
          }
        }
    """
    queries = {}

    for table, spec in manifest.items():

        hook_query = None

        if spec.get("managed") is True:
            hook_query_expr = hook.build_hook_query(
                source_table=exp.Table(
                    this=exp.to_identifier(spec["table"]),
                    db=exp.to_identifier(spec["schema"]),
                    catalog=exp.to_identifier(spec["database"])
                ),
                hooks=spec.get("hooks", []),
                grain=spec.get("grain", [])
            )
            hook_query = hook_query_expr.sql(dialect=dialect, pretty=True, identify=identify) if as_sql else hook_query_expr

        uss_bridge_query_expr = uss.build_bridge_query(
            manifest=manifest,
            source_table=exp.Table(
                this=exp.to_identifier(table),
                db=exp.to_identifier(hook_target_schema),
                catalog=exp.to_identifier(hook_target_db)
            )
        )
        uss_bridge_query = uss_bridge_query_expr.sql(dialect=dialect, pretty=True, identify=identify) if as_sql else uss_bridge_query_expr

        uss_peripheral_query_expr = uss.build_peripheral_query(
            source_table=exp.Table(
                this=exp.to_identifier(table),
                db=exp.to_identifier(hook_target_schema),
                catalog=exp.to_identifier(hook_target_db)
            ),
            source_columns=spec.get("columns", []),
        )
        uss_peripheral_query = uss_peripheral_query_expr.sql(dialect=dialect, pretty=True, identify=identify) if as_sql else uss_peripheral_query_expr

        queries[table] = {
            "hook": {
                "target_database": hook_target_db,
                "target_schema": hook_target_schema,
                "target_table": table,
                "query": hook_query,
            },
            "uss_bridge": {
                "target_database": uss_target_db,
                "target_schema": uss_target_schema,
                "target_table": f"_bridge__{table}",
                "query": uss_bridge_query,
            },
            "uss_peripheral": {
                "target_database": uss_target_db,
                "target_schema": uss_target_schema,
                "target_table": table,
                "query": uss_peripheral_query,
            }
        }

    if export_path is not None:
        export_queries(queries, Path(export_path), dialect=dialect, identify=identify)

    return queries