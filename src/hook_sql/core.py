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
        identify: Whether to quote identifiers

    Example:
        >>> from pathlib import Path
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmpdir:
        ...     queries = {
        ...         "test_table": {
        ...             "hook": {
        ...                 "query": "SELECT * FROM source",
        ...                 "target_database": "silver",
        ...                 "target_schema": "hook",
        ...                 "target_table": "test_table"
        ...             }
        ...         }
        ...     }
        ...     export_queries(queries, Path(tmpdir))
        ...     (Path(tmpdir) / "silver" / "hook" / "test_table.sql").exists()
        True
    """

    for table, query_types in queries.items():
        for query_type, query_info in query_types.items():
            query = query_info.get("query")
            target_database = query_info.get("target_database")
            target_schema = query_info.get("target_schema")
            target_table = query_info.get("target_table", table)

            if query is not None and target_database and target_schema:
                # Convert to SQL string if it's an expression
                if isinstance(query, exp.Expression):
                    query = query.sql(dialect=dialect, pretty=True, identify=identify)

                # Create folder structure: export_path / target_database / target_schema / target_table.sql
                target_dir = export_path / target_database / target_schema
                target_dir.mkdir(parents=True, exist_ok=True)

                file_path = target_dir / f"{target_table}.sql"
                write_query_file(file_path, query)


def build_queries(
    *,
    manifest: dict[str, dict],
    hook_target_db: str = "silver",
    hook_target_schema: str = "hook",
    hook_prefix: str | None = None,
    uss_target_db: str = "gold",
    uss_target_schema: str = "uss",
    uss_bridge_prefix: str | None = "_bridge",
    uss_peripheral_prefix: str | None = None,
    as_sql: bool = True,
    dialect: str | None = None,
    export_path: str | Path | None = None,
    identify: bool = True,
    as_blueprints: bool = False,
) -> dict[str, dict] | list[dict]:
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
        >>> print(json.dumps(queries, indent=2))  # doctest: +ELLIPSIS
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
          },
          ...
        }

        >>> # Test as_blueprints format for SQLMesh integration
        >>> blueprints = build_queries(manifest=manifest, as_blueprints=True)
        >>> isinstance(blueprints, list)
        True
        >>> len(blueprints)
        3
        >>> blueprints[0].keys()  # doctest: +ELLIPSIS
        dict_keys(['table', 'hook_target_database', 'hook_target_schema', 'hook_target_table', 'hook_query', 'uss_bridge_target_database', 'uss_bridge_target_schema', 'uss_bridge_target_table', 'uss_bridge_query', 'uss_peripheral_target_database', 'uss_peripheral_target_schema', 'uss_peripheral_target_table', 'uss_peripheral_query'])
        >>> blueprints[0]['table'] in ['northwind__orders', 'northwind__customers', 'northwind__regions']
        True
    """
    queries: dict[str, dict] = {}

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
        )
        uss_peripheral_query = uss_peripheral_query_expr.sql(dialect=dialect, pretty=True, identify=identify) if as_sql else uss_peripheral_query_expr

        queries[table] = {
            "hook": {
                "target_database": hook_target_db,
                "target_schema": hook_target_schema,
                "target_table": f"{hook_prefix}__{table}" if hook_prefix else table,
                "query": hook_query,
            },
            "uss_bridge": {
                "target_database": uss_target_db,
                "target_schema": uss_target_schema,
                "target_table": f"{uss_bridge_prefix}__{table}" if uss_bridge_prefix else table,
                "query": uss_bridge_query,
            },
            "uss_peripheral": {
                "target_database": uss_target_db,
                "target_schema": uss_target_schema,
                "target_table": f"{uss_peripheral_prefix}__{table}" if uss_peripheral_prefix else table,
                "query": uss_peripheral_query,
            }
        }

    if export_path is not None:
        export_queries(queries, Path(export_path), dialect=dialect, identify=identify)

    if not as_blueprints:
        return queries

    blueprints: list[dict] = [
        {
            "table": key,
            "hook_target_database": value["hook"]["target_database"],
            "hook_target_schema": value["hook"]["target_schema"],
            "hook_target_table": value["hook"]["target_table"],
            "hook_query": value["hook"]["query"],
            "uss_bridge_target_database": value["uss_bridge"]["target_database"],
            "uss_bridge_target_schema": value["uss_bridge"]["target_schema"],
            "uss_bridge_target_table": value["uss_bridge"]["target_table"],
            "uss_bridge_query": value["uss_bridge"]["query"],
            "uss_peripheral_target_database": value["uss_peripheral"]["target_database"],
            "uss_peripheral_target_schema": value["uss_peripheral"]["target_schema"],
            "uss_peripheral_target_table": value["uss_peripheral"]["target_table"],
            "uss_peripheral_query": value["uss_peripheral"]["query"],
        }
        for key, value in queries.items()
    ]
    return blueprints