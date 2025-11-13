from . import hook, uss
from sqlglot import exp

def build_queries(
    *,
    manifest: dict[str, dict],
    dialect: str | None = None,
    hook_target_db: str = "silver",
    hook_target_schema: str = "hook",
    uss_target_db: str = "gold",
    uss_target_schema: str = "uss"
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
            hook_query = hook.build_hook_query(
                source_table=exp.Table(
                    this=spec["table"],
                    db=spec["schema"],
                    catalog=spec["database"]
                ),
                hooks=spec.get("hooks", []),
                grain=spec.get("grain", [])
            ).sql(dialect=dialect, pretty=True)

        uss_bridge_query = uss.build_bridge_query(
            manifest=manifest,
            source_table=exp.Table(
                this=table,
                db=hook_target_schema,
                catalog=hook_target_db
            )
        ).sql(dialect=dialect, pretty=True)

        uss_peripheral_query = uss.build_peripheral_query(
            source_table=exp.Table(
                this=table,
                db=hook_target_schema,
                catalog=hook_target_db
            ),
            source_columns=spec.get("columns", []),
        ).sql(dialect=dialect, pretty=True)

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

    return queries