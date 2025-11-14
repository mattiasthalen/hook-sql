from .manifest import build_dag, build_dag_manifest

from sqlglot import exp

def build_select_clause(
    tables: list[exp.Table]
) -> list[exp.Expression]:
    """
    Build the SELECT clause with keys and temporal fields.

    >>> keys = ['order_id', 'customer_id']
    >>> orders_table = exp.Table(this='orders')
    >>> customers_table = exp.Table(this='customers')
    >>> tables = [orders_table, customers_table]
    >>> select_items = build_select_clause(tables)
    >>> query = exp.select(*select_items)
    >>> print(query.sql(pretty=True))
    SELECT
      orders._record__uid AS _UID__orders,
      customers._record__uid AS _UID__customers,
      GREATEST(orders._record__valid_from, customers._record__valid_from) AS _record__valid_from,
      LEAST(orders._record__valid_to, customers._record__valid_to) AS _record__valid_to,
      GREATEST(orders._record__updated_at, customers._record__updated_at) AS _record__updated_at,
      LEAST(orders._record__is_current, customers._record__is_current) AS _record__is_current

    >>> # Single table should not have aggregations
    >>> products_table = exp.Table(this='products')
    >>> select_single = build_select_clause([products_table])
    >>> query_single = exp.select(*select_single)
    >>> print(query_single.sql(pretty=True))
    SELECT
      _record__uid AS _UID__products,
      _record__valid_from,
      _record__valid_to,
      _record__updated_at,
      _record__is_current
    """
    select_items: list[exp.Expression] = []

    # Add key columns
    if len(tables) == 1:
        # Single table - direct key selection
        table = tables[0]
        select_items.append(exp.column("_record__uid").as_(f"_UID__{table.this}"))
    else:
        # Multiple tables - qualified key selection with aliases
        for table in tables:
            select_items.append(exp.column("_record__uid", table=table.this).as_(f"_UID__{table.this}"))

    # Add temporal fields
    select_items.extend(build_temporal_fields(tables))

    return select_items


def build_temporal_fields(
    tables: list[exp.Table]
) -> list[exp.Expression]:
    """
    Build temporal field expressions - aggregated for multiple tables, direct for single table.

    >>> # Single table - direct fields
    >>> orders_table = exp.Table(this='orders')
    >>> fields_single = build_temporal_fields([orders_table])
    >>> query = exp.select(*fields_single)
    >>> print(query.sql(pretty=True))
    SELECT
      _record__valid_from,
      _record__valid_to,
      _record__updated_at,
      _record__is_current

    >>> # Multiple tables - aggregated fields
    >>> customers_table = exp.Table(this='customers')
    >>> fields_multi = build_temporal_fields([orders_table, customers_table])
    >>> query = exp.select(*fields_multi)
    >>> print(query.sql(pretty=True))
    SELECT
      GREATEST(orders._record__valid_from, customers._record__valid_from) AS _record__valid_from,
      LEAST(orders._record__valid_to, customers._record__valid_to) AS _record__valid_to,
      GREATEST(orders._record__updated_at, customers._record__updated_at) AS _record__updated_at,
      LEAST(orders._record__is_current, customers._record__is_current) AS _record__is_current
    """
    temporal_fields = []

    if len(tables) > 1:
        temporal_fields.extend(build_temporal_aggregations(tables))
    else:
        temporal_fields.extend(build_direct_temporal_fields())

    return temporal_fields


def build_temporal_aggregations(
    tables: list[exp.Table]
) -> list[exp.Expression]:
    """
    Build temporal aggregation expressions for multiple tables.

    >>> orders_table = exp.Table(this='orders')
    >>> customers_table = exp.Table(this='customers')
    >>> regions_table = exp.Table(this='regions')
    >>> tables = [orders_table, customers_table, regions_table]
    >>> aggregations = build_temporal_aggregations(tables)
    >>> query = exp.select(*aggregations)
    >>> print(query.sql(pretty=True))
    SELECT
      GREATEST(
        orders._record__valid_from,
        customers._record__valid_from,
        regions._record__valid_from
      ) AS _record__valid_from,
      LEAST(orders._record__valid_to, customers._record__valid_to, regions._record__valid_to) AS _record__valid_to,
      GREATEST(
        orders._record__updated_at,
        customers._record__updated_at,
        regions._record__updated_at
      ) AS _record__updated_at,
      LEAST(
        orders._record__is_current,
        customers._record__is_current,
        regions._record__is_current
      ) AS _record__is_current
    """
    temporal_aggregations = {
        "_record__valid_from": "GREATEST",
        "_record__valid_to": "LEAST",
        "_record__updated_at": "GREATEST",
        "_record__is_current": "LEAST"
    }

    expressions: list[exp.Expression] = []
    for field, agg_func in temporal_aggregations.items():
        table_columns = [exp.column(field, table=table.this) for table in tables]

        aggregation = exp.Alias(
            this=exp.func(agg_func, *table_columns),
            alias=field
        )
        expressions.append(aggregation)

    return expressions


def build_direct_temporal_fields() -> list[exp.Expression]:
    """
    Build direct temporal field selections for single tables.

    >>> fields = build_direct_temporal_fields()
    >>> query = exp.select(*fields)
    >>> print(query.sql(pretty=True))
    SELECT
      _record__valid_from,
      _record__valid_to,
      _record__updated_at,
      _record__is_current
    """
    temporal_field_names = [
        "_record__valid_from",
        "_record__valid_to",
        "_record__updated_at",
        "_record__is_current"
    ]

    return [exp.column(field) for field in temporal_field_names]


def build_temporal_overlap_conditions(
    main_table: exp.Table,
    joined_table: exp.Table
) -> list[exp.Expression]:
    """
    Build temporal overlap conditions between two tables.

    >>> orders_table = exp.Table(this='orders')
    >>> customers_table = exp.Table(this='customers')
    >>> conditions = build_temporal_overlap_conditions(orders_table, customers_table)
    >>> where_clause = exp.and_(*conditions)
    >>> query = exp.select("*").from_("orders").where(where_clause)
    >>> print(query.sql(pretty=True))
    SELECT
      *
    FROM orders
    WHERE
      orders._record__valid_from < customers._record__valid_to
      AND orders._record__valid_to > customers._record__valid_from
    """
    return [
        exp.LT(
            this=exp.column("_record__valid_from", table=main_table.this),
            expression=exp.column("_record__valid_to", table=joined_table.this)
        ),
        exp.GT(
            this=exp.column("_record__valid_to", table=main_table.this),
            expression=exp.column("_record__valid_from", table=joined_table.this)
        )
    ]


def create_join_expression(
    left_table: exp.Table,
    right_table: exp.Table,
    join_column: str
) -> exp.Join:
    """
    Create a LEFT JOIN expression between two tables on a specific column with temporal overlap conditions.

    >>> orders_table = exp.Table(this='orders')
    >>> customers_table = exp.Table(this='customers')
    >>> join_expr = create_join_expression(orders_table, customers_table, 'customer_id')
    >>> query = exp.select("*").from_("orders").join(join_expr)
    >>> print(query.sql(pretty=True))
    SELECT
      *
    FROM orders
    LEFT JOIN customers
      ON orders.customer_id = customers.customer_id
      AND orders._record__valid_from < customers._record__valid_to
      AND orders._record__valid_to > customers._record__valid_from
    """
    # Build the join column equality condition
    join_condition = exp.EQ(
        this=exp.column(join_column, table=left_table.this),
        expression=exp.column(join_column, table=right_table.this),
    )

    # Add temporal overlap conditions
    temporal_conditions = build_temporal_overlap_conditions(left_table, right_table)

    # Combine all conditions with AND
    combined_condition = exp.and_(join_condition, *temporal_conditions)

    return exp.Join(
        this=right_table,
        on=combined_condition,
        kind="LEFT"
    )


def build_joins(
    left_table: exp.Table,
    joins: dict
) -> list[exp.Expression]:
    """
    Build JOIN expressions recursively from the joins manifest with temporal overlap conditions.

    >>> # Simple join
    >>> orders_table = exp.Table(this='orders')
    >>> simple_joins = {'customers': 'customer_id'}
    >>> joins_simple = build_joins(orders_table, simple_joins)
    >>> query = exp.select("*").from_("orders")
    >>> for join in joins_simple:
    ...     query = query.join(join)
    >>> print(query.sql(pretty=True))
    SELECT
      *
    FROM orders
    LEFT JOIN customers
      ON orders.customer_id = customers.customer_id
      AND orders._record__valid_from < customers._record__valid_to
      AND orders._record__valid_to > customers._record__valid_from

    >>> # Complex nested joins
    >>> complex_joins = {
    ...     'customers': {
    ...         'on': 'customer_id',
    ...         'joins': {
    ...             'regions': 'region_id'
    ...         }
    ...     }
    ... }
    >>> joins_complex = build_joins(orders_table, complex_joins)
    >>> query = exp.select("*").from_("orders")
    >>> for join in joins_complex:
    ...     query = query.join(join)
    >>> print(query.sql(pretty=True))
    SELECT
      *
    FROM orders
    LEFT JOIN customers
      ON orders.customer_id = customers.customer_id
      AND orders._record__valid_from < customers._record__valid_to
      AND orders._record__valid_to > customers._record__valid_from
    LEFT JOIN regions
      ON customers.region_id = regions.region_id
      AND customers._record__valid_from < regions._record__valid_to
      AND customers._record__valid_to > regions._record__valid_from
    """
    expressions: list[exp.Expression] = []

    for right_table_id, condition in joins.items():
        # The right_table_id is the node ID which is the actual table name
        # (e.g., "northwind__customers")
        right_table = exp.Table(
            this=exp.to_identifier(right_table_id),
            db=left_table.args.get("db"),
            catalog=left_table.args.get("catalog")
        )

        if isinstance(condition, dict):
            # Complex join with nested joins
            join_column = condition["on"]
            nested_joins = condition.get("joins", {})

            # Add the join for this table
            expressions.append(create_join_expression(left_table, right_table, join_column))

            # Recursively add nested joins
            if nested_joins:
                expressions.extend(build_joins(right_table, nested_joins))
        else:
            # Simple join - condition is the column name
            expressions.append(create_join_expression(left_table, right_table, condition))

    return expressions


def build_bridge_query(
    *,
    source_table: exp.Table,
    manifest: dict,
) -> exp.Expression:
    """
    Generate SQL for bridge tables with temporal aggregations and joins.

    Orchestrates the SELECT, JOIN, and WHERE clause generation by calling helper functions.

    >>> # Simple case - single table with no joins
    >>> products = exp.Table(this='shop__products', db='hook', catalog='silver')
    >>> manifest = {
    ...     'shop__products': {'schema': 'shop', 'table': 'products', 'grain': ['product_id']}
    ... }
    >>> query = build_bridge_query(source_table=products, manifest=manifest)
    >>> print(query.sql(pretty=True))
    SELECT
      _record__uid AS _UID__shop__products,
      _record__valid_from,
      _record__valid_to,
      _record__updated_at,
      _record__is_current
    FROM silver.hook.shop__products

    >>> # Complex case - multi-table with joins and temporal aggregations
    >>> orders = exp.Table(this='shop__orders', db='hook', catalog='silver')
    >>> manifest = {
    ...     'shop__orders': {'schema': 'shop', 'table': 'orders', 'grain': ['order_id'], 'references': ['customer_id']},
    ...     'shop__customers': {'schema': 'shop', 'table': 'customers', 'grain': ['customer_id'], 'references': ['region_id']},
    ...     'shop__regions': {'schema': 'shop', 'table': 'regions', 'grain': ['region_id']}
    ... }
    >>> query = build_bridge_query(source_table=orders, manifest=manifest)
    >>> print(query.sql(pretty=True))
    SELECT
      shop__orders._record__uid AS _UID__shop__orders,
      shop__customers._record__uid AS _UID__shop__customers,
      shop__regions._record__uid AS _UID__shop__regions,
      GREATEST(
        shop__orders._record__valid_from,
        shop__customers._record__valid_from,
        shop__regions._record__valid_from
      ) AS _record__valid_from,
      LEAST(
        shop__orders._record__valid_to,
        shop__customers._record__valid_to,
        shop__regions._record__valid_to
      ) AS _record__valid_to,
      GREATEST(
        shop__orders._record__updated_at,
        shop__customers._record__updated_at,
        shop__regions._record__updated_at
      ) AS _record__updated_at,
      LEAST(
        shop__orders._record__is_current,
        shop__customers._record__is_current,
        shop__regions._record__is_current
      ) AS _record__is_current
    FROM silver.hook.shop__orders
    LEFT JOIN silver.hook.shop__customers
      ON shop__orders.customer_id = shop__customers.customer_id
      AND shop__orders._record__valid_from < shop__customers._record__valid_to
      AND shop__orders._record__valid_to > shop__customers._record__valid_from
    LEFT JOIN silver.hook.shop__regions
      ON shop__customers.region_id = shop__regions.region_id
      AND shop__customers._record__valid_from < shop__regions._record__valid_to
      AND shop__customers._record__valid_to > shop__regions._record__valid_from
    """

    dag = build_dag(manifest)
    full_manifest = build_dag_manifest(dag)
    
    # The source table name is the node ID in the manifest
    # (e.g., "northwind__orders" which is schema__table from the original source)
    node_id = source_table.this
    node_manifest = full_manifest.get(node_id, {"tables": [], "joins": {}})

    tables = node_manifest.get("tables", [])
    joins_dict = node_manifest.get("joins", {})

    # Convert string table names (node IDs) to exp.Table objects
    # Node IDs are the actual table names (e.g., "northwind__orders")
    tables = [exp.Table(this=table, db=source_table.db, catalog=source_table.catalog) for table in tables]
    
    # If no tables found (single table with no joins), use the source table
    if not tables:
        tables = [source_table]

    # Build query components
    select_items = build_select_clause(tables)
    query = exp.select(*select_items).from_(source_table)

    # Add joins
    if joins_dict:
        for join in build_joins(source_table, joins_dict):
            query = query.join(join)

    return query

def build_peripheral_query(
    *,
    source_table: exp.Table,
) -> exp.Expression:
    """
    Generate SQL for peripheral tables selecting the UID and all source columns.

    >>> products = exp.Table(this='shop__products', db='hook', catalog='silver')
    >>> query = build_peripheral_query(source_table=products)
    >>> print(query.sql(pretty=True))
    SELECT
      shop__products._record__uid AS _UID__shop__products,
      *
    FROM silver.hook.shop__products
    """

    record_uid = exp.column("_record__uid", table=source_table.this).as_(f"_UID__{source_table.this}")

    query = (
        exp.select(
            record_uid,
            exp.Star(),
        )
        .from_(source_table)
    )

    return query