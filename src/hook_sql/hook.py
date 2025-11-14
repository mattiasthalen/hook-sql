from sqlglot import exp, parse_one

def build_hooks(hooks: list[dict]) -> list[exp.Expression]:
    """Build SQL expressions for hooks from hook configurations.

    Args:
        hooks: List of hook dictionaries, each containing 'name', 'keyset',
               and 'expression' keys.

    Returns:
        List of SQLGlot expressions for CASE statements that build hook values.

    Examples:
        >>> hooks = [
        ...     {
        ...         "name": "test_hook",
        ...         "keyset": "test_keyset",
        ...         "expression": "column1"
        ...     }
        ... ]
        >>> expressions = build_hooks(hooks)
        >>> str(expressions[0])
        "CASE WHEN NOT column1 IS NULL THEN 'test_keyset|' + column1 END AS test_hook"
    """
    hook_expressions = []
    for hook in hooks:
        name = hook["name"]
        keyset = hook["keyset"]
        expression = hook["expression"]
        
        # Parse expression string to SQLGlot expression
        expr_col = parse_one(expression)
        
        # Build: CASE WHEN {expression} IS NOT NULL THEN '{keyset}|' + {expression} END AS {name}
        hook_expression = exp.alias_(
            exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Not(
                            this=exp.Is(
                                this=expr_col.copy(),
                                expression=exp.Null()
                            )
                        ),
                        true=exp.Add(
                            this=exp.Literal.string(f"{keyset}|"),
                            expression=expr_col.copy()
                        )
                    )
                ]
            ),
            name
        )

        hook_expressions.append(hook_expression)

    return hook_expressions

def build_hook_cte(
    *,
    source_table: exp.Table,
    hooks: list[dict],
) -> exp.Expression:
    """Build a CTE (Common Table Expression) that adds hook columns to the source table.

    Args:
        source_table: SQLGlot Table expression representing the source table.
        hooks: List of hook dictionaries for generating hook expressions.

    Returns:
        SQLGlot Expression for a SELECT statement that includes hook columns
        and all original columns from the source table.

    Examples:
        >>> from sqlglot import exp
        >>> source_table = exp.Table(this="test_table", db="test_schema")
        >>> hooks = [
        ...     {
        ...         "name": "test_hook",
        ...         "keyset": "test_keyset",
        ...         "expression": "id"
        ...     }
        ... ]
        >>> cte = build_hook_cte(source_table=source_table, hooks=hooks)
        >>> print(cte.sql(pretty=True))
        SELECT
          CASE WHEN NOT id IS NULL THEN 'test_keyset|' + id END AS test_hook,
          *
        FROM test_schema.test_table
    """
    hook_expressions = build_hooks(hooks)

    sql = (
        exp.select(
            *hook_expressions,
            exp.Star()
        )
        .from_(source_table)
    )

    return sql

def build_validity_cte(
    from_table: exp.Table,
    grain: list[str]
) -> exp.Expression:
    """Build a CTE that adds validity tracking columns to the data.

    This function creates window functions to track record validity periods,
    versions, and current status based on the grain columns.

    Args:
        from_table: SQLGlot Table expression to select from.
        grain: List of column names that define the grain for partitioning.

    Returns:
        SQLGlot Expression for a SELECT statement with validity tracking columns:
        - _record__valid_from: Start of validity period
        - _record__valid_to: End of validity period
        - _record__version: Version number within grain
        - _record__is_current: Boolean indicating if this is the current record
        - _record__updated_at: Timestamp when record was last updated

    Examples:
        >>> from sqlglot import exp
        >>> from_table = exp.Table(this="test_table")
        >>> grain = ["id"]
        >>> cte = build_validity_cte(from_table, grain)
        >>> print(cte.sql(pretty=True))
        SELECT
          *,
          COALESCE(
            LAG(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at),
            CAST('1970-01-01 00:00:00' AS DATETIME(6))
          ) AS _record__valid_from,
          COALESCE(
            LEAST(
              _record__hash_removed_at,
              LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at)
            ),
            CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))
          ) AS _record__valid_to,
          ROW_NUMBER() OVER (PARTITION BY id ORDER BY _record__loaded_at) AS _record__version,
          CASE
            WHEN LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at) IS NULL
            THEN 1
            ELSE 0
          END AS _record__is_current,
          COALESCE(
            LEAST(
              _record__hash_removed_at,
              LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at)
            ),
            _record__loaded_at
          ) AS _record__updated_at,
          CONCAT_WS('|', COALESCE(id, ''), COALESCE(_record__loaded_at, '')) AS _record__uid
        FROM test_table
    """

    # Build grain column expressions for PARTITION BY
    partition_by = [exp.to_column(col) for col in grain]
    order_by_col = exp.column("_record__loaded_at")

    # COALESCE(LAG(_record__loaded_at) OVER (...), CAST('1970-01-01 00:00:00' AS DATETIME(6)))
    record_valid_from = exp.alias_(
        exp.Coalesce(
            this=exp.Window(
                this=exp.Lag(this=exp.column("_record__loaded_at")),
                partition_by=partition_by.copy() if partition_by else None,
                order=exp.Order(expressions=[order_by_col.copy()])
            ),
            expressions=[
                exp.Cast(
                    this=exp.Literal.string("1970-01-01 00:00:00"),
                    to=exp.DataType.build("DATETIME(6)", dialect="fabric")
                )
            ]
        ),
        "_record__valid_from"
    )

    # COALESCE(LEAST(_record__hash_removed_at, LEAD(_record__loaded_at) OVER (...)), CAST('9999-12-31 23:59:59.999999' AS DATETIME(6)))
    record_valid_to = exp.alias_(
        exp.Coalesce(
            this=exp.Least(
                this=exp.column("_record__hash_removed_at"),
                expressions=[
                    exp.Window(
                        this=exp.Lead(this=exp.column("_record__loaded_at")),
                        partition_by=partition_by.copy() if partition_by else None,
                        order=exp.Order(expressions=[order_by_col.copy()])
                    )
                ]
            ),
            expressions=[
                exp.Cast(
                    this=exp.Literal.string("9999-12-31 23:59:59.999999"),
                    to=exp.DataType.build("DATETIME(6)", dialect="fabric")
                )
            ]
        ),
        "_record__valid_to"
    )

    # ROW_NUMBER() OVER (PARTITION BY ... ORDER BY _record__loaded_at)
    record_version = exp.alias_(
        exp.Window(
            this=exp.RowNumber(),
            partition_by=partition_by.copy() if partition_by else None,
            order=exp.Order(expressions=[order_by_col.copy()])
        ),
        "_record__version"
    )

    # CASE WHEN LEAD(_record__loaded_at) OVER (...) IS NULL THEN 1 ELSE 0 END
    record_is_current = exp.alias_(
        exp.Case(
            ifs=[
                exp.If(
                    this=exp.Is(
                        this=exp.Window(
                            this=exp.Lead(this=exp.column("_record__loaded_at")),
                            partition_by=partition_by.copy() if partition_by else None,
                            order=exp.Order(expressions=[order_by_col.copy()])
                        ),
                        expression=exp.Null()
                    ),
                    true=exp.Literal.number(1)
                )
            ],
            default=exp.Literal.number(0)
        ),
        "_record__is_current"
    )

    # COALESCE(LEAST(_record__hash_removed_at, LEAD(_record__loaded_at) OVER (...)), _record__loaded_at)
    record_updated_at = exp.alias_(
        exp.Coalesce(
            this=exp.Least(
                this=exp.column("_record__hash_removed_at"),
                expressions=[
                    exp.Window(
                        this=exp.Lead(this=exp.column("_record__loaded_at")),
                        partition_by=partition_by.copy() if partition_by else None,
                        order=exp.Order(expressions=[order_by_col.copy()])
                    )
                ]
            ),
            expressions=[exp.column("_record__loaded_at")]
        ),
        "_record__updated_at"
    )

    # CONCAT_WS('|', COALESCE(col1, ''), COALESCE(col2, ''), ...)
    concat_cols: list[exp.Expression] = [exp.Literal.string("|")]  # Start with separator
    for col in grain:
        col_expr = exp.to_column(col)
        concat_cols.append(
            exp.Coalesce(
                this=col_expr,
                expressions=[exp.Literal.string("")]
            )
        )
    concat_cols.append(
        exp.Coalesce(
            this=exp.column("_record__loaded_at"),
            expressions=[exp.Literal.string("")]
        )
    )
    
    record_uid = exp.alias_(
        exp.ConcatWs(expressions=concat_cols),
        "_record__uid"
    )

    sql = exp.select(
        exp.Star(),
        record_valid_from,
        record_valid_to,
        record_version,
        record_is_current,
        record_updated_at,
        record_uid
    ).from_(from_table)

    return sql

def build_hook_query(
    *,
    source_table: exp.Table,
    hooks: list[dict],
    grain: list[str],
) -> exp.Expression:
    """Build the complete query with CTEs for hooks and validity tracking.

    This function combines hook generation and validity tracking into a single
    query with time-based filtering for incremental processing.

    Args:
        source_table: SQLGlot Table expression for the source data.
        hooks: List of hook configurations for generating hook columns.
        grain: List of column names defining the partitioning grain.
        time_column: Name of the column used for time-based filtering.
        start_ts: Start timestamp for incremental processing (can be None).
        end_ts: End timestamp for incremental processing (can be None).

    Returns:
        SQLGlot Expression for the complete query with CTEs and time filtering.

    Examples:
        >>> from sqlglot import exp
        >>> source_table = exp.Table(this="test_table", db="test_schema")
        >>> hooks = [{"name": "hook1", "keyset": "key1", "expression": "col1"}]
        >>> grain = ["id"]
        >>> query = build_hook_query(
        ...     source_table=source_table,
        ...     hooks=hooks,
        ...     grain=grain,
        ... )
        >>> print(query.sql(pretty=True))
        WITH cte__hook AS (
          SELECT
            CASE WHEN NOT col1 IS NULL THEN 'key1|' + col1 END AS hook1,
            *
          FROM test_schema.test_table
        ), cte__validity AS (
          SELECT
            *,
            COALESCE(
              LAG(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at),
              CAST('1970-01-01 00:00:00' AS DATETIME(6))
            ) AS _record__valid_from,
            COALESCE(
              LEAST(
                _record__hash_removed_at,
                LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at)
              ),
              CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))
            ) AS _record__valid_to,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _record__loaded_at) AS _record__version,
            CASE
              WHEN LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at) IS NULL
              THEN 1
              ELSE 0
            END AS _record__is_current,
            COALESCE(
              LEAST(
                _record__hash_removed_at,
                LEAD(_record__loaded_at) OVER (PARTITION BY id ORDER BY _record__loaded_at)
              ),
              _record__loaded_at
            ) AS _record__updated_at,
            CONCAT_WS('|', COALESCE(id, ''), COALESCE(_record__loaded_at, '')) AS _record__uid
          FROM cte__hook
        )
        SELECT
          *
        FROM cte__validity
    """

    cte__hook = build_hook_cte(
        source_table=source_table,
        hooks=hooks
    )

    cte__validity = build_validity_cte(
        from_table=exp.table_("cte__hook"),
        grain=grain
    )

    # Build the query with CTEs using pure SQLGlot expressions
    query = (
        exp.select(exp.Star())
        .from_(exp.table_("cte__validity"))
        .with_("cte__hook", cte__hook)
        .with_("cte__validity", cte__validity)
    )

    return query