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
        hook_expression = parse_one(
            f"CASE WHEN {expression} IS NOT NULL THEN '{keyset}|' + {expression} END AS {name}",
            dialect="fabric"
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

    grain_columns = ', '.join(grain)

    record_valid_from = parse_one(
        f"""
        COALESCE(
            LAG(_record__loaded_at)
            OVER (
                PARTITION BY {grain_columns}
                ORDER BY _record__loaded_at
            ),
            CAST('1970-01-01 00:00:00' AS DATETIME(6))
        ) AS _record__valid_from
        """,
        dialect="fabric"
    )

    record_valid_to = parse_one(
        f"""
        COALESCE(
            LEAST(
                _record__hash_removed_at,
                LEAD(_record__loaded_at)
                OVER (
                    PARTITION BY {grain_columns}
                    ORDER BY _record__loaded_at
                )
            ),
            CAST('9999-12-31 23:59:59.999999' AS DATETIME(6))
        ) AS _record__valid_to
        """,
        dialect="fabric"
    )
    record_version = parse_one(
        f"""
        ROW_NUMBER()
        OVER (
            PARTITION BY {grain_columns} ORDER BY _record__loaded_at
        ) AS _record__version
        """,
        dialect="fabric"
    )

    record_is_current = parse_one(
        f"""
        CASE
            WHEN LEAD(_record__loaded_at)
            OVER (
                PARTITION BY {grain_columns}
                ORDER BY _record__loaded_at
            ) IS NULL THEN 1
            ELSE 0
        END AS _record__is_current
        """,
        dialect="fabric"
    )

    record_updated_at = parse_one(
        f"""
        COALESCE(
            LEAST(
                _record__hash_removed_at,
                LEAD(_record__loaded_at)
                OVER (
                    PARTITION BY {grain_columns}
                    ORDER BY _record__loaded_at
                )
            ),
            _record__loaded_at
        ) AS _record__updated_at
        """,
        dialect="fabric"
    )

    record_uid = parse_one(
        f"""
        CONCAT_WS('|', {', '.join(grain)}, _record__loaded_at) AS _record__uid
        """,
        dialect="fabric"
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

def build_query(
    *,
    source_table: exp.Table,
    hooks: list[dict],
    grain: list[str],
    time_column: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
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
        >>> query = build_query(
        ...     source_table=source_table,
        ...     hooks=hooks,
        ...     grain=grain,
        ...     time_column="_record__updated_at",
        ...     start_ts="2023-01-01 00:00:00",
        ...     end_ts="2023-01-02 00:00:00"
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
            CONCAT_WS('|', COALESCE(COALESCE(id, ''), ''), COALESCE(COALESCE(_record__loaded_at, ''), '')) AS _record__uid
          FROM cte__hook
        )
        SELECT
          *
        FROM cte__validity
        WHERE
          _record__updated_at BETWEEN CAST('2023-01-01 00:00:00' AS DATETIME(6)) AND CAST('2023-01-02 00:00:00' AS DATETIME(6))
    """

    cte__hook = build_hook_cte(
        source_table=source_table,
        hooks=hooks
    )

    cte__validity = build_validity_cte(
        from_table=exp.Table(this="cte__hook"),
        grain=grain
    )

    where = parse_one("1=1", dialect="fabric")

    if start_ts and end_ts:
        where = parse_one(
            f"{time_column} BETWEEN CAST('{start_ts}' AS DATETIME(6)) AND CAST('{end_ts}' AS DATETIME(6))",
            dialect="fabric"
        )

    query = parse_one(
        f"""
        WITH cte__hook AS (
            {cte__hook.sql()}
        ), cte__validity AS (
            {cte__validity.sql()}
        )
        SELECT *
        FROM cte__validity
        WHERE {where.sql()}
        """,
        dialect="fabric"
    )

    return query