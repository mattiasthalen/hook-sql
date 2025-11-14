import networkx as nx
import warnings
from collections import deque

def define_hook(
    *,
    name: str,
    concept: str,
    keyset: str,
    expression: str,
    qualifier: str | None = None
) -> dict:
    """
    Example:
        >>> define_hook(
        ...     name="_HK__order",
        ...     concept="order",
        ...     keyset="order_id",
        ...     expression="1",
        ... )
        {'name': '_HK__order', 'concept': 'order', 'qualifier': None, 'keyset': 'order_id', 'expression': '1'}
        >>> define_hook(
        ...     name="_HK__order",
        ...     concept="order",
        ...     qualifier="parent",
        ...     keyset="order_id",
        ...     expression="1",
        ... )
        {'name': '_HK__order', 'concept': 'order', 'qualifier': 'parent', 'keyset': 'order_id', 'expression': '1'}
    """
    return {
        "name": name,
        "concept": concept,
        "qualifier": qualifier,
        "keyset": keyset,
        "expression": expression,
    }


def define_measure(
    name: str,
    expression: str
) -> dict:

    return {
        "name": name,
        "expression": expression,
    }

def define_event(
    name: str,
    expression: str,
    measures: list[dict] | None = None
) -> dict:
    measures = [define_measure(**measure) for measure in measures] if measures else []

    return {
        "name": name,
        "expression": expression,
        "measures": measures,
    }

def define_table_spec(
    *,
    database: str,
    schema: str,
    table: str,
    grain: str | list[str] | None = None,
    grain_aliases: list[str] | None = None,
    hooks: list[dict] | None = None,
    events: list[dict] | None = None,
    invalidate_hard_deletes: bool = False,
    managed: bool = True
) -> dict:
    """
    Examples:
        # Minimal table spec with only required parameters
        >>> define_table_spec(database="default", schema="staging", table="users")
        {'database': 'default', 'schema': 'staging', 'table': 'users', 'grain': [], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': False, 'managed': True}

        # Table spec with single grain column (string)
        >>> define_table_spec(database="default", schema="staging", table="products", grain="product_id")
        {'database': 'default', 'schema': 'staging', 'table': 'products', 'grain': ['product_id'], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': False, 'managed': True}

        # Table spec with composite grain (list)
        >>> define_table_spec(database="default", schema="staging", table="order_items", grain=["order_id", "item_id"])
        {'database': 'default', 'schema': 'staging', 'table': 'order_items', 'grain': ['order_id', 'item_id'], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': False, 'managed': True}
        
        # Table spec with invalidate_hard_deletes enabled
        >>> define_table_spec(database="default", schema="production", table="customers", invalidate_hard_deletes=True)
        {'database': 'default', 'schema': 'production', 'table': 'customers', 'grain': [], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': True, 'managed': True}

        # Table spec with managed disabled
        >>> define_table_spec(database="default", schema="raw", table="logs", managed=False)
        {'database': 'default', 'schema': 'raw', 'table': 'logs', 'grain': [], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': False, 'managed': False}
        
        # Table spec with single hook, no qualifier
        >>> define_table_spec(
        ...     database="default",
        ...     schema="analytics",
        ...     table="events",
        ...     hooks=[{
        ...         "name": "_HK__event",
        ...         "concept": "event",
        ...         "keyset": "event_id",
        ...         "expression": "id",
        ...     }]
        ... )
        {'database': 'default', 'schema': 'analytics', 'table': 'events', 'grain': [], 'grain_aliases': [], 'references': ['_HK__event'], 'hooks': [{'name': '_HK__event', 'concept': 'event', 'qualifier': None, 'keyset': 'event_id', 'expression': 'id'}], 'events': [], 'invalidate_hard_deletes': False, 'managed': True}

        # Complete table spec with all parameters
        >>> define_table_spec(
        ...     database="default",
        ...     schema="omnium",
        ...     table="orders",
        ...     grain=["order_id"],
        ...     hooks=[
        ...         {
        ...             "name": "_HK__order",
        ...             "concept": "order",
        ...             "keyset": "order_id",
        ...             "expression": 1,
        ...         },
        ...         {
        ...             "name": "_HK__customer__external",
        ...             "concept": "customer",
        ...             "qualifier": "external",
        ...             "keyset": "customer_id",
        ...             "expression": 1,
        ...         }
        ...     ],
        ...     invalidate_hard_deletes=True,
        ...     managed=False
        ... )
        {'database': 'default', 'schema': 'omnium', 'table': 'orders', 'grain': ['order_id'], 'grain_aliases': [], 'references': ['_HK__order', '_HK__customer__external'], 'hooks': [{'name': '_HK__order', 'concept': 'order', 'qualifier': None, 'keyset': 'order_id', 'expression': 1}, {'name': '_HK__customer__external', 'concept': 'customer', 'qualifier': 'external', 'keyset': 'customer_id', 'expression': 1}], 'events': [], 'invalidate_hard_deletes': True, 'managed': False}

        # Table spec with composite grain (list)
        >>> define_table_spec(
        ...     database="default",
        ...     schema="omnium",
        ...     table="orders",
        ...     grain=["_HK__order", "_HK__product"],
        ...     hooks=[
        ...         {
        ...             "name": "_HK__order",
        ...             "concept": "order",
        ...             "keyset": "order_id",
        ...             "expression": 1,
        ...         },
        ...         {
        ...             "name": "_HK__product",
        ...             "concept": "product",
        ...             "keyset": "product_id",
        ...             "expression": 1,
        ...         }
        ...     ],
        ...     invalidate_hard_deletes=True,
        ...     managed=False
        ... )
        {'database': 'default', 'schema': 'omnium', 'table': 'orders', 'grain': ['_HK__order', '_HK__product'], 'grain_aliases': [], 'references': ['_HK__order', '_HK__product'], 'hooks': [{'name': '_HK__order', 'concept': 'order', 'qualifier': None, 'keyset': 'order_id', 'expression': 1}, {'name': '_HK__product', 'concept': 'product', 'qualifier': None, 'keyset': 'product_id', 'expression': 1}], 'events': [], 'invalidate_hard_deletes': True, 'managed': False}

        # Table spec with grain and hard deletes but no hooks
        >>> define_table_spec(
        ...     database="default",
        ...     schema="warehouse",
        ...     table="inventory",
        ...     grain="inventory_id",
        ...     events=[
        ...         {
        ...             "name": "inventory updated",
        ...             "expression": "updated_at",
        ...             "measures": [
        ...                 {
        ...                     "name": "measure__inventory_updated",
        ...                     "expression": 1
        ...                 }
        ...             ]
        ...         }
        ...     ],
        ...     invalidate_hard_deletes=True
        ... )
        {'database': 'default', 'schema': 'warehouse', 'table': 'inventory', 'grain': ['inventory_id'], 'grain_aliases': [], 'references': [], 'hooks': [], 'events': [{'name': 'inventory updated', 'expression': 'updated_at', 'measures': [{'name': 'measure__inventory_updated', 'expression': 1}]}], 'invalidate_hard_deletes': True, 'managed': True}

        # Table spec with grain aliases
        >>> define_table_spec(
        ...     database="default",
        ...     schema="warehouse",
        ...     table="inventory",
        ...     grain="inventory_id",
        ...     grain_aliases=["inventory_id_alias"],
        ...     invalidate_hard_deletes=True
        ... )
        {'database': 'default', 'schema': 'warehouse', 'table': 'inventory', 'grain': ['inventory_id'], 'grain_aliases': ['inventory_id_alias'], 'references': [], 'hooks': [], 'events': [], 'invalidate_hard_deletes': True, 'managed': True}
    """
    if grain is None:
        grain = []

    if grain_aliases is None:
        grain_aliases = []

    if isinstance(grain, str):
        grain = [grain]

    hooks_spec = [define_hook(**hook) for hook in hooks] if hooks else []
    references = [hook["name"] for hook in hooks_spec if not (len(grain) == 1 and hook["name"] in grain)]

    events_spec = [define_event(**event) for event in events] if events else []

    return {
        "database": database,
        "schema": schema,
        "table": table,
        "grain": grain,
        "grain_aliases": grain_aliases,
        "references": references,
        "hooks": hooks_spec,
        "events": events_spec,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "managed": managed,
    }


def build_dag(
    specs: dict[str, dict],
    show_warnings: bool = False
) -> nx.DiGraph:
    """
    Build a directed acyclic graph (DAG) from a table specification list.

    This function creates a NetworkX DiGraph where nodes represent tables and edges
    represent foreign key relationships between tables. Each table's grain (key columns)
    and references (foreign keys) are used to establish the graph structure.

    Args:
        spec (list[dict]): A list of table specification dictionaries. Each table config
            should have 'schema' and 'table' keys, and optional 'grain' and 'references' keys:
            - 'schema': str representing the schema name
            - 'table': str representing the table name
            - 'grain': str or list of str representing the table's key columns
            - 'grain_aliases': list of str representing alternative key column names
            - 'references': str or list of str representing foreign key references

    Returns:
        nx.DiGraph: A directed graph where:
            - Nodes represent tables (identified by schema.table) with 'grain' attribute
            - Edges represent foreign key relationships from child to parent tables
            - Edge attributes include 'key' representing the foreign key column

    Notes:
        - String values for 'grain' and 'references' are automatically converted to lists
        - Tables with single-column grains or grain_aliases can be referenced as parents
        - Skips references that cannot be resolved to existing parent tables
        - Warns if the resulting graph contains cycles (not a valid DAG)
        - Prints informational messages for skipped references and cycle warnings
        - Node identifiers are unique on schema.table combination

    Example:
        >>> specs = {
        ...     "omnium__order_lines": {"schema": "omnium", "table": "order_lines", "grain": ["order_id__alias", "product_id"], "references": ["order_id__alias", "product_id"]},
        ...     "omnium__orders": {"schema": "omnium", "table": "orders", "grain": ["order_id"], "grain_aliases": ["order_id__alias"], "references": ["customer_id"]},
        ...     "omnium__customers": {"schema": "omnium", "table": "customers", "grain": ["customer_id"], "references": ["region_id"]},
        ...     "omnium__products": {"schema": "omnium", "table": "products", "grain": ["product_id"]},
        ...     "omnium__regions": {"schema": "omnium", "table": "regions", "grain": ["region_id"]}
        ... }
        >>> build_dag(specs).edges(data=True)
        OutEdgeDataView([('omnium__order_lines', 'omnium__orders', {'key': 'order_id__alias'}), ('omnium__order_lines', 'omnium__products', {'key': 'product_id'}), ('omnium__orders', 'omnium__customers', {'key': 'customer_id'}), ('omnium__customers', 'omnium__regions', {'key': 'region_id'})])
    """
    G: nx.DiGraph = nx.DiGraph()

    # Create a dictionary keyed by node_id for easier processing
    spec_dict = {}
    for _, cfg in specs.items():
        node_id = f"{cfg['schema']}__{cfg['table']}"
        grain = cfg.get("grain", [])
        refs = cfg.get("references", [])
        if isinstance(grain, str):
            grain = [grain]
        if isinstance(refs, str):
            refs = [refs]

        # Store normalized config
        normalized_cfg = {**cfg, "grain": grain, "references": refs}
        spec_dict[node_id] = normalized_cfg
        G.add_node(node_id, grain=grain, grain_aliases=cfg.get("grain_aliases", []))

    # Lookup map: single-key grain or grain_alias -> node_id
    grain_to_table = {}
    for node_id, cfg in spec_dict.items():
        # Add single-column grains
        if len(cfg["grain"]) == 1:
            grain_to_table[cfg["grain"][0]] = node_id
        # Add grain_aliases
        for alias in cfg.get("grain_aliases", []):
            grain_to_table[alias] = node_id

    # Add edges
    for child_id, cfg in spec_dict.items():
        for fk in cfg["references"]:
            parent_id = grain_to_table.get(fk)
            if not parent_id:
                warnings.warn(f"Skipping {child_id}: no parent with grain or grain_alias == [{fk}]", UserWarning) if show_warnings else None
                continue
            if parent_id == child_id:
                warnings.warn(f"Skipping {child_id}: self-reference to [{fk}] ignored", UserWarning) if show_warnings else None
                continue
            G.add_edge(child_id, parent_id, key=fk)

    # Warn on cycles
    if not nx.is_directed_acyclic_graph(G):
        warnings.warn("Graph has cycles; may not be a valid DAG.", UserWarning) if show_warnings else None

    return G

def build_dag_manifest(G: nx.DiGraph) -> dict[str, dict]:
    """Create a join manifest that captures reachable join targets per node.

    The manifest is a dictionary where each node in the graph is associated
    with the nodes it must join to, along with the join key that connects them.
    Direct parents are always included, and all additional reachable nodes are
    discovered by traversing outward from the source node. Targets are emitted in
    topological order so the manifest is deterministic.

    Args:
        G: Directed graph describing join relationships via the ``key`` edge attr.

    Returns:
        dict[str, dict]: Mapping from source node to a dictionary containing:
            - tables: list containing the source node and all its ancestors in topological order
            - joins: nested dictionary structure representing join relationships

    Example:
        >>> import json
        >>> specs = {
        ...     "omnium__order_lines": {"schema": "omnium", "table": "order_lines", "grain": ["order_id__alias", "product_id"], "references": ["order_id__alias", "product_id"]},
        ...     "omnium__orders": {"schema": "omnium", "table": "orders", "grain": ["order_id"], "grain_aliases": ["order_id__alias"], "references": ["customer_id"]},
        ...     "omnium__customers": {"schema": "omnium", "table": "customers", "grain": ["customer_id"], "references": ["region_id"]},
        ...     "omnium__products": {"schema": "omnium", "table": "products", "grain": ["product_id"]},
        ...     "omnium__regions": {"schema": "omnium", "table": "regions", "grain": ["region_id"]}
        ... }
        >>> dag = build_dag(specs)
        >>> manifest = build_dag_manifest(dag)
        >>> print(json.dumps(manifest, indent=4))
        {
            "omnium__order_lines": {
                "tables": [
                    "omnium__order_lines",
                    "omnium__orders",
                    "omnium__products",
                    "omnium__customers",
                    "omnium__regions"
                ],
                "joins": {
                    "omnium__orders": {
                        "on": "order_id__alias",
                        "joins": {
                            "omnium__customers": {
                                "on": "customer_id",
                                "joins": {
                                    "omnium__regions": "region_id"
                                }
                            }
                        }
                    },
                    "omnium__products": "product_id"
                }
            },
            "omnium__orders": {
                "tables": [
                    "omnium__orders",
                    "omnium__customers",
                    "omnium__regions"
                ],
                "joins": {
                    "omnium__customers": {
                        "on": "customer_id",
                        "joins": {
                            "omnium__regions": "region_id"
                        }
                    }
                }
            },
            "omnium__customers": {
                "tables": [
                    "omnium__customers",
                    "omnium__regions"
                ],
                "joins": {
                    "omnium__regions": "region_id"
                }
            },
            "omnium__products": {
                "tables": [
                    "omnium__products"
                ],
                "joins": {}
            },
            "omnium__regions": {
                "tables": [
                    "omnium__regions"
                ],
                "joins": {}
            }
        }
    """

    manifest: dict[str, dict] = {}

    try:
        topo = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        topo = list(G.nodes)
        warnings.warn("Could not topologically sort (cycle present). Using node order.", UserWarning)

    topo_pos = {n: i for i, n in enumerate(topo)}

    for src in G.nodes:
        # Find all ancestors by traversing the graph
        ancestors = set()
        visited = {src}
        queue = deque([src])

        while queue:
            node = queue.popleft()
            for _, neighbor, data in G.out_edges(node, data=True):
                if neighbor == src:
                    continue

                if neighbor not in visited:
                    ancestors.add(neighbor)
                    visited.add(neighbor)
                    queue.append(neighbor)



        # Build nested joins structure using iterative approach with stack
        nested_joins: dict = {}
        
        # Use a stack to build nested joins iteratively
        # Each item in stack is (current_node, current_dict_ref)
        stack = [(src, nested_joins)]
        processed = set()
        
        while stack:
            current_node, current_dict = stack.pop()
            
            if current_node in processed:
                continue
            processed.add(current_node)
            
            for _, neighbor, data in G.out_edges(current_node, data=True):
                if neighbor != src:  # Avoid self-references
                    key = data.get("key")
                    
                    # Check if this neighbor has further outgoing edges
                    has_children = any(n != src for _, n, _ in G.out_edges(neighbor, data=True))
                    
                    if has_children:
                        current_dict[neighbor] = {
                            "on": key,
                            "joins": {}
                        }
                        # Add to stack to process children
                        stack.append((neighbor, current_dict[neighbor]["joins"]))
                    else:
                        current_dict[neighbor] = key

        # Sort ancestors in topological order for predictable output
        ordered_ancestors = sorted(ancestors, key=lambda n: topo_pos.get(n, 10**9))

        # Create tables list including the source node and its ancestors
        tables = [src] + ordered_ancestors

        # Include all nodes in the manifest
        manifest[src] = {
            "tables": tables,
            "joins": nested_joins
        }

    return manifest

def build_mermaid_from_graph(G: nx.DiGraph) -> str:
    """Render the DAG as a Mermaid ``flowchart`` definition.

    Args:
        G: Directed graph representing the DAG.

    Returns:
        str: Mermaid flowchart definition as a string.

    Example:
        >>> specs = {
        ...     "omnium__order_lines": {"schema": "omnium", "table": "order_lines", "grain": ["order_id__alias", "product_id"], "references": ["order_id__alias", "product_id"]},
        ...     "omnium__orders": {"schema": "omnium", "table": "orders", "grain": ["order_id"], "grain_aliases": ["order_id__alias"], "references": ["customer_id"]},
        ...     "omnium__customers": {"schema": "omnium", "table": "customers", "grain": ["customer_id"], "references": ["region_id"]},
        ...     "omnium__products": {"schema": "omnium", "table": "products", "grain": ["product_id"]},
        ...     "omnium__regions": {"schema": "omnium", "table": "regions", "grain": ["region_id"]}
        ... }
        >>> dag = build_dag(specs)
        >>> print(build_mermaid_from_graph(dag))  # doctest: +NORMALIZE_WHITESPACE
        flowchart LR
            omnium__order_lines@{shape: rounded}
            omnium__orders@{shape: rounded}
            omnium__customers@{shape: rounded}
            omnium__products@{shape: rounded}
            omnium__regions@{shape: rounded}
            %% Relationships
            omnium__order_lines -- "order_id__alias" --> omnium__orders
            omnium__order_lines -- "product_id" --> omnium__products
            omnium__orders -- "customer_id" --> omnium__customers
            omnium__customers -- "region_id" --> omnium__regions

    """
    lines = ["flowchart LR"]

    for n in G.nodes:
        lines.append(f'    {n}@{{shape: rounded}}')

    lines.append("    %% Relationships")
    for u, v, d in G.edges(data=True):
        key = d.get("key", "")
        label = f' -- "{key}" --> ' if key else " --> "
        lines.append(f"    {u}{label}{v}")

    return "\n".join(lines)