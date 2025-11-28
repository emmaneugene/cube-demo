"""Streamlit app for visualizing cube models as interactive graphs."""

import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from cube_demo import Model, db

st.set_page_config(
    page_title="Cube Model Visualizer",
    page_icon=":cube:",
    layout="wide",
)

# Initialize database
db.init_db()
db.init_sample_data()

# Custom CSS for better styling
st.markdown(
    """
    <style>
    .stApp {
        background: #f8f9fa;
    }
    .main-header {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        background: linear-gradient(90deg, #e94560, #ff6b6b);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        color: #666;
        font-size: 1rem;
        margin-bottom: 2rem;
    }
    .cube-card {
        background: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 0.5rem;
    }
    .cube-name {
        color: #e94560;
        font-weight: 600;
        font-size: 1.1rem;
    }
    .column-list {
        color: #555;
        font-size: 0.85rem;
        font-family: 'JetBrains Mono', monospace;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_model() -> Model:
    """Load model from database."""
    return db.load_model_from_db()


def model_to_agraph(model: Model) -> tuple[list[Node], list[Edge]]:
    """Convert a Model to streamlit-agraph nodes and edges."""
    nodes = []
    edges = []

    # Color palette for nodes
    colors = ["#e94560", "#0f3460", "#00b4d8", "#90be6d", "#f9c74f"]

    for i, cube in enumerate(model.cubes.values()):
        # Create node label with columns
        columns_str = "\n".join(f"• {col}" for col in cube.columns)
        label = f"{cube.name}\n─────────\n{columns_str}"

        nodes.append(
            Node(
                id=cube.name,
                label=label,
                size=30,
                color=colors[i % len(colors)],
                font={"size": 12, "face": "monospace", "color": "#ffffff", "align": "left"},
                shape="box",
                borderWidth=2,
                shadow=True,
            )
        )

    for relation in model.relations:
        edges.append(
            Edge(
                source=relation.left_cube.name,
                target=relation.right_cube.name,
                label=f"{relation.left_column} → {relation.right_column}",
                color="#666666",
                font={"size": 10, "color": "#333333", "strokeWidth": 0},
                arrows="to",
                smooth={"type": "curvedCW", "roundness": 0.2},
                length=300,
            )
        )

    return nodes, edges


def generate_sql_query(model: Model, selected_columns: list[str]) -> str:
    """Generate a SQL query with JOINs based on selected columns.

    Args:
        model: The cube model containing cubes and relations
        selected_columns: List of columns in "cube.column" format

    Returns:
        SQL query string or error message starting with "Error:"
    """
    if not selected_columns:
        return "Error: No columns selected"

    # Parse selected columns to get cube names
    columns_by_cube: dict[str, list[str]] = {}
    for col_ref in selected_columns:
        if "." not in col_ref:
            return f"Error: Invalid column format: {col_ref}"
        cube_name, col_name = col_ref.split(".", 1)
        if cube_name not in model.cubes:
            return f"Error: Cube '{cube_name}' not found"
        if col_name not in model.cubes[cube_name].columns:
            return f"Error: Column '{col_name}' not found in cube '{cube_name}'"
        if cube_name not in columns_by_cube:
            columns_by_cube[cube_name] = []
        columns_by_cube[cube_name].append(col_name)

    involved_cubes = list(columns_by_cube.keys())

    # If only one cube, no JOINs needed
    if len(involved_cubes) == 1:
        cube_name = involved_cubes[0]
        cols = ", ".join(f"{cube_name}.{c}" for c in columns_by_cube[cube_name])
        return f"SELECT {cols}\nFROM {cube_name}"

    # Build adjacency graph from relations
    # Each entry: (neighbor, left_cube, left_col, right_cube, right_col)
    adjacency: dict[str, list[tuple[str, str, str, str, str]]] = {name: [] for name in model.cubes}
    for rel in model.relations:
        left = rel.left_cube.name
        right = rel.right_cube.name
        # Bidirectional for pathfinding
        adjacency[left].append((right, left, rel.left_column, right, rel.right_column))
        adjacency[right].append((left, left, rel.left_column, right, rel.right_column))

    # BFS to find path connecting all involved cubes
    # Start from first involved cube and find paths to all others
    start_cube = involved_cubes[0]
    visited = {start_cube}
    queue = [start_cube]
    parent: dict[str, tuple[str, str, str, str, str] | None] = {start_cube: None}

    while queue:
        current = queue.pop(0)
        for neighbor, left_cube, left_col, right_cube, right_col in adjacency[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                parent[neighbor] = (current, left_cube, left_col, right_cube, right_col)
                queue.append(neighbor)

    # Check if all involved cubes are reachable
    for cube_name in involved_cubes:
        if cube_name not in visited:
            return f"Error: Cannot connect cube '{cube_name}' - no path exists"

    # Reconstruct joins needed to connect all involved cubes
    cubes_to_join = set(involved_cubes)
    cubes_to_join.remove(start_cube)
    joined_cubes = {start_cube}
    join_clauses = []

    # For each cube we need, trace back to find the join path
    for target in list(cubes_to_join):
        path = []
        current = target
        while current != start_cube and current not in joined_cubes:
            p = parent.get(current)
            if p is None:
                break
            prev, left_cube, left_col, right_cube, right_col = p
            path.append((left_cube, left_col, right_cube, right_col))
            current = prev

        # Add joins in reverse order (from joined cube toward target)
        for left_cube, left_col, right_cube, right_col in reversed(path):
            # Determine which side is already joined
            if left_cube in joined_cubes and right_cube not in joined_cubes:
                join_clauses.append(
                    f"JOIN {right_cube} ON {left_cube}.{left_col} = {right_cube}.{right_col}"
                )
                joined_cubes.add(right_cube)
            elif right_cube in joined_cubes and left_cube not in joined_cubes:
                join_clauses.append(
                    f"JOIN {left_cube} ON {left_cube}.{left_col} = {right_cube}.{right_col}"
                )
                joined_cubes.add(left_cube)

    # Build SELECT clause
    select_cols = ", ".join(selected_columns)

    # Build FROM clause
    from_clause = f"FROM {start_cube}"

    # Combine
    sql_parts = [f"SELECT {select_cols}", from_clause]
    sql_parts.extend(join_clauses)

    return "\n".join(sql_parts)


def render_cube_editor(model: Model):
    """Render the cube editing section in sidebar."""
    st.markdown("### Cubes")

    # Add new cube form in its own container
    with st.container(border=True):
        with st.expander("Add New", expanded=False, icon="➕"):
            with st.form("add_cube_form"):
                new_cube_name = st.text_input("Cube Name", placeholder="e.g., users")
                new_cube_columns = st.text_area(
                    "Columns (one per line)",
                    placeholder="id\nname\nemail",
                    height=100,
                )
                submitted = st.form_submit_button("Add Cube", use_container_width=True)

                if submitted and new_cube_name:
                    columns = [c.strip() for c in new_cube_columns.strip().split("\n") if c.strip()]
                    try:
                        db.create_cube(new_cube_name, columns)
                        st.success(f"Created cube '{new_cube_name}'")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # List existing cubes with edit/delete
    if model.cubes:
        st.caption("Existing Cubes")
    for cube in model.cubes.values():
        with st.expander(f"{cube.name}", expanded=False):
            # Edit cube name
            with st.form(f"edit_cube_{cube.name}"):
                new_name = st.text_input("Name", value=cube.name, key=f"name_{cube.name}")
                new_columns = st.text_area(
                    "Columns (one per line)",
                    value="\n".join(cube.columns),
                    height=100,
                    key=f"cols_{cube.name}",
                )

                col1, col2 = st.columns(2)
                with col1:
                    save_clicked = st.form_submit_button("Save", use_container_width=True)
                with col2:
                    delete_clicked = st.form_submit_button("Delete", use_container_width=True)

                if save_clicked:
                    columns = [c.strip() for c in new_columns.strip().split("\n") if c.strip()]
                    try:
                        db.update_cube(cube.name, new_name=new_name, columns=columns)
                        st.success(f"Updated cube '{new_name}'")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

                if delete_clicked:
                    db.delete_cube(cube.name)
                    st.success(f"Deleted cube '{cube.name}'")
                    st.rerun()


def render_relation_editor(model: Model):
    """Render the relation editing section in sidebar."""
    st.markdown("### Relations")

    cube_names = list(model.cubes.keys())

    # Add new relation form in its own container
    if len(cube_names) >= 2:
        with st.container(border=True):
            with st.expander("Add New", expanded=False, icon="➕"):
                with st.form("add_relation_form"):
                    col1, col2 = st.columns(2)

                    with col1:
                        left_cube_name = st.selectbox(
                            "From Cube",
                            cube_names,
                            key="new_rel_left_cube",
                        )

                    with col2:
                        right_cube_name = st.selectbox(
                            "To Cube",
                            cube_names,
                            key="new_rel_right_cube",
                        )

                    # Get columns for selected cubes
                    left_cube = model.cubes.get(left_cube_name)
                    right_cube = model.cubes.get(right_cube_name)

                    col3, col4 = st.columns(2)

                    with col3:
                        left_columns = left_cube.columns if left_cube else []
                        left_column = st.selectbox(
                            "From Column",
                            left_columns,
                            key="new_rel_left_col",
                        )

                    with col4:
                        right_columns = right_cube.columns if right_cube else []
                        right_column = st.selectbox(
                            "To Column",
                            right_columns,
                            key="new_rel_right_col",
                        )

                    submitted = st.form_submit_button("Add Relation", use_container_width=True)

                    if submitted and left_cube_name and right_cube_name and left_column and right_column:
                        try:
                            db.create_relation(left_cube_name, right_cube_name, left_column, right_column)
                            st.success(f"Created relation: {left_cube_name}.{left_column} -> {right_cube_name}.{right_column}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
    else:
        st.info("Add at least 2 cubes to create relations")

    # List existing relations with delete
    relations_data = db.get_all_relations()

    if relations_data:
        st.caption("Existing Relations")
    for rel in relations_data:
        rel_label = f"{rel['left_cube']}.{rel['left_column']} → {rel['right_cube']}.{rel['right_column']}"

        with st.expander(f"{rel_label}", expanded=False):
            with st.form(f"edit_rel_{rel['id']}"):
                st.markdown(f"**From:** `{rel['left_cube']}`")
                st.markdown(f"**To:** `{rel['right_cube']}`")

                # Get available columns
                left_cube = model.cubes.get(rel['left_cube'])
                right_cube = model.cubes.get(rel['right_cube'])

                col1, col2 = st.columns(2)

                with col1:
                    left_cols = left_cube.columns if left_cube else [rel['left_column']]
                    left_idx = left_cols.index(rel['left_column']) if rel['left_column'] in left_cols else 0
                    new_left_col = st.selectbox(
                        "From Column",
                        left_cols,
                        index=left_idx,
                        key=f"rel_left_{rel['id']}",
                    )

                with col2:
                    right_cols = right_cube.columns if right_cube else [rel['right_column']]
                    right_idx = right_cols.index(rel['right_column']) if rel['right_column'] in right_cols else 0
                    new_right_col = st.selectbox(
                        "To Column",
                        right_cols,
                        index=right_idx,
                        key=f"rel_right_{rel['id']}",
                    )

                col3, col4 = st.columns(2)

                with col3:
                    save_clicked = st.form_submit_button("Save", use_container_width=True)

                with col4:
                    delete_clicked = st.form_submit_button("Delete", use_container_width=True)

                if save_clicked:
                    try:
                        db.update_relation(rel['id'], left_column=new_left_col, right_column=new_right_col)
                        st.success("Updated relation")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

                if delete_clicked:
                    db.delete_relation(rel['id'])
                    st.success("Deleted relation")
                    st.rerun()


def main():
    st.markdown('<h1 class="main-header">Cube Model Visualizer</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Interactive visualization of database cubes and their relationships</p>',
        unsafe_allow_html=True,
    )

    # Load model from database
    model = load_model()

    # Sidebar with editing
    with st.sidebar:
        st.markdown("### Model Info")
        st.markdown(f"**Cubes:** {len(model.cubes)}")
        st.markdown(f"**Relations:** {len(model.relations)}")

        st.markdown("---")

        # Cube editor
        render_cube_editor(model)

        st.markdown("---")

        # Relation editor
        render_relation_editor(model)

        st.markdown("---")
        st.markdown("### Graph Settings")

        physics_enabled = st.checkbox("Enable Physics", value=False)
        hierarchical = st.checkbox("Hierarchical Layout", value=False)

    # Convert model to agraph format
    nodes, edges = model_to_agraph(model)

    # Graph configuration
    config = Config(
        width=1200,
        height=600,
        directed=True,
        physics=physics_enabled,
        hierarchical=hierarchical,
        nodeHighlightBehavior=True,
        highlightColor="#e94560",
        collapsible=False,
        node={
            "labelProperty": "label",
            "renderLabel": True,
        },
        link={
            "labelProperty": "label",
            "renderLabel": True,
        },
    )

    # Render the graph
    st.markdown("### Model Graph")
    st.markdown("*Drag nodes to rearrange • Scroll to zoom • Click to select*")

    if nodes:
        selected_node = agraph(nodes=nodes, edges=edges, config=config)

        # Show selected node details
        if selected_node:
            st.markdown("---")
            st.markdown(f"### Selected: `{selected_node}`")
            if selected_node in model.cubes:
                cube = model.cubes[selected_node]
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Columns:**")
                    for col in cube.columns:
                        st.code(col)
                with col2:
                    st.markdown("**Outgoing Relations:**")
                    for rel in model.relations:
                        if rel.left_cube.name == selected_node:
                            st.markdown(f"→ `{rel.right_cube.name}` via `{rel.left_column}`")

                    st.markdown("**Incoming Relations:**")
                    for rel in model.relations:
                        if rel.right_cube.name == selected_node:
                            st.markdown(f"← `{rel.left_cube.name}` via `{rel.right_column}`")
    else:
        st.info("No cubes yet. Add some using the sidebar!")

    # SQL Query Builder Section
    st.markdown("---")
    st.markdown("### SQL Query Builder")
    st.markdown("*Select columns to generate a SQL query with automatic JOINs*")

    if model.cubes:
        # Build list of all columns in cube.column format
        all_columns = []
        for cube in model.cubes.values():
            for col in cube.columns:
                all_columns.append(f"{cube.name}.{col}")

        # Multi-select for columns
        selected_columns = st.multiselect(
            "Select columns",
            options=all_columns,
            placeholder="Choose columns to include in SELECT...",
        )

        # Generate SQL button
        if st.button("Generate SQL", use_container_width=True):
            if not selected_columns:
                st.error("Please select at least one column")
            else:
                # Generate SQL (placeholder for now)
                sql = generate_sql_query(model, selected_columns)
                if sql.startswith("Error:"):
                    st.error(sql)
                else:
                    st.code(sql, language="sql")
    else:
        st.info("Add cubes to start building SQL queries")


if __name__ == "__main__":
    main()
