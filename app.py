"""Streamlit app for visualizing cube models as interactive graphs."""

import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from cube_demo import Cube, Model, Relation

st.set_page_config(
    page_title="Cube Model Visualizer",
    page_icon="🧊",
    layout="wide",
)

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


def create_sample_model() -> Model:
    """Create a sample e-commerce model for demonstration."""
    model = Model(name="E-Commerce Data Model")

    # Create cubes
    customers = Cube(
        name="customers",
        columns=["id", "name", "email", "created_at"],
    )
    orders = Cube(
        name="orders",
        columns=["id", "customer_id", "order_date", "total", "status"],
    )
    order_items = Cube(
        name="order_items",
        columns=["id", "order_id", "product_id", "quantity", "unit_price"],
    )
    products = Cube(
        name="products",
        columns=["id", "name", "category", "price", "stock"],
    )
    categories = Cube(
        name="categories",
        columns=["id", "name", "description"],
    )

    # Add cubes to model
    for cube in [customers, orders, order_items, products, categories]:
        model.add_cube(cube)

    # Create relations (joins)
    model.add_relation(Relation(orders, customers, "customer_id", "id"))
    model.add_relation(Relation(order_items, orders, "order_id", "id"))
    model.add_relation(Relation(order_items, products, "product_id", "id"))

    return model


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


def main():
    st.markdown('<h1 class="main-header">🧊 Cube Model Visualizer</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Interactive visualization of database cubes and their relationships</p>',
        unsafe_allow_html=True,
    )

    # Create sample model
    model = create_sample_model()

    # Sidebar with model info
    with st.sidebar:
        st.markdown("### 📊 Model Info")
        st.markdown(f"**Name:** {model.name}")
        st.markdown(f"**Cubes:** {len(model.cubes)}")
        st.markdown(f"**Relations:** {len(model.relations)}")

        st.markdown("---")
        st.markdown("### 🧊 Cubes")

        for cube in model.cubes.values():
            with st.expander(f"📦 {cube.name}", expanded=False):
                st.markdown("**Columns:**")
                for col in cube.columns:
                    st.markdown(f"- `{col}`")

        st.markdown("---")
        st.markdown("### 🔗 Relations")

        for relation in model.relations:
            st.markdown(
                f"- `{relation.left_cube.name}.{relation.left_column}` → "
                f"`{relation.right_cube.name}.{relation.right_column}`"
            )

        st.markdown("---")
        st.markdown("### ⚙️ Graph Settings")

        physics_enabled = st.checkbox("Enable Physics", value=False)
        hierarchical = st.checkbox("Hierarchical Layout", value=False)

    # Convert model to agraph format
    nodes, edges = model_to_agraph(model)

    # Graph configuration
    config = Config(
        width="100%",
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
    st.markdown("### 📈 Model Graph")
    st.markdown("*Drag nodes to rearrange • Scroll to zoom • Click to select*")

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


if __name__ == "__main__":
    main()

