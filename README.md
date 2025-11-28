# Cube Demo

A Python library for representing database cubes (tables) and their relationships, with an interactive web visualization powered by Streamlit.

## Features

- **Cube Management** - Create, edit, rename, and delete cubes with columns
- **Relation Management** - Define joins between cubes with column mappings
- **Interactive Visualization** - Drag, zoom, and explore your data model
- **Persistence** - All changes are saved to a SQLite database

## Setup

### Using uv (recommended)

```bash
# Install dependencies
uv sync --all-extras

# Run the web app
uv run streamlit run app.py

# Run tests
uv run pytest -v
```

### Using pip

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package with dev dependencies
pip install -e ".[dev]"

# Run the web app
streamlit run app.py

# Run tests
pytest -v
```

## Usage

### Web Interface

Launch the visualization app:

```bash
uv run streamlit run app.py
# or
streamlit run app.py
```

The web interface allows you to:

- **Add/Edit/Delete Cubes** - Use the sidebar to manage cubes and their columns
- **Add/Edit/Delete Relations** - Define joins between cubes
- **Visualize** - See your model as an interactive graph
- **Drag nodes** to rearrange the layout
- **Scroll** to zoom in/out
- **Click nodes** to see details

### Python API

```python
from cube_demo import Cube, Model, Relation

# Create cubes (database tables)
customers = Cube(name="customers", columns=["id", "name", "email"])
orders = Cube(name="orders", columns=["id", "customer_id", "total"])

# Create a model and add cubes
model = Model(name="E-Commerce")
model.add_cube(customers)
model.add_cube(orders)

# Create a relation (join) between cubes
relation = Relation(
    left_cube=orders,
    right_cube=customers,
    left_column="customer_id",
    right_column="id",
)
model.add_relation(relation)

# Export for visualization
graph_data = model.to_graph_data()
```

### Database Operations

```python
from cube_demo import db

# Initialize the database
db.init_db()

# Create a cube
db.create_cube("users", ["id", "name", "email"])

# Create a relation
db.create_relation("orders", "users", "user_id", "id")

# Load the full model
model = db.load_model_from_db()
```

## Running Tests

Run the test suite with pytest:

```bash
# Using uv
uv run pytest -v

# Using pip (with venv activated)
pytest -v
```

Run a specific test file or test:

```bash
uv run pytest tests/test_model.py -v
uv run pytest tests/test_model.py::TestCube -v
uv run pytest tests/test_model.py::TestCube::test_create_cube_with_name -v
```

Run with coverage:

```bash
uv run pytest --cov=cube_demo -v
```

## Project Structure

```
cube-demo/
├── cube_demo/
│   ├── __init__.py      # Package exports
│   ├── cube.py          # Cube class
│   ├── relation.py      # Relation class
│   ├── model.py         # Model class
│   └── database.py      # SQLite persistence
├── tests/
│   └── test_model.py    # Test cases
├── app.py               # Streamlit web app
├── pyproject.toml       # Project configuration
└── README.md
```

## Requirements

- Python 3.13+
- streamlit
- streamlit-agraph
- pytest (dev)
