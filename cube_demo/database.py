"""SQLite database layer for persisting cubes and relations."""

import json
import sqlite3
from pathlib import Path

from cube_demo.cube import Cube
from cube_demo.relation import Relation

DEFAULT_DB_PATH = Path(__file__).parent.parent / "cube_model.db"


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Get a database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize the database schema."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cubes (
            name TEXT PRIMARY KEY,
            columns TEXT NOT NULL DEFAULT '[]'
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            left_cube TEXT NOT NULL,
            right_cube TEXT NOT NULL,
            left_column TEXT NOT NULL,
            right_column TEXT NOT NULL,
            FOREIGN KEY (left_cube) REFERENCES cubes(name) ON DELETE CASCADE,
            FOREIGN KEY (right_cube) REFERENCES cubes(name) ON DELETE CASCADE
        )
    """)

    conn.commit()
    conn.close()


def init_sample_data(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize with sample e-commerce data if database is empty."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM cubes")
    if cursor.fetchone()[0] > 0:
        conn.close()
        return

    # Sample cubes
    sample_cubes = [
        ("customers", ["id", "name", "email", "created_at"]),
        ("orders", ["id", "customer_id", "order_date", "total", "status"]),
        ("order_items", ["id", "order_id", "product_id", "quantity", "unit_price"]),
        ("products", ["id", "name", "category", "price", "stock"]),
        ("categories", ["id", "name", "description"]),
    ]

    for name, columns in sample_cubes:
        cursor.execute(
            "INSERT INTO cubes (name, columns) VALUES (?, ?)",
            (name, json.dumps(columns)),
        )

    # Sample relations
    sample_relations = [
        ("orders", "customers", "customer_id", "id"),
        ("order_items", "orders", "order_id", "id"),
        ("order_items", "products", "product_id", "id"),
    ]

    for left_cube, right_cube, left_col, right_col in sample_relations:
        cursor.execute(
            "INSERT INTO relations (left_cube, right_cube, left_column, right_column) VALUES (?, ?, ?, ?)",
            (left_cube, right_cube, left_col, right_col),
        )

    conn.commit()
    conn.close()


# Cube CRUD operations


def create_cube(name: str, columns: list[str], db_path: Path = DEFAULT_DB_PATH) -> Cube:
    """Create a new cube in the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO cubes (name, columns) VALUES (?, ?)",
        (name, json.dumps(columns)),
    )

    conn.commit()
    conn.close()

    return Cube(name=name, columns=columns)


def get_cube(name: str, db_path: Path = DEFAULT_DB_PATH) -> Cube | None:
    """Get a cube by name."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name, columns FROM cubes WHERE name = ?", (name,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return None

    return Cube(name=row["name"], columns=json.loads(row["columns"]))


def get_all_cubes(db_path: Path = DEFAULT_DB_PATH) -> list[Cube]:
    """Get all cubes from the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name, columns FROM cubes ORDER BY name")
    rows = cursor.fetchall()
    conn.close()

    return [Cube(name=row["name"], columns=json.loads(row["columns"])) for row in rows]


def update_cube(
    name: str,
    new_name: str | None = None,
    columns: list[str] | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> Cube | None:
    """Update a cube's name and/or columns."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Get current cube
    cursor.execute("SELECT name, columns FROM cubes WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is None:
        conn.close()
        return None

    current_columns = json.loads(row["columns"])
    final_name = new_name if new_name is not None else name
    final_columns = columns if columns is not None else current_columns

    if new_name is not None and new_name != name:
        # Rename: update cube name and all relations referencing it
        cursor.execute(
            "UPDATE cubes SET name = ?, columns = ? WHERE name = ?",
            (final_name, json.dumps(final_columns), name),
        )
        cursor.execute(
            "UPDATE relations SET left_cube = ? WHERE left_cube = ?",
            (final_name, name),
        )
        cursor.execute(
            "UPDATE relations SET right_cube = ? WHERE right_cube = ?",
            (final_name, name),
        )
    else:
        cursor.execute(
            "UPDATE cubes SET columns = ? WHERE name = ?",
            (json.dumps(final_columns), name),
        )

    conn.commit()
    conn.close()

    return Cube(name=final_name, columns=final_columns)


def delete_cube(name: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Delete a cube and all its relations."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Delete relations first
    cursor.execute(
        "DELETE FROM relations WHERE left_cube = ? OR right_cube = ?",
        (name, name),
    )

    # Delete cube
    cursor.execute("DELETE FROM cubes WHERE name = ?", (name,))
    deleted = cursor.rowcount > 0

    conn.commit()
    conn.close()

    return deleted


# Relation CRUD operations


def create_relation(
    left_cube: str,
    right_cube: str,
    left_column: str,
    right_column: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """Create a new relation in the database. Returns the relation ID."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO relations (left_cube, right_cube, left_column, right_column) VALUES (?, ?, ?, ?)",
        (left_cube, right_cube, left_column, right_column),
    )

    relation_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return relation_id


def get_all_relations(db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get all relations from the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, left_cube, right_cube, left_column, right_column
        FROM relations
        ORDER BY id
    """)
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "left_cube": row["left_cube"],
            "right_cube": row["right_cube"],
            "left_column": row["left_column"],
            "right_column": row["right_column"],
        }
        for row in rows
    ]


def update_relation(
    relation_id: int,
    left_column: str | None = None,
    right_column: str | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> bool:
    """Update a relation's column mappings."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    updates = []
    params = []

    if left_column is not None:
        updates.append("left_column = ?")
        params.append(left_column)

    if right_column is not None:
        updates.append("right_column = ?")
        params.append(right_column)

    if not updates:
        conn.close()
        return False

    params.append(relation_id)
    cursor.execute(
        f"UPDATE relations SET {', '.join(updates)} WHERE id = ?",
        params,
    )

    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()

    return updated


def delete_relation(relation_id: int, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Delete a relation by ID."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM relations WHERE id = ?", (relation_id,))
    deleted = cursor.rowcount > 0

    conn.commit()
    conn.close()

    return deleted


def load_model_from_db(db_path: Path = DEFAULT_DB_PATH):
    """Load a complete Model from the database."""
    from cube_demo.model import Model

    cubes = get_all_cubes(db_path)
    relations_data = get_all_relations(db_path)

    model = Model(name="Cube Model")

    # Add all cubes
    cube_map = {}
    for cube in cubes:
        model.add_cube(cube)
        cube_map[cube.name] = cube

    # Add all relations
    for rel_data in relations_data:
        left_cube = cube_map.get(rel_data["left_cube"])
        right_cube = cube_map.get(rel_data["right_cube"])

        if left_cube and right_cube:
            try:
                relation = Relation(
                    left_cube=left_cube,
                    right_cube=right_cube,
                    left_column=rel_data["left_column"],
                    right_column=rel_data["right_column"],
                )
                model.relations.append(relation)
            except ValueError:
                # Skip invalid relations (e.g., column no longer exists)
                pass

    return model

