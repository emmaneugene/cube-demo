"""SQLite database layer for persisting cubes and relations."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from cube_demo.model import Cardinality, Cube, Relation, RelationData

DEFAULT_DB_PATH = Path(__file__).parent.parent / "cube_model.db"


@contextmanager
def get_connection(db_path: Path = DEFAULT_DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """Get a database connection as a context manager.

    Auto-commits on success and always closes the connection.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize the database schema."""
    with get_connection(db_path) as conn:
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
                cardinality TEXT NOT NULL DEFAULT 'one-to-many',
                FOREIGN KEY (left_cube) REFERENCES cubes(name) ON DELETE CASCADE,
                FOREIGN KEY (right_cube) REFERENCES cubes(name) ON DELETE CASCADE
            )
        """)


def init_sample_data(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize with sample e-commerce data if database is empty."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM cubes")
        if cursor.fetchone()[0] > 0:
            return

        # Sample cubes
        sample_cubes = [
            ("1", ["id", "field1"]),
            ("2", ["id", "field2"]),
            ("3", ["id", "order_id", "product_id", "quantity", "unit_price"]),
            ("4", ["id", "name", "category", "price", "stock"]),
            ("5", ["id", "name", "description"]),
            ("6", ["id", "customer_id", "order_date", "total", "status"]),
            ("7", ["id", "name", "email", "created_at"]),
            ("8", ["id", "count", "field"]),
        ]

        for name, columns in sample_cubes:
            cursor.execute(
                "INSERT INTO cubes (name, columns) VALUES (?, ?)",
                (name, json.dumps(columns)),
            )

        # Sample relations (left_cube, right_cube, left_col, right_col, cardinality)
        sample_relations = [
            ("1", "3", "id", "id", Cardinality.MANY_TO_ONE),
            ("2", "3", "id", "id", Cardinality.ONE_TO_MANY),
            ("3", "4", "product_id", "id", Cardinality.MANY_TO_ONE),
            ("4", "5", "id", "id", Cardinality.ONE_TO_ONE),
            ("3", "6", "order_id", "id", Cardinality.MANY_TO_ONE),
            ("6", "7", "customer_id", "id", Cardinality.MANY_TO_ONE),
        ]

        for left_cube, right_cube, left_col, right_col, cardinality in sample_relations:
            cursor.execute(
                "INSERT INTO relations (left_cube, right_cube, left_column, right_column, cardinality) VALUES (?, ?, ?, ?, ?)",
                (left_cube, right_cube, left_col, right_col, cardinality.value),
            )


def delete_all_data(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Delete all data from the database."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM relations")
        cursor.execute("DELETE FROM cubes")


# Cube CRUD operations


def create_cube(name: str, columns: list[str], db_path: Path = DEFAULT_DB_PATH) -> Cube:
    """Create a new cube in the database."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO cubes (name, columns) VALUES (?, ?)",
            (name, json.dumps(columns)),
        )
    return Cube(name=name, columns=columns)


def get_cube(name: str, db_path: Path = DEFAULT_DB_PATH) -> Cube | None:
    """Get a cube by name."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name, columns FROM cubes WHERE name = ?", (name,))
        row = cursor.fetchone()

    if row is None:
        return None
    return Cube(name=row["name"], columns=json.loads(row["columns"]))


def get_all_cubes(db_path: Path = DEFAULT_DB_PATH) -> list[Cube]:
    """Get all cubes from the database."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name, columns FROM cubes ORDER BY name")
        rows = cursor.fetchall()
    return [Cube(name=row["name"], columns=json.loads(row["columns"])) for row in rows]


def update_cube(
    name: str,
    new_name: str | None = None,
    columns: list[str] | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> Cube | None:
    """Update a cube's name and/or columns."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # Get current cube
        cursor.execute("SELECT name, columns FROM cubes WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row is None:
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

    return Cube(name=final_name, columns=final_columns)


def delete_cube(name: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Delete a cube and all its relations."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # Delete relations first
        cursor.execute(
            "DELETE FROM relations WHERE left_cube = ? OR right_cube = ?",
            (name, name),
        )

        # Delete cube
        cursor.execute("DELETE FROM cubes WHERE name = ?", (name,))
        deleted = cursor.rowcount > 0

    return deleted


# Relation CRUD operations


def create_relation(
    left_cube: str,
    right_cube: str,
    left_column: str,
    right_column: str,
    cardinality: Cardinality,
    db_path: Path = DEFAULT_DB_PATH,
) -> int | None:
    """Create a new relation in the database. Returns the relation ID."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO relations (left_cube, right_cube, left_column, right_column, cardinality) VALUES (?, ?, ?, ?, ?)",
            (left_cube, right_cube, left_column, right_column, cardinality.value),
        )
        relation_id = cursor.lastrowid
    return relation_id


def get_all_relations(db_path: Path = DEFAULT_DB_PATH) -> list[RelationData]:
    """Get all relations from the database."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, left_cube, right_cube, left_column, right_column, cardinality
            FROM relations
            ORDER BY id
        """)
        rows = cursor.fetchall()

    return [
        RelationData(
            id=row["id"],
            left_cube=row["left_cube"],
            right_cube=row["right_cube"],
            left_column=row["left_column"],
            right_column=row["right_column"],
            cardinality=Cardinality(row["cardinality"]),
        )
        for row in rows
    ]


def update_relation(
    relation_id: int,
    left_column: str | None = None,
    right_column: str | None = None,
    cardinality: Cardinality | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> bool:
    """Update a relation's column mappings and/or cardinality."""
    updates = []
    params: list[str | int] = []

    if left_column is not None:
        updates.append("left_column = ?")
        params.append(left_column)

    if right_column is not None:
        updates.append("right_column = ?")
        params.append(right_column)

    if cardinality is not None:
        updates.append("cardinality = ?")
        params.append(cardinality.value)

    if not updates:
        return False

    params.append(relation_id)

    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE relations SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        updated = cursor.rowcount > 0

    return updated


def delete_relation(relation_id: int, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Delete a relation by ID."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM relations WHERE id = ?", (relation_id,))
        deleted = cursor.rowcount > 0
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
        left_cube = cube_map.get(rel_data.left_cube)
        right_cube = cube_map.get(rel_data.right_cube)

        if left_cube and right_cube:
            try:
                relation = Relation(
                    left_cube=left_cube,
                    right_cube=right_cube,
                    left_column=rel_data.left_column,
                    right_column=rel_data.right_column,
                    cardinality=rel_data.cardinality,
                )
                model.add_relation(relation)
            except ValueError:
                # Skip invalid relations (e.g., column no longer exists, or would create cycle)
                pass

    return model
