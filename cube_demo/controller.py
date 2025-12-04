"""Controller layer for cube model operations.

Validates all operations through the Model before persisting to the database.
"""

from pathlib import Path

from cube_demo import database as db
from cube_demo.model import Cardinality, Cube, Model, Relation


class ModelController:
    """Controller for managing cube model operations with validation."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or db.DEFAULT_DB_PATH
        self._model: Model | None = None

    @property
    def model(self) -> Model:
        """Get the current model, loading from DB if needed."""
        if self._model is None:
            self._model = self._load_model()
        return self._model

    def _load_model(self) -> Model:
        """Load model from database."""
        return db.load_model_from_db(self._db_path)

    def refresh(self) -> Model:
        """Force reload model from database."""
        self._model = self._load_model()
        return self._model

    def init_db(self) -> None:
        """Initialize the database schema."""
        db.init_db(self._db_path)

    def init_sample_data(self) -> None:
        """Initialize sample data if database is empty."""
        db.init_sample_data(self._db_path)

    # Cube operations

    def create_cube(self, name: str, columns: list[str]) -> Cube:
        """Create a new cube.

        Validates the cube can be added to the model before persisting.

        Raises:
            ValueError: If cube name already exists.
        """
        cube = Cube(name=name, columns=columns)

        # Validate through model
        self.model.add_cube(cube)

        # Persist to database
        db.create_cube(name, columns, self._db_path)

        return cube

    def update_cube(
        self,
        name: str,
        new_name: str | None = None,
        columns: list[str] | None = None,
    ) -> Cube | None:
        """Update a cube's name and/or columns.

        Validates changes through the model before persisting.

        Raises:
            ValueError: If new_name already exists (and differs from name).
        """
        model = self.model

        if name not in model.cubes:
            return None

        # Validate rename
        if new_name is not None and new_name != name:
            if new_name in model.cubes:
                raise ValueError(f"Cube '{new_name}' already exists")
            model.rename_cube(name, new_name)

        # Validate column update
        final_name = new_name if new_name is not None else name
        if columns is not None:
            model.update_cube_columns(final_name, columns)

        # Persist to database
        result = db.update_cube(name, new_name, columns, self._db_path)

        # Refresh model to sync with DB state
        self.refresh()

        return result

    def delete_cube(self, name: str) -> bool:
        """Delete a cube and all its relations."""
        if name not in self.model.cubes:
            return False

        # Remove from model
        self.model.remove_cube(name)

        # Persist to database
        return db.delete_cube(name, self._db_path)

    def get_cube(self, name: str) -> Cube | None:
        """Get a cube by name."""
        return self.model.cubes.get(name)

    # Relation operations

    def create_relation(
        self,
        left_cube: str,
        right_cube: str,
        left_column: str,
        right_column: str,
        cardinality: Cardinality,
    ) -> int | None:
        """Create a new relation.

        Validates through the model before persisting. This catches:
        - Self-relations
        - Missing cubes
        - Duplicate paths
        - Cycles

        Raises:
            ValueError: If the relation is invalid.
        """
        model = self.model

        # Get cube objects
        left = model.cubes.get(left_cube)
        right = model.cubes.get(right_cube)

        if left is None:
            raise ValueError(f"Left cube '{left_cube}' not found in model")
        if right is None:
            raise ValueError(f"Right cube '{right_cube}' not found in model")

        # Create and validate relation through model
        relation = Relation(
            left_cube=left,
            right_cube=right,
            left_column=left_column,
            right_column=right_column,
            cardinality=cardinality,
        )

        # This raises ValueError for cycles, duplicates, self-relations
        model.add_relation(relation)

        # Persist to database
        return db.create_relation(
            left_cube,
            right_cube,
            left_column,
            right_column,
            cardinality,
            self._db_path,
        )

    def update_relation(
        self,
        relation_id: int,
        left_column: str | None = None,
        right_column: str | None = None,
        cardinality: Cardinality | None = None,
    ) -> bool:
        """Update a relation's column mappings and/or cardinality.

        Raises:
            ValueError: If the new column doesn't exist in the cube.
        """
        # Get relation data from DB to find the relation object
        relations_data = db.get_all_relations(self._db_path)
        rel_data = next((r for r in relations_data if r["id"] == relation_id), None)

        if rel_data is None:
            return False

        model = self.model
        left_cube = model.cubes.get(rel_data["left_cube"])
        right_cube = model.cubes.get(rel_data["right_cube"])

        if left_cube is None or right_cube is None:
            return False

        # Find the relation in the model
        old_relation = None
        for rel in model.adjacency.get(rel_data["left_cube"], []):
            if (
                rel.right_cube.name == rel_data["right_cube"]
                and rel.left_column == rel_data["left_column"]
                and rel.right_column == rel_data["right_column"]
            ):
                old_relation = rel
                break

        if old_relation is None:
            # Relation exists in DB but not in model (was invalid)
            # Just update DB directly
            return db.update_relation(
                relation_id, left_column, right_column, cardinality, self._db_path
            )

        # Validate through model
        model.update_relation(old_relation, left_column, right_column)

        # Persist to database
        result = db.update_relation(
            relation_id, left_column, right_column, cardinality, self._db_path
        )

        # Refresh model
        self.refresh()

        return result

    def delete_relation(self, relation_id: int) -> bool:
        """Delete a relation by ID."""
        # Get relation data to remove from model
        relations_data = db.get_all_relations(self._db_path)
        rel_data = next((r for r in relations_data if r["id"] == relation_id), None)

        if rel_data is None:
            return False

        model = self.model

        # Find and remove from model
        for rel in model.adjacency.get(rel_data["left_cube"], []):
            if (
                rel.right_cube.name == rel_data["right_cube"]
                and rel.left_column == rel_data["left_column"]
                and rel.right_column == rel_data["right_column"]
            ):
                model.remove_relation(rel)
                break

        # Persist to database
        return db.delete_relation(relation_id, self._db_path)

    def get_all_relations(self) -> list[dict]:
        """Get all relations from the database."""
        return db.get_all_relations(self._db_path)

