from dataclasses import dataclass, field
from typing import Any

from cube_demo.cube import Cube
from cube_demo.relation import Relation


@dataclass
class Model:
    """Stores cubes and their relations to each other."""

    name: str = "Model"
    cubes: dict[str, Cube] = field(default_factory=dict)
    relations: list[Relation] = field(default_factory=list)

    def add_cube(self, cube: Cube) -> None:
        """Add a cube to the model."""
        if cube.name in self.cubes:
            raise ValueError(f"Cube '{cube.name}' already exists in model")
        self.cubes[cube.name] = cube

    def get_cube(self, name: str) -> Cube:
        """Get a cube by name."""
        if name not in self.cubes:
            raise KeyError(f"Cube '{name}' not found in model")
        return self.cubes[name]

    def add_relation(self, relation: Relation) -> None:
        """Add a relation between two cubes."""
        if relation.left_cube.name not in self.cubes:
            raise ValueError(
                f"Left cube '{relation.left_cube.name}' not found in model"
            )
        if relation.right_cube.name not in self.cubes:
            raise ValueError(
                f"Right cube '{relation.right_cube.name}' not found in model"
            )
        self.relations.append(relation)

    def remove_cube(self, name: str) -> bool:
        """Remove a cube and all relations referencing it."""
        if name not in self.cubes:
            return False

        # Remove all relations involving this cube
        self.relations = [
            rel
            for rel in self.relations
            if rel.left_cube.name != name and rel.right_cube.name != name
        ]

        # Remove the cube
        del self.cubes[name]
        return True

    def rename_cube(self, old_name: str, new_name: str) -> bool:
        """Rename a cube, updating all references."""
        if old_name not in self.cubes:
            return False
        if new_name in self.cubes and new_name != old_name:
            raise ValueError(f"Cube '{new_name}' already exists")

        cube = self.cubes[old_name]
        cube.name = new_name

        # Update cubes dict
        del self.cubes[old_name]
        self.cubes[new_name] = cube

        return True

    def update_cube_columns(self, name: str, columns: list[str]) -> bool:
        """Update a cube's columns."""
        if name not in self.cubes:
            return False

        self.cubes[name].columns = columns

        # Remove relations with invalid columns
        self.relations = [
            rel
            for rel in self.relations
            if (
                (rel.left_cube.name != name or rel.left_column in columns)
                and (rel.right_cube.name != name or rel.right_column in columns)
            )
        ]

        return True

    def remove_relation(self, index: int) -> bool:
        """Remove a relation by index."""
        if 0 <= index < len(self.relations):
            self.relations.pop(index)
            return True
        return False

    def update_relation(
        self,
        index: int,
        left_column: str | None = None,
        right_column: str | None = None,
    ) -> bool:
        """Update a relation's column mappings."""
        if not (0 <= index < len(self.relations)):
            return False

        relation = self.relations[index]

        if left_column is not None:
            if left_column not in relation.left_cube.columns:
                raise ValueError(
                    f"Column '{left_column}' not in cube '{relation.left_cube.name}'"
                )
            relation.left_column = left_column

        if right_column is not None:
            if right_column not in relation.right_cube.columns:
                raise ValueError(
                    f"Column '{right_column}' not in cube '{relation.right_cube.name}'"
                )
            relation.right_column = right_column

        return True

    def to_graph_data(self) -> dict[str, Any]:
        """Export model as graph data for visualization.

        Returns a dict with 'nodes' and 'edges' suitable for graph libraries.
        """
        nodes = []
        edges = []

        for cube in self.cubes.values():
            nodes.append(
                {
                    "id": cube.name,
                    "label": cube.name,
                    "columns": cube.columns,
                }
            )

        for i, relation in enumerate(self.relations):
            edges.append(
                {
                    "id": f"edge_{i}",
                    "source": relation.left_cube.name,
                    "target": relation.right_cube.name,
                    "label": f"{relation.left_column} → {relation.right_column}",
                }
            )

        return {"nodes": nodes, "edges": edges}

