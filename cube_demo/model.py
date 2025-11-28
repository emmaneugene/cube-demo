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

