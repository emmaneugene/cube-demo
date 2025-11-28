from dataclasses import dataclass, field
from typing import Any

from cube_demo.cube import Cube
from cube_demo.relation import Relation


@dataclass
class Model:
    """Stores cubes and their relations to each other."""

    name: str = "Model"
    cubes: dict[str, Cube] = field(default_factory=dict)
    relations: set[Relation] = field(default_factory=set)

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
        self.relations.add(relation)

    def remove_cube(self, name: str) -> bool:
        """Remove a cube and all relations referencing it."""
        if name not in self.cubes:
            return False

        # Remove all relations involving this cube
        self.relations = {
            rel
            for rel in self.relations
            if rel.left_cube.name != name and rel.right_cube.name != name
        }

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
        self.relations = {
            rel
            for rel in self.relations
            if (
                (rel.left_cube.name != name or rel.left_column in columns)
                and (rel.right_cube.name != name or rel.right_column in columns)
            )
        }

        return True

    def remove_relation(self, relation: Relation) -> bool:
        """Remove a relation from the model."""
        if relation in self.relations:
            self.relations.discard(relation)
            return True
        return False

    def update_relation(
        self,
        old_relation: Relation,
        left_column: str | None = None,
        right_column: str | None = None,
    ) -> bool:
        """Update a relation's column mappings by replacing it."""
        if old_relation not in self.relations:
            return False

        new_left_col = left_column if left_column is not None else old_relation.left_column
        new_right_col = right_column if right_column is not None else old_relation.right_column

        if new_left_col not in old_relation.left_cube.columns:
            raise ValueError(
                f"Column '{new_left_col}' not in cube '{old_relation.left_cube.name}'"
            )
        if new_right_col not in old_relation.right_cube.columns:
            raise ValueError(
                f"Column '{new_right_col}' not in cube '{old_relation.right_cube.name}'"
            )

        # Remove old and add new (since relations are hashable, we can't mutate in place)
        self.relations.discard(old_relation)
        new_relation = Relation(
            left_cube=old_relation.left_cube,
            right_cube=old_relation.right_cube,
            left_column=new_left_col,
            right_column=new_right_col,
        )
        self.relations.add(new_relation)
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

    def generate_sql_query(self, selected_columns: list[str]) -> str:
        """Generate a SQL query with JOINs based on selected columns.

        Args:
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
            if cube_name not in self.cubes:
                return f"Error: Cube '{cube_name}' not found"
            if col_name not in self.cubes[cube_name].columns:
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
        adjacency: dict[str, list[tuple[str, str, str, str, str]]] = {
            name: [] for name in self.cubes
        }
        for rel in self.relations:
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

