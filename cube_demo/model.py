from dataclasses import dataclass, field
from functools import cached_property
from typing import Any

from cube_demo.cube import Cube
from cube_demo.relation import Cardinality, Relation


@dataclass
class Model:
    """Stores cubes and their relations to each other as a DAG."""

    name: str = "Model"
    cubes: dict[str, Cube] = field(default_factory=dict)
    _adjacency: dict[str, list[Relation]] = field(default_factory=dict)

    @property
    def relations(self) -> set[Relation]:
        """Returns all relations as a flat set."""
        return {rel for rels in self._adjacency.values() for rel in rels}

    def _invalidate_reachability_cache(self) -> None:
        """Clear the cached reachability data."""
        self.__dict__.pop("reachability", None)
        self.__dict__.pop("_all_reachability", None)

    @cached_property
    def reachability(self) -> dict[str, dict[str, int]]:
        """Compute reachability for all cubes (cached).

        For each cube, determines which other cubes are accessible by
        following directed edges in the DAG, along with the join distance.
        Cache is invalidated when cubes or relations are modified.

        Returns:
            Dict mapping each cube to {reachable_cube: distance}.
        """
        result: dict[str, dict[str, int]] = {}

        for cube_name in self.cubes:
            distances: dict[str, int] = {}
            queue = [(cube_name, 0)]
            visited = {cube_name}

            while queue:
                current, dist = queue.pop(0)
                for rel in self._adjacency.get(current, []):
                    target = rel.right_cube.name
                    if target not in visited:
                        visited.add(target)
                        distances[target] = dist + 1
                        queue.append((target, dist + 1))

            result[cube_name] = distances

        return result

    @cached_property
    def _all_reachability(self) -> dict[str, set[str]]:
        """For each cube, all cubes it can be queried with (bidirectional).

        Derived from reachability: if A can reach B, then both A and B
        can be queried together.

        Returns:
            Dict mapping each cube to set of queryable cubes.
        """
        result: dict[str, set[str]] = {name: set() for name in self.cubes}

        for cube_name, reachable in self.reachability.items():
            for target in reachable:
                result[cube_name].add(target)
                result[target].add(cube_name)

        return result

    def add_cube(self, cube: Cube) -> None:
        """Add a cube to the model."""
        if cube.name in self.cubes:
            raise ValueError(f"Cube '{cube.name}' already exists in model")
        self.cubes[cube.name] = cube
        self._invalidate_reachability_cache()

    def get_cube(self, name: str) -> Cube:
        """Get a cube by name."""
        if name not in self.cubes:
            raise KeyError(f"Cube '{name}' not found in model")
        return self.cubes[name]

    def get_root_cubes(self) -> list[str]:
        """Returns cubes with no incoming edges (source cubes)."""
        # Find all cubes that are targets of relations
        cubes_with_incoming: set[str] = set()
        for rels in self._adjacency.values():
            for rel in rels:
                cubes_with_incoming.add(rel.right_cube.name)

        # Return cubes that have no incoming edges
        return [name for name in self.cubes if name not in cubes_with_incoming]

    def topological_sort(self) -> list[str]:
        """Returns cubes in topological order (dependencies first).

        Uses Kahn's algorithm.
        """
        # Calculate in-degree for each cube
        in_degree: dict[str, int] = {name: 0 for name in self.cubes}
        for rels in self._adjacency.values():
            for rel in rels:
                in_degree[rel.right_cube.name] += 1

        # Start with cubes that have no incoming edges
        queue = [name for name in self.cubes if in_degree[name] == 0]
        result: list[str] = []

        while queue:
            current = queue.pop(0)
            result.append(current)

            # Reduce in-degree for neighbors
            for rel in self._adjacency.get(current, []):
                neighbor = rel.right_cube.name
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result

    def _would_create_cycle(self, from_cube: str, to_cube: str) -> bool:
        """Check if adding an edge from from_cube to to_cube would create a cycle."""
        # If adding edge A -> B, check if B can reach A (which would create a cycle)
        visited: set[str] = set()
        stack = [to_cube]

        while stack:
            current = stack.pop()
            if current == from_cube:
                return True
            if current in visited:
                continue
            visited.add(current)
            # Follow outgoing edges
            for rel in self._adjacency.get(current, []):
                stack.append(rel.right_cube.name)

        return False

    def add_relation(self, relation: Relation) -> None:
        """Add a relation between two cubes.

        Raises ValueError if the relation would create a cycle in the DAG.
        """
        left_name = relation.left_cube.name
        right_name = relation.right_cube.name

        if left_name not in self.cubes:
            raise ValueError(f"Left cube '{left_name}' not found in model")
        if right_name not in self.cubes:
            raise ValueError(f"Right cube '{right_name}' not found in model")

        # Check for cycle
        if self._would_create_cycle(left_name, right_name):
            raise ValueError(
                f"Adding relation {left_name} -> {right_name} would create a cycle"
            )

        # Add to adjacency list
        if left_name not in self._adjacency:
            self._adjacency[left_name] = []
        self._adjacency[left_name].append(relation)
        self._invalidate_reachability_cache()

    def remove_cube(self, name: str) -> bool:
        """Remove a cube and all relations referencing it."""
        if name not in self.cubes:
            return False

        # Remove outgoing relations from this cube
        if name in self._adjacency:
            del self._adjacency[name]

        # Remove incoming relations to this cube from all other cubes
        for source in list(self._adjacency.keys()):
            self._adjacency[source] = [
                rel for rel in self._adjacency[source] if rel.right_cube.name != name
            ]
            # Clean up empty lists
            if not self._adjacency[source]:
                del self._adjacency[source]

        # Remove the cube
        del self.cubes[name]
        self._invalidate_reachability_cache()
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
        self._invalidate_reachability_cache()

        return True

    def update_cube_columns(self, name: str, columns: list[str]) -> bool:
        """Update a cube's columns."""
        if name not in self.cubes:
            return False

        self.cubes[name].columns = columns

        # Remove relations with invalid columns from all adjacency lists
        for source in list(self._adjacency.keys()):
            self._adjacency[source] = [
                rel
                for rel in self._adjacency[source]
                if (
                    (rel.left_cube.name != name or rel.left_column in columns)
                    and (rel.right_cube.name != name or rel.right_column in columns)
                )
            ]
            # Clean up empty lists
            if not self._adjacency[source]:
                del self._adjacency[source]

        self._invalidate_reachability_cache()
        return True

    def remove_relation(self, relation: Relation) -> bool:
        """Remove a relation from the model."""
        left_name = relation.left_cube.name
        if left_name not in self._adjacency:
            return False

        original_len = len(self._adjacency[left_name])
        self._adjacency[left_name] = [
            rel for rel in self._adjacency[left_name] if rel != relation
        ]

        if len(self._adjacency[left_name]) < original_len:
            # Clean up empty lists
            if not self._adjacency[left_name]:
                del self._adjacency[left_name]
            self._invalidate_reachability_cache()
            return True
        return False

    def update_relation(
        self,
        old_relation: Relation,
        left_column: str | None = None,
        right_column: str | None = None,
    ) -> bool:
        """Update a relation's column mappings by replacing it."""
        left_name = old_relation.left_cube.name
        if (
            left_name not in self._adjacency
            or old_relation not in self._adjacency[left_name]
        ):
            return False

        new_left_col = (
            left_column if left_column is not None else old_relation.left_column
        )
        new_right_col = (
            right_column if right_column is not None else old_relation.right_column
        )

        if new_left_col not in old_relation.left_cube.columns:
            raise ValueError(
                f"Column '{new_left_col}' not in cube '{old_relation.left_cube.name}'"
            )
        if new_right_col not in old_relation.right_cube.columns:
            raise ValueError(
                f"Column '{new_right_col}' not in cube '{old_relation.right_cube.name}'"
            )

        # Remove old relation
        self._adjacency[left_name] = [
            rel for rel in self._adjacency[left_name] if rel != old_relation
        ]

        # Add new relation (preserving cardinality)
        new_relation = Relation(
            left_cube=old_relation.left_cube,
            right_cube=old_relation.right_cube,
            left_column=new_left_col,
            right_column=new_right_col,
            cardinality=old_relation.cardinality,
        )
        self._adjacency[left_name].append(new_relation)
        self._invalidate_reachability_cache()
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
                    "label": f"{relation.left_column} → {relation.right_column} [{relation.cardinality.value}]",
                    "cardinality": relation.cardinality.value,
                }
            )

        return {"nodes": nodes, "edges": edges}

    def generate_sql_query(self, selected_columns: list[str]) -> str:
        """Generate a SQL query with JOINs based on selected columns.

        Uses the precomputed reachability dictionary to find a starting cube
        that can reach all involved cubes. Includes all columns from the
        starting cube in the SELECT clause.

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

        involved_cubes = set(columns_by_cube.keys())

        # If only one cube, no JOINs needed - return all columns from that cube
        if len(involved_cubes) == 1:
            cube_name = next(iter(involved_cubes))
            cols = ", ".join(f"{cube_name}.{c}" for c in self.cubes[cube_name].columns)
            return f"SELECT {cols}\nFROM {cube_name}"

        # Build directed adjacency graph from relations (only left → right)
        # Each entry: (target, left_col, right_col, cardinality)
        adjacency: dict[str, list[tuple[str, str, str, Cardinality]]] = {
            name: [] for name in self.cubes
        }
        for rel in self.relations:
            left = rel.left_cube.name
            right = rel.right_cube.name
            adjacency[left].append(
                (right, rel.left_column, rel.right_column, rel.cardinality)
            )

        # Find all candidate starting cubes that can reach all involved cubes
        reachability = self.reachability
        candidates: list[str] = []
        for cube_name, reachable in reachability.items():
            other_cubes = involved_cubes - {cube_name}
            if other_cubes <= set(reachable.keys()):
                candidates.append(cube_name)

        if not candidates:
            return "Error: No cube can reach all selected cubes. Check reachability."

        # Use distances to find candidate with minimum total joins
        def count_joins(candidate: str) -> int:
            """Sum distances to all involved cubes using reachability."""
            distances = reachability.get(candidate, {})
            return sum(
                distances.get(cube, 0) for cube in involved_cubes if cube != candidate
            )

        # Select the candidate with the minimum total joins
        start_cube = min(candidates, key=count_joins)

        # BFS to find paths from start_cube following directed edges
        visited = {start_cube}
        queue = [start_cube]
        parent: dict[str, tuple[str, str, str, Cardinality] | None] = {start_cube: None}

        while queue:
            current = queue.pop(0)
            for target, left_col, right_col, cardinality in adjacency[current]:
                if target not in visited:
                    visited.add(target)
                    parent[target] = (current, left_col, right_col, cardinality)
                    queue.append(target)

        # Determine which cubes need to be joined (all involved cubes except start)
        cubes_to_join = involved_cubes - {start_cube}
        joined_cubes = {start_cube}
        join_clauses: list[str] = []

        # Map cardinality to SQL JOIN type
        def get_join_type(cardinality: Cardinality) -> str:
            match cardinality:
                case Cardinality.ONE_TO_ONE:
                    return "INNER JOIN"
                case Cardinality.ONE_TO_MANY:
                    return "LEFT JOIN"
                case Cardinality.MANY_TO_ONE:
                    return "RIGHT JOIN"

        # For each cube we need, trace back to find the join path
        for target in list(cubes_to_join):
            # path entries: (from_cube, to_cube, left_col, right_col, cardinality)
            path: list[tuple[str, str, str, str, Cardinality]] = []
            current = target
            while current != start_cube and current not in joined_cubes:
                p = parent.get(current)
                if p is None:
                    break
                prev, left_col, right_col, cardinality = p
                path.append((prev, current, left_col, right_col, cardinality))
                current = prev

            # Add joins in reverse order (from joined cube toward target)
            for from_cube, to_cube, left_col, right_col, cardinality in reversed(path):
                if to_cube not in joined_cubes:
                    join_type = get_join_type(cardinality)
                    join_clauses.append(
                        f"{join_type} {to_cube} ON {from_cube}.{left_col} = {to_cube}.{right_col}"
                    )
                    joined_cubes.add(to_cube)

        # Build SELECT clause: all columns from start_cube + selected columns from other cubes
        select_parts: list[str] = []

        # Add all columns from the starting cube
        for col in self.cubes[start_cube].columns:
            select_parts.append(f"{start_cube}.{col}")

        # Add selected columns from other cubes (avoiding duplicates)
        start_cube_cols = {f"{start_cube}.{c}" for c in self.cubes[start_cube].columns}
        for col_ref in selected_columns:
            if col_ref not in start_cube_cols:
                select_parts.append(col_ref)

        select_cols = ", ".join(select_parts)

        # Build FROM clause
        from_clause = f"FROM {start_cube}"

        # Combine
        sql_parts = [f"SELECT {select_cols}", from_clause]
        sql_parts.extend(join_clauses)

        return "\n".join(sql_parts)
