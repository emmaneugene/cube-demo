from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import Any


@dataclass
class Cube:
    """Represents a database table (cube) with a name and columns."""

    name: str
    columns: list[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Cube):
            return NotImplemented
        return self.name == other.name


class Cardinality(Enum):
    """Represents the cardinality of a relation between two cubes."""

    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"
    MANY_TO_ONE = "many-to-one"

    @property
    def sql_join(self) -> str:
        """Returns the SQL JOIN type for this cardinality."""
        match self:
            case Cardinality.ONE_TO_ONE:
                return "INNER JOIN"
            case Cardinality.ONE_TO_MANY:
                return "LEFT JOIN"
            case Cardinality.MANY_TO_ONE:
                return "RIGHT JOIN"


@dataclass(eq=False)
class Relation:
    """Represents a directed join between two cubes.

    Visually represented as: left_cube.left_column → right_cube.right_column
    """

    left_cube: Cube
    right_cube: Cube
    left_column: str
    right_column: str
    cardinality: Cardinality

    def __post_init__(self) -> None:
        if self.left_column not in self.left_cube.columns:
            raise ValueError(
                f"Column '{self.left_column}' not found in cube '{self.left_cube.name}'"
            )
        if self.right_column not in self.right_cube.columns:
            raise ValueError(
                f"Column '{self.right_column}' not found in cube '{self.right_cube.name}'"
            )

    def __hash__(self) -> int:
        return hash(
            (
                self.left_cube.name,
                self.right_cube.name,
                self.left_column,
                self.right_column,
                self.cardinality,
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Relation):
            return NotImplemented
        return (
            self.left_cube.name == other.left_cube.name
            and self.right_cube.name == other.right_cube.name
            and self.left_column == other.left_column
            and self.right_column == other.right_column
            and self.cardinality == other.cardinality
        )

    @property
    def label(self) -> str:
        """Returns a label describing the join."""
        return f"{self.left_cube.name}.{self.left_column} → {self.right_cube.name}.{self.right_column} ({self.cardinality.value})"


@dataclass
class RelationData:
    """Represents relation data from the database (without Cube object references)."""

    id: int
    left_cube: str
    right_cube: str
    left_column: str
    right_column: str
    cardinality: Cardinality


@dataclass
class Join:
    """Represents a SQL JOIN clause between two cubes."""

    from_cube: str
    to_cube: str
    left_column: str
    right_column: str
    cardinality: Cardinality

    def to_sql(self) -> str:
        """Generate the SQL JOIN clause."""
        return f"{self.cardinality.sql_join} {self.to_cube} ON {self.from_cube}.{self.left_column} = {self.to_cube}.{self.right_column}"


@dataclass
class Model:
    """Stores cubes and their relations to each other as a DAG."""

    name: str = "Model"
    cubes: dict[str, Cube] = field(default_factory=dict)
    adjacency: dict[str, list[Relation]] = field(default_factory=dict)

    @property
    def relations(self) -> set[Relation]:
        """Returns all relations as a flat set."""
        return {rel for rels in self.adjacency.values() for rel in rels}

    def _invalidate_reachability_caches(self) -> None:
        """Clear the cached reachability data."""
        self.__dict__.pop("reachability", None)
        self.__dict__.pop("all_reachability", None)

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
            queue: deque[tuple[str, int]] = deque([(cube_name, 0)])
            visited = {cube_name}

            while queue:
                current, dist = queue.popleft()
                for rel in self.adjacency.get(current, []):
                    target = rel.right_cube.name
                    if target not in visited:
                        visited.add(target)
                        distances[target] = dist + 1
                        queue.append((target, dist + 1))

            result[cube_name] = distances

        return result

    @cached_property
    def all_reachability(self) -> dict[str, set[str]]:
        """For each cube, all cubes it can be queried

        Derived from reachability: if A can reach B, then both A and B
        can be queried together.

        Returns:
            Dict mapping each cube to set of queryable cubes.
        """
        result: dict[str, set[str]] = {name: set() for name in self.cubes}

        for cube_name, reachable in self.reachability.items():
            connected_cubes = set(reachable.keys())
            connected_cubes.add(cube_name)
            for target in connected_cubes:
                result[target] = result[target].union(connected_cubes)

        return result

    def add_cube(self, cube: Cube) -> None:
        """Add a cube to the model."""
        if cube.name in self.cubes:
            raise ValueError(f"Cube '{cube.name}' already exists in model")
        self.cubes[cube.name] = cube
        self._invalidate_reachability_caches()

    def remove_cube(self, name: str) -> bool:
        """Remove a cube and all relations referencing it."""
        if name not in self.cubes:
            return False

        # Remove outgoing relations from this cube
        if name in self.adjacency:
            del self.adjacency[name]

        # Remove incoming relations to this cube from all other cubes
        for source in list(self.adjacency.keys()):
            self.adjacency[source] = [
                rel for rel in self.adjacency[source] if rel.right_cube.name != name
            ]
            # Clean up empty lists
            if not self.adjacency[source]:
                del self.adjacency[source]

        # Remove the cube
        del self.cubes[name]
        self._invalidate_reachability_caches()
        return True

    def get_cube(self, name: str) -> Cube:
        """Get a cube by name."""
        if name not in self.cubes:
            raise KeyError(f"Cube '{name}' not found in model")
        return self.cubes[name]

    def get_root_cubes(self) -> list[str]:
        """Returns cubes with no incoming edges (source cubes)."""
        # Find all cubes that are targets of relations
        cubes_with_incoming: set[str] = set()
        for rels in self.adjacency.values():
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
        for rels in self.adjacency.values():
            for rel in rels:
                in_degree[rel.right_cube.name] += 1

        # Start with cubes that have no incoming edges
        queue: deque[str] = deque(name for name in self.cubes if in_degree[name] == 0)
        result: list[str] = []

        while queue:
            current = queue.popleft()
            result.append(current)

            # Reduce in-degree for neighbors
            for rel in self.adjacency.get(current, []):
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
            for rel in self.adjacency.get(current, []):
                stack.append(rel.right_cube.name)

        return False

    def add_relation(self, relation: Relation) -> None:
        """Add a relation between two cubes.

        Raises ValueError if:
        - the relation would create a cycle in the DAG.
        - the relation creates a duplicate path between any 2 cubes.
        - the relation connects a cube to itself.
        """
        left_name = relation.left_cube.name
        right_name = relation.right_cube.name

        # Check for self-relation
        if left_name == right_name:
            raise ValueError(
                f"Cannot add relation: cube '{left_name}' cannot connect to itself"
            )

        if left_name not in self.cubes:
            raise ValueError(f"Left cube '{left_name}' not found in model")
        if right_name not in self.cubes:
            raise ValueError(f"Right cube '{right_name}' not found in model")

        # Check for duplicate path (if right_cube is already reachable from left_cube)
        if right_name in self.reachability.get(left_name, {}):
            raise ValueError(
                f"Adding relation {left_name} -> {right_name} would create a duplicate path"
            )

        # Check for cycle
        if self._would_create_cycle(left_name, right_name):
            raise ValueError(
                f"Adding relation {left_name} -> {right_name} would create a cycle"
            )

        # Add to adjacency list
        if left_name not in self.adjacency:
            self.adjacency[left_name] = []
        self.adjacency[left_name].append(relation)
        self._invalidate_reachability_caches()

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
        self._invalidate_reachability_caches()

        return True

    def update_cube_columns(self, name: str, columns: list[str]) -> bool:
        """Update a cube's columns."""
        if name not in self.cubes:
            return False

        self.cubes[name].columns = columns

        # Remove relations with invalid columns from all adjacency lists
        for source in list(self.adjacency.keys()):
            self.adjacency[source] = [
                rel
                for rel in self.adjacency[source]
                if (
                    (rel.left_cube.name != name or rel.left_column in columns)
                    and (rel.right_cube.name != name or rel.right_column in columns)
                )
            ]
            # Clean up empty lists
            if not self.adjacency[source]:
                del self.adjacency[source]

        self._invalidate_reachability_caches()
        return True

    def remove_relation(self, relation: Relation) -> bool:
        """Remove a relation from the model."""
        left_name = relation.left_cube.name
        if left_name not in self.adjacency:
            return False

        original_len = len(self.adjacency[left_name])
        self.adjacency[left_name] = [
            rel for rel in self.adjacency[left_name] if rel != relation
        ]

        if len(self.adjacency[left_name]) < original_len:
            # Clean up empty lists
            if not self.adjacency[left_name]:
                del self.adjacency[left_name]
            self._invalidate_reachability_caches()
            return True
        return False

    def update_relation(
        self,
        old_relation: Relation,
        left_column: str | None = None,
        right_column: str | None = None,
        cardinality: Cardinality | None = None,
    ) -> bool:
        """Update a relation's column mappings and/or cardinality by replacing it."""
        left_name = old_relation.left_cube.name
        if (
            left_name not in self.adjacency
            or old_relation not in self.adjacency[left_name]
        ):
            return False

        new_left_col = (
            left_column if left_column is not None else old_relation.left_column
        )
        new_right_col = (
            right_column if right_column is not None else old_relation.right_column
        )
        new_cardinality = (
            cardinality if cardinality is not None else old_relation.cardinality
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
        self.adjacency[left_name] = [
            rel for rel in self.adjacency[left_name] if rel != old_relation
        ]

        # Add new relation
        new_relation = Relation(
            left_cube=old_relation.left_cube,
            right_cube=old_relation.right_cube,
            left_column=new_left_col,
            right_column=new_right_col,
            cardinality=new_cardinality,
        )
        self.adjacency[left_name].append(new_relation)
        self._invalidate_reachability_caches()
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

    def get_join_path(self, selected_columns: list[str]) -> list[Join]:
        """Get the ordered list of joins needed to query the selected columns.

        Uses the precomputed reachability dictionary to find a starting cube
        that can reach all involved cubes, then traces the join paths.

        Args:
            selected_columns: List of columns in "cube.column" format

        Returns:
            List of Join objects in the order they should be applied.
            Returns empty list if only one cube is involved.

        Raises:
            ValueError: If columns are invalid or cubes are unreachable.
        """
        if not selected_columns:
            raise ValueError("No columns selected")

        # Parse selected columns to get cube names
        needed_cubes: set[str] = set()
        for col_ref in selected_columns:
            if "." not in col_ref:
                raise ValueError(f"Invalid column format: {col_ref}")
            cube_name, col_name = col_ref.split(".", 1)
            if cube_name not in self.cubes:
                raise ValueError(f"Cube '{cube_name}' not found")
            if col_name not in self.cubes[cube_name].columns:
                raise ValueError(f"Column '{col_name}' not found in cube '{cube_name}'")
            needed_cubes.add(cube_name)

        # If only one cube, no JOINs needed
        if len(needed_cubes) == 1:
            return []

        # Find all candidate starting cubes that can reach all involved cubes
        reachability = self.reachability
        candidates: list[tuple[str, int]] = []
        for cube_name, reachable in reachability.items():
            other_cubes = needed_cubes - {cube_name}
            if other_cubes <= set(reachable.keys()):
                candidates.append(
                    (cube_name, sum(reachable.get(cube, 0) for cube in other_cubes))
                )

        if not candidates:
            raise ValueError("No cube can reach all selected cubes. Check reachability.")

        # Select the candidate with the minimum total joins
        start_cube = min(candidates, key=lambda x: x[1])[0]

        # Do BFS from start_cube to find join paths
        visited = {start_cube}
        queue: deque[str] = deque([start_cube])
        join_to: dict[str, Join | None] = {start_cube: None}

        while queue:
            current = queue.popleft()
            for rel in self.adjacency.get(current, []):
                target = rel.right_cube.name
                if target not in visited:
                    visited.add(target)
                    join_to[target] = Join(
                        from_cube=current,
                        to_cube=target,
                        left_column=rel.left_column,
                        right_column=rel.right_column,
                        cardinality=rel.cardinality,
                    )
                    queue.append(target)

        # Trace path for cubes to be joined
        cubes_to_join = needed_cubes - {start_cube}
        joined_cubes = {start_cube}
        joins: list[Join] = []

        # For each cube needed, trace back to find the join path
        for target in list(cubes_to_join):
            path: list[Join] = []
            current = target
            while current != start_cube and current not in joined_cubes:
                join = join_to.get(current)
                if join is None:
                    break
                path.insert(0, join)
                current = join.from_cube

            for join in path:
                if join.to_cube not in joined_cubes:
                    joins.append(join)
                    joined_cubes.add(join.to_cube)

        return joins

    def generate_sql_query(self, selected_columns: list[str]) -> str:
        """Generate a SQL query with JOINs based on selected columns.

        Uses get_join_path to determine the necessary joins.

        Args:
            selected_columns: List of columns in "cube.column" format

        Returns:
            SQL query string or error message starting with "Error:"
        """
        try:
            joins = self.get_join_path(selected_columns)
        except ValueError as e:
            return f"Error: {e}"

        # Determine start cube
        if joins:
            start_cube = joins[0].from_cube
        else:
            # Single cube case - extract from first selected column
            cube_name = selected_columns[0].split(".", 1)[0]
            cols = ", ".join(f"{cube_name}.{c}" for c in self.cubes[cube_name].columns)
            return f"SELECT {cols}\nFROM {cube_name}"

        # Build SQL with joins
        join_clauses = [join.to_sql() for join in joins]
        sql_parts = [
            f"SELECT {', '.join(selected_columns)}",
            f"FROM {start_cube}",
            *join_clauses,
        ]

        return "\n".join(sql_parts)
