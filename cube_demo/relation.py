from dataclasses import dataclass

from cube_demo.cube import Cube


@dataclass(eq=False)
class Relation:
    """Represents a directed join between two cubes.

    Visually represented as: left_cube.left_column → right_cube.right_column
    """

    left_cube: Cube
    right_cube: Cube
    left_column: str
    right_column: str

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
            (self.left_cube.name, self.right_cube.name, self.left_column, self.right_column)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Relation):
            return NotImplemented
        return (
            self.left_cube.name == other.left_cube.name
            and self.right_cube.name == other.right_cube.name
            and self.left_column == other.left_column
            and self.right_column == other.right_column
        )

    @property
    def label(self) -> str:
        """Returns a label describing the join."""
        return f"{self.left_cube.name}.{self.left_column} → {self.right_cube.name}.{self.right_column}"

