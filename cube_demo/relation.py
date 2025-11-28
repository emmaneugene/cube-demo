from dataclasses import dataclass

from cube_demo.cube import Cube


@dataclass
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

    @property
    def label(self) -> str:
        """Returns a label describing the join."""
        return f"{self.left_cube.name}.{self.left_column} → {self.right_cube.name}.{self.right_column}"

