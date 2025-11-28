from dataclasses import dataclass, field


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
