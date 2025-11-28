"""Cube Demo - A library for representing database cubes and their relations."""

from cube_demo.cube import Cube
from cube_demo.model import Model
from cube_demo.relation import Relation
from cube_demo import database as db

__all__ = ["Cube", "Relation", "Model", "db"]
