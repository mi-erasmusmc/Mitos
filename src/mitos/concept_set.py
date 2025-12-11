from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

from .criteria import Concept


class ConceptSetItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    concept: Concept = Field(..., alias="concept")
    is_excluded: Optional[bool] = Field(None, alias="isExcluded")
    include_descendants: Optional[bool] = Field(None, alias="includeDescendants")
    include_mapped: Optional[bool] = Field(None, alias="includeMapped")


class ConceptSetExpression(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    items: list[ConceptSetItem] = Field(default_factory=list)

    @classmethod
    def from_json(cls, json_str: str) -> ConceptSetExpression:
        return cls.model_validate_json(json_str)


class ConceptSet(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: int
    name: str
    expression: Optional[ConceptSetExpression] = None

    def __hash__(self):
        return hash((self.id, self.name, self.expression))

    def __eq__(self, other):
        """Check for equality with another ConceptSet instance"""
        if not isinstance(other, ConceptSet):
            return False
        return (self.id == other.id) and (self.name == other.name) and (self.expression == other.expression)
