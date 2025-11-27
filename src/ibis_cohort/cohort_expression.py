from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Optional, Union, Any, Dict

from pydantic import BaseModel, Field, field_validator, ConfigDict, field_serializer, AliasChoices

from .criteria import Criteria, CriteriaGroup
from .concept_set import ConceptSet
from .strategy import EndStrategy
from .tables import (
    ConditionOccurrence,
    ConditionEra,
    VisitOccurrence,
    DrugExposure,
    DrugEra,
    DoseEra,
    Measurement,
    Observation,
    ObservationPeriod,
    Specimen,
    parse_criteria_list,
    serialize_criteria,
)


class ResultLimit(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    type: str = Field(default="All", alias="Type")


class ObservationFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    prior_days: int = Field(default=0, alias="PriorDays")
    post_days: int = Field(default=0, alias="PostDays")


class PrimaryCriteria(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    criteria_list: list[Criteria] = Field(default_factory=list, alias="CriteriaList")
    observation_window: ObservationFilter = Field(default=None, alias="ObservationWindow")
    primary_limit: ResultLimit = Field(default_factory=ResultLimit, alias="PrimaryCriteriaLimit")


    @field_validator('criteria_list', mode="before")
    @classmethod
    def validate_criteria_list(cls, v):
        return parse_criteria_list(v)

    @field_serializer('criteria_list')
    def serialize_criteria_list(self, criteria_list: list[Criteria]) -> list[dict[str, Any]]:
        return [serialize_criteria(criteria) for criteria in criteria_list]

class InclusionRule(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("Name", "name"),
        serialization_alias="name",
    )
    description: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("Description", "description"),
        serialization_alias="description",
    )
    expression: Optional[CriteriaGroup] = Field(
        default=None,
        validation_alias=AliasChoices("Expression", "expression"),
        serialization_alias="expression",
    )


class CollapseType(str, Enum):
    ERA = "ERA"


class Period(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    start_date: Optional[datetime] = Field(None, alias="StartDate")
    end_date: Optional[datetime] = Field(None, alias="EndDate")

    @field_serializer("start_date", "end_date", when_used="json")
    def _serialize_dates(self, value: Optional[datetime]):
        return value.strftime("%Y-%m-%d") if value else None


class CollapseSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=False, use_enum_values=True)

    collapse_type: CollapseType = Field(default=CollapseType.ERA, alias="CollapseType")
    era_pad: int = Field(default=0, alias="EraPad")


class CohortExpression(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    cdm_version_range: Optional[str] = Field(default=None, alias="cdmVersionRange")
    title: Optional[str] = Field(default=None, alias="Title")
    primary_criteria: PrimaryCriteria = Field(..., alias="PrimaryCriteria")
    additional_criteria: Optional[CriteriaGroup] = Field(None, alias="AdditionalCriteria")
    concept_sets: list[ConceptSet] = Field(..., alias="ConceptSets")
    qualified_limit: ResultLimit = Field(default_factory=ResultLimit, alias="QualifiedLimit")
    expression_limit: ResultLimit = Field(default_factory=ResultLimit, alias="ExpressionLimit")
    inclusion_rules: list[InclusionRule] = Field(default_factory=list, alias="InclusionRules")
    end_strategy: Optional[EndStrategy] = Field(default=None, alias="EndStrategy")
    censoring_criteria: list[Criteria] = Field(default_factory=list, alias="CensoringCriteria")
    collapse_settings: CollapseSettings = Field(default_factory=CollapseSettings, alias="CollapseSettings")
    censor_window: Optional[Period] = Field(default=None, alias="CensorWindow")

    @field_validator("censoring_criteria", mode="before")
    @classmethod
    def validate_censoring_criteria(cls, v):
        return parse_criteria_list(v)

    @field_serializer("censoring_criteria")
    def serialize_censoring_criteria(self, criteria_list: list[Criteria]) -> list[dict[str, Any]]:
        return [serialize_criteria(criteria) for criteria in criteria_list]


CohortExpression.model_rebuild()
