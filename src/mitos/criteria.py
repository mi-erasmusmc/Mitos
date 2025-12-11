from __future__ import annotations
from typing import Union, Optional, Any
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict, field_validator, field_serializer


class DateType(str, Enum):
    START_DATE = "StartDate"
    END_DATE = "EndDate"


class DateAdjustment(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    start_with: DateType = Field(default=DateType.START_DATE, alias="StartWith")
    start_offset: int = Field(default=0, alias="StartOffset")
    end_with: DateType = Field(default=DateType.END_DATE, alias="EndWith")
    end_offset: int = Field(default=0, alias="EndOffset")


class CriteriaColumn(Enum):
    DAYS_SUPPLY = "days_supply"
    DOMAIN_CONCEPT = "domain_concept_id"
    DOMAIN_SOURCE_CONCEPT = "domain_source_concept_id"
    DURATION = "duration"
    END_DATE = "end_date"
    ERA_OCCURRENCES = "occurrence_count"
    GAP_DAYS = "gap_days"
    QUANTITY = "quantity"
    RANGE_HIGH = "range_high"
    RANGE_LOW = "range_low"
    REFILLS = "refills"
    START_DATE = "start_date"
    UNIT = "unit_concept_id"
    VALUE_AS_NUMBER = "value_as_number"
    VISIT_ID = "visit_occurrence_id"
    VISIT_DETAIL_ID = "visit_detail_id"

    def __str__(self):
        """
        Override the __str__ method to return the string representation
        of the Enum which is the column name in this case.
        """
        return self.value


class OccurrenceType(Enum):
    EXACTLY = 0
    AT_MOST = 1
    AT_LEAST = 2


class Occurrence(BaseModel):
    model_config = ConfigDict(populate_by_name=False, use_enum_values=True)

    type: OccurrenceType = Field(..., alias="Type")
    count: int = Field(..., alias="Count")
    is_distinct: Optional[bool] = Field(default=None, alias="IsDistinct")
    count_column: Optional[CriteriaColumn] = Field(default=None, alias="CountColumn")

    @field_validator("count_column", mode="before")
    @classmethod
    def normalize_count_column(cls, value):
        if value is None or isinstance(value, CriteriaColumn):
            return value
        value_str = str(value)
        member_name = value_str.upper()
        if member_name in CriteriaColumn.__members__:
            return CriteriaColumn[member_name]
        normalized = value_str.lower()
        for column in CriteriaColumn:
            if column.value == normalized:
                return column
        if normalized.endswith("_id"):
            trimmed = normalized.removesuffix("_id")
            for column in CriteriaColumn:
                if column.value.endswith("_id") and column.value.removesuffix("_id") == trimmed:
                    return column
        raise ValueError(f"Unsupported occurrence count column: {value}")

    @field_serializer("count_column")
    def serialize_count_column(self, count_column: CriteriaColumn | None):
        if count_column is None:
            return None
        if isinstance(count_column, CriteriaColumn):
            return count_column.name
        value_str = str(count_column)
        for column in CriteriaColumn:
            if column.value == value_str.lower():
                return column.name
        return value_str


class Endpoint(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    days: Optional[int] = Field(None, alias="Days")
    coeff: int = Field(..., alias="Coeff")


class Window(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    start: Optional[Endpoint] = Field(None, alias="Start")
    end: Optional[Endpoint] = Field(None, alias="End")
    use_index_end: Optional[bool] = Field(None, alias="UseIndexEnd")
    use_event_end: Optional[bool] = Field(None, alias="UseEventEnd")


class WindowCriteria(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    criteria: Optional[dict[str, Any]] = Field(default=None, alias="Criteria")
    start_window: Optional[Window] = Field(default=None, alias="StartWindow")
    end_window: Optional[Window] = Field(default=None, alias="EndWindow")
    restrict_visit: Optional[bool] = Field(default=None, alias="RestrictVisit")
    ignore_observation_period: Optional[bool] = Field(default=None, alias="IgnoreObservationPeriod")


class CorrelatedCriteria(WindowCriteria):
    occurrence: Optional[Occurrence] = Field(default=None, alias="Occurrence")


Number = Union[int, float]


class NumericRange(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    value: Optional[Number] = Field(None, alias="Value")
    op: Optional[str] = Field(None, alias="Op")
    extent: Optional[Number] = Field(None, alias="Extent")


class DateRange(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    value: str = Field(..., alias="Value")
    op: str = Field(..., alias="Op")
    extent: Optional[str] = Field(default=None, alias="Extent")


class TextFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    text: Optional[str] = Field(None, alias="Text")
    op: Optional[str] = Field(None, alias="Op")


class ConceptSetSelection(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    is_exclusion: bool = Field(default=False, alias="IsExclusion")


class Concept(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    concept_id: Optional[int] = Field(default=None, alias="CONCEPT_ID")
    concept_name: Optional[str] = Field(default=None, alias="CONCEPT_NAME")
    standard_concept: Optional[str] = Field(default=None, alias="STANDARD_CONCEPT")
    invalid_reason: Optional[str] = Field(default=None, alias="INVALID_REASON")
    invalid_reason_caption: Optional[str] = Field(default=None, alias="INVALID_REASON_CAPTION")
    concept_code: Optional[str] = Field(default=None, alias="CONCEPT_CODE")
    domain_id: Optional[str] = Field(default=None, alias="DOMAIN_ID")
    vocabulary_id: Optional[str] = Field(default=None, alias="VOCABULARY_ID")
    concept_class_id: Optional[str] = Field(default=None, alias="CONCEPT_CLASS_ID")
    standard_concept_caption: Optional[str] = Field(default=None, alias="STANDARD_CONCEPT_CAPTION")

    def model_dump(self, *args, **kwargs):
        by_alias = kwargs.get("by_alias", False)
        data = super().model_dump(*args, by_alias=by_alias, exclude_none=False, exclude_unset=False)
        included_keys = set()
        for name in getattr(self, "model_fields_set", set()):
            field = self.model_fields.get(name)
            if field is None:
                included_keys.add(name)
            else:
                key = field.alias or name
                included_keys.add(key if by_alias else name)
        return {k: v for k, v in data.items() if v is not None or k in included_keys}

class DemoGraphicCriteria(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    race: list[Concept] = Field(default_factory=list, alias="Race")
    race_cs: Optional[ConceptSetSelection] = Field(default=None, alias="RaceCS")
    ethnicity: list[Concept] = Field(default_factory=list, alias="Ethnicity")
    ethnicity_cs: Optional[ConceptSetSelection] = Field(default=None, alias="EthnicityCS")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")

    @field_serializer("gender", "race", "ethnicity", when_used="always")
    def _serialize_nonempty_lists(self, value):
        return value or None


class CriteriaGroup(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    type: str = Field(None, alias="Type")
    count: int = Field(None, alias="Count")
    criteria_list: list[CorrelatedCriteria] = Field(default_factory=list, alias="CriteriaList")
    demographic_criteria_list: list[DemoGraphicCriteria] = Field(default_factory=list, alias="DemographicCriteriaList")
    groups: list[CriteriaGroup] = Field(default_factory=list, alias="Groups")

    def is_empty(self):
        return not any([self.criteria_list, self.demographic_criteria_list, self.groups])


def to_snake_case(name: str) -> str:
    return "".join(["_" + c.lower() if c.isupper() else c for c in name]).lstrip("_")


class Criteria(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    correlated_criteria: CriteriaGroup = Field(default=None, alias="CorrelatedCriteria")
    date_adjustment: DateAdjustment = Field(default=None, alias="DateAdjustment")

    @classmethod
    def snake_case_class_name(cls) -> str:
        return to_snake_case(cls.__name__)

    def get_concept_id_column(self) -> str:
        table_name = self.snake_case_class_name()
        # Construct the concept_id column name
        concept_id_column = f"{table_name.split('_')[0]}_concept_id"
        return concept_id_column

    def get_primary_key_column(self) -> str:
        return f"{self.snake_case_class_name()}_id"

    def get_start_date_column(self) -> str:
        return f"{self.snake_case_class_name().split('_')[0]}_start_date"

    def get_end_date_column(self) -> str:
        return f"{self.snake_case_class_name().split('_')[0]}_end_date"
