from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, field_serializer, AliasChoices
from typing import Optional, Any, Union

from .criteria import (
    Criteria,
    DateRange,
    NumericRange,
    Concept,
    TextFilter,
    ConceptSetSelection,
    TextFilter,
)


class UserDefinedPeriod(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    start_date: Optional[datetime] = Field(default=None, alias="StartDate")
    end_date: Optional[datetime] = Field(default=None, alias="EndDate")

    @field_serializer("start_date", "end_date")
    def _serialize_dates(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.date().isoformat()


class ConditionEra(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(None, alias="CodesetId")
    first: Optional[bool] = Field(None, alias="First")
    era_start_date: Optional[DateRange] = Field(None, alias="EraStartDate")
    era_end_date: Optional[DateRange] = Field(None, alias="EraEndDate")
    occurrence_count: Optional[NumericRange] = Field(None, alias="OccurrenceCount")
    era_length: Optional[NumericRange] = Field(None, alias="EraLength")
    age_at_start: Optional[NumericRange] = Field(None, alias="AgeAtStart")
    age_at_end: Optional[NumericRange] = Field(None, alias="AgeAtEnd")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")

    def get_primary_key_column(self) -> str:
        return "condition_era_id"

    def get_start_date_column(self) -> str:
        return "condition_era_start_date"

    def get_end_date_column(self) -> str:
        return "condition_era_end_date"


class ConditionOccurrence(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(None, alias="OccurrenceEndDate")
    condition_type: list[Concept] = Field(default_factory=list, alias="ConditionType")
    condition_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ConditionTypeCS")
    condition_type_exclude: Optional[bool] = Field(None, alias="ConditionTypeExclude")
    stop_reason: Optional[TextFilter] = Field(None, alias="StopReason")
    condition_source_concept: Optional[Union[int, ConceptSetSelection]] = Field(None, alias="ConditionSourceConcept")
    age: Optional[NumericRange] = Field(None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    visit_source_concept: Optional[int] = Field(default=None, alias="VisitSourceConcept")
    condition_status: list[Concept] = Field(default_factory=list, alias="ConditionStatus")
    condition_status_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ConditionStatusCS")


class DrugExposure(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    drug_type: list[Concept] = Field(default_factory=list, alias="DrugType")
    drug_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="DrugTypeCS")
    drug_type_exclude: Optional[bool] = Field(default=None, alias="DrugTypeExclude")
    route_concept: list[Concept] = Field(default_factory=list, alias="RouteConcept")
    route_concept_cs: Optional[ConceptSetSelection] = Field(default=None, alias="RouteConceptCS")
    effective_drug_dose: Optional[NumericRange] = Field(default=None, alias="EffectiveDrugDose")
    dose_unit: list[Concept] = Field(default_factory=list, alias="DoseUnit")
    dose_unit_cs: Optional[ConceptSetSelection] = Field(default=None, alias="DoseUnitCS")
    quantity: Optional[NumericRange] = Field(default=None, alias="Quantity")
    days_supply: Optional[NumericRange] = Field(default=None, alias="DaysSupply")
    refills: Optional[NumericRange] = Field(default=None, alias="Refills")
    stop_reason: Optional[TextFilter] = Field(default=None, alias="StopReason")
    lot_number: Optional[TextFilter] = Field(default=None, alias="LotNumber")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    drug_source_concept: Optional[int] = Field(default=None, alias="DrugSourceConcept")

    def get_start_date_column(self) -> str:
        return "drug_exposure_start_date"

    def get_end_date_column(self) -> str:
        return "drug_exposure_end_date"


class VisitOccurrence(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    visit_type_exclude: Optional[bool] = Field(default=None, alias="VisitTypeExclude")
    visit_source_concept: Optional[int] = Field(default=None, alias="VisitSourceConcept")
    visit_length: Optional[NumericRange] = Field(default=None, alias="VisitLength")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    place_of_service: list[Concept] = Field(default_factory=list, alias="PlaceOfService")
    place_of_service_cs: Optional[ConceptSetSelection] = Field(default=None, alias="PlaceOfServiceCS")
    place_of_service_location: Optional[int] = Field(default=None, alias="PlaceOfServiceLocation")

class Measurement(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    measurement_type: list[Concept] = Field(default_factory=list, alias="MeasurementType")
    measurement_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="MeasurementTypeCS")
    measurement_type_exclude: Optional[bool] = Field(default=None, alias="MeasurementTypeExclude")
    operator_concept: list[Concept] = Field(
        default_factory=list,
        validation_alias=AliasChoices("OperatorConcept", "Operator"),
        serialization_alias="Operator",
    )
    operator_concept_cs: Optional[ConceptSetSelection] = Field(
        default=None,
        validation_alias=AliasChoices("OperatorConceptCS", "OperatorCS"),
        serialization_alias="OperatorCS",
    )
    value_as_number: Optional[NumericRange] = Field(default=None, alias="ValueAsNumber")
    value_as_concept: list[Concept] = Field(default_factory=list, alias="ValueAsConcept")
    value_as_concept_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ValueAsConceptCS")
    unit: list[Concept] = Field(default_factory=list, alias="Unit")
    unit_cs: Optional[ConceptSetSelection] = Field(default=None, alias="UnitCS")
    range_low: Optional[NumericRange] = Field(default=None, alias="RangeLow")
    range_high: Optional[NumericRange] = Field(default=None, alias="RangeHigh")
    range_low_ratio: Optional[NumericRange] = Field(default=None, alias="RangeLowRatio")
    range_high_ratio: Optional[NumericRange] = Field(default=None, alias="RangeHighRatio")
    abnormal: Optional[bool] = Field(default=None, alias="Abnormal")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    measurement_source_concept: Optional[int] = Field(default=None, alias="MeasurementSourceConcept")

    def get_start_date_column(self) -> str:
        return "measurement_date"

    def get_end_date_column(self) -> str:
        return "measurement_date"


class Observation(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    observation_type: list[Concept] = Field(default_factory=list, alias="ObservationType")
    observation_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ObservationTypeCS")
    observation_type_exclude: Optional[bool] = Field(default=None, alias="ObservationTypeExclude")
    qualifier: list[Concept] = Field(default_factory=list, alias="Qualifier")
    qualifier_cs: Optional[ConceptSetSelection] = Field(default=None, alias="QualifierCS")
    unit: list[Concept] = Field(default_factory=list, alias="Unit")
    unit_cs: Optional[ConceptSetSelection] = Field(default=None, alias="UnitCS")
    value_as_number: Optional[NumericRange] = Field(default=None, alias="ValueAsNumber")
    value_as_concept: list[Concept] = Field(default_factory=list, alias="ValueAsConcept")
    value_as_concept_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ValueAsConceptCS")
    value_as_string: Optional[TextFilter] = Field(default=None, alias="ValueAsString")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    observation_source_concept: Optional[int] = Field(default=None, alias="ObservationSourceConcept")

    def get_start_date_column(self) -> str:
        return "observation_date"

    def get_end_date_column(self) -> str:
        return "observation_date"


class DeviceExposure(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    device_type: list[Concept] = Field(default_factory=list, alias="DeviceType")
    device_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="DeviceTypeCS")
    device_type_exclude: Optional[bool] = Field(default=None, alias="DeviceTypeExclude")
    quantity: Optional[NumericRange] = Field(default=None, alias="Quantity")
    unique_device_id: Optional[TextFilter] = Field(default=None, alias="UniqueDeviceId")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    device_source_concept: Optional[int] = Field(default=None, alias="DeviceSourceConcept")

    def get_start_date_column(self) -> str:
        return "device_exposure_start_date"

    def get_end_date_column(self) -> str:
        return "device_exposure_end_date"


class ProcedureOccurrence(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    occurrence_end_date: Optional[DateRange] = Field(default=None, alias="OccurrenceEndDate")
    procedure_type: list[Concept] = Field(default_factory=list, alias="ProcedureType")
    procedure_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProcedureTypeCS")
    procedure_type_exclude: Optional[bool] = Field(default=None, alias="ProcedureTypeExclude")
    modifier: list[Concept] = Field(default_factory=list, alias="Modifier")
    modifier_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ModifierCS")
    quantity: Optional[NumericRange] = Field(default=None, alias="Quantity")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty: list[Concept] = Field(default_factory=list, alias="ProviderSpecialty")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    visit_type: list[Concept] = Field(default_factory=list, alias="VisitType")
    visit_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitTypeCS")
    procedure_source_concept: Optional[int] = Field(default=None, alias="ProcedureSourceConcept")

    def get_start_date_column(self) -> str:
        return "procedure_date"

    def get_end_date_column(self) -> str:
        return "procedure_date"


class DrugEra(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    era_start_date: Optional[DateRange] = Field(default=None, alias="EraStartDate")
    era_end_date: Optional[DateRange] = Field(default=None, alias="EraEndDate")
    occurrence_count: Optional[NumericRange] = Field(default=None, alias="OccurrenceCount")
    era_length: Optional[NumericRange] = Field(default=None, alias="EraLength")
    gap_days: Optional[NumericRange] = Field(default=None, alias="GapDays")
    age_at_start: Optional[NumericRange] = Field(default=None, alias="AgeAtStart")
    age_at_end: Optional[NumericRange] = Field(default=None, alias="AgeAtEnd")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")

    def get_primary_key_column(self) -> str:
        return "drug_era_id"

    def get_start_date_column(self) -> str:
        return "drug_era_start_date"

    def get_end_date_column(self) -> str:
        return "drug_era_end_date"


class DoseEra(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    era_start_date: Optional[DateRange] = Field(default=None, alias="EraStartDate")
    era_end_date: Optional[DateRange] = Field(default=None, alias="EraEndDate")
    unit: list[Concept] = Field(default_factory=list, alias="Unit")
    unit_cs: Optional[ConceptSetSelection] = Field(default=None, alias="UnitCS")
    dose_value: Optional[NumericRange] = Field(default=None, alias="DoseValue")
    era_length: Optional[NumericRange] = Field(default=None, alias="EraLength")
    age_at_start: Optional[NumericRange] = Field(default=None, alias="AgeAtStart")
    age_at_end: Optional[NumericRange] = Field(default=None, alias="AgeAtEnd")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")

    def get_primary_key_column(self) -> str:
        return "dose_era_id"

    def get_start_date_column(self) -> str:
        return "dose_era_start_date"

    def get_end_date_column(self) -> str:
        return "dose_era_end_date"


class ObservationPeriod(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    first: Optional[bool] = Field(default=None, alias="First")
    period_start_date: Optional[DateRange] = Field(default=None, alias="PeriodStartDate")
    period_end_date: Optional[DateRange] = Field(default=None, alias="PeriodEndDate")
    user_defined_period: Optional[UserDefinedPeriod] = Field(default=None, alias="UserDefinedPeriod")
    period_type: list[Concept] = Field(default_factory=list, alias="PeriodType")
    period_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="PeriodTypeCS")
    period_length: Optional[NumericRange] = Field(default=None, alias="PeriodLength")
    age_at_start: Optional[NumericRange] = Field(default=None, alias="AgeAtStart")
    age_at_end: Optional[NumericRange] = Field(default=None, alias="AgeAtEnd")

    def get_primary_key_column(self) -> str:
        return "observation_period_id"

    def get_start_date_column(self) -> str:
        return "observation_period_start_date"

    def get_end_date_column(self) -> str:
        return "observation_period_end_date"


class Specimen(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    specimen_type: list[Concept] = Field(default_factory=list, alias="SpecimenType")
    specimen_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="SpecimenTypeCS")
    specimen_type_exclude: bool = Field(default=False, alias="SpecimenTypeExclude")
    quantity: Optional[NumericRange] = Field(default=None, alias="Quantity")
    unit: list[Concept] = Field(default_factory=list, alias="Unit")
    unit_cs: Optional[ConceptSetSelection] = Field(default=None, alias="UnitCS")
    anatomic_site: list[Concept] = Field(default_factory=list, alias="AnatomicSite")
    anatomic_site_cs: Optional[ConceptSetSelection] = Field(default=None, alias="AnatomicSiteCS")
    disease_status: list[Concept] = Field(default_factory=list, alias="DiseaseStatus")
    disease_status_cs: Optional[ConceptSetSelection] = Field(default=None, alias="DiseaseStatusCS")
    source_id: Optional[TextFilter] = Field(default=None, alias="SourceId")
    specimen_source_concept: Optional[int] = Field(default=None, alias="SpecimenSourceConcept")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")

    def get_primary_key_column(self) -> str:
        return "specimen_id"

    def get_start_date_column(self) -> str:
        return "specimen_date"

    def get_end_date_column(self) -> str:
        return "specimen_date"


class Death(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    occurrence_start_date: Optional[DateRange] = Field(default=None, alias="OccurrenceStartDate")
    death_type: list[Concept] = Field(default_factory=list, alias="DeathType")
    death_type_exclude: Optional[bool] = Field(default=None, alias="DeathTypeExclude")
    death_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="DeathTypeCS")
    death_source_concept: Optional[int] = Field(default=None, alias="DeathSourceConcept")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")

    def get_concept_id_column(self) -> str:
        return "cause_concept_id"

    def get_start_date_column(self) -> str:
        return "death_date"

    def get_end_date_column(self) -> str:
        return "death_date"

    def get_primary_key_column(self) -> str:
        return "person_id"


class VisitDetail(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    codeset_id: Optional[int] = Field(default=None, alias="CodesetId")
    first: Optional[bool] = Field(default=None, alias="First")
    visit_detail_start_date: Optional[DateRange] = Field(default=None, alias="VisitDetailStartDate")
    visit_detail_end_date: Optional[DateRange] = Field(default=None, alias="VisitDetailEndDate")
    visit_detail_type_cs: Optional[ConceptSetSelection] = Field(default=None, alias="VisitDetailTypeCS")
    visit_detail_source_concept: Optional[int] = Field(default=None, alias="VisitDetailSourceConcept")
    visit_detail_length: Optional[NumericRange] = Field(default=None, alias="VisitDetailLength")
    age: Optional[NumericRange] = Field(default=None, alias="Age")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    provider_specialty_cs: Optional[ConceptSetSelection] = Field(default=None, alias="ProviderSpecialtyCS")
    place_of_service_cs: Optional[ConceptSetSelection] = Field(default=None, alias="PlaceOfServiceCS")
    place_of_service_location: Optional[int] = Field(default=None, alias="PlaceOfServiceLocation")

    def get_primary_key_column(self) -> str:
        return "visit_detail_id"

    def get_start_date_column(self) -> str:
        return "visit_detail_start_date"

    def get_end_date_column(self) -> str:
        return "visit_detail_end_date"


class PayerPlanPeriod(Criteria):
    model_config = ConfigDict(populate_by_name=True)

    first: Optional[bool] = Field(default=None, alias="First")
    period_start_date: Optional[DateRange] = Field(default=None, alias="PeriodStartDate")
    period_end_date: Optional[DateRange] = Field(default=None, alias="PeriodEndDate")
    user_defined_period: Optional[UserDefinedPeriod] = Field(default=None, alias="UserDefinedPeriod")
    period_length: Optional[NumericRange] = Field(default=None, alias="PeriodLength")
    age_at_start: Optional[NumericRange] = Field(default=None, alias="AgeAtStart")
    age_at_end: Optional[NumericRange] = Field(default=None, alias="AgeAtEnd")
    gender: list[Concept] = Field(default_factory=list, alias="Gender")
    gender_cs: Optional[ConceptSetSelection] = Field(default=None, alias="GenderCS")
    payer_concept: Optional[int] = Field(default=None, alias="PayerConcept")
    plan_concept: Optional[int] = Field(default=None, alias="PlanConcept")
    sponsor_concept: Optional[int] = Field(default=None, alias="SponsorConcept")
    stop_reason_concept: Optional[int] = Field(default=None, alias="StopReasonConcept")
    payer_source_concept: Optional[int] = Field(default=None, alias="PayerSourceConcept")
    plan_source_concept: Optional[int] = Field(default=None, alias="PlanSourceConcept")
    sponsor_source_concept: Optional[int] = Field(default=None, alias="SponsorSourceConcept")
    stop_reason_source_concept: Optional[int] = Field(default=None, alias="StopReasonSourceConcept")

    def get_primary_key_column(self) -> str:
        return "payer_plan_period_id"

    def get_start_date_column(self) -> str:
        return "payer_plan_period_start_date"

    def get_end_date_column(self) -> str:
        return "payer_plan_period_end_date"


CRITERIA_TYPE_MAP = {
    "ConditionOccurrence": ConditionOccurrence,
    "ConditionEra": ConditionEra,
    "VisitOccurrence": VisitOccurrence,
    "DrugExposure": DrugExposure,
    "DrugEra": DrugEra,
    "DoseEra": DoseEra,
    "ObservationPeriod": ObservationPeriod,
    "Measurement": Measurement,
    "Observation": Observation,
    "Specimen": Specimen,
    "DeviceExposure": DeviceExposure,
    "ProcedureOccurrence": ProcedureOccurrence,
    "Death": Death,
    "VisitDetail": VisitDetail,
    "PayerPlanPeriod": PayerPlanPeriod,
}


def parse_single_criteria(criteria_dict):
    if isinstance(criteria_dict, Criteria):
        return criteria_dict
    if isinstance(criteria_dict, dict):
        for criteria_type, criteria_data in criteria_dict.items():
            model_cls = CRITERIA_TYPE_MAP.get(criteria_type)
            if not model_cls:
                raise ValueError(f"Unsupported criteria type: {criteria_type}")
            return model_cls(**criteria_data)
    return None


def parse_criteria_list(criteria_list_data: list[Criteria]):
    criteria_instances = []
    for criteria_dict in criteria_list_data:
        parsed = parse_single_criteria(criteria_dict)
        if parsed:
            criteria_instances.append(parsed)
    return criteria_instances


def serialize_criteria(criteria: Criteria) -> dict[str, Any]:
    payload = criteria.model_dump(
        by_alias=True,
        exclude_none=True,
        exclude_defaults=False,
        exclude_unset=True,
    )
    return {criteria.__class__.__name__: payload}
