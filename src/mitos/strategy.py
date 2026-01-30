from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class DateField(str, Enum):
    START_DATE = "StartDate"
    END_DATE = "EndDate"


class DateOffsetStrategy(BaseModel):
    model_config = ConfigDict(populate_by_name=False, use_enum_values=True)

    date_field: DateField = Field(default=DateField.START_DATE, alias="DateField")
    offset: int = Field(default=0, alias="Offset")


class CustomEraStrategy(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    drug_codeset_id: Optional[int] = Field(default=None, alias="DrugCodesetId")
    gap_days: int = Field(default=0, alias="GapDays")
    offset: int = Field(default=0, alias="Offset")
    days_supply_override: Optional[int] = Field(
        default=None, alias="DaysSupplyOverride"
    )


class EndStrategy(BaseModel):
    model_config = ConfigDict(populate_by_name=False)

    date_offset: Optional[DateOffsetStrategy] = Field(default=None, alias="DateOffset")
    custom_era: Optional[CustomEraStrategy] = Field(default=None, alias="CustomEra")

    def is_empty(self) -> bool:
        return not any([self.date_offset, self.custom_era])
