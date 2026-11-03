"""HTTP input contracts. Domain checks remain in the shared tools."""
from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class InputModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


Code = Annotated[str, Field(strict=True, min_length=1, max_length=256)]


class PANInput(InputModel):
    pan: Code


class GSTINInput(InputModel):
    gstin: Code


class UPIInput(InputModel):
    vpa: Code


class INRInput(InputModel):
    amount: Decimal = Field(allow_inf_nan=False, ge=-10**15, le=10**15)
    paise: bool = Field(default=False, strict=True)
