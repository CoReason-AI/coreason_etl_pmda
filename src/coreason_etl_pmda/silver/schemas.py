# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from pydantic import BaseModel, ConfigDict


class SilverApprovalSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    approval_id: str | None = None
    approval_date: str | None = None
    brand_name_jp: str | None = None
    generic_name_jp: str | None = None
    applicant_name_jp: str | None = None
    indication: str | None = None
    coreason_id: str | None = None
    application_type: str | None = None
    generic_name_en: str | None = None
    review_report_url: str | None = None
    _translation_status: str | None = None


class SilverJaderDemoSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    id: str | None = None
    sex: str | None = None
    age: str | None = None
    # reporting_year is typically an integer
    reporting_year: int | None = None


class SilverJaderDrugSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    id: str | None = None
    drug_name: str | None = None
    characterization: str | None = None


class SilverJaderReacSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    id: str | None = None
    reaction: str | None = None
