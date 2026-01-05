# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    # Scraping Configuration
    PMDA_BASE_URL: str = "https://www.pmda.go.jp"
    SCRAPING_REQUEST_TIMEOUT: int = 30  # seconds
    SCRAPING_RATE_LIMIT_DELAY: float = 1.0  # seconds (Mandatory)
    USER_AGENT: str = "CoReasonETL/1.0"

    # Database Configuration
    DUCKDB_PATH: str | None = None  # If None, use in-memory or default dlt behavior

    # Source Specific URLs (defaults provided as per current code)
    URL_APPROVALS: str = "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0001.html"
    URL_JADER: str = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0008.html"
    URL_JAN_INN: str = "https://www.nihs.go.jp/drug/jan_data_e.html"
    URL_PMDA_SEARCH: str = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
