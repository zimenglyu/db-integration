from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Settings:
    pg_host: str
    pg_port: str
    pg_database: str
    pg_user: str
    pg_password: str
    pg_table: str
    dataset_dir: str
    first_dataset_file: str
    second_dataset_file: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _must_get(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing environment variable: {name}")
    return value


def load_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env.local")
    load_dotenv(PROJECT_ROOT / ".env")
    return Settings(
        pg_host=_must_get("PG_HOST", "localhost"),
        pg_port=_must_get("PG_PORT", "5432"),
        pg_database=_must_get("PG_DATABASE"),
        pg_user=_must_get("PG_USER"),
        pg_password=os.getenv("PG_PASSWORD", ""),
        pg_table=_must_get("PG_TABLE", "public.historical_purchases"),
        dataset_dir=_must_get("DATASET_DIR", "dataset"),
        first_dataset_file=_must_get(
            "FIRST_DATASET_FILE", "historical_purchases_1.csv"
        ),
        second_dataset_file=_must_get(
            "SECOND_DATASET_FILE", "historical_purchases_2.csv"
        ),
    )


def dataset_path(settings: Settings, file_name: str) -> str:
    return str((PROJECT_ROOT / settings.dataset_dir / file_name).resolve())
