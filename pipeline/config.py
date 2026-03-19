"""Configuration for the Political Purchaser data pipeline."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "politicalpurchaser"

    # FEC
    fec_api_key: str = "DEMO_KEY"
    fec_bulk_data_dir: Path = Path("data/fec")

    # OpenCorporates
    opencorporates_api_token: str = ""

    # Wikidata (no auth needed)
    wikidata_sparql_endpoint: str = "https://query.wikidata.org/sparql"

    # ProPublica Congress API
    propublica_api_key: str = ""

    # Pipeline
    data_dir: Path = Path("data")
    scores_output: Path = Path("extension/scores.json")
    max_ownership_depth: int = 10
    fec_cycles: list[int] = [2022, 2024]
    scorecard_year: int = 2026

    model_config = {"env_prefix": "PP_", "env_file": ".env"}


settings = Settings()


def get_neo4j_driver():
    """Create and return a Neo4j driver instance."""
    from neo4j import GraphDatabase

    return GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


def ensure_data_dirs():
    """Create data directories if they don't exist."""
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.fec_bulk_data_dir.mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "wikidata").mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "scorecards").mkdir(parents=True, exist_ok=True)
