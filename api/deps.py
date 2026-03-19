"""FastAPI dependency providers."""

from __future__ import annotations

from fastapi import Request
from neo4j import AsyncDriver


async def get_driver(request: Request) -> AsyncDriver:
    """Yield the Neo4j async driver from app state."""
    return request.app.state.neo4j_driver
