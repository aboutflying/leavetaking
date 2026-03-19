"""FastAPI application for Political Purchaser."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncGraphDatabase

from api.routes import config, graph_trail, scores
from pipeline.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage Neo4j driver lifecycle."""
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    app.state.neo4j_driver = driver
    try:
        yield
    finally:
        await driver.close()


app = FastAPI(
    title="Political Purchaser API",
    description="Expose the political spending trail behind products",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["chrome-extension://*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(scores.router, prefix="/api/v1")
app.include_router(graph_trail.router, prefix="/api/v1")
app.include_router(config.router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok"}
