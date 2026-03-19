"""Configuration endpoints for the browser extension."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["config"])


class UserPreferences(BaseModel):
    """User preference configuration for issue scoring."""

    issues: list[str] = [
        "environment",
        "civil_liberties",
        "labor",
        "lgbtq_rights",
        "gun_policy",
    ]
    trusted_scorecards: list[str] = [
        "ACLU",
        "League of Conservation Voters",
        "AFL-CIO",
        "Human Rights Campaign",
        "EFF",
    ]
    issue_weights: dict[str, float] = {}
    show_low_confidence: bool = False
    badge_style: str = "compact"  # "compact" or "detailed"


# In-memory storage for MVP. Production would use a proper store.
_user_prefs: dict[str, UserPreferences] = {}


@router.get("/config/{user_id}")
async def get_preferences(user_id: str):
    """Get user preferences. Returns defaults if not set."""
    prefs = _user_prefs.get(user_id, UserPreferences())
    return prefs.model_dump()


@router.put("/config/{user_id}")
async def update_preferences(user_id: str, prefs: UserPreferences):
    """Update user preferences."""
    _user_prefs[user_id] = prefs
    return {"status": "updated", "preferences": prefs.model_dump()}


@router.get("/config/issues/available")
async def list_available_issues():
    """List all available issues and scorecards."""
    return {
        "issues": [
            {"name": "civil_liberties", "label": "Civil Liberties"},
            {"name": "environment", "label": "Environment"},
            {"name": "digital_rights", "label": "Digital Rights"},
            {"name": "labor", "label": "Labor"},
            {"name": "lgbtq_rights", "label": "LGBTQ+ Rights"},
            {"name": "immigration", "label": "Immigration"},
            {"name": "gun_policy", "label": "Gun Policy"},
            {"name": "healthcare", "label": "Healthcare"},
            {"name": "education", "label": "Education"},
            {"name": "fiscal_policy", "label": "Fiscal Policy"},
        ],
        "scorecards": [
            {"org": "ACLU", "issue": "civil_liberties", "perspective": "progressive"},
            {"org": "League of Conservation Voters", "issue": "environment", "perspective": "progressive"},
            {"org": "Human Rights Campaign", "issue": "lgbtq_rights", "perspective": "progressive"},
            {"org": "AFL-CIO", "issue": "labor", "perspective": "progressive"},
            {"org": "EFF", "issue": "digital_rights", "perspective": "progressive"},
        ],
    }
