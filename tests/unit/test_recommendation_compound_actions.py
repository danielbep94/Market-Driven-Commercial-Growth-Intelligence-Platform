"""
Unit tests for Recommendation Engine compound action logic (v2.2).
Key design: compound outputs — never collapse conflicted signals to a single label.
"""
import pytest


def compute_recommendation(
    gqs_4w_avg: float | None,
    waste_risk_category: str | None,
    waste_adjusted_roi: float | None,
    share_change_pp: float | None,
    consecutive_forecast_misses: int = 0,
    weeks_of_history: int = 12,
) -> dict:
    """
    Simplified Recommendation Engine for unit testing.
    Mirrors the logic in models/recommendation_engine/rules_config.yaml
    """
    # Minimum data check
    if weeks_of_history < 4 or gqs_4w_avg is None or waste_risk_category is None or waste_adjusted_roi is None:
        return {
            "primary_action": "WATCH",
            "secondary_action": None,
            "action_compound": "WATCH",
            "conflict_flag": False,
            "rationale": "INSUFFICIENT_DATA"
        }

    # Primary action
    if gqs_4w_avg >= 75 and waste_adjusted_roi >= 1.5 and (share_change_pp or 0) >= 0:
        primary = "INCREASE"
    elif gqs_4w_avg >= 60 and waste_adjusted_roi >= 1.0:
        primary = "MAINTAIN"
    elif waste_adjusted_roi < 1.0 and gqs_4w_avg >= 50:
        primary = "OPTIMIZE"
    elif gqs_4w_avg < 40 and waste_adjusted_roi < 0.8 and (share_change_pp or 0) < -1.0:
        primary = "REDUCE"
    else:
        primary = "WATCH"

    # Secondary action (independent of primary)
    secondary = None
    conflict = False
    if waste_risk_category == "HIGH":
        secondary = "WASTE_REVIEW"
        conflict = primary in ("INCREASE", "MAINTAIN")
    if consecutive_forecast_misses >= 3:
        secondary = (secondary + "+FORECAST_REVIEW") if secondary else "FORECAST_REVIEW"

    compound = f"{primary} + {secondary}" if secondary else primary

    return {
        "primary_action": primary,
        "secondary_action": secondary,
        "action_compound": compound,
        "conflict_flag": conflict,
        "rationale": "computed"
    }


class TestCompoundActions:
    def test_high_gqs_high_waste_gives_increase_plus_waste_review(self):
        """The key scenario: high growth + high waste → INCREASE + WASTE_REVIEW, not REDUCE."""
        result = compute_recommendation(
            gqs_4w_avg=85,
            waste_risk_category="HIGH",
            waste_adjusted_roi=2.4,
            share_change_pp=1.2,
        )
        assert result["primary_action"] == "INCREASE"
        assert result["secondary_action"] == "WASTE_REVIEW"
        assert result["action_compound"] == "INCREASE + WASTE_REVIEW"
        assert result["conflict_flag"] is True  # Signals are in conflict

    def test_solid_brand_with_forecast_miss(self):
        """Good brand + forecast problem → MAINTAIN + FORECAST_REVIEW."""
        result = compute_recommendation(
            gqs_4w_avg=68,
            waste_risk_category="LOW",
            waste_adjusted_roi=1.3,
            share_change_pp=0.2,
            consecutive_forecast_misses=4,
        )
        assert result["primary_action"] == "MAINTAIN"
        assert "FORECAST_REVIEW" in result["secondary_action"]
        assert result["conflict_flag"] is False  # No commercial conflict

    def test_declining_brand_high_waste_both_agree(self):
        """Declining brand + high waste → REDUCE + WASTE_REVIEW (signals aligned)."""
        result = compute_recommendation(
            gqs_4w_avg=32,
            waste_risk_category="HIGH",
            waste_adjusted_roi=0.6,
            share_change_pp=-2.1,
        )
        assert result["primary_action"] == "REDUCE"
        assert result["secondary_action"] == "WASTE_REVIEW"
        assert result["conflict_flag"] is False  # Signals agree — not conflicted

    def test_clean_increase_no_secondary(self):
        """Strong brand, no issues → INCREASE with no secondary action."""
        result = compute_recommendation(
            gqs_4w_avg=82,
            waste_risk_category="LOW",
            waste_adjusted_roi=2.1,
            share_change_pp=1.5,
        )
        assert result["primary_action"] == "INCREASE"
        assert result["secondary_action"] is None
        assert result["action_compound"] == "INCREASE"
        assert result["conflict_flag"] is False

    def test_insufficient_data_returns_watch(self):
        """Less than 4 weeks of history → WATCH regardless of other signals."""
        result = compute_recommendation(
            gqs_4w_avg=82,
            waste_risk_category="LOW",
            waste_adjusted_roi=2.1,
            share_change_pp=1.5,
            weeks_of_history=2,
        )
        assert result["primary_action"] == "WATCH"
        assert result["rationale"] == "INSUFFICIENT_DATA"

    def test_null_gqs_returns_watch(self):
        """NULL GQS (< 4 weeks history) → WATCH."""
        result = compute_recommendation(
            gqs_4w_avg=None,
            waste_risk_category="LOW",
            waste_adjusted_roi=1.5,
            share_change_pp=0.5,
        )
        assert result["primary_action"] == "WATCH"
