"""
Unit tests for KPI edge cases.
Tests all scenarios documented in the implementation plan §4.1.
"""

import pytest


def compute_waste_rate(waste_units, sell_in_units):
    """Python equivalent of fn_waste_rate_v1 for testing."""
    if sell_in_units is None or sell_in_units == 0:
        return {"waste_rate": None, "is_anomaly": False,
                "anomaly_reason": "SELL_IN_ZERO" if sell_in_units == 0 else "SELL_IN_NULL"}
    if waste_units is None:
        return {"waste_rate": None, "is_anomaly": False, "anomaly_reason": "WASTE_NULL"}
    rate = waste_units / sell_in_units
    is_anomaly = rate > 1.0
    return {
        "waste_rate": rate,
        "is_anomaly": is_anomaly,
        "anomaly_reason": "WASTE_EXCEEDS_SELL_IN" if is_anomaly else None
    }


def compute_sell_out_ratio(sell_out_units, sell_in_units):
    """Python equivalent of fn_sell_out_sell_in_ratio_v1 for testing."""
    if sell_in_units is None or sell_in_units == 0:
        return {"ratio": None, "is_valid": False, "flag": "SELL_IN_ZERO"}
    if sell_out_units is None:
        return {"ratio": None, "is_valid": False, "flag": "SELL_OUT_NULL"}
    ratio = sell_out_units / sell_in_units
    flag = None
    if ratio > 1.5:
        flag = "RATIO_EXCEEDS_1_5"
    elif ratio > 1.0:
        flag = "RATIO_ABOVE_1_PRIOR_INVENTORY"
    return {"ratio": ratio, "is_valid": True, "flag": flag}


class TestWasteRate:
    def test_normal_case(self):
        result = compute_waste_rate(100, 1000)
        assert result["waste_rate"] == 0.1
        assert not result["is_anomaly"]

    def test_sell_in_zero_returns_null(self):
        """Edge case: sell_in = 0 must return NULL, not infinity."""
        result = compute_waste_rate(0, 0)
        assert result["waste_rate"] is None
        assert result["anomaly_reason"] == "SELL_IN_ZERO"

    def test_sell_in_null_returns_null(self):
        result = compute_waste_rate(100, None)
        assert result["waste_rate"] is None

    def test_waste_null_returns_null(self):
        result = compute_waste_rate(None, 1000)
        assert result["waste_rate"] is None

    def test_waste_exceeds_sell_in_is_flagged(self):
        """Edge case: waste > sell_in (prior-period returns). Should flag, not error."""
        result = compute_waste_rate(1200, 1000)
        assert result["waste_rate"] == pytest.approx(1.2)
        assert result["is_anomaly"] is True
        assert result["anomaly_reason"] == "WASTE_EXCEEDS_SELL_IN"

    def test_zero_waste_is_valid(self):
        result = compute_waste_rate(0, 1000)
        assert result["waste_rate"] == 0.0
        assert not result["is_anomaly"]


class TestSellOutRatio:
    def test_normal_case(self):
        result = compute_sell_out_ratio(800, 1000)
        assert result["ratio"] == pytest.approx(0.8)
        assert result["is_valid"]
        assert result["flag"] is None

    def test_sell_in_zero_returns_null(self):
        result = compute_sell_out_ratio(500, 0)
        assert result["ratio"] is None
        assert not result["is_valid"]

    def test_ratio_above_1_is_valid_and_flagged(self):
        """Edge case: sell-out > sell-in (selling from prior inventory). Valid business scenario."""
        result = compute_sell_out_ratio(1200, 1000)
        assert result["ratio"] == pytest.approx(1.2)
        assert result["is_valid"]
        assert result["flag"] == "RATIO_ABOVE_1_PRIOR_INVENTORY"

    def test_ratio_above_1_5_flagged_for_review(self):
        result = compute_sell_out_ratio(2000, 1000)
        assert result["ratio"] == pytest.approx(2.0)
        assert result["flag"] == "RATIO_EXCEEDS_1_5"


class TestGrowthQualityScore:
    """Test GQS edge cases — partial scores and confidence flags."""

    def compute_gqs(self, so_growth=75, share=70, waste=80, fa=85, roi=70):
        """Simplified GQS for unit testing."""
        weights = {"so": 0.25, "share": 0.25, "waste": 0.20, "fa": 0.15, "roi": 0.15}
        components = {"so": so_growth, "share": share, "waste": waste, "fa": fa, "roi": roi}
        weight_map = {"so": weights["so"], "share": weights["share"],
                      "waste": weights["waste"], "fa": weights["fa"], "roi": weights["roi"]}

        null_count = sum(1 for v in components.values() if v is None)
        if null_count > 2:
            return {"score": None, "confidence": "NULL"}

        available = {k: v for k, v in components.items() if v is not None}
        total_avail_weight = sum(weight_map[k] for k in available)
        score = sum(available[k] * (weight_map[k] / total_avail_weight) for k in available)

        confidence = "HIGH" if null_count == 0 else ("MEDIUM" if null_count == 1 else "LOW")
        return {"score": round(score, 1), "confidence": confidence}

    def test_full_score_high_confidence(self):
        result = self.compute_gqs()
        assert result["confidence"] == "HIGH"
        assert result["score"] is not None

    def test_one_null_component_medium_confidence(self):
        result = self.compute_gqs(share=None)
        assert result["confidence"] == "MEDIUM"
        assert result["score"] is not None  # Redistributed weights

    def test_two_null_components_low_confidence(self):
        result = self.compute_gqs(share=None, fa=None)
        assert result["confidence"] == "LOW"
        assert result["score"] is not None

    def test_three_null_components_returns_null_score(self):
        """Edge case: too many missing components — suppress score."""
        result = self.compute_gqs(share=None, fa=None, roi=None)
        assert result["score"] is None
        assert result["confidence"] == "NULL"
