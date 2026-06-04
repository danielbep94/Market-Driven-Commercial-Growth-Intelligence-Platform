"""
Unit tests for rationale template rendering.
Validates that every rendered rationale contains ≥ 2 actual metric values.
A rationale that only echoes the badge text (e.g., "INCREASE + WASTE_REVIEW") is a build failure.
"""
import pytest
import re


def render_rationale(template: str, variables: dict) -> str:
    """Fill template variable slots with actual metric values."""
    result = template
    for key, value in variables.items():
        if isinstance(value, float):
            result = result.replace(f"{{{key}}}", f"{value:.1f}")
            result = result.replace(f"{{{key}:.0%}}", f"{value:.0%}")
            result = result.replace(f"{{{key}:.2f}}", f"{value:.2f}")
        else:
            result = result.replace(f"{{{key}}}", str(value))
    return result


def count_metric_values_in_rationale(rationale: str) -> int:
    """Count number of numeric metric values in a rationale string."""
    # Match numbers (including decimals and percentages)
    numbers = re.findall(r'\d+\.?\d*[x%]?', rationale)
    return len(numbers)


TEMPLATES = {
    "INCREASE": (
        "{brand_name} shows a {gqs_score_4w_avg}-point quality score and {roi}x ROI. "
        "Share is gaining {share_change_pp}pp vs. prior period.",
        {"brand_name": "Alpha", "gqs_score_4w_avg": 82.0, "roi": 2.4, "share_change_pp": 1.2}
    ),
    "INCREASE + WASTE_REVIEW": (
        "{brand_name} qualifies for increased investment: {gqs_score_4w_avg}-point quality score, "
        "{roi}x ROI. Waste Risk Score is {waste_risk_score:.0%}, above HIGH threshold ({waste_risk_threshold:.0%}).",
        {"brand_name": "Beta", "gqs_score_4w_avg": 85.0, "roi": 2.4,
         "waste_risk_score": 0.72, "waste_risk_threshold": 0.70, "share_change_pp": 1.2}
    ),
    "MAINTAIN": (
        "{brand_name} shows solid health: {gqs_score_4w_avg}-point quality score, {roi}x ROI.",
        {"brand_name": "Gamma", "gqs_score_4w_avg": 68.0, "roi": 1.3}
    ),
    "REDUCE": (
        "{brand_name} shows declining health: {gqs_score_4w_avg}-point score, {share_change_pp}pp share loss, {roi}x ROI.",
        {"brand_name": "Delta", "gqs_score_4w_avg": 32.0, "share_change_pp": -2.1, "roi": 0.6}
    ),
    "WATCH": (
        "Signals for {brand_name} are insufficient: {weeks_of_history} weeks available (minimum {min_weeks_required}).",
        {"brand_name": "Epsilon", "weeks_of_history": 2, "min_weeks_required": 4}
    ),
}


class TestRationaleTemplates:
    @pytest.mark.parametrize("compound_action,template_vars", TEMPLATES.items())
    def test_rendered_rationale_contains_at_least_2_metrics(self, compound_action, template_vars):
        """Every rendered rationale must contain ≥ 2 numeric metric values."""
        template, variables = template_vars
        rendered = render_rationale(template, variables)
        metric_count = count_metric_values_in_rationale(rendered)
        assert metric_count >= 2, (
            f"Rationale for '{compound_action}' contains only {metric_count} metric value(s). "
            f"Rendered: '{rendered}'"
        )

    @pytest.mark.parametrize("compound_action,template_vars", TEMPLATES.items())
    def test_rendered_rationale_is_not_just_badge_text(self, compound_action, template_vars):
        """Rationale must not just repeat the badge text."""
        template, variables = template_vars
        rendered = render_rationale(template, variables)
        # The rationale must be longer than the compound action label itself
        assert len(rendered) > len(compound_action) + 50, (
            f"Rationale for '{compound_action}' appears to be just the badge text: '{rendered}'"
        )

    @pytest.mark.parametrize("compound_action,template_vars", TEMPLATES.items())
    def test_all_template_variables_filled(self, compound_action, template_vars):
        """No unfilled {variable_name} slots should remain in the rendered rationale."""
        template, variables = template_vars
        rendered = render_rationale(template, variables)
        unfilled = re.findall(r'\{[a-z_:.%0-9]+\}', rendered)
        assert len(unfilled) == 0, (
            f"Template for '{compound_action}' has unfilled slots: {unfilled}. "
            f"Rendered: '{rendered}'"
        )

    def test_increase_waste_review_mentions_both_signals(self):
        """INCREASE + WASTE_REVIEW must reference both the growth signal and the waste risk signal."""
        template, variables = TEMPLATES["INCREASE + WASTE_REVIEW"]
        rendered = render_rationale(template, variables)
        # Must mention ROI/GQS (investment signal) AND waste risk
        assert any(word in rendered.lower() for word in ["roi", "quality", "score"])
        assert any(word in rendered.lower() for word in ["waste", "risk"])

    def test_reduce_mentions_all_three_decline_signals(self):
        """REDUCE rationale must reference score, share loss, and ROI."""
        template, variables = TEMPLATES["REDUCE"]
        rendered = render_rationale(template, variables)
        assert "32" in rendered or "score" in rendered.lower()
        assert "2.1" in rendered or "share" in rendered.lower()
        assert "0.6" in rendered or "roi" in rendered.lower()


class TestCompetitiveRiskScoreV1:
    """Tests for the v1 rule-based composite score."""

    def compute_score(self, share_loss=0.0, price_gap=0.0, num_dist=0.0, wgt_dist=0.0, share_yoy=0.0):
        """Compute composite score from normalized 0–100 components."""
        return (
            0.30 * share_loss +
            0.25 * price_gap +
            0.20 * num_dist +
            0.15 * wgt_dist +
            0.10 * share_yoy
        )

    def test_maximum_risk_all_components_100(self):
        score = self.compute_score(100, 100, 100, 100, 100)
        assert score == pytest.approx(100.0)

    def test_zero_risk_all_components_zero(self):
        score = self.compute_score(0, 0, 0, 0, 0)
        assert score == pytest.approx(0.0)

    def test_weights_sum_to_one(self):
        weights = [0.30, 0.25, 0.20, 0.15, 0.10]
        assert sum(weights) == pytest.approx(1.0)

    def test_share_loss_dominates(self):
        """Share loss (30% weight) should dominate the score more than any other single signal."""
        score_share_only = self.compute_score(share_loss=100)
        score_price_only = self.compute_score(price_gap=100)
        assert score_share_only > score_price_only

    def test_partial_null_redistributes_weights(self):
        """If one component is NULL, its weight redistributes to available components."""
        # Normal score with all components
        full_score = self.compute_score(80, 60, 70, 50, 40)

        # Simulate NULL for price_gap (25% weight redistributed)
        available_weight = 1.0 - 0.25
        partial_score = (
            (0.30 / available_weight) * 80 +
            (0.20 / available_weight) * 70 +
            (0.15 / available_weight) * 50 +
            (0.10 / available_weight) * 40
        )
        # Partial score should differ from full score but still be in 0–100 range
        assert 0 <= partial_score <= 100
        assert partial_score != pytest.approx(full_score)
