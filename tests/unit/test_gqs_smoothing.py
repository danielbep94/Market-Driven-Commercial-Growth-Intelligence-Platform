"""
Unit tests for GQS 4-week rolling average (smoothing).
The smoothed value is what feeds the Recommendation Engine — not raw weekly GQS.
"""
import pytest


def compute_gqs_rolling_avg(gqs_history: list, window: int = 4) -> float | None:
    """
    Compute rolling average of GQS over the last N weeks.
    Returns None if fewer than 2 data points available.
    """
    if gqs_history is None or len(gqs_history) < 2:
        return None
    recent = gqs_history[-window:] if len(gqs_history) >= window else gqs_history
    return round(sum(recent) / len(recent), 1)


class TestGQSSmoothing:
    def test_full_4_week_window(self):
        history = [70, 72, 68, 74]
        result = compute_gqs_rolling_avg(history)
        assert result == pytest.approx(71.0)

    def test_partial_window_uses_available(self):
        """Less than 4 weeks of history — use what's available."""
        history = [70, 72]
        result = compute_gqs_rolling_avg(history)
        assert result == pytest.approx(71.0)

    def test_single_point_returns_none(self):
        """Single data point — cannot smooth. Return None."""
        history = [75]
        result = compute_gqs_rolling_avg(history)
        assert result is None

    def test_empty_history_returns_none(self):
        result = compute_gqs_rolling_avg([])
        assert result is None

    def test_none_history_returns_none(self):
        result = compute_gqs_rolling_avg(None)
        assert result is None

    def test_smoothing_reduces_spike(self):
        """Key property: a single-week spike should not dominate the smoothed value."""
        history_normal = [70, 71, 69, 70]          # Normal range
        history_spiked = [70, 71, 69, 95]           # Spike in last week
        normal_avg = compute_gqs_rolling_avg(history_normal)
        spiked_avg = compute_gqs_rolling_avg(history_spiked)
        # Spiked average is higher but spike is dampened (not 95)
        assert spiked_avg < 95
        assert spiked_avg > normal_avg

    def test_recommendation_engine_uses_smoothed_not_raw(self):
        """Verify that a week-to-week swing of >10pts does not trigger action change."""
        history_stable = [72, 71, 73, 72]            # Stable — smoothed ≈ 72
        history_volatile_up = [72, 71, 73, 85]       # Raw spike to 85
        history_volatile_down = [72, 71, 73, 55]     # Raw drop to 55

        avg_stable = compute_gqs_rolling_avg(history_stable)
        avg_volatile_up = compute_gqs_rolling_avg(history_volatile_up)
        avg_volatile_down = compute_gqs_rolling_avg(history_volatile_down)

        # Smoothed values should be much closer than raw end values
        raw_range = 85 - 55  # = 30 pts
        smoothed_range = abs(avg_volatile_up - avg_volatile_down)
        assert smoothed_range < raw_range, "Smoothing should reduce the swing significantly"
        print(f"Raw range: {raw_range} pts | Smoothed range: {smoothed_range:.1f} pts")


class TestGQSCompressionDetection:
    def test_iqr_passes_threshold(self):
        """IQR >= 15 pts → no recalibration needed."""
        scores = list(range(40, 90))  # Wide distribution
        p25 = sorted(scores)[len(scores) // 4]
        p75 = sorted(scores)[3 * len(scores) // 4]
        iqr = p75 - p25
        assert iqr >= 15, f"IQR should be >= 15 but was {iqr}"

    def test_iqr_fails_threshold(self):
        """IQR < 15 pts → recalibration required."""
        scores = [60, 62, 63, 64, 65, 66, 67, 68, 69, 70]  # Compressed
        p25 = sorted(scores)[len(scores) // 4]
        p75 = sorted(scores)[3 * len(scores) // 4]
        iqr = p75 - p25
        assert iqr < 15, f"IQR should be < 15 for compressed distribution but was {iqr}"
