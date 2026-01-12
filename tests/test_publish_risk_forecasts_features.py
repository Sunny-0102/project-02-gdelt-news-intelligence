import pandas as pd

from src.publish_risk_forecasts import make_features


def test_make_features_creates_expected_columns():
    # This uses a tiny fake time series so the test is fast and deterministic.
    df = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=20, freq="D"),
            "risk_raw": list(range(20)),
        }
    )

    out = make_features(df)

    # These columns must exist for the model to train consistently.
    for col in ["lag_1", "lag_7", "lag_14", "roll_mean_7", "roll_std_7", "target_next_day"]:
        assert col in out.columns


def test_make_features_shifts_targets_correctly():
    # This verifies the “next day” target is aligned the way forecasting expects.
    df = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=20, freq="D"),
            "risk_raw": list(range(20)),
        }
    )

    out = make_features(df).sort_values("date")

    # On day 1, target should be day 2's risk_raw.
    assert out["target_next_day"].iloc[0] == 1

    # lag_14 at index 14 should point back to index 0.
    assert out["lag_14"].iloc[14] == 0
