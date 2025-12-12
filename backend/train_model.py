"""Train and persist the NILM RandomForest model.

This script mirrors the experimentation done inside `NILM_RF.ipynb`
but makes it reproducible from the command line so we can generate
an artifact that is later used for online inference.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

RANDOM_STATE = 45
DEFAULT_DROP_COLUMNS = [
    "timestamp",
    "reliable",
    "device_id",
    "load_type",
    "probe_id",
    "frequency",
    "energy",
    "temperature",
    "bulb",
    "fan",
    "iron",
]
TARGET_COLUMN = "labels"


@dataclass(frozen=True)
class TrainingArtifacts:
    model: RandomForestClassifier
    feature_columns: list[str]
    class_labels: list


def _sanitize_features(df: pd.DataFrame, drop_columns: Sequence[str]) -> pd.DataFrame:
    """Drop unused columns and fill missing values."""

    existing_drop_cols = [col for col in drop_columns if col in df.columns]
    feature_df = df.drop(columns=existing_drop_cols, errors="ignore")

    # Ensure the label column is not part of the features
    if TARGET_COLUMN in feature_df.columns:
        feature_df = feature_df.drop(columns=[TARGET_COLUMN])

    # Fill NaNs with column medians so inference can do the same
    return feature_df.fillna(feature_df.median(numeric_only=True))


def train_model(dataset_path: Path, artifact_path: Path, drop_columns: Sequence[str]) -> TrainingArtifacts:
    raw_df = pd.read_csv(dataset_path)
    if TARGET_COLUMN not in raw_df.columns:
        raise ValueError(f"Dataset missing required target column '{TARGET_COLUMN}'")

    feature_df = _sanitize_features(raw_df, drop_columns)
    label_series = raw_df[TARGET_COLUMN]

    X_train, X_tmp, y_train, y_tmp = train_test_split(
        feature_df,
        label_series,
        test_size=0.4,
        random_state=RANDOM_STATE,
        stratify=label_series if label_series.nunique() > 1 else None,
    )
    X_cv, X_test, y_cv, y_test = train_test_split(
        X_tmp,
        y_tmp,
        test_size=0.5,
        random_state=RANDOM_STATE,
        stratify=y_tmp if y_tmp.nunique() > 1 else None,
    )

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=5,
        min_samples_split=200,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    def _report(split: str, X, y):
        preds = model.predict(X)
        print(f"\n{split} accuracy: {accuracy_score(y, preds):.4f}")
        print(classification_report(y, preds, zero_division=0))

    _report("Train", X_train, y_train)
    _report("Validation", X_cv, y_cv)
    _report("Test", X_test, y_test)

    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "model": model,
        "feature_columns": feature_df.columns.tolist(),
        "class_labels": model.classes_.tolist(),
        "random_state": RANDOM_STATE,
    }
    joblib.dump(artifact, artifact_path)
    print(f"Saved artifact to {artifact_path.resolve()}")

    return TrainingArtifacts(model=model, feature_columns=feature_df.columns.tolist(), class_labels=model.classes_.tolist())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train NILM RandomForest classifier")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("full dataset.csv"),
        help="Path to the CSV dataset",
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path("backend/artifacts/nilm_rf.joblib"),
        help="Where to persist the trained model",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train_model(
        dataset_path=args.data,
        artifact_path=args.artifact,
        drop_columns=DEFAULT_DROP_COLUMNS,
    )
