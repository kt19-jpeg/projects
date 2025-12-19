from __future__ import annotations

import os
from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, Field

APP_TITLE = os.getenv("APP_TITLE", "Crime Violence Predictor")
MODEL_PATH = Path(os.getenv("MODEL_PATH", "models/best_model.pkl"))

FEATURE_COLS = [
    "hour_of_day",
    "latitude",
    "longitude",
    "zip_code",
    "council_district",
    "day_of_week",
    "police_district",
    "neighborhood",
]

class CrimeFeatures(BaseModel):
    hour_of_day: int = Field(..., ge=0, le=23)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

    # keep as string to match OneHotEncoder categories consistently
    zip_code: str
    council_district: str
    day_of_week: str
    police_district: str
    neighborhood: str

class PredictionOut(BaseModel):
    violent_pred: int
    violent_label: str
    proba_violent: float | None = None

app = FastAPI(title=APP_TITLE)

_model = None

def get_model():
    global _model
    if _model is None:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Model file not found at {MODEL_PATH.resolve()}")
        _model = joblib.load(MODEL_PATH)
    return _model

@app.get("/health")
def health():
    return {"status": "ok", "model_path": str(MODEL_PATH)}

@app.post("/predict", response_model=PredictionOut)
def predict(features: CrimeFeatures):
    model = get_model()
    row = features.model_dump()
    X = pd.DataFrame([row], columns=FEATURE_COLS)

    pred = int(model.predict(X)[0])

    proba = None
    if hasattr(model, "predict_proba"):
        try:
            proba = float(model.predict_proba(X)[0][1])
        except Exception:
            proba = None

    label = "Violent" if pred == 1 else "Non-Violent"
    return PredictionOut(violent_pred=pred, violent_label=label, proba_violent=proba)
