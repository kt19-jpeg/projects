import os
import json
from pathlib import Path

import requests
import streamlit as st

st.set_page_config(page_title="Crime Violence Predictor", layout="centered")

API_URL = os.getenv("API_URL", "http://api:8000")  # docker-compose service name
OPTIONS_PATH = Path(os.getenv("OPTIONS_PATH", "streamlit/options.json"))

st.title("Violent vs Non-Violent Incident Prediction")
st.caption("Streamlit frontend calls FastAPI backend for inference.")

with st.sidebar:
    st.header("Backend")
    st.write("API_URL:", API_URL)
    if st.button("Check /health"):
        try:
            r = requests.get(f"{API_URL}/health", timeout=10)
            st.json(r.json())
        except Exception as e:
            st.error(f"Health check failed: {e}")

options = {}
if OPTIONS_PATH.exists():
    options = json.loads(OPTIONS_PATH.read_text(encoding="utf-8"))
else:
    st.warning(f"options.json not found at {OPTIONS_PATH}. You can still type values manually.")

def opt_list(key):
    vals = options.get(key, [])
    return [""] + vals

col1, col2 = st.columns(2)
with col1:
    hour_of_day = st.number_input("Hour of Day (0-23)", min_value=0, max_value=23, value=12, step=1)
    latitude = st.number_input("Latitude", value=42.8864, format="%.6f")
    zip_code = st.selectbox("ZIP Code", opt_list("zip_code")) if options else st.text_input("ZIP Code", "")
    day_of_week = st.selectbox("Day of Week", opt_list("day_of_week")) if options else st.text_input("Day of Week", "")
with col2:
    longitude = st.number_input("Longitude", value=-78.8784, format="%.6f")
    council_district = st.selectbox("Council District", opt_list("council_district")) if options else st.text_input("Council District", "")
    police_district = st.selectbox("Police District", opt_list("police_district")) if options else st.text_input("Police District", "")
    neighborhood = st.selectbox("Neighborhood", opt_list("neighborhood")) if options else st.text_input("Neighborhood", "")

if st.button("Predict"):
    payload = {
        "hour_of_day": int(hour_of_day),
        "latitude": float(latitude),
        "longitude": float(longitude),
        "zip_code": str(zip_code),
        "council_district": str(council_district),
        "day_of_week": str(day_of_week),
        "police_district": str(police_district),
        "neighborhood": str(neighborhood),
    }
    missing = [k for k, v in payload.items() if (v is None) or (isinstance(v, str) and v.strip() == "")]
    if missing:
        st.error(f"Please fill: {', '.join(missing)}")
    else:
        try:
            r = requests.post(f"{API_URL}/predict", json=payload, timeout=30)
            if r.status_code != 200:
                st.error(f"API error {r.status_code}: {r.text}")
            else:
                out = r.json()
                st.subheader("Result")
                st.write("Prediction:", out.get("violent_label"))
                if out.get("proba_violent") is not None:
                    st.write("Probability (violent):", round(out["proba_violent"], 4))
                st.json(out)
        except Exception as e:
            st.error(f"Request failed: {e}")

st.divider()
st.markdown("### Example payload")
st.code(json.dumps({
    "hour_of_day": 12,
    "latitude": 42.8864,
    "longitude": -78.8784,
    "zip_code": "14201",
    "council_district": "1",
    "day_of_week": "Monday",
    "police_district": "A",
    "neighborhood": "Downtown"
}, indent=2), language="json")
