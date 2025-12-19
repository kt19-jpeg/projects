# Crime Violence Predictor — Deployment

## Prereqs
- Put your trained model at: `models/best_model.pkl` (copy from the best experiment folder)

## Generate Streamlit dropdown options (optional)
Run: `04_generate_streamlit_options_CRIME.ipynb`
This writes: `streamlit/options.json`

## Run locally with Docker
```bash
docker compose up --build
```

- FastAPI docs: http://localhost:8000/docs
- Streamlit UI: http://localhost:8501
