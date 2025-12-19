# project1
🤖 Natural Language SQL Agent

Talk to your database in plain English

A from-scratch SQL agent that converts natural language into SQL queries for effortless data analysis.
What It Does
Ask questions like a human:

"Show me users who signed up last month"
"What's the average order value by region?"
"Top 10 products by revenue"

Get SQL queries instantly. No syntax memorization required.
Why It's Cool

Built from scratch — every line of code is custom
Natural conversations — describe what you want, get working SQL
Faster analysis — focus on insights, not syntax

Perfect For

Quick data exploration
Learning SQL through examples
Prototyping queries
Skipping the "how do I write this?" phase


#link
https://sqlagent101.streamlit.app/

#project2

🚨 Crime Violence Prediction System (End-to-End ML Project)
📌 Project Overview

This project is an end-to-end machine learning classification system that predicts whether a reported crime incident is Violent or Non-Violent based on temporal, geographic, and categorical attributes.

The project replaces the instructor’s regression dataset with a custom classification dataset, follows the provided repository structure, runs 16 tracked experiments, and deploys a FastAPI + Streamlit application using Docker.

🎯 Problem Statement

Task: Binary classification

Target: violent

1 → Violent incident

0 → Non-violent incident

Why classification?
The output is a discrete class label, not a continuous value.

📊 Dataset

Source: Crime_Incidents_20251014.csv

Storage: Normalized SQLite database (crime.db)

Target creation: Incident categories are mapped to a binary violent label during preprocessing.

🗄️ Database Design

A normalized relational schema (3NF) is used to store the data.

Key Tables

incidents – core incident attributes

labels – violent vs non-violent target

Dimension tables:

day_of_week_dim

police_district_dim

neighborhood_dim

council_district_dim

The ML pipeline queries the database using SQL joins to generate Pandas DataFrames for training and experiments.

🔧 Feature Engineering & Preprocessing
Features Used

hour_of_day

latitude, longitude

zip_code

council_district

day_of_week

police_district

neighborhood

Preprocessing Pipeline

Numerical features

Median imputation

Standard scaling

Categorical features

Most-frequent imputation

One-hot encoding (handle_unknown="ignore")

Implemented using ColumnTransformer and saved inside the model pipeline.

🧪 Experiments & Modeling
Models Used (4)

Logistic Regression

Ridge Classifier

Gradient Boosting Classifier

Random Forest / ExtraTrees Classifier

Experiment Conditions (4 per model)

PCA + no hyperparameter tuning

PCA + Optuna hyperparameter tuning

No PCA + no hyperparameter tuning

No PCA + Optuna hyperparameter tuning

✅ Total Experiments

4 models × 4 conditions = 16 experiments

Evaluation Metric

F1-score (chosen due to class imbalance)

All experiments save:

model.pkl

metrics.json

params.json

A leaderboard CSV summarizes results.

📈 Experiment Tracking (DagsHub)

MLflow + DagsHub used for experiment tracking

Each run logs:

Model type

PCA usage

Tuning status

F1-score

All 16 experiments are visible as separate runs with artifacts.

🚀 Deployment Architecture

The system is deployed as two separate services:

Backend — FastAPI

Endpoint: /predict

Endpoint: /health

Loads trained Pipeline for inference

Ensures preprocessing consistency between training and inference

Frontend — Streamlit

User-friendly UI for inputting incident attributes

Communicates with FastAPI for predictions

Displays predicted class and probability (if available)

🐳 Docker & Deployment

Both services are containerized using Docker

Locally orchestrated using docker-compose

Deployed on Render as two Docker web services:

FastAPI service

Streamlit service

Why Docker?

Reproducible environments

Consistent deployment across local and cloud setups

🧠 Key Learnings

Designing normalized databases for ML pipelines

Handling imbalanced classification problems

Systematic experiment design (16 experiments)

MLflow/DagsHub experiment tracking

Production-style deployment with API + UI separation

🔮 Future Improvements

Add geospatial feature engineering (crime hotspots)

Add model monitoring and drift detection

Automate retraining pipeline

Improve calibration and threshold optimization

Add authentication and rate-limiting to API


#link
https://projects-3-5tki.onrender.com/

