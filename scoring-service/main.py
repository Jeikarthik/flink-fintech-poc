from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore
import os
import uvicorn
from typing import Dict, Any

app = FastAPI(
    title="Pre-Delinquency Intervention Scoring Service",
    description="Serves risk scores using real-time features from Feast"
)

# Initialize Feast Store
REPO_PATH = os.getenv("FEAST_REPO_PATH", "/opt/feast/feature_repo")
try:
    store = FeatureStore(repo_path=REPO_PATH)
except Exception as e:
    print(f"Warning: Could not load Feast store: {e}. Will attempt to load on first request.")
    store = None

class ScoreResponse(BaseModel):
    customer_id: str
    risk_probability: float
    risk_tier: str
    credit_score: int
    features_used: Dict[str, Any]

@app.get("/health")
def health_check():
    return {"status": "healthy", "feast_loaded": store is not None}

@app.get("/features/{customer_id}")
def get_raw_features(customer_id: str):
    """Debug endpoint to view raw features retreived from Feast."""
    global store
    if not store:
        store = FeatureStore(repo_path=REPO_PATH)
        
    try:
        feature_vector = store.get_online_features(
            feature_refs=[
                "transaction_features:discretionary_spend_7d",
                "transaction_features:atm_withdrawals_count_7d",
                "transaction_features:lending_app_txn_count_7d",
                "transaction_features:weighted_lending_risk_7d",
                "transaction_features:failed_autodebits_count_7d",
                "batch_features:salary_delay_days",
                "batch_features:utility_payment_avg_delay_days"
            ],
            entity_rows=[{"customer_id": customer_id}]
        ).to_dict()
        
        # Clean up Feast output format for readability
        result = {}
        for key, value in feature_vector.items():
            if isinstance(value, list) and len(value) > 0:
                result[key] = value[0]
                
        return {"customer_id": customer_id, "features": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/score/{customer_id}", response_model=ScoreResponse)
def compute_risk_score(customer_id: str):
    """
    Main scoring endpoint. 
    1. Retrieves features from Feast.
    2. Runs model inference (Mocked here).
    3. Maps probability to Risk Tier and Credit Score.
    """
    try:
        features = get_raw_features(customer_id)["features"]
        
        # --- Mock Model Inference ---
        # In reality, you'd pass the feature array to XGBoost/MLflow here.
        # We simulate risk calculation based on extracted features:
        
        base_risk = 0.10
        weighted_lending = float(features.get("weighted_lending_risk_7d") or 0.0)
        atm_count = float(features.get("atm_withdrawals_count_7d") or 0)
        failed_debits = float(features.get("failed_autodebits_count_7d") or 0)
        salary_delay = float(features.get("salary_delay_days") or 0)
        
        # Simple programmatic risk score computation to demonstrate feature importance
        risk_probability = base_risk + (weighted_lending * 0.15) + (atm_count * 0.02) + (failed_debits * 0.20) + (salary_delay * 0.05)
        
        # Clamp to [0, 1]
        risk_probability = max(0.0, min(1.0, risk_probability))
        
        # Map to Credit Score (300 to 850)
        credit_score = int(850 - (risk_probability * 550))
        
        # Map to Tier
        if risk_probability >= 0.70:
            tier = "critical"
        elif risk_probability >= 0.50:
            tier = "watch"
        else:
            tier = "stable"
            
        return ScoreResponse(
            customer_id=customer_id,
            risk_probability=risk_probability,
            risk_tier=tier,
            credit_score=credit_score,
            features_used=features
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
