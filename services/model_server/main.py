# /services/model_server/main.py
import joblib
from fastapi import FastAPI
from pydantic import BaseModel

# A Pydantic model for input validation
class EditFeatures(BaseModel):
    is_anonymous: bool
    char_delta: int
    revision_comment_length: int
    # ... add all other features the model expects

# Load the pre-trained model
model = joblib.load("model.pkl")

app = FastAPI()

@app.post("/predict")
async def predict_vandalism(features: EditFeatures):
    """
    Predicts the probability of an edit being vandalism.
    """
    # Convert Pydantic model to a format the model can use (e.g., numpy array)
    feature_vector = [list(features.dict().values())]
    
    # Get probability score from the model
    probability = model.predict_proba(feature_vector)[0][1] # Prob of class '1' (vandalism)
    
    return {"vandalism_probability": probability}
