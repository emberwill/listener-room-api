from fastapi import FastAPI
import os
import requests

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]

@app.get("/")
def root():
    return {"status": "gateway running"}