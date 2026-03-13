from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import requests

app = FastAPI()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY")

ALLOWED_FUNCTIONS = {
    "sender_sufficiency_reserve_source",
    "sender_sufficiency_heartbeat",
    "sender_sufficiency_close_source_no_rows",
    "sender_sufficiency_commit_row_and_close_source",
}

class RpcRequest(BaseModel):
    function_name: str
    params: dict = {}

@app.get("/")
def root():
    return {"status": "gateway running"}

@app.post("/rpc")
def call_rpc(payload: RpcRequest):
    if payload.function_name not in ALLOWED_FUNCTIONS:
        raise HTTPException(status_code=403, detail="Function not allowed")

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/{payload.function_name}",
        headers={
            "apikey": SUPABASE_SECRET_KEY,
            "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
            "Content-Type": "application/json",
        },
        json=payload.params,
        timeout=30,
    )

    try:
        body = r.json()
    except Exception:
        body = {"raw_text": r.text}

    if not r.ok:
        raise HTTPException(status_code=r.status_code, detail=body)

    return body
