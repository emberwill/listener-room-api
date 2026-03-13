from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
import os
import requests
import uuid
import logging

app = FastAPI()

# Environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY")
GATEWAY_API_KEY = os.environ.get("GATEWAY_API_KEY")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("listener-room-gateway")

# Allowed RPC functions
ALLOWED_FUNCTIONS = {
    # sender sufficiency
    "sender_sufficiency_reserve_source",
    "sender_sufficiency_heartbeat",
    "sender_sufficiency_close_source_no_rows",
    "sender_sufficiency_commit_row_and_close_source",

    # re-entry
    "reentry_reserve_source",
    "reentry_heartbeat",
    "reentry_close_source_no_rows",
    "reentry_commit_row_and_close_source",
}

class RpcRequest(BaseModel):
    function_name: str
    params: dict = Field(default_factory=dict)

@app.get("/")
def root():
    return {"status": "gateway running"}

@app.post("/rpc")
def call_rpc(payload: RpcRequest, x_api_key: str = Header(default=None)):
    request_id = str(uuid.uuid4())

    logger.info(
        "request_start request_id=%s function_name=%s",
        request_id,
        payload.function_name,
    )

    if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
        logger.error("config_error request_id=%s missing_supabase_config=true", request_id)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Supabase not configured",
                "request_id": request_id,
            },
        )

    if not GATEWAY_API_KEY:
        logger.error("config_error request_id=%s missing_gateway_key=true", request_id)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Gateway API key not configured",
                "request_id": request_id,
            },
        )

    if x_api_key != GATEWAY_API_KEY:
        logger.warning(
            "auth_failed request_id=%s function_name=%s",
            request_id,
            payload.function_name,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Unauthorized",
                "request_id": request_id,
            },
        )

    if payload.function_name not in ALLOWED_FUNCTIONS:
        logger.warning(
            "function_blocked request_id=%s function_name=%s",
            request_id,
            payload.function_name,
        )
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Function not allowed",
                "request_id": request_id,
            },
        )

    try:
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
    except requests.RequestException as e:
        logger.exception(
            "supabase_request_failed request_id=%s function_name=%s error=%s",
            request_id,
            payload.function_name,
            str(e),
        )
        raise HTTPException(
            status_code=502,
            detail={
                "error": "Error calling Supabase",
                "message": str(e),
                "request_id": request_id,
            },
        )

    try:
        body = r.json()
    except Exception:
        body = {"raw_text": r.text}

    if not r.ok:
        logger.warning(
            "supabase_error request_id=%s function_name=%s status_code=%s body=%s",
            request_id,
            payload.function_name,
            r.status_code,
            body,
        )
        raise HTTPException(
            status_code=r.status_code,
            detail={
                "error": "Supabase returned an error",
                "supabase_body": body,
                "request_id": request_id,
            },
        )

    logger.info(
        "request_success request_id=%s function_name=%s status_code=%s",
        request_id,
        payload.function_name,
        r.status_code,
    )

    return {
        "request_id": request_id,
        "data": body,
    }
