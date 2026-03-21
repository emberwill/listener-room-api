from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
import os
import requests
import uuid
import logging
from typing import Any

app = FastAPI()

# Environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY")
GATEWAY_API_KEY = os.environ.get("GATEWAY_API_KEY")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("listener-room-gateway")

# Allowed RPCs across active listener rooms
ALLOWED_FUNCTIONS = {
    "sender_sufficiency_reserve_source",
    "sender_sufficiency_heartbeat",
    "sender_sufficiency_close_source_no_rows",
    "sender_sufficiency_commit_row_and_close_source",
    "non_reply_message_reserve_source",
    "non_reply_message_heartbeat",
    "non_reply_message_close_source_no_rows",
    "non_reply_message_commit_row_and_close_source",
    "re_entry_outcomes_reserve_source",
    "re_entry_outcomes_heartbeat",
    "re_entry_outcomes_close_source_no_rows",
    "re_entry_outcomes_commit_row_and_close_source",
    "non_reply_message_get_dedupe_digest",
    "sender_sufficiency_get_dedupe_digest",
    "re_entry_outcomes_get_dedupe_digest",
    "ensure_listener_room_pass",
    "non_reply_message_enqueue_source",
    "sender_sufficiency_enqueue_source",
    "re_entry_outcomes_enqueue_source",
}

# Only commit RPCs accept p_gateway_request_id
REQUEST_ID_FUNCTIONS = {
    "sender_sufficiency_commit_row_and_close_source",
    "non_reply_message_commit_row_and_close_source",
    "re_entry_outcomes_commit_row_and_close_source",
}

FUNCTION_PARAM_ALLOWLIST = {
    "ensure_listener_room_pass": {
        "p_pass_id", "p_room_name", "p_pass_label", "p_collector_type", "p_worker_id", "p_notes"
    },

    "sender_sufficiency_enqueue_source": {
        "p_pass_id", "p_worker_id", "p_source_type", "p_source_title", "p_source_author",
        "p_source_url", "p_source_date", "p_relevance_note", "p_search_query", "p_priority", "p_metadata_json"
    },
    "sender_sufficiency_reserve_source": {"p_worker_id", "p_pass_id"},
    "sender_sufficiency_heartbeat": {"p_source_id", "p_worker_id", "p_pass_id"},
    "sender_sufficiency_close_source_no_rows": {"p_source_id", "p_worker_id", "p_pass_id", "p_notes"},
    "sender_sufficiency_commit_row_and_close_source": {
        "p_source_id", "p_pass_id", "p_gateway_request_id", "p_worker_id", "p_created_by",
        "p_unit_index", "p_row", "p_notes"
    },
    "sender_sufficiency_get_dedupe_digest": {
        "p_limit", "p_source_url", "p_channel", "p_relationship", "p_message_archetype"
    },

    "re_entry_outcomes_enqueue_source": {
        "p_pass_id", "p_worker_id", "p_source_type", "p_source_title", "p_source_author",
        "p_source_url", "p_source_date", "p_relevance_note", "p_search_query", "p_priority", "p_metadata_json"
    },
    "re_entry_outcomes_reserve_source": {"p_worker_id", "p_pass_id"},
    "re_entry_outcomes_heartbeat": {"p_source_id", "p_worker_id", "p_pass_id"},
    "re_entry_outcomes_close_source_no_rows": {"p_source_id", "p_worker_id", "p_pass_id", "p_notes"},
    "re_entry_outcomes_commit_row_and_close_source": {
        "p_source_id", "p_pass_id", "p_gateway_request_id", "p_worker_id", "p_created_by",
        "p_unit_index", "p_row", "p_notes"
    },
    "re_entry_outcomes_get_dedupe_digest": {
        "p_limit", "p_source_url", "p_channel", "p_relationship", "p_message_archetype"
    },

    "non_reply_message_enqueue_source": {
        "p_pass_id", "p_worker_id", "p_source_type", "p_source_title", "p_source_author",
        "p_source_url", "p_source_date", "p_relevance_note", "p_search_query", "p_priority", "p_metadata_json"
    },
    "non_reply_message_reserve_source": {"p_worker_id", "p_pass_id"},
    "non_reply_message_heartbeat": {"p_source_id", "p_worker_id", "p_pass_id"},
    "non_reply_message_close_source_no_rows": {"p_source_id", "p_worker_id", "p_pass_id", "p_notes"},
    "non_reply_message_commit_row_and_close_source": {
        "p_source_id", "p_pass_id", "p_gateway_request_id", "p_worker_id", "p_created_by",
        "p_unit_index", "p_row", "p_notes"
    },
    "non_reply_message_get_dedupe_digest": {
        "p_limit", "p_source_url", "p_channel", "p_relationship", "p_message_archetype"
    },
}

NULL_IF_EMPTY_BY_FUNCTION = {
    "sender_sufficiency_get_dedupe_digest": {"p_source_url", "p_channel", "p_relationship", "p_message_archetype"},
    "re_entry_outcomes_get_dedupe_digest": {"p_source_url", "p_channel", "p_relationship", "p_message_archetype"},
    "non_reply_message_get_dedupe_digest": {"p_source_url", "p_channel", "p_relationship", "p_message_archetype"},
}


def _sanitize_params(function_name: str, params: dict[str, Any]) -> dict[str, Any]:
    allowed = FUNCTION_PARAM_ALLOWLIST.get(function_name)
    out = dict(params or {})
    if allowed is not None:
        out = {k: v for k, v in out.items() if k in allowed}

    for k in NULL_IF_EMPTY_BY_FUNCTION.get(function_name, set()):
        if k in out and isinstance(out[k], str) and out[k].strip() == "":
            out[k] = None

    return out


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
            detail={"error": "Supabase not configured", "request_id": request_id},
        )

    if not GATEWAY_API_KEY:
        logger.error("config_error request_id=%s missing_gateway_key=true", request_id)
        raise HTTPException(
            status_code=500,
            detail={"error": "Gateway API key not configured", "request_id": request_id},
        )

    if x_api_key != GATEWAY_API_KEY:
        logger.warning("auth_failed request_id=%s function_name=%s", request_id, payload.function_name)
        raise HTTPException(
            status_code=401,
            detail={"error": "Unauthorized", "request_id": request_id},
        )

    if payload.function_name not in ALLOWED_FUNCTIONS:
        logger.warning("function_blocked request_id=%s function_name=%s", request_id, payload.function_name)
        raise HTTPException(
            status_code=403,
            detail={"error": "Function not allowed", "request_id": request_id},
        )

    supabase_params = _sanitize_params(payload.function_name, payload.params)

    if payload.function_name in REQUEST_ID_FUNCTIONS:
        if "p_gateway_request_id" in FUNCTION_PARAM_ALLOWLIST.get(payload.function_name, set()):
            supabase_params["p_gateway_request_id"] = request_id

    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/{payload.function_name}",
            headers={
                "apikey": SUPABASE_SECRET_KEY,
                "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
                "Content-Type": "application/json",
            },
            json=supabase_params,
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
            detail={"error": "Error calling Supabase", "message": str(e), "request_id": request_id},
        )

    try:
        body = r.json()
    except Exception:
        body = {"raw_text": r.text}

    if not r.ok:
        logger.warning(
            "supabase_error request_id=%s function_name=%s status_code=%s body=%s",
            request_id, payload.function_name, r.status_code, body
        )
        raise HTTPException(
            status_code=r.status_code,
            detail={"error": "Supabase returned an error", "supabase_body": body, "request_id": request_id},
        )

    logger.info(
        "request_success request_id=%s function_name=%s status_code=%s",
        request_id, payload.function_name, r.status_code
    )

    return {"request_id": request_id, "data": body}
