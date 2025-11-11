from __future__ import annotations

import uuid
import logging
from typing import Any, Dict

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

def _payload(type_: str, message: str, code: str, trace_id: str) -> Dict[str, Any]:
    return{
        "error": {
            "type": type_,
            "message": message,
            "code": code,
            "trace_id": trace_id,
        }
    }

async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    trace_id = str(uuid.uuid4())
    status = exc.status_code or 500

    if status == 404:
        type_, code = "not_found", "NOT_FOUND"
    elif status == 400:
        type_, code = "bad_request", "BAD_REQUEST"
    elif status == 401:
        type_, code = "unauthorized", "UNAUTHORIZED"
    elif status == 403:
        type_, code = "forbidden", "FORBIDDEN"
    else: 
        type_, code = "http_error", f"HTTP_{status}"

    message = exc.detail if getattr(exc, "detail", None) else "HTTP error"
    return JSONResponse(status_code=status, content=_payload(type_, message, code, trace_id))


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    trace_id = str(uuid.uuid4())
    return JSONResponse(
        status_code=422,
        content=_payload("validation_error", "Validation failed", "VALIDATION_ERROR", trace_id),
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    trace_id = str(uuid.uuid4())
    logging.exception("Unhandled error (trace_id=%s)", trace_id, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content=_payload("internal_error", "Internal server error", "INTERNAL_ERROR", trace_id),
    )


def register_error_handlers(app) -> None:
    """Call from main.py once to install global handlers."""
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)