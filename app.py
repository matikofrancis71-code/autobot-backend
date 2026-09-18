# ============================================================
# FIXED RISK BOOSTER - DERIV OPTIONS BACKEND
# ============================================================
# FastAPI + Deriv OAuth2 PKCE + Deriv Options WebSocket
#
# IMPORTANT:
# - Demo and Real accounts are completely separated.
# - Real trading requires REAL_TRADING_ENABLED=true.
# - Automatic trades always perform fresh market analysis.
# - No martingale.
# - No forced trade when data quality is insufficient.
# - Manual Demo trades can be executed independently.
# - Stop Trading stops FUTURE automatic trades.
# - Already purchased Deriv contracts continue until completion.
# ============================================================

import asyncio
import base64
import hashlib
import json
import math
import os
import secrets
import statistics
import time
import urllib.parse

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import websockets

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field


# ============================================================
# CONFIGURATION
# ============================================================

APP_VERSION = "8.0.0"

FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "",
).strip()

DERIV_CLIENT_ID = os.getenv(
    "DERIV_CLIENT_ID",
    "",
).strip()

DERIV_REDIRECT_URI = os.getenv(
    "DERIV_REDIRECT_URI",
    "",
).strip()

DERIV_OAUTH_SCOPE = os.getenv(
    "DERIV_OAUTH_SCOPE",
    "trade",
).strip()

REAL_TRADING_ENABLED = (
    os.getenv(
        "REAL_TRADING_ENABLED",
        "false",
    ).strip().lower()
    in {
        "1",
        "true",
        "yes",
        "on",
    }
)

DERIV_AUTH_BASE = (
    "https://auth.deriv.com"
)

DERIV_REST_BASE = (
    "https://api.derivws.com"
)

DERIV_PUBLIC_WS = (
    "wss://api.derivws.com"
    "/trading/v1/options/ws/public"
)

HISTORY_TICKS = int(
    os.getenv(
        "HISTORY_TICKS",
        "2000",
    )
)

PREDICTION_HORIZON_TICKS = int(
    os.getenv(
        "PREDICTION_HORIZON_TICKS",
        "5",
    )
)

MIN_BACKTEST_SAMPLES = int(
    os.getenv(
        "MIN_BACKTEST_SAMPLES",
        "150",
    )
)

SELECTION_ACCURACY_WEIGHT = float(
    os.getenv(
        "SELECTION_ACCURACY_WEIGHT",
        "0.5",
    )
)

SELECTION_CONFIDENCE_WEIGHT = float(
    os.getenv(
        "SELECTION_CONFIDENCE_WEIGHT",
        "0.5",
    )
)

MAX_SYMBOLS_TO_ANALYZE = int(
    os.getenv(
        "MAX_SYMBOLS_TO_ANALYZE",
        "10",
    )
)

PREDICTION_CACHE_SECONDS = float(
    os.getenv(
        "PREDICTION_CACHE_SECONDS",
        "3",
    )
)

MARKET_DISCOVERY_CACHE_SECONDS = float(
    os.getenv(
        "MARKET_DISCOVERY_CACHE_SECONDS",
        "60",
    )
)

DEFAULT_DURATION = int(
    os.getenv(
        "DEFAULT_DURATION",
        "5",
    )
)

DEFAULT_DURATION_UNIT = os.getenv(
    "DEFAULT_DURATION_UNIT",
    "t",
).strip()

DEFAULT_BARRIER = int(
    os.getenv(
        "DEFAULT_BARRIER",
        "5",
    )
)

MAX_STAKE = float(
    os.getenv(
        "MAX_STAKE",
        "1000",
    )
)

MAX_HISTORY_RECORDS = int(
    os.getenv(
        "MAX_HISTORY_RECORDS",
        "100",
    )
)

MAX_CONSECUTIVE_LOSSES = int(
    os.getenv(
        "MAX_CONSECUTIVE_LOSSES",
        "3",
    )
)

WS_TIMEOUT_SECONDS = float(
    os.getenv(
        "WS_TIMEOUT_SECONDS",
        "20",
    )
)

SESSION_TTL_SECONDS = int(
    os.getenv(
        "SESSION_TTL_SECONDS",
        str(12 * 60 * 60),
    )
)

MAX_SESSIONS = int(
    os.getenv(
        "MAX_SESSIONS",
        "1000",
    )
)

OAUTH_STATE_TTL_SECONDS = int(
    os.getenv(
        "OAUTH_STATE_TTL_SECONDS",
        "600",
    )
)

MAX_OAUTH_STATES = int(
    os.getenv(
        "MAX_OAUTH_STATES",
        "500",
    )
)


# ============================================================
# FASTAPI
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="Fixed Risk Booster",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# GLOBAL STATE
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}

OAUTH_STATES: Dict[str, Dict[str, Any]] = {}

PREDICTION_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": None,
}

MARKET_DISCOVERY_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": None,
}

PREDICTION_LOCK = asyncio.Lock()
MARKET_DISCOVERY_LOCK = asyncio.Lock()


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def clean_symbol(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip().upper()


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:

    try:
        if value is None:
            return default

        result = float(value)

        if not math.isfinite(result):
            return default

        return result

    except Exception:
        return default


def safe_int(
    value: Any,
    default: Optional[int] = None,
) -> Optional[int]:

    try:
        if value is None:
            return default

        return int(float(value))

    except Exception:
        return default


def clamp(
    value: float,
    minimum: float,
    maximum: float,
) -> float:

    return max(
        minimum,
        min(
            maximum,
            value,
        ),
    )


def readable_api_error(
    value: Any,
    fallback: str = "Unknown error.",
) -> str:

    if value is None:
        return fallback

    if isinstance(value, str):
        return value

    if isinstance(value, dict):

        # Deriv sometimes returns:
        # {"errors":[{"message":"..."}]}
        errors = value.get("errors")

        if isinstance(errors, list) and errors:

            messages = []

            for item in errors:

                if isinstance(item, dict):

                    message = (
                        item.get("message")
                        or item.get("detail")
                        or item.get("description")
                        or item.get("reason")
                    )

                    code = item.get("code")

                    if message:

                        if code:
                            messages.append(
                                f"{code}: {message}"
                            )
                        else:
                            messages.append(
                                str(message)
                            )

            if messages:
                return "; ".join(messages)

        message = (
            value.get("message")
            or value.get("detail")
            or value.get("reason")
            or value.get("error_description")
            or value.get("description")
        )

        code = value.get("code")

        if message:

            if code:
                return f"{code}: {message}"

            return str(message)

        nested = value.get("error")

        if nested is not None:

            return readable_api_error(
                nested,
                fallback,
            )

        try:
            return json.dumps(
                value,
                ensure_ascii=False,
            )

        except Exception:
            return fallback

    if isinstance(
        value,
        (list, tuple),
    ):

        try:
            return json.dumps(
                value,
                ensure_ascii=False,
            )

        except Exception:
            return fallback

    try:
        return str(value)

    except Exception:
        return fallback


def normalize_account_type(
    value: Any,
) -> str:

    value = str(
        value or "demo"
    ).strip().lower()

    if value in {
        "real",
        "live",
        "real_account",
    }:

        return "real"

    return "demo"


def empty_stats() -> Dict[str, Any]:

    return {
        "profit": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
    }


# ============================================================
# SESSION MANAGEMENT
# ============================================================

def cleanup_memory() -> None:

    now = time.time()

    expired_sessions = []

    for session_id, session in list(
        USER_SESSIONS.items()
    ):

        last_seen = float(
            session.get(
                "last_seen",
                now,
            )
        )

        if (
            now - last_seen
            > SESSION_TTL_SECONDS
        ):

            if not session.get(
                "active_trade"
            ):

                expired_sessions.append(
                    session_id
                )

    for session_id in expired_sessions:

        USER_SESSIONS.pop(
            session_id,
            None,
        )

    expired_states = []

    for state, data in list(
        OAUTH_STATES.items()
    ):

        created_at = float(
            data.get(
                "created_at",
                0,
            )
        )

        if (
            now - created_at
            > OAUTH_STATE_TTL_SECONDS
        ):

            expired_states.append(
                state
            )

    for state in expired_states:

        OAUTH_STATES.pop(
            state,
            None,
        )

    if (
        len(USER_SESSIONS)
        > MAX_SESSIONS
    ):

        removable = sorted(
            USER_SESSIONS.items(),
            key=lambda item: float(
                item[1].get(
                    "last_seen",
                    0,
                )
            ),
        )

        for session_id, session in removable:

            if (
                len(USER_SESSIONS)
                <= MAX_SESSIONS
            ):
                break

            if not session.get(
                "active_trade"
            ):

                USER_SESSIONS.pop(
                    session_id,
                    None,
                )

    if (
        len(OAUTH_STATES)
        > MAX_OAUTH_STATES
    ):

        removable = sorted(
            OAUTH_STATES.items(),
            key=lambda item: float(
                item[1].get(
                    "created_at",
                    0,
                )
            ),
        )

        for state, _ in removable:

            if (
                len(OAUTH_STATES)
                <= MAX_OAUTH_STATES
            ):
                break

            OAUTH_STATES.pop(
                state,
                None,
            )


def new_session(
    session_id: Optional[str] = None,
) -> Dict[str, Any]:

    cleanup_memory()

    if not session_id:

        session_id = secrets.token_urlsafe(
            24
        )

    session = {
        "session_id": session_id,

        "created_at": time.time(),

        "last_seen": time.time(),

        "connected": False,

        "connection_status": "disconnected",

        "access_token": None,

        "refresh_token": None,

        "token_expires_at": None,

        "accounts": {
            "demo": None,
            "real": None,
        },

        "account_currencies": {
            "demo": "USD",
            "real": "USD",
        },

        "balances": {
            "demo": None,
            "real": None,
        },

        "account_debug": [],

        "account": "demo",

        "stake": 2.0,

        "duration": DEFAULT_DURATION,

        "duration_unit": DEFAULT_DURATION_UNIT,

        "real_market_mode": False,

        "trading": False,

        "demo_stats": empty_stats(),

        "real_stats": empty_stats(),

        "demo_history": [],

        "real_history": [],

        "consecutive_losses_by_account": {
            "demo": 0,
            "real": 0,
        },

        "consecutive_losses": 0,

        "session_profit": 0.0,

        "session_trades": 0,

        "session_wins": 0,

        "session_losses": 0,

        "active_trade": None,

        "last_prediction": None,

        "lock": asyncio.Lock(),
    }

    USER_SESSIONS[
        session_id
    ] = session

    return session


def get_session(
    session_id: str,
) -> Dict[str, Any]:

    cleanup_memory()

    session_id = str(
        session_id or ""
    ).strip()

    if not session_id:

        raise HTTPException(
            status_code=400,
            detail="Session ID is required.",
        )

    session = USER_SESSIONS.get(
        session_id
    )

    if session is None:

        session = new_session(
            session_id
        )

        session[
            "connection_status"
        ] = "session_recreated_after_restart"

    session[
        "last_seen"
    ] = time.time()

    return session


def ensure_session(
    session_id: str,
) -> Dict[str, Any]:

    return get_session(
        session_id
    )


def get_consecutive_losses(
    session: Dict[str, Any],
    account_type: str,
) -> int:

    account_type = normalize_account_type(
        account_type
    )

    return int(
        session.get(
            "consecutive_losses_by_account",
            {},
        ).get(
            account_type,
            0,
        )
        or 0
    )


def set_consecutive_losses(
    session: Dict[str, Any],
    account_type: str,
    value: int,
) -> None:

    account_type = normalize_account_type(
        account_type
    )

    session.setdefault(
        "consecutive_losses_by_account",
        {
            "demo": 0,
            "real": 0,
        },
    )

    session[
        "consecutive_losses_by_account"
    ][
        account_type
    ] = max(
        0,
        int(value),
    )

    if normalize_account_type(
        session.get(
            "account",
            "demo",
        )
    ) == account_type:

        session[
            "consecutive_losses"
        ] = get_consecutive_losses(
            session,
            account_type,
        )


# ============================================================
# PKCE
# ============================================================

def create_pkce_verifier() -> str:

    return secrets.token_urlsafe(
        64
    )[:128]


def create_pkce_challenge(
    verifier: str,
) -> str:

    digest = hashlib.sha256(
        verifier.encode("ascii")
    ).digest()

    return base64.urlsafe_b64encode(
        digest
    ).decode(
        "ascii"
    ).rstrip("=")


# ============================================================
# DERIV REST
# ============================================================

async def deriv_rest_request(
    session: Dict[str, Any],
    method: str,
    path: str,
    **kwargs,
) -> Any:

    token = session.get(
        "access_token"
    )

    if not token:

        raise HTTPException(
            status_code=401,
            detail=(
                "Deriv access token is missing. "
                "Connect the Deriv account again."
            ),
        )

    headers = kwargs.pop(
        "headers",
        {},
    )

    headers[
        "Authorization"
    ] = f"Bearer {token}"

    headers.setdefault(
        "Accept",
        "application/json",
    )

    url = (
        DERIV_REST_BASE.rstrip("/")
        + "/"
        + path.lstrip("/")
    )

    try:

        async with httpx.AsyncClient(
            timeout=25
        ) as client:

            response = await client.request(
                method,
                url,
                headers=headers,
                **kwargs,
            )

    except httpx.HTTPError as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv REST request failed: "
                f"{readable_api_error(exc)}"
            ),
        )

    try:

        data = response.json()

    except Exception:

        data = response.text

    if response.status_code >= 400:

        raise HTTPException(
            status_code=response.status_code,
            detail=readable_api_error(
                data,
                "Deriv REST request failed.",
            ),
        )

    return data


# ============================================================
# DERIV OPTIONS ACCOUNTS
# ============================================================

def _extract_accounts(
    value: Any,
) -> List[Dict[str, Any]]:

    found: List[
        Dict[str, Any]
    ] = []

    def walk(item: Any):

        if isinstance(
            item,
            dict,
        ):

            if (
                "account_id" in item
                or "accountId" in item
            ):

                found.append(
                    item
                )

            for child in item.values():
                walk(child)

        elif isinstance(
            item,
            list,
        ):

            for child in item:
                walk(child)

    walk(value)

    unique = []
    seen = set()

    for item in found:

        account_id = (
            item.get("account_id")
            or item.get("accountId")
        )

        if not account_id:
            continue

        account_id = str(
            account_id
        )

        if account_id in seen:
            continue

        seen.add(
            account_id
        )

        unique.append(
            item
        )

    return unique


def detect_account_type(
    account: Dict[str, Any],
) -> Optional[str]:

    raw = str(
        account.get(
            "account_type",
            account.get(
                "type",
                account.get(
                    "accountType",
                    "",
                ),
            ),
        )
        or ""
    ).lower()

    if "demo" in raw:

        return "demo"

    if "real" in raw or "live" in raw:

        return "real"

    account_id = str(
        account.get(
            "account_id",
            account.get(
                "accountId",
                "",
            ),
        )
        or ""
    ).upper()

    # DOT Options IDs are returned with account metadata,
    # but don't guess real/demo solely from an ID.
    return None


async def load_options_accounts(
    session: Dict[str, Any],
) -> Dict[str, Any]:

    data = await deriv_rest_request(
        session,
        "GET",
        "/trading/v1/options/accounts",
    )

    accounts = _extract_accounts(
        data
    )

    if not accounts:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv returned no Options "
                "accounts for this connection."
            ),
        )

    session[
        "account_debug"
    ] = accounts

    session[
        "accounts"
    ] = {
        "demo": None,
        "real": None,
    }

    session[
        "account_currencies"
    ] = {
        "demo": "USD",
        "real": "USD",
    }

    for account in accounts:

        account_type = detect_account_type(
            account
        )

        if account_type not in {
            "demo",
            "real",
        }:
            continue

        account_id = (
            account.get(
                "account_id"
            )
            or account.get(
                "accountId"
            )
        )

        if not account_id:
            continue

        session[
            "accounts"
        ][
            account_type
        ] = str(
            account_id
        )

        currency = str(
            account.get(
                "currency",
                "USD",
            )
            or "USD"
        ).upper()

        session[
            "account_currencies"
        ][
            account_type
        ] = currency

        balance = safe_float(
            account.get(
                "balance"
            )
        )

        if balance is not None:

            session[
                "balances"
            ][
                account_type
            ] = balance

    if not any(
        session[
            "accounts"
        ].values()
    ):

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv returned Options accounts, "
                "but no demo or real account type "
                "could be identified."
            ),
        )

    return data


# ============================================================
# OPTIONS OTP + AUTHENTICATED WS
# ============================================================

async def get_deriv_otp(
    session: Dict[str, Any],
    account_type: str,
) -> str:

    account_type = normalize_account_type(
        account_type
    )

    account_id = session[
        "accounts"
    ].get(
        account_type
    )

    if not account_id:

        raise HTTPException(
            status_code=400,
            detail=(
                f"No {account_type} Options "
                "account is available."
            ),
        )

    data = await deriv_rest_request(
        session,
        "POST",
        (
            "/trading/v1/options/accounts/"
            f"{urllib.parse.quote(str(account_id), safe='')}"
            "/otp"
        ),
    )

    # Current API returns:
    # {"data":{"url":"wss://.../otp=..."}}
    if isinstance(
        data,
        dict,
    ):

        payload = data.get(
            "data"
        )

        if isinstance(
            payload,
            dict,
        ):

            url = payload.get(
                "url"
            )

            if url:
                return str(url)

        url = data.get(
            "url"
        )

        if url:
            return str(url)

    raise HTTPException(
        status_code=502,
        detail=(
            "Deriv OTP response did not "
            "contain a WebSocket URL."
        ),
    )


async def authenticated_ws_connection(
    session: Dict[str, Any],
    account_type: str,
):

    account_type = normalize_account_type(
        account_type
    )

    if (
        account_type == "real"
        and not REAL_TRADING_ENABLED
    ):

        raise HTTPException(
            status_code=403,
            detail=(
                "Real trading is disabled "
                "on this backend."
            ),
        )

    ws_url = await get_deriv_otp(
        session,
        account_type,
    )

    try:

        async with websockets.connect(
            ws_url,
            open_timeout=WS_TIMEOUT_SECONDS,
            close_timeout=5,
            ping_interval=20,
            ping_timeout=20,
            max_size=4 * 1024 * 1024,
        ) as ws:

            yield ws

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                f"Deriv {account_type} "
                "WebSocket connection failed: "
                f"{readable_api_error(exc)}"
            ),
        )


authenticated_ws_connection = (
    asynccontextmanager(
        authenticated_ws_connection
    )
)


async def send_and_receive(
    ws: Any,
    payload: Dict[str, Any],
    expected_type: Optional[str] = None,
    timeout: float = WS_TIMEOUT_SECONDS,
) -> Dict[str, Any]:

    request_id = secrets.randbelow(
        2_000_000_000
    )

    message = dict(
        payload
    )

    message[
        "req_id"
    ] = request_id

    await ws.send(
        json.dumps(
            message
        )
    )

    deadline = (
        time.monotonic()
        + timeout
    )

    while time.monotonic() < deadline:

        remaining = max(
            0.5,
            deadline - time.monotonic(),
        )

        try:

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

        except asyncio.TimeoutError:

            break

        try:

            response = json.loads(
                raw
            )

        except Exception:

            continue

        if not isinstance(
            response,
            dict,
        ):
            continue

        if response.get(
            "error"
        ):

            return response

        if (
            response.get(
                "req_id"
            )
            == request_id
        ):

            return response

        if (
            expected_type
            and response.get(
                "msg_type"
            )
            == expected_type
        ):

            return response

    raise HTTPException(
        status_code=504,
        detail=(
            "Timed out waiting for Deriv "
            "WebSocket response."
        ),
    )


async def authenticated_ws_request(
    session: Dict[str, Any],
    account_type: str,
    payload: Dict[str, Any],
    expected_type: Optional[str] = None,
) -> Dict[str, Any]:

    async with authenticated_ws_connection(
        session,
        account_type,
    ) as ws:

        return await send_and_receive(
            ws,
            payload,
            expected_type,
        )


# ============================================================
# BALANCE
# ============================================================

async def get_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> float:

    response = await authenticated_ws_request(
        session,
        account_type,
        {
            "balance": 1,
        },
        "balance",
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=400,
            detail=readable_api_error(
                response["error"],
                "Unable to retrieve account balance.",
            ),
        )

    balance_data = response.get(
        "balance"
    )

    if not isinstance(
        balance_data,
        dict,
    ):

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv returned an invalid "
                "balance response."
            ),
        )

    balance = safe_float(
        balance_data.get(
            "balance"
        )
    )

    if balance is None:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv balance value was missing."
            ),
        )

    session[
        "balances"
    ][
        normalize_account_type(
            account_type
        )
    ] = balance

    return balance


async def refresh_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> float:

    return await get_account_balance(
        session,
        account_type,
    )


async def refresh_all_balances(
    session: Dict[str, Any],
) -> None:

    for account_type in (
        "demo",
        "real",
    ):

        if not session[
            "accounts"
        ].get(
            account_type
        ):
            continue

        if (
            account_type == "real"
            and not REAL_TRADING_ENABLED
        ):
            # We may still read the real balance after
            # OAuth if the account exists. This does not
            # enable trading.
            pass

        try:

            await refresh_account_balance(
                session,
                account_type,
            )

        except Exception as exc:

            print(
                "[BALANCE] "
                f"{account_type}: "
                f"{readable_api_error(exc)}"
            )


# ============================================================
# PUBLIC WS
# ============================================================

async def public_ws_connection():

    try:

        async with websockets.connect(
            DERIV_PUBLIC_WS,
            open_timeout=WS_TIMEOUT_SECONDS,
            close_timeout=5,
            ping_interval=20,
            ping_timeout=20,
            max_size=8 * 1024 * 1024,
        ) as ws:

            yield ws

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv public WebSocket failed: "
                f"{readable_api_error(exc)}"
            ),
        )


public_ws_connection = (
    asynccontextmanager(
        public_ws_connection
    )
)


async def public_ws_request(
    ws: Any,
    payload: Dict[str, Any],
    expected_type: Optional[str] = None,
    timeout: float = WS_TIMEOUT_SECONDS,
) -> Dict[str, Any]:

    return await send_and_receive(
        ws,
        payload,
        expected_type,
        timeout,
    )


# ============================================================
# MARKET DISCOVERY
# ============================================================

def normalize_active_symbol(
    item: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    symbol = clean_symbol(
        item.get(
            "symbol"
        )
    )

    if not symbol:
        return None

    market = str(
        item.get(
            "market",
            "",
        )
        or ""
    ).lower()

    subgroup = str(
        item.get(
            "subgroup",
            "",
        )
        or ""
    ).lower()

    submarket = str(
        item.get(
            "submarket",
            "",
        )
        or ""
    ).lower()

    display_name = (
        item.get(
            "display_name"
        )
        or item.get(
            "display"
        )
        or symbol
    )

    pip = safe_float(
        item.get(
            "pip"
        ),
        0.0001,
    )

    exchange_open = item.get(
        "exchange_is_open",
        1,
    )

    suspended = item.get(
        "is_trading_suspended",
        0,
    )

    if str(
        exchange_open
    ).lower() in {
        "0",
        "false",
    }:

        return None

    if str(
        suspended
    ).lower() in {
        "1",
        "true",
    }:

        return None

    eligible_text = (
        f"{market} "
        f"{subgroup} "
        f"{submarket}"
    )

    if not (
        "synthetic" in eligible_text
        or "derived" in eligible_text
    ):

        return None

    return {
        "symbol": symbol,
        "display_name": str(
            display_name
        ),
        "market": market,
        "subgroup": subgroup,
        "submarket": submarket,
        "pip": pip or 0.0001,
        "exchange_is_open": 1,
        "is_trading_suspended": 0,
    }


async def get_active_symbols_on_ws(
    ws: Any,
) -> List[Dict[str, Any]]:

    response = await public_ws_request(
        ws,
        {
            "active_symbols": "brief",
        },
        "active_symbols",
        timeout=25,
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=400,
            detail=readable_api_error(
                response["error"],
                "Unable to retrieve active symbols.",
            ),
        )

    raw = response.get(
        "active_symbols"
    )

    if not isinstance(
        raw,
        list,
    ):

        return []

    output = []

    for item in raw:

        if not isinstance(
            item,
            dict,
        ):
            continue

        normalized = normalize_active_symbol(
            item
        )

        if normalized:

            output.append(
                normalized
            )

    return output


async def verify_digit_contract_support_on_ws(
    ws: Any,
    symbol: str,
) -> bool:

    try:

        response = await public_ws_request(
            ws,
            {
                "contracts_for": symbol,
                "currency": "USD",
            },
            "contracts_for",
            timeout=15,
        )

        if response.get(
            "error"
        ):
            return False

        contracts = response.get(
            "contracts_for"
        )

        if not isinstance(
            contracts,
            dict,
        ):
            return False

        available = contracts.get(
            "available"
        )

        if not isinstance(
            available,
            list,
        ):
            return False

        for item in available:

            if not isinstance(
                item,
                dict,
            ):
                continue

            contract_type = str(
                item.get(
                    "contract_type",
                    "",
                )
                or ""
            ).upper()

            if contract_type in {
                "DIGITOVER",
                "DIGITUNDER",
            }:

                return True

        return False

    except Exception:

        return False


async def get_tradeable_symbols(
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:

    now = time.monotonic()

    cached = MARKET_DISCOVERY_CACHE.get(
        "data"
    )

    if (
        not force_refresh
        and cached
        and now
        - float(
            MARKET_DISCOVERY_CACHE.get(
                "timestamp",
                0,
            )
        )
        < MARKET_DISCOVERY_CACHE_SECONDS
    ):

        return cached

    async with MARKET_DISCOVERY_LOCK:

        now = time.monotonic()

        cached = MARKET_DISCOVERY_CACHE.get(
            "data"
        )

        if (
            not force_refresh
            and cached
            and now
            - float(
                MARKET_DISCOVERY_CACHE.get(
                    "timestamp",
                    0,
                )
            )
            < MARKET_DISCOVERY_CACHE_SECONDS
        ):

            return cached

        async with public_ws_connection() as ws:

            symbols = (
                await get_active_symbols_on_ws(
                    ws
                )
            )

            verified = []

            for item in symbols:

                if await verify_digit_contract_support_on_ws(
                    ws,
                    item[
                        "symbol"
                    ],
                ):

                    verified.append(
                        item
                    )

        MARKET_DISCOVERY_CACHE.update(
            timestamp=time.monotonic(),
            data=verified,
        )

        return verified


# ============================================================
# TICK DATA
# ============================================================

async def get_tick_history_on_ws(
    ws: Any,
    symbol: str,
    count: int,
) -> List[float]:

    response = await public_ws_request(
        ws,
        {
            "ticks_history": symbol,
            "count": count,
            "end": "latest",
            "style": "ticks",
        },
        "history",
        timeout=30,
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=400,
            detail=readable_api_error(
                response["error"],
                "Tick history request failed.",
            ),
        )

    history = response.get(
        "history"
    )

    if not isinstance(
        history,
        dict,
    ):

        return []

    prices = history.get(
        "prices"
    )

    if not isinstance(
        prices,
        list,
    ):

        return []

    result = []

    for price in prices:

        value = safe_float(
            price
        )

        if (
            value is not None
            and value > 0
        ):

            result.append(
                value
            )

    return result


async def get_tick_history(
    symbol: str,
    count: int,
) -> List[float]:

    async with public_ws_connection() as ws:

        return await get_tick_history_on_ws(
            ws,
            symbol,
            count,
        )


async def get_latest_tick_on_ws(
    ws: Any,
    symbol: str,
) -> Optional[float]:

    response = await public_ws_request(
        ws,
        {
            "ticks": symbol,
        },
        "tick",
        timeout=15,
    )

    if response.get(
        "error"
    ):

        return None

    tick = response.get(
        "tick"
    )

    if not isinstance(
        tick,
        dict,
    ):

        return None

    return safe_float(
        tick.get(
            "quote"
        )
    )


# ============================================================
# DIGIT ANALYSIS
# ============================================================

def extract_last_digit(
    price: float,
    pip: float,
) -> int:

    price = safe_float(
        price,
        0.0,
    ) or 0.0

    pip = safe_float(
        pip,
        0.0001,
    ) or 0.0001

    decimals = 0

    if pip < 1:

        try:

            decimals = max(
                0,
                int(
                    round(
                        -math.log10(
                            pip
                        )
                    )
                ),
            )

        except Exception:

            decimals = 4

    scaled = round(
        price,
        decimals,
    )

    text = f"{scaled:.{decimals}f}"

    digits = [
        char
        for char in text
        if char.isdigit()
    ]

    if not digits:
        return 0

    return int(
        digits[-1]
    )


def digit_distribution(
    digits: List[int],
) -> Dict[int, float]:

    counts = {
        digit: 0
        for digit in range(10)
    }

    for digit in digits:

        if digit in counts:

            counts[
                digit
            ] += 1

    total = len(
        digits
    )

    if total <= 0:

        return {
            digit: 10.0
            for digit in range(10)
        }

    return {
        digit: (
            counts[digit]
            / total
            * 100
        )
        for digit in range(10)
    }


def predict_digit_direction(
    digits: List[int],
    barrier: int,
):

    if not digits:

        return (
            "NO TRADE",
            0.0,
            {},
        )

    distribution = digit_distribution(
        digits[-500:]
    )

    over_probability = sum(
        probability
        for digit, probability
        in distribution.items()
        if digit > barrier
    )

    under_probability = sum(
        probability
        for digit, probability
        in distribution.items()
        if digit < barrier
    )

    if (
        over_probability
        >= under_probability
    ):

        direction = "OVER"
        confidence = over_probability

    else:

        direction = "UNDER"
        confidence = under_probability

    return (
        direction,
        round(
            confidence,
            2,
        ),
        distribution,
    )


def calculate_ema(
    prices: List[float],
    period: int,
) -> Optional[float]:

    if len(prices) < period:

        return None

    alpha = 2 / (
        period + 1
    )

    ema = sum(
        prices[:period]
    ) / period

    for price in prices[
        period:
    ]:

        ema = (
            alpha * price
            + (1 - alpha) * ema
        )

    return ema


def calculate_rsi(
    prices: List[float],
    period: int = 14,
) -> Optional[float]:

    if len(prices) <= period:

        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(prices),
    ):

        change = (
            prices[i]
            - prices[i - 1]
        )

        gains.append(
            max(
                change,
                0,
            )
        )

        losses.append(
            max(
                -change,
                0,
            )
        )

    recent_gains = gains[
        -period:
    ]

    recent_losses = losses[
        -period:
    ]

    avg_gain = (
        sum(recent_gains)
        / period
    )

    avg_loss = (
        sum(recent_losses)
        / period
    )

    if avg_loss == 0:

        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return 100 - (
        100 / (
            1 + rs
        )
    )


def calculate_volatility(
    prices: List[float],
) -> Optional[float]:

    if len(prices) < 2:
        return None

    returns = []

    for i in range(
        1,
        len(prices),
    ):

        previous = prices[
            i - 1
        ]

        current = prices[
            i
        ]

        if previous == 0:
            continue

        returns.append(
            (
                current
                - previous
            )
            / previous
        )

    if len(returns) < 2:
        return None

    return statistics.pstdev(
        returns
    ) * 100


# ============================================================
# WALK-FORWARD BACKTEST
# ============================================================

def evaluate_digit_strategy(
    prices: List[float],
    pip: float,
    barrier: int,
) -> Dict[str, Any]:

    if len(prices) < (
        MIN_BACKTEST_SAMPLES
        + PREDICTION_HORIZON_TICKS
        + 20
    ):

        return {
            "valid": False,
            "accuracy": 0.0,
            "samples": 0,
        }

    digits = [
        extract_last_digit(
            price,
            pip,
        )
        for price in prices
    ]

    correct = 0
    samples = 0

    start = max(
        50,
        len(digits)
        - MIN_BACKTEST_SAMPLES
        - PREDICTION_HORIZON_TICKS,
    )

    end = (
        len(digits)
        - PREDICTION_HORIZON_TICKS
    )

    for index in range(
        start,
        end,
    ):

        window = digits[
            max(
                0,
                index - 200,
            ):index
        ]

        if len(window) < 30:
            continue

        distribution = digit_distribution(
            window
        )

        over_probability = sum(
            value
            for digit, value
            in distribution.items()
            if digit > barrier
        )

        under_probability = sum(
            value
            for digit, value
            in distribution.items()
            if digit < barrier
        )

        predicted = (
            "OVER"
            if over_probability
            >= under_probability
            else "UNDER"
        )

        future_digit = digits[
            index
            + PREDICTION_HORIZON_TICKS
            - 1
        ]

        actual = (
            "OVER"
            if future_digit > barrier
            else "UNDER"
            if future_digit < barrier
            else None
        )

        if actual is None:
            continue

        samples += 1

        if predicted == actual:
            correct += 1

    if samples <= 0:

        return {
            "valid": False,
            "accuracy": 0.0,
            "samples": 0,
        }

    accuracy = (
        correct
        / samples
        * 100
    )

    return {
        "valid": (
            samples
            >= MIN_BACKTEST_SAMPLES
        ),
        "accuracy": round(
            accuracy,
            2,
        ),
        "samples": samples,
    }


def calculate_selection_score(
    accuracy: float,
    confidence: float,
) -> float:

    accuracy = clamp(
        safe_float(
            accuracy,
            0.0,
        )
        or 0.0,
        0.0,
        100.0,
    )

    confidence = clamp(
        safe_float(
            confidence,
            0.0,
        )
        or 0.0,
        0.0,
        100.0,
    )

    total_weight = (
        SELECTION_ACCURACY_WEIGHT
        + SELECTION_CONFIDENCE_WEIGHT
    )

    if total_weight <= 0:
        total_weight = 1.0

    return round(
        (
            accuracy
            * SELECTION_ACCURACY_WEIGHT
            + confidence
            * SELECTION_CONFIDENCE_WEIGHT
        )
        / total_weight,
        4,
    )


def market_is_valid_candidate(
    market: Dict[str, Any],
) -> bool:

    direction = str(
        market.get(
            "direction",
            "",
        )
    ).upper()

    accuracy = safe_float(
        market.get(
            "historical_accuracy"
        ),
        0.0,
    ) or 0.0

    confidence = safe_float(
        market.get(
            "confidence"
        ),
        0.0,
    ) or 0.0

    samples = safe_int(
        market.get(
            "backtest_samples"
        ),
        0,
    ) or 0

    live_price = safe_float(
        market.get(
            "latest_price"
        )
    )

    return (
        direction
        in {
            "OVER",
            "UNDER",
        }
        and accuracy >= 0
        and confidence >= 0
        and samples
        >= MIN_BACKTEST_SAMPLES
        and live_price is not None
        and live_price > 0
    )


# ============================================================
# MARKET ANALYZER
# ============================================================

async def analyze_symbol(
    symbol_data: Dict[str, Any],
    barrier: int = DEFAULT_BARRIER,
    ws: Any = None,
) -> Dict[str, Any]:

    symbol = clean_symbol(
        symbol_data.get(
            "symbol"
        )
    )

    display_name = (
        symbol_data.get(
            "display_name"
        )
        or symbol
    )

    pip = (
        safe_float(
            symbol_data.get(
                "pip"
            ),
            0.0001,
        )
        or 0.0001
    )

    try:

        prices = (
            await get_tick_history_on_ws(
                ws,
                symbol,
                HISTORY_TICKS,
            )
            if ws is not None
            else await get_tick_history(
                symbol,
                HISTORY_TICKS,
            )
        )

    except Exception as exc:

        return {
            "asset": symbol,
            "display_name": display_name,
            "direction": "NO TRADE",
            "confidence": 0.0,
            "historical_accuracy": 0.0,
            "backtest_samples": 0,
            "tradeable": False,
            "selected": False,
            "selection_score": 0.0,
            "samples": 0,
            "latest_price": None,
            "reason": (
                "Tick history unavailable: "
                f"{readable_api_error(exc)}"
            ),
        }

    if len(prices) < 50:

        return {
            "asset": symbol,
            "display_name": display_name,
            "direction": "NO TRADE",
            "confidence": 0.0,
            "historical_accuracy": 0.0,
            "backtest_samples": 0,
            "tradeable": False,
            "selected": False,
            "selection_score": 0.0,
            "samples": len(prices),
            "latest_price": (
                prices[-1]
                if prices
                else None
            ),
            "reason": (
                "Insufficient fresh tick history."
            ),
        }

    # Fresh live price.
    latest_price = None

    if ws is not None:

        try:

            latest_price = (
                await get_latest_tick_on_ws(
                    ws,
                    symbol,
                )
            )

        except Exception:
            latest_price = None

    if (
        latest_price is None
        and prices
    ):

        fallback = safe_float(
            prices[-1]
        )

        if (
            fallback is not None
            and fallback > 0
        ):

            latest_price = fallback

    digits = [
        extract_last_digit(
            price,
            pip,
        )
        for price in prices
    ]

    (
        direction,
        confidence,
        probabilities,
    ) = predict_digit_direction(
        digits,
        barrier,
    )

    backtest = evaluate_digit_strategy(
        prices,
        pip,
        barrier,
    )

    rsi = calculate_rsi(
        prices
    )

    ema_fast = calculate_ema(
        prices,
        12,
    )

    ema_slow = calculate_ema(
        prices,
        26,
    )

    volatility = calculate_volatility(
        prices
    )

    technical_bias = "NEUTRAL"

    if (
        ema_fast is not None
        and ema_slow is not None
    ):

        if ema_fast > ema_slow:
            technical_bias = "UP"

        elif ema_fast < ema_slow:
            technical_bias = "DOWN"

    valid = bool(
        direction
        in {
            "OVER",
            "UNDER",
        }
        and backtest.get(
            "valid",
            False,
        )
        and latest_price is not None
        and latest_price > 0
    )

    selection_score = (
        calculate_selection_score(
            backtest.get(
                "accuracy",
                0.0,
            ),
            confidence,
        )
        if valid
        else 0.0
    )

    reasons = []

    if not valid:

        if direction not in {
            "OVER",
            "UNDER",
        }:

            reasons.append(
                "No valid OVER/UNDER direction."
            )

        if not backtest.get(
            "valid",
            False,
        ):

            reasons.append(
                "Insufficient valid "
                "walk-forward samples."
            )

        if (
            latest_price is None
            or latest_price <= 0
        ):

            reasons.append(
                "Current live price is "
                "missing or invalid."
            )

        tradeable = False

    else:

        tradeable = True

        reasons.append(
            "Valid candidate."
        )

    return {
        "asset": symbol,

        "display_name": display_name,

        "direction": direction,

        "confidence": round(
            confidence,
            2,
        ),

        "historical_accuracy": round(
            backtest.get(
                "accuracy",
                0.0,
            ),
            2,
        ),

        "backtest_samples": backtest.get(
            "samples",
            0,
        ),

        "tradeable": tradeable,

        "selected": False,

        "selection_score": selection_score,

        "markets": [
            symbol
        ],

        "latest_price": latest_price,

        "pip": pip,

        "barrier": barrier,

        "prediction_horizon_ticks": (
            PREDICTION_HORIZON_TICKS
        ),

        "digit_probabilities": probabilities,

        "rsi": (
            round(
                rsi,
                2,
            )
            if rsi is not None
            else None
        ),

        "ema_fast": ema_fast,

        "ema_slow": ema_slow,

        "technical_bias": technical_bias,

        "volatility": volatility,

        "samples": len(prices),

        "reason": " ".join(
            reasons
        ),
    }


async def analyze_live_markets(
    barrier: int = DEFAULT_BARRIER,
    force_fresh: bool = True,
) -> Dict[str, Any]:

    now = time.monotonic()

    cached = PREDICTION_CACHE.get(
        "data"
    )

    if (
        not force_fresh
        and cached
        and now
        - float(
            PREDICTION_CACHE.get(
                "timestamp",
                0,
            )
        )
        < PREDICTION_CACHE_SECONDS
    ):

        return cached

    async with PREDICTION_LOCK:

        candidates = (
            await get_tradeable_symbols(
                force_refresh=False
            )
        )

        if not candidates:

            output = {
                "status": "ok",
                "prediction": {
                    "direction": "NO TRADE",
                    "asset": None,
                    "confidence": 0.0,
                    "historical_accuracy": 0.0,
                    "backtest_samples": 0,
                    "tradeable": False,
                    "selected": False,
                    "selection_score": 0.0,
                    "markets": [],
                    "latest_price": None,
                    "reason": (
                        "No usable market data "
                        "was available."
                    ),
                },
                "markets": [],
                "generated_at": iso_now(),
            }

            PREDICTION_CACHE.update(
                timestamp=time.monotonic(),
                data=output,
            )

            return output

        candidates = candidates[
            :MAX_SYMBOLS_TO_ANALYZE
        ]

        cleaned = []

        async with public_ws_connection() as ws:

            for candidate in candidates:

                try:

                    result = (
                        await analyze_symbol(
                            candidate,
                            barrier,
                            ws=ws,
                        )
                    )

                except Exception as exc:

                    result = {
                        "asset": candidate[
                            "symbol"
                        ],
                        "display_name": candidate.get(
                            "display_name",
                            candidate[
                                "symbol"
                            ],
                        ),
                        "direction": "NO TRADE",
                        "confidence": 0.0,
                        "historical_accuracy": 0.0,
                        "backtest_samples": 0,
                        "tradeable": False,
                        "selected": False,
                        "selection_score": 0.0,
                        "samples": 0,
                        "latest_price": None,
                        "reason": (
                            "Analysis failed: "
                            f"{readable_api_error(exc)}"
                        ),
                    }

                cleaned.append(
                    result
                )

        valid_candidates = [
            item
            for item in cleaned
            if market_is_valid_candidate(
                item
            )
        ]

        valid_candidates.sort(
            key=lambda item: (
                float(
                    item.get(
                        "selection_score",
                        0.0,
                    )
                ),
                float(
                    item.get(
                        "historical_accuracy",
                        0.0,
                    )
                ),
                float(
                    item.get(
                        "confidence",
                        0.0,
                    )
                ),
                int(
                    item.get(
                        "backtest_samples",
                        0,
                    )
                ),
            ),
            reverse=True,
        )

        for market in cleaned:

            market[
                "selected"
            ] = False

            if market.get(
                "tradeable"
            ):

                market[
                    "tradeable"
                ] = False

                market[
                    "reason"
                ] = (
                    "Valid candidate, "
                    "waiting for market ranking."
                )

        best = (
            valid_candidates[0]
            if valid_candidates
            else None
        )

        for rank, market in enumerate(
            valid_candidates,
            start=1,
        ):

            market[
                "rank"
            ] = rank

        if best:

            best[
                "selected"
            ] = True

            best[
                "tradeable"
            ] = True

            best[
                "reason"
            ] = (
                "BEST AVAILABLE SIGNAL. "
                "Selected by combined historical "
                "walk-forward accuracy and current "
                "confidence."
            )

        for market in cleaned:

            if "rank" not in market:

                market[
                    "rank"
                ] = None

        if best:

            prediction = {
                "direction": best[
                    "direction"
                ],

                "asset": best[
                    "asset"
                ],

                "confidence": best[
                    "confidence"
                ],

                "historical_accuracy": best[
                    "historical_accuracy"
                ],

                "backtest_samples": best[
                    "backtest_samples"
                ],

                "tradeable": True,

                "selected": True,

                "selection_score": best[
                    "selection_score"
                ],

                "latest_price": best[
                    "latest_price"
                ],

                "markets": [
                    item.get(
                        "asset"
                    )
                    for item in cleaned
                    if item.get(
                        "asset"
                    )
                ],

                "candidate_count": len(
                    valid_candidates
                ),

                "reason": (
                    "BEST AVAILABLE SIGNAL. "
                    "No fixed accuracy/confidence "
                    "threshold is being used."
                ),
            }

            print(
                "[PREDICTION] "
                f"selected={best.get('asset')} "
                f"direction={best.get('direction')} "
                f"accuracy={best.get('historical_accuracy')}% "
                f"confidence={best.get('confidence')}% "
                f"score={best.get('selection_score')} "
                f"price={best.get('latest_price')}"
            )

        else:

            prediction = {
                "direction": "NO TRADE",
                "asset": None,
                "confidence": 0.0,
                "historical_accuracy": 0.0,
                "backtest_samples": 0,
                "tradeable": False,
                "selected": False,
                "selection_score": 0.0,
                "latest_price": None,
                "markets": [
                    item.get(
                        "asset"
                    )
                    for item in cleaned
                    if item.get(
                        "asset"
                    )
                ],
                "candidate_count": 0,
                "reason": (
                    "No market passed the required "
                    "fresh-data quality checks."
                ),
            }

        output = {
            "status": "ok",
            "prediction": prediction,
            "markets": cleaned,
            "generated_at": iso_now(),
        }

        PREDICTION_CACHE.update(
            timestamp=time.monotonic(),
            data=output,
        )

        return output


# ============================================================
# TRADING VALIDATION
# ============================================================

def validate_stake(
    amount: float,
) -> float:

    value = safe_float(
        amount,
        0.0,
    ) or 0.0

    if value <= 0:

        raise HTTPException(
            status_code=400,
            detail=(
                "Stake must be greater than zero."
            ),
        )

    if value > MAX_STAKE:

        raise HTTPException(
            status_code=400,
            detail=(
                f"Stake exceeds "
                f"MAX_STAKE={MAX_STAKE}."
            ),
        )

    return round(
        value,
        2,
    )


def validate_duration(
    duration: int,
) -> int:

    value = safe_int(
        duration,
        DEFAULT_DURATION,
    ) or DEFAULT_DURATION

    if (
        value < 1
        or value > 100
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Duration must be between "
                "1 and 100 ticks."
            ),
        )

    return value


def validate_barrier_for_direction(
    direction: str,
    barrier: int,
) -> int:

    direction = str(
        direction
    ).upper()

    value = safe_int(
        barrier,
        DEFAULT_BARRIER,
    )

    if value is None:
        value = DEFAULT_BARRIER

    if (
        value < 0
        or value > 9
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Barrier must be between "
                "0 and 9."
            ),
        )

    if (
        direction == "OVER"
        and value >= 9
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "DIGITOVER barrier must "
                "be between 0 and 8."
            ),
        )

    if (
        direction == "UNDER"
        and value <= 0
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "DIGITUNDER barrier must "
                "be between 1 and 9."
            ),
        )

    return int(
        value
    )


# ============================================================
# PROPOSAL
# ============================================================

async def request_proposal(
    session: Dict[str, Any],
    account_type: str,
    asset: str,
    direction: str,
    amount: float,
    duration: int,
    barrier: int,
) -> Dict[str, Any]:

    account_type = normalize_account_type(
        account_type
    )

    if (
        account_type == "real"
        and not REAL_TRADING_ENABLED
    ):

        raise HTTPException(
            status_code=403,
            detail=(
                "Real trading is disabled "
                "on this backend."
            ),
        )

    contract_type = (
        "DIGITOVER"
        if direction == "OVER"
        else "DIGITUNDER"
    )

    currency = (
        session[
            "account_currencies"
        ].get(
            account_type
        )
        or "USD"
    )

    payload = {
        "proposal": 1,
        "amount": amount,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": currency,
        "duration": duration,
        "duration_unit": session.get(
            "duration_unit",
            "t",
        ),
        "symbol": asset,
        "barrier": str(
            barrier
        ),
    }

    response = await authenticated_ws_request(
        session,
        account_type,
        payload,
        "proposal",
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=400,
            detail=readable_api_error(
                response["error"],
                "Deriv proposal request failed.",
            ),
        )

    proposal = response.get(
        "proposal"
    )

    if not isinstance(
        proposal,
        dict,
    ):

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv did not return "
                "a valid proposal."
            ),
        )

    proposal_id = (
        proposal.get(
            "id"
        )
        or proposal.get(
            "proposal_id"
        )
    )

    if not proposal_id:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv proposal ID was missing."
            ),
        )

    ask_price = safe_float(
        proposal.get(
            "ask_price"
        )
    )

    if ask_price is None:

        ask_price = safe_float(
            proposal.get(
                "buy_price"
            )
        )

    if ask_price is None:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv proposal did not "
                "return an ask price."
            ),
        )

    return {
        "proposal": proposal,
        "proposal_id": str(
            proposal_id
        ),
        "price": ask_price,
    }


# ============================================================
# TRADE RECORDING
# ============================================================

def record_trade_result(
    session: Dict[str, Any],
    account_type: str,
    trade: Dict[str, Any],
    profit: float,
) -> None:

    account_type = normalize_account_type(
        account_type
    )

    stats_key = (
        "demo_stats"
        if account_type == "demo"
        else "real_stats"
    )

    history_key = (
        "demo_history"
        if account_type == "demo"
        else "real_history"
    )

    stats = session[
        stats_key
    ]

    stats[
        "trades"
    ] += 1

    stats[
        "profit"
    ] += profit

    losses = get_consecutive_losses(
        session,
        account_type,
    )

    if profit > 0:

        stats[
            "wins"
        ] += 1

        set_consecutive_losses(
            session,
            account_type,
            0,
        )

    elif profit < 0:

        stats[
            "losses"
        ] += 1

        set_consecutive_losses(
            session,
            account_type,
            losses + 1,
        )

    history_item = {
        **trade,

        "account": account_type,

        "profit": round(
            profit,
            2,
        ),

        "result": (
            "WIN"
            if profit > 0
            else "LOSS"
            if profit < 0
            else "BREAK_EVEN"
        ),

        "completed_at": iso_now(),
    }

    history = session[
        history_key
    ]

    history.insert(
        0,
        history_item,
    )

    del history[
        MAX_HISTORY_RECORDS:
    ]

    if normalize_account_type(
        session.get(
            "account",
            "demo",
        )
    ) == account_type:

        session[
            "session_profit"
        ] = stats[
            "profit"
        ]

        session[
            "session_trades"
        ] = stats[
            "trades"
        ]

        session[
            "session_wins"
        ] = stats[
            "wins"
        ]

        session[
            "session_losses"
        ] = stats[
            "losses"
        ]

        session[
            "consecutive_losses"
        ] = get_consecutive_losses(
            session,
            account_type,
        )


# ============================================================
# BUY + MONITOR
# ============================================================

async def buy_and_monitor(
    session: Dict[str, Any],
    account_type: str,
    proposal_id: str,
    proposal_price: float,
    trade_metadata: Dict[str, Any],
) -> Dict[str, Any]:

    account_type = normalize_account_type(
        account_type
    )

    async with authenticated_ws_connection(
        session,
        account_type,
    ) as ws:

        # ----------------------------
        # BUY
        # ----------------------------

        bought = await send_and_receive(
            ws,
            {
                "buy": proposal_id,
                "price": proposal_price,
            },
            "buy",
            timeout=20,
        )

        if bought.get(
            "error"
        ):

            raise HTTPException(
                status_code=400,
                detail=readable_api_error(
                    bought["error"],
                    "Deriv buy request failed.",
                ),
            )

        buy_data = bought.get(
            "buy"
        )

        if not isinstance(
            buy_data,
            dict,
        ):

            raise HTTPException(
                status_code=502,
                detail=(
                    "Deriv did not return "
                    "buy data."
                ),
            )

        contract_id = (
            buy_data.get(
                "contract_id"
            )
            or buy_data.get(
                "contractId"
            )
        )

        if not contract_id:

            raise HTTPException(
                status_code=502,
                detail=(
                    "Deriv did not return "
                    "a contract ID."
                ),
            )

        contract_id = str(
            contract_id
        )

        session[
            "active_trade"
        ] = {
            **trade_metadata,
            "account": account_type,
            "contract_id": contract_id,
            "status": "OPEN",
            "bought_at": iso_now(),
        }

        # ----------------------------
        # SUBSCRIBE TO CONTRACT
        # ----------------------------

        req_id = secrets.randbelow(
            2_000_000_000
        )

        await ws.send(
            json.dumps(
                {
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 1,
                    "req_id": req_id,
                }
            )
        )

        duration = safe_int(
            trade_metadata.get(
                "duration"
            ),
            DEFAULT_DURATION,
        ) or DEFAULT_DURATION

        # Give a generous window. A tick contract
        # should normally finish much earlier.
        deadline = (
            time.monotonic()
            + max(
                120,
                (
                    duration
                    * 10
                )
                + 90,
            )
        )

        last_contract = None

        while (
            time.monotonic()
            < deadline
        ):

            try:

                raw = await asyncio.wait_for(
                    ws.recv(),
                    timeout=15,
                )

            except asyncio.TimeoutError:

                if session.get(
                    "active_trade"
                ):

                    session[
                        "active_trade"
                    ][
                        "monitor_error"
                    ] = (
                        "Waiting for Deriv "
                        "contract update."
                    )

                continue

            try:

                response = json.loads(
                    raw
                )

            except Exception:

                continue

            if not isinstance(
                response,
                dict,
            ):
                continue

            if response.get(
                "error"
            ):

                message = readable_api_error(
                    response["error"],
                    "Contract monitoring error.",
                )

                if session.get(
                    "active_trade"
                ):

                    session[
                        "active_trade"
                    ][
                        "monitor_error"
                    ] = message

                continue

            contract = response.get(
                "proposal_open_contract"
            )

            if not isinstance(
                contract,
                dict,
            ):
                continue

            last_contract = contract

            if session.get(
                "active_trade"
            ):

                session[
                    "active_trade"
                ][
                    "contract"
                ] = contract

            status = str(
                contract.get(
                    "status",
                    "",
                )
                or ""
            ).lower()

            is_sold = contract.get(
                "is_sold"
            ) in {
                1,
                True,
                "1",
                "true",
                "True",
            }

            finished = (
                is_sold
                or status in {
                    "sold",
                    "won",
                    "lost",
                    "expired",
                    "closed",
                }
            )

            if not finished:
                continue

            profit = None

            for key in (
                "profit",
                "sell_profit",
            ):

                candidate = safe_float(
                    contract.get(
                        key
                    )
                )

                if candidate is not None:

                    profit = candidate
                    break

            if profit is None:

                if session.get(
                    "active_trade"
                ):

                    session[
                        "active_trade"
                    ][
                        "status"
                    ] = "UNKNOWN_RESULT"

                return {
                    "status": "unknown_result",
                    "account": account_type,
                    "message": (
                        "Contract finished, but "
                        "Deriv did not provide "
                        "a final profit value."
                    ),
                    "contract": contract,
                    "contract_id": contract_id,
                }

            record_trade_result(
                session,
                account_type,
                trade_metadata,
                profit,
            )

            session[
                "active_trade"
            ] = None

            try:

                await refresh_account_balance(
                    session,
                    account_type,
                )

            except Exception as exc:

                print(
                    "[BALANCE AFTER TRADE] "
                    f"{readable_api_error(exc)}"
                )

            consecutive_losses = (
                get_consecutive_losses(
                    session,
                    account_type,
                )
            )

            auto_stopped = (
                consecutive_losses
                >= MAX_CONSECUTIVE_LOSSES
            )

            if auto_stopped:

                session[
                    "trading"
                ] = False

            return {
                "status": "completed",

                "account": account_type,

                "message": (
                    f"{account_type.capitalize()} "
                    "trade completed."
                ),

                "contract_id": contract_id,

                "profit": round(
                    profit,
                    2,
                ),

                "contract": contract,

                "consecutive_losses": (
                    consecutive_losses
                ),

                "trading": bool(
                    session.get(
                        "trading",
                        False,
                    )
                ),

                "auto_stopped": auto_stopped,
            }

        # ----------------------------
        # MONITOR TIMEOUT
        # ----------------------------

        if session.get(
            "active_trade"
        ):

            session[
                "active_trade"
            ][
                "status"
            ] = "MONITORING_TIMEOUT"

        return {
            "status": "monitoring_timeout",

            "account": account_type,

            "message": (
                "Deriv contract was purchased, "
                "but monitoring timed out. "
                "The contract may still be open "
                "on Deriv."
            ),

            "contract_id": contract_id,

            "contract": last_contract,
        }


# ============================================================
# EXECUTE TRADE
# ============================================================

async def execute_trade(
    session: Dict[str, Any],
    account_type: str,
    asset: str,
    direction: str,
    amount: float,
    duration: int,
    barrier: int,
) -> Dict[str, Any]:

    account_type = normalize_account_type(
        account_type
    )

    if session.get(
        "active_trade"
    ):

        raise HTTPException(
            status_code=409,
            detail=(
                "Another trade is currently active."
            ),
        )

    if not session[
        "accounts"
    ].get(
        account_type
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                f"No {account_type} Options "
                "account is available."
            ),
        )

    if account_type == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled "
                    "on this backend."
                ),
            )

        if not session.get(
            "real_market_mode",
            False,
        ):

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real market mode is not "
                    "enabled for this session."
                ),
            )

    consecutive_losses = (
        get_consecutive_losses(
            session,
            account_type,
        )
    )

    if (
        consecutive_losses
        >= MAX_CONSECUTIVE_LOSSES
    ):

        raise HTTPException(
            status_code=403,
            detail=(
                f"{account_type.capitalize()} "
                "trading is locked after "
                f"{MAX_CONSECUTIVE_LOSSES} "
                "consecutive losses."
            ),
        )

    amount = validate_stake(
        amount
    )

    duration = validate_duration(
        duration
    )

    direction = str(
        direction
    ).upper()

    if direction == "NO TRADE":

        return {
            "status": "no_trade",
            "account": account_type,
            "message": (
                "Prediction returned NO TRADE."
            ),
        }

    if direction not in {
        "OVER",
        "UNDER",
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "Direction must be OVER or UNDER."
            ),
        )

    barrier = (
        validate_barrier_for_direction(
            direction,
            barrier,
        )
    )

    balance = session[
        "balances"
    ].get(
        account_type
    )

    # Refresh immediately before a real purchase.
    try:

        balance = await refresh_account_balance(
            session,
            account_type,
        )

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                f"Unable to verify "
                f"{account_type} balance: "
                f"{readable_api_error(exc)}"
            ),
        )

    if (
        balance is not None
        and amount > balance
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                f"Stake ${amount:.2f} exceeds "
                f"available {account_type} "
                f"balance ${balance:.2f}."
            ),
        )

    asset = clean_symbol(
        asset
    )

    if not asset:

        raise HTTPException(
            status_code=400,
            detail="Asset is required.",
        )

    proposal_result = await request_proposal(
        session,
        account_type,
        asset,
        direction,
        amount,
        duration,
        barrier,
    )

    proposal = proposal_result[
        "proposal"
    ]

    proposal_id = proposal_result[
        "proposal_id"
    ]

    proposal_price = proposal_result[
        "price"
    ]

    trade_metadata = {
        "asset": asset,

        "direction": direction,

        "amount": amount,

        "duration": duration,

        "duration_unit": session.get(
            "duration_unit",
            DEFAULT_DURATION_UNIT,
        ),

        "barrier": barrier,

        "account": account_type,

        "proposal_id": proposal_id,

        "proposal_price": proposal_price,

        "started_at": iso_now(),
    }

    print(
        "[TRADE] "
        f"account={account_type} "
        f"asset={asset} "
        f"direction={direction} "
        f"stake={amount} "
        f"duration={duration} "
        f"barrier={barrier} "
        f"proposal={proposal_id}"
    )

    return await buy_and_monitor(
        session,
        account_type,
        proposal_id,
        proposal_price,
        trade_metadata,
    )


# ============================================================
# REQUEST MODELS
# ============================================================

class TradingStartRequest(BaseModel):

    session_id: str

    account: str = "demo"

    stake: float = Field(
        default=2.0,
        gt=0,
    )

    duration: int = Field(
        default=DEFAULT_DURATION,
        ge=1,
        le=100,
    )

    real_market_mode: bool = False


class TradingStopRequest(BaseModel):

    session_id: str

    account: str = "demo"


class TradeRequest(BaseModel):

    session_id: str

    asset: str

    direction: str

    amount: float = Field(
        gt=0
    )

    account: str = "demo"

    duration: int = Field(
        default=DEFAULT_DURATION,
        ge=1,
        le=100,
    )

    barrier: int = Field(
        default=DEFAULT_BARRIER,
        ge=0,
        le=9,
    )


class DisconnectRequest(BaseModel):

    session_id: str


# ============================================================
# ROOT / HEALTH
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "ok",

        "app": "Fixed Risk Booster",

        "version": APP_VERSION,

        "real_trading_enabled": (
            REAL_TRADING_ENABLED
        ),

        "selection_mode": (
            "best_available_market"
        ),

        "selection_weights": {
            "historical_accuracy": (
                SELECTION_ACCURACY_WEIGHT
            ),
            "confidence": (
                SELECTION_CONFIDENCE_WEIGHT
            ),
        },

        "accuracy_threshold": None,

        "confidence_threshold": None,

        "deriv_api": "options_v1",
    }


@app.get("/health")
async def health():

    return {
        "status": "ok",

        "version": APP_VERSION,

        "timestamp": iso_now(),

        "sessions": len(
            USER_SESSIONS
        ),

        "prediction_scan": (
            "single-flight"
        ),

        "selection_mode": (
            "best_available_market"
        ),
    }


# ============================================================
# SESSION
# ============================================================

@app.post("/api/session")
async def create_session():

    session = new_session()

    return {
        "status": "ok",
        "session_id": session[
            "session_id"
        ],
    }


@app.post(
    "/api/session/register"
)
async def register_session(
    request: Request,
):

    session_id = None

    try:

        body = await request.json()

        if isinstance(
            body,
            dict,
        ):

            session_id = body.get(
                "session_id"
            )

    except Exception:
        pass

    if session_id is not None:

        session_id = str(
            session_id
        ).strip()

        if not session_id:

            session_id = None

    session = new_session(
        session_id
    )

    return {
        "status": "ok",
        "session_id": session[
            "session_id"
        ],
    }


@app.get(
    "/api/session/status/{session_id}"
)
async def session_status(
    session_id: str,
):

    session = get_session(
        session_id
    )

    account = normalize_account_type(
        session.get(
            "account",
            "demo",
        )
    )

    return {
        "status": "ok",

        "session_id": session[
            "session_id"
        ],

        "connected": session[
            "connected"
        ],

        "connection_status": session[
            "connection_status"
        ],

        "account": account,

        "accounts": {
            "demo": bool(
                session[
                    "accounts"
                ].get(
                    "demo"
                )
            ),
            "real": bool(
                session[
                    "accounts"
                ].get(
                    "real"
                )
            ),
        },

        "balances": {
            "demo": session[
                "balances"
            ].get(
                "demo"
            ),
            "real": session[
                "balances"
            ].get(
                "real"
            ),
        },

        "currencies": session[
            "account_currencies"
        ],

        "trading": session[
            "trading"
        ],

        "real_market_mode": session[
            "real_market_mode"
        ],

        "real_trading_enabled": (
            REAL_TRADING_ENABLED
        ),

        "active_trade": session[
            "active_trade"
        ],

        "consecutive_losses": (
            get_consecutive_losses(
                session,
                account,
            )
        ),

        "consecutive_losses_by_account": (
            session[
                "consecutive_losses_by_account"
            ]
        ),
    }


# ============================================================
# OAUTH LOGIN
# ============================================================

@app.get(
    "/auth/deriv/login"
)
async def deriv_login(
    session_id: str = Query(...),
):

    session = ensure_session(
        session_id
    )

    if not DERIV_CLIENT_ID:

        raise HTTPException(
            status_code=500,
            detail=(
                "DERIV_CLIENT_ID is not configured."
            ),
        )

    if not DERIV_REDIRECT_URI:

        raise HTTPException(
            status_code=500,
            detail=(
                "DERIV_REDIRECT_URI is not configured."
            ),
        )

    cleanup_memory()

    verifier = create_pkce_verifier()

    challenge = create_pkce_challenge(
        verifier
    )

    state = secrets.token_urlsafe(
        32
    )

    OAUTH_STATES[
        state
    ] = {
        "session_id": session[
            "session_id"
        ],
        "verifier": verifier,
        "created_at": time.time(),
    }

    params = {
        "response_type": "code",

        "client_id": DERIV_CLIENT_ID,

        "redirect_uri": (
            DERIV_REDIRECT_URI
        ),

        "scope": DERIV_OAUTH_SCOPE,

        "state": state,

        "code_challenge": challenge,

        "code_challenge_method": "S256",
    }

    authorization_url = (
        f"{DERIV_AUTH_BASE}/oauth2/auth?"
        + urllib.parse.urlencode(
            params
        )
    )

    return {
        "status": "ok",
        "authorization_url": authorization_url,
    }


@app.get(
    "/auth/deriv/callback"
)
async def deriv_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
):

    if error:

        error_text = (
            error_description
            or error
        )

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                (
                    f"{FRONTEND_ORIGIN}"
                    f"?deriv=error"
                    f"&error="
                    f"{urllib.parse.quote(str(error_text))}"
                ),
                status_code=302,
            )

        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": str(error_text),
                "detail": str(error_text),
                "message": str(error_text),
            },
        )

    if not code or not state:

        raise HTTPException(
            status_code=400,
            detail=(
                "Missing OAuth code or state."
            ),
        )

    oauth_data = OAUTH_STATES.pop(
        state,
        None,
    )

    if not oauth_data:

        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid or expired OAuth state. "
                "Start Connect again."
            ),
        )

    if (
        time.time()
        - float(
            oauth_data.get(
                "created_at",
                0,
            )
        )
        > OAUTH_STATE_TTL_SECONDS
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "OAuth state expired. "
                "Start Connect again."
            ),
        )

    session = ensure_session(
        oauth_data[
            "session_id"
        ]
    )

    token_payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": DERIV_CLIENT_ID,
        "redirect_uri": DERIV_REDIRECT_URI,
        "code_verifier": oauth_data[
            "verifier"
        ],
    }

    try:

        async with httpx.AsyncClient(
            timeout=25
        ) as client:

            response = await client.post(
                (
                    f"{DERIV_AUTH_BASE}"
                    "/oauth2/token"
                ),
                data=token_payload,
                headers={
                    "Accept": (
                        "application/json"
                    ),
                    "Content-Type": (
                        "application/x-www-form-urlencoded"
                    ),
                },
            )

    except httpx.HTTPError as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv OAuth token exchange "
                f"failed: {readable_api_error(exc)}"
            ),
        )

    try:

        token_data = response.json()

    except Exception:

        token_data = response.text

    if response.status_code >= 400:

        message = readable_api_error(
            token_data,
            "Deriv OAuth token exchange failed.",
        )

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                (
                    f"{FRONTEND_ORIGIN}"
                    f"?deriv=error"
                    f"&error="
                    f"{urllib.parse.quote(message)}"
                ),
                status_code=302,
            )

        raise HTTPException(
            status_code=502,
            detail=message,
        )

    if not isinstance(
        token_data,
        dict,
    ):

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv OAuth returned "
                "an invalid token response."
            ),
        )

    session[
        "access_token"
    ] = token_data.get(
        "access_token"
    )

    session[
        "refresh_token"
    ] = token_data.get(
        "refresh_token"
    )

    expires_in = safe_int(
        token_data.get(
            "expires_in"
        ),
        3600,
    ) or 3600

    session[
        "token_expires_at"
    ] = (
        time.time()
        + expires_in
    )

    if not session[
        "access_token"
    ]:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv did not return "
                "an access token."
            ),
        )

    try:

        await load_options_accounts(
            session
        )

        await refresh_all_balances(
            session
        )

        session[
            "connected"
        ] = True

        session[
            "connection_status"
        ] = "connected"

        # Always start in Demo UI mode after connection.
        # The frontend can explicitly switch to Real.
        session[
            "account"
        ] = "demo"

        session[
            "real_market_mode"
        ] = False

    except Exception as exc:

        session[
            "connected"
        ] = False

        session[
            "connection_status"
        ] = (
            "account_load_failed: "
            f"{readable_api_error(exc)}"
        )

        message = readable_api_error(
            exc
        )

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                (
                    f"{FRONTEND_ORIGIN}"
                    f"?deriv=error"
                    f"&error="
                    f"{urllib.parse.quote(message)}"
                ),
                status_code=302,
            )

        raise

    if FRONTEND_ORIGIN:

        return RedirectResponse(
            (
                f"{FRONTEND_ORIGIN}"
                f"?deriv=connected"
                f"&session_id="
                f"{urllib.parse.quote(session['session_id'])}"
            ),
            status_code=302,
        )

    return {
        "status": "connected",
        "session_id": session[
            "session_id"
        ],
        "message": (
            "Deriv account connected successfully."
        ),
    }


# ============================================================
# ACCOUNT DIAGNOSTICS
# ============================================================

@app.get(
    "/api/account/diagnostics/{session_id}"
)
async def account_diagnostics(
    session_id: str,
):

    session = get_session(
        session_id
    )

    return {
        "status": "ok",

        "connected": session[
            "connected"
        ],

        "connection_status": session[
            "connection_status"
        ],

        "accounts": session[
            "accounts"
        ],

        "currencies": session[
            "account_currencies"
        ],

        "balances": session[
            "balances"
        ],

        "account_debug": session.get(
            "account_debug",
            [],
        ),

        "real_trading_enabled": (
            REAL_TRADING_ENABLED
        ),
    }


@app.get(
    "/api/account/balance/{session_id}"
)
async def account_balance(
    session_id: str,
):

    session = get_session(
        session_id
    )

    errors = {}

    if session[
        "connected"
    ]:

        for account_type in (
            "demo",
            "real",
        ):

            if not session[
                "accounts"
            ].get(
                account_type
            ):
                continue

            try:

                await refresh_account_balance(
                    session,
                    account_type,
                )

            except Exception as exc:

                errors[
                    account_type
                ] = readable_api_error(
                    exc,
                    (
                        f"Unable to refresh "
                        f"{account_type} balance."
                    ),
                )

    return {
        "status": "ok",

        "connected": session[
            "connected"
        ],

        "accounts": session[
            "accounts"
        ],

        "balances": session[
            "balances"
        ],

        "currencies": session[
            "account_currencies"
        ],

        "errors": errors,
    }


# ============================================================
# MARKETS
# ============================================================

@app.get(
    "/api/markets"
)
async def markets(
    force_refresh: bool = False,
):

    try:

        eligible = (
            await get_tradeable_symbols(
                force_refresh=force_refresh
            )
        )

    except Exception as exc:

        message = readable_api_error(
            exc,
            "Market discovery failed.",
        )

        return {
            "status": "error",
            "markets": [],
            "count": 0,
            "generated_at": iso_now(),
            "error": message,
            "message": message,
        }

    output = []

    async with public_ws_connection() as ws:

        for item in eligible:

            symbol = item[
                "symbol"
            ]

            latest = None

            try:

                latest = (
                    await get_latest_tick_on_ws(
                        ws,
                        symbol,
                    )
                )

            except Exception:
                pass

            output.append(
                {
                    **item,
                    "asset": symbol,
                    "latest_price": latest,
                    "tradeable": True,
                }
            )

    return {
        "status": "ok",

        "markets": output,

        "count": len(output),

        "generated_at": iso_now(),
    }


@app.get(
    "/api/market/prediction"
)
async def market_prediction(
    barrier: int = Query(
        DEFAULT_BARRIER,
        ge=0,
        le=9,
    ),
):

    return await analyze_live_markets(
        barrier,
        force_fresh=True,
    )


# ============================================================
# STATS
# ============================================================

@app.get(
    "/api/stats/{session_id}"
)
async def stats(
    session_id: str,
    account: str = Query("demo"),
):

    session = get_session(
        session_id
    )

    account = normalize_account_type(
        account
    )

    stats_key = (
        "demo_stats"
        if account == "demo"
        else "real_stats"
    )

    current = session[
        stats_key
    ]

    trades = int(
        current.get(
            "trades",
            0,
        )
    )

    wins = int(
        current.get(
            "wins",
            0,
        )
    )

    losses = int(
        current.get(
            "losses",
            0,
        )
    )

    profit = safe_float(
        current.get(
            "profit",
            0,
        ),
        0.0,
    ) or 0.0

    return {
        "status": "ok",

        "account": account,

        "profit": round(
            profit,
            2,
        ),

        "trades": trades,

        "wins": wins,

        "losses": losses,

        "win_rate": round(
            (
                wins
                / trades
                * 100
            )
            if trades
            else 0.0,
            2,
        ),
    }


# ============================================================
# HISTORY
# ============================================================

@app.get(
    "/api/trades/{session_id}"
)
async def trades(
    session_id: str,
    account: str = Query("demo"),
):

    session = get_session(
        session_id
    )

    account = normalize_account_type(
        account
    )

    history_key = (
        "demo_history"
        if account == "demo"
        else "real_history"
    )

    return {
        "status": "ok",

        "account": account,

        "trades": list(
            session[
                history_key
            ]
        ),
    }


# ============================================================
# TRADING STATUS
# ============================================================

@app.get(
    "/api/trading/status/{session_id}"
)
async def trading_status(
    session_id: str,
):

    session = get_session(
        session_id
    )

    account = normalize_account_type(
        session.get(
            "account",
            "demo",
        )
    )

    return {
        "status": "ok",

        "trading": bool(
            session.get(
                "trading",
                False,
            )
        ),

        "account": account,

        "real_market_mode": bool(
            session.get(
                "real_market_mode",
                False,
            )
        ),

        "active_trade": session.get(
            "active_trade"
        ),

        "consecutive_losses": (
            get_consecutive_losses(
                session,
                account,
            )
        ),

        "consecutive_losses_by_account": (
            session[
                "consecutive_losses_by_account"
            ]
        ),

        "max_consecutive_losses": (
            MAX_CONSECUTIVE_LOSSES
        ),

        "real_trading_enabled": (
            REAL_TRADING_ENABLED
        ),
    }


# ============================================================
# START TRADING
# ============================================================

@app.post(
    "/api/trading/start"
)
async def start_trading(
    request: TradingStartRequest,
):

    session = get_session(
        request.session_id
    )

    if not session[
        "connected"
    ]:

        raise HTTPException(
            status_code=401,
            detail=(
                "Connect Deriv account first."
            ),
        )

    account = normalize_account_type(
        request.account
    )

    stake = validate_stake(
        request.stake
    )

    duration = validate_duration(
        request.duration
    )

    if account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled "
                    "on the backend. Set "
                    "REAL_TRADING_ENABLED=true "
                    "on Render to enable it."
                ),
            )

        if not session[
            "accounts"
        ].get(
            "real"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No real Options account "
                    "is available."
                ),
            )

        if not request.real_market_mode:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real market mode must be "
                    "explicitly enabled before "
                    "starting Real trading."
                ),
            )

    else:

        if not session[
            "accounts"
        ].get(
            "demo"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No demo Options account "
                    "is available."
                ),
            )

    session[
        "account"
    ] = account

    session[
        "stake"
    ] = stake

    session[
        "duration"
    ] = duration

    session[
        "real_market_mode"
    ] = (
        bool(
            request.real_market_mode
        )
        if account == "real"
        else False
    )

    session[
        "consecutive_losses"
    ] = get_consecutive_losses(
        session,
        account,
    )

    if (
        session[
            "consecutive_losses"
        ]
        >= MAX_CONSECUTIVE_LOSSES
    ):

        raise HTTPException(
            status_code=403,
            detail=(
                f"{account.capitalize()} "
                "trading is locked after "
                f"{MAX_CONSECUTIVE_LOSSES} "
                "consecutive losses."
            ),
        )

    session[
        "trading"
    ] = True

    return {
        "status": "ok",

        "message": (
            f"{account.capitalize()} trading started."
        ),

        "trading": True,

        "account": account,

        "stake": stake,

        "duration": duration,

        "real_market_mode": session[
            "real_market_mode"
        ],

        "balance": session[
            "balances"
        ].get(
            account
        ),

        "consecutive_losses": (
            get_consecutive_losses(
                session,
                account,
            )
        ),
    }


# ============================================================
# STOP TRADING
# ============================================================

@app.post(
    "/api/trading/stop"
)
async def stop_trading(
    request: TradingStopRequest,
):

    session = get_session(
        request.session_id
    )

    account = normalize_account_type(
        request.account
    )

    session[
        "trading"
    ] = False

    return {
        "status": "ok",

        "trading": False,

        "account": account,

        "active_trade": session.get(
            "active_trade"
        ),

        "message": (
            f"{account.capitalize()} trading stopped. "
            "Future automatic trades are stopped. "
            "Any contract already purchased on Deriv "
            "continues until it finishes."
        ),
    }


# ============================================================
# MANUAL TRADE
# ============================================================

@app.post(
    "/api/trade"
)
async def manual_trade(
    request: TradeRequest,
):

    session = get_session(
        request.session_id
    )

    if not session[
        "connected"
    ]:

        raise HTTPException(
            status_code=401,
            detail=(
                "Connect Deriv account first."
            ),
        )

    account = normalize_account_type(
        request.account
    )

    if account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled "
                    "on the backend."
                ),
            )

        if not session[
            "accounts"
        ].get(
            "real"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No real Options account "
                    "is available."
                ),
            )

        if not session.get(
            "real_market_mode",
            False,
        ):

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real market mode is not "
                    "enabled for this session."
                ),
            )

    else:

        if not session[
            "accounts"
        ].get(
            "demo"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No demo Options account "
                    "is available."
                ),
            )

    asset = clean_symbol(
        request.asset
    )

    if not asset:

        raise HTTPException(
            status_code=400,
            detail="Asset is required.",
        )

    direction = str(
        request.direction
    ).upper()

    if direction == "NO TRADE":

        return {
            "status": "no_trade",

            "account": account,

            "message": (
                "NO TRADE signal."
            ),
        }

    if direction not in {
        "OVER",
        "UNDER",
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "Direction must be "
                "OVER or UNDER."
            ),
        )

    amount = validate_stake(
        request.amount
    )

    duration = validate_duration(
        request.duration
    )

    barrier = (
        validate_barrier_for_direction(
            direction,
            request.barrier,
        )
    )

    async with session[
        "lock"
    ]:

        result = await execute_trade(
            session,
            account,
            asset,
            direction,
            amount,
            duration,
            barrier,
        )

    return {
        "status": "ok",

        "account": account,

        "message": (
            f"{account.capitalize()} "
            "manual trade completed."
        ),

        "trade": {
            "asset": asset,
            "direction": direction,
            "stake": amount,
            "duration": duration,
            "barrier": barrier,
            "account": account,
        },

        "result": result,
    }


# ============================================================
# AUTOMATIC CURRENT BEST PREDICTION
# ============================================================

@app.post(
    "/api/trading/execute"
)
async def execute_current_prediction(
    session_id: str = Query(...),
):

    session = get_session(
        session_id
    )

    if not session[
        "connected"
    ]:

        raise HTTPException(
            status_code=401,
            detail=(
                "Connect Deriv account first."
            ),
        )

    if not session[
        "trading"
    ]:

        raise HTTPException(
            status_code=400,
            detail=(
                "Trading is stopped."
            ),
        )

    if session.get(
        "active_trade"
    ):

        raise HTTPException(
            status_code=409,
            detail=(
                "A trade is already active."
            ),
        )

    account = normalize_account_type(
        session.get(
            "account",
            "demo",
        )
    )

    if account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled."
                ),
            )

        if not session[
            "accounts"
        ].get(
            "real"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No real Options account "
                    "is available."
                ),
            )

        if not session.get(
            "real_market_mode",
            False,
        ):

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real market mode is "
                    "not enabled."
                ),
            )

    else:

        if not session[
            "accounts"
        ].get(
            "demo"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No demo Options account "
                    "is available."
                ),
            )

    consecutive_losses = (
        get_consecutive_losses(
            session,
            account,
        )
    )

    if (
        consecutive_losses
        >= MAX_CONSECUTIVE_LOSSES
    ):

        session[
            "trading"
        ] = False

        raise HTTPException(
            status_code=403,
            detail=(
                f"{account.capitalize()} "
                "trading automatically stopped "
                "after "
                f"{MAX_CONSECUTIVE_LOSSES} "
                "consecutive losses."
            ),
        )

    # --------------------------------------------------------
    # ALWAYS FRESH ANALYSIS
    # --------------------------------------------------------

    prediction_result = (
        await analyze_live_markets(
            DEFAULT_BARRIER,
            force_fresh=True,
        )
    )

    prediction = prediction_result.get(
        "prediction",
        {},
    )

    session[
        "last_prediction"
    ] = prediction

    direction = str(
        prediction.get(
            "direction",
            "NO TRADE",
        )
    ).upper()

    asset = clean_symbol(
        prediction.get(
            "asset"
        )
    )

    tradeable = bool(
        prediction.get(
            "tradeable",
            False,
        )
    )

    latest_price = safe_float(
        prediction.get(
            "latest_price"
        )
    )

    if (
        not asset
        or latest_price is None
        or latest_price <= 0
    ):

        return {
            "status": "no_trade",

            "account": account,

            "prediction": prediction,

            "message": (
                "Selected market has no valid "
                "positive live price. "
                "Trade blocked."
            ),
        }

    if (
        direction not in {
            "OVER",
            "UNDER",
        }
        or not tradeable
    ):

        return {
            "status": "no_trade",

            "account": account,

            "prediction": prediction,

            "message": (
                "No valid best-available "
                "market was selected."
            ),
        }

    amount = validate_stake(
        session.get(
            "stake",
            2.0,
        )
    )

    duration = validate_duration(
        session.get(
            "duration",
            DEFAULT_DURATION,
        )
    )

    async with session[
        "lock"
    ]:

        # Recheck because another request may have
        # entered the lock after the initial check.
        if session.get(
            "active_trade"
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "A trade became active "
                    "before execution."
                ),
            )

        if not session[
            "trading"
        ]:

            raise HTTPException(
                status_code=400,
                detail=(
                    "Trading was stopped "
                    "before execution."
                ),
            )

        result = await execute_trade(
            session,
            account,
            asset,
            direction,
            amount,
            duration,
            DEFAULT_BARRIER,
        )

    return {
        "status": "ok",

        "account": account,

        "message": (
            f"{account.capitalize()} "
            "automatic trade completed."
        ),

        "prediction": prediction,

        "result": result,
    }


# ============================================================
# DISCONNECT
# ============================================================

@app.post(
    "/api/disconnect"
)
async def disconnect(
    request: DisconnectRequest,
):

    session = get_session(
        request.session_id
    )

    session[
        "trading"
    ] = False

    session[
        "connected"
    ] = False

    session[
        "connection_status"
    ] = "disconnected"

    session[
        "access_token"
    ] = None

    session[
        "refresh_token"
    ] = None

    session[
        "token_expires_at"
    ] = None

    session[
        "accounts"
    ] = {
        "demo": None,
        "real": None,
    }

    session[
        "balances"
    ] = {
        "demo": None,
        "real": None,
    }

    session[
        "account_currencies"
    ] = {
        "demo": "USD",
        "real": "USD",
    }

    session[
        "real_market_mode"
    ] = False

    # Stats and history intentionally remain.
    return {
        "status": "ok",

        "connected": False,

        "trading": False,

        "message": (
            "Deriv account disconnected."
        ),

        "active_trade": session.get(
            "active_trade"
        ),
    }


# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request,
    exc: Exception,
):

    message = readable_api_error(
        exc,
        "Internal server error.",
    )

    print(
        "[ERROR] "
        f"{request.method} "
        f"{request.url}: "
        f"{message}"
    )

    if isinstance(
        exc,
        HTTPException,
    ):

        detail = readable_api_error(
            exc.detail,
            f"HTTP {exc.status_code} error.",
        )

        return JSONResponse(
            status_code=exc.status_code,

            content={
                "status": "error",

                # ALWAYS a string.
                # Prevents frontend [object Object].
                "detail": detail,

                "message": detail,
            },
        )

    return JSONResponse(
        status_code=500,

        content={
            "status": "error",

            "detail": message,

            "message": message,
        },
    )
