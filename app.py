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
from typing import Any, Dict, List, Optional, Tuple

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

# ============================================================
# CONFIGURATION
# ============================================================

APP_VERSION = "7.1.1"

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "").strip().rstrip("/")
DERIV_CLIENT_ID = os.getenv("DERIV_CLIENT_ID", "").strip()
DERIV_REDIRECT_URI = os.getenv("DERIV_REDIRECT_URI", "").strip()

DERIV_REST_BASE = "https://api.derivws.com"
DERIV_AUTH_BASE = "https://auth.deriv.com"
DERIV_PUBLIC_WS = "wss://api.derivws.com/trading/v1/options/ws/public"
DERIV_OAUTH_SCOPE = "trade"

REAL_TRADING_ENABLED = os.getenv("REAL_TRADING_ENABLED", "false").lower() in {
    "1", "true", "yes", "on"
}

# Keep these configurable, but use conservative defaults for Render.
HISTORY_TICKS = max(200, min(int(os.getenv("HISTORY_TICKS", "2000")), 5000))
PREDICTION_HORIZON_TICKS = max(
    1, min(int(os.getenv("PREDICTION_HORIZON_TICKS", "5")), 20)
)
MIN_BACKTEST_SAMPLES = max(
    50, min(int(os.getenv("MIN_BACKTEST_SAMPLES", "150")), 500)
)
MIN_EDGE_ACCURACY = float(os.getenv("MIN_EDGE_ACCURACY", "58"))
MIN_CONFIDENCE_TO_TRADE = float(os.getenv("MIN_CONFIDENCE_TO_TRADE", "62"))
MAX_SYMBOLS_TO_ANALYZE = max(
    1, min(int(os.getenv("MAX_SYMBOLS_TO_ANALYZE", "10")), 12)
)
PREDICTION_CACHE_SECONDS = float(os.getenv("PREDICTION_CACHE_SECONDS", "3"))
MARKET_DISCOVERY_CACHE_SECONDS = float(
    os.getenv("MARKET_DISCOVERY_CACHE_SECONDS", "60")
)
DEFAULT_DURATION = int(os.getenv("DEFAULT_DURATION", "5"))
DEFAULT_DURATION_UNIT = os.getenv("DEFAULT_DURATION_UNIT", "t").strip() or "t"
DEFAULT_BARRIER = int(os.getenv("DEFAULT_BARRIER", "5"))
MAX_STAKE = float(os.getenv("MAX_STAKE", "1000"))
MAX_HISTORY_RECORDS = 100
MAX_CONSECUTIVE_LOSSES = 3

PUBLIC_WS_OPEN_TIMEOUT = float(os.getenv("PUBLIC_WS_OPEN_TIMEOUT", "8"))
PUBLIC_WS_RESPONSE_TIMEOUT = float(os.getenv("PUBLIC_WS_RESPONSE_TIMEOUT", "12"))
AUTH_WS_OPEN_TIMEOUT = float(os.getenv("AUTH_WS_OPEN_TIMEOUT", "10"))
AUTH_WS_RESPONSE_TIMEOUT = float(os.getenv("AUTH_WS_RESPONSE_TIMEOUT", "15"))

SESSION_TTL_SECONDS = float(
    os.getenv("SESSION_TTL_SECONDS", str(12 * 3600))
)
MAX_SESSIONS = max(50, int(os.getenv("MAX_SESSIONS", "1000")))
OAUTH_STATE_TTL_SECONDS = 600
MAX_OAUTH_STATES = 500

# ============================================================
# APP
# ============================================================

app = FastAPI(
    title="Fixed Risk Booster API",
    version=APP_VERSION,
    description="Multi-user Deriv Options trading backend.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=86400,
)

# ============================================================
# IN-MEMORY STATE
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}
OAUTH_STATES: Dict[str, Dict[str, Any]] = {}

PREDICTION_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": None,
}

MARKET_DISCOVERY_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": [],
}

PREDICTION_LOCK = asyncio.Lock()
MARKET_DISCOVERY_LOCK = asyncio.Lock()

# ============================================================
# GENERAL HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def clean_symbol(value: Any) -> str:
    return "" if value is None else str(value).strip()


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def safe_int(
    value: Any,
    default: Optional[int] = None,
) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalize_account_type(account: str) -> str:
    value = str(account or "").strip().lower()

    if value not in {"demo", "real"}:
        raise HTTPException(
            status_code=400,
            detail="Account must be demo or real.",
        )

    return value


def new_stats() -> Dict[str, Any]:
    return {
        "profit": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
    }


def cleanup_memory() -> None:
    now = time.time()

    # Clean OAuth states.
    for state, item in list(OAUTH_STATES.items()):
        if (
            now - float(item.get("created_at", 0))
            > OAUTH_STATE_TTL_SECONDS
        ):
            OAUTH_STATES.pop(state, None)

    # Clean inactive sessions.
    for sid, session in list(USER_SESSIONS.items()):
        last = session.get("last_activity_epoch", now)

        inactive = (
            now - float(last) > SESSION_TTL_SECONDS
        )

        if (
            inactive
            and not session.get("active_trade")
            and not session.get("connected")
        ):
            USER_SESSIONS.pop(sid, None)

    # OAuth state hard limit.
    if len(OAUTH_STATES) > MAX_OAUTH_STATES:
        oldest = sorted(
            OAUTH_STATES.items(),
            key=lambda x: x[1].get("created_at", 0),
        )

        for state, _ in oldest[
            : len(OAUTH_STATES) - MAX_OAUTH_STATES
        ]:
            OAUTH_STATES.pop(state, None)

    # Session hard limit.
    if len(USER_SESSIONS) > MAX_SESSIONS:
        candidates = [
            (sid, s)
            for sid, s in USER_SESSIONS.items()
            if not s.get("active_trade")
            and not s.get("connected")
        ]

        candidates.sort(
            key=lambda x: x[1].get(
                "last_activity_epoch",
                0,
            )
        )

        remove_count = len(USER_SESSIONS) - MAX_SESSIONS

        for sid, _ in candidates[:remove_count]:
            USER_SESSIONS.pop(sid, None)


def new_session(
    session_id: Optional[str] = None,
) -> Dict[str, Any]:

    cleanup_memory()

    sid = session_id or secrets.token_urlsafe(32)
    now = time.time()

    session = {
        "session_id": sid,
        "created_at": iso_now(),
        "last_activity": iso_now(),
        "last_activity_epoch": now,

        "connected": False,
        "connection_status": "disconnected",

        "access_token": None,
        "refresh_token": None,
        "token_expires_at": None,

        "accounts": {
            "demo": None,
            "real": None,
        },

        "account_debug": [],

        "account_currencies": {
            "demo": "USD",
            "real": "USD",
        },

        "balances": {
            "demo": None,
            "real": None,
        },

        "demo_stats": new_stats(),
        "real_stats": new_stats(),

        "demo_history": [],
        "real_history": [],

        "account": "demo",

        "stake": 2.0,

        "duration": DEFAULT_DURATION,
        "duration_unit": DEFAULT_DURATION_UNIT,

        "real_market_mode": False,

        "trading": False,

        "session_profit": 0.0,
        "session_trades": 0,
        "session_wins": 0,
        "session_losses": 0,

        "consecutive_losses": 0,

        "active_trade": None,

        "lock": asyncio.Lock(),

        "last_prediction": None,
    }

    USER_SESSIONS[sid] = session

    return session


def get_session(
    session_id: str,
) -> Dict[str, Any]:

    if not session_id:
        raise HTTPException(
            status_code=400,
            detail="session_id is required.",
        )

    cleanup_memory()

    session = USER_SESSIONS.get(session_id)

    # Render restarts wipe RAM while browser can retain the old ID.
    if session is None:
        session = new_session(session_id)
        session["connection_status"] = (
            "session_recreated_after_restart"
        )

    session["last_activity"] = iso_now()
    session["last_activity_epoch"] = time.time()

    return session


def ensure_session(
    session_id: str,
) -> Dict[str, Any]:
    return get_session(session_id)


# ============================================================
# PKCE / OAUTH
# ============================================================

def create_pkce_verifier() -> str:
    return base64.urlsafe_b64encode(
        secrets.token_bytes(48)
    ).rstrip(b"=").decode("ascii")


def create_pkce_challenge(
    verifier: str,
) -> str:

    digest = hashlib.sha256(
        verifier.encode("ascii")
    ).digest()

    return base64.urlsafe_b64encode(
        digest
    ).rstrip(b"=").decode("ascii")


# ============================================================
# DERIV REST
# ============================================================

async def deriv_rest_async(
    method: str,
    path: str,
    session: Dict[str, Any],
    *,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 20,
) -> Dict[str, Any]:

    token = session.get("access_token")

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Deriv account is not connected.",
        )

    url = f"{DERIV_REST_BASE}{path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(
            timeout=timeout
        ) as client:

            response = await client.request(
                method,
                url,
                headers=headers,
                json=json_body,
                params=params,
            )

    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail=f"Deriv REST timeout: {exc}",
        )

    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Deriv REST connection error: {exc}",
        )

    if response.status_code >= 400:
        text = response.text[:1200]

        raise HTTPException(
            status_code=response.status_code,
            detail=text,
        )

    try:
        data = response.json()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Deriv returned invalid JSON.",
        )

    if not isinstance(data, dict):
        raise HTTPException(
            status_code=502,
            detail="Unexpected Deriv REST response.",
        )

    return data


async def get_deriv_accounts(
    session: Dict[str, Any],
) -> Dict[str, Any]:

    return await deriv_rest_async(
        "GET",
        "/trading/v1/options/accounts",
        session,
    )


def extract_account_id(
    account: Dict[str, Any],
) -> Optional[str]:

    for key in (
        "account_id",
        "id",
        "accountId",
        "loginid",
        "login_id",
    ):
        value = account.get(key)

        if value:
            return str(value)

    return None


def _account_type_from_flags(
    account: Dict[str, Any],
) -> Optional[str]:

    for key in (
        "is_demo",
        "is_virtual",
    ):
        value = account.get(key)

        if value in {
            True,
            1,
            "1",
            "true",
            "True",
        }:
            return "demo"

    value = account.get("is_real")

    if value in {
        True,
        1,
        "1",
        "true",
        "True",
    }:
        return "real"

    return None


def detect_account_type(
    account: Dict[str, Any],
) -> Optional[str]:

    flagged = _account_type_from_flags(account)

    if flagged:
        return flagged

    values = [
        account.get("account_type"),
        account.get("accountType"),
        account.get("type"),
        account.get("environment"),
        account.get("mode"),
        account.get("loginid"),
        account.get("login_id"),
        account.get("account_name"),
        account.get("name"),
    ]

    text = " ".join(
        str(v).lower()
        for v in values
        if v is not None
    )

    if any(
        x in text
        for x in (
            "demo",
            "virtual",
            "practice",
        )
    ):
        return "demo"

    if any(
        x in text
        for x in (
            "real",
            "live",
        )
    ):
        return "real"

    return None


def detect_currency(
    account: Dict[str, Any],
) -> Optional[str]:

    for key in (
        "currency",
        "currency_code",
    ):
        if account.get(key):
            return str(account[key])

    return None


def _extract_accounts(
    response: Dict[str, Any],
) -> List[Dict[str, Any]]:

    found: List[Dict[str, Any]] = []
    seen = set()

    def walk(value: Any) -> None:

        if isinstance(value, dict):

            account_id = extract_account_id(value)

            if account_id and any(
                key in value
                for key in (
                    "account_type",
                    "accountType",
                    "type",
                    "environment",
                    "mode",
                    "currency",
                    "currency_code",
                    "loginid",
                    "login_id",
                    "is_demo",
                    "is_virtual",
                    "is_real",
                )
            ):

                marker = account_id

                if marker not in seen:
                    seen.add(marker)
                    found.append(value)

            for child in value.values():
                walk(child)

        elif isinstance(value, list):

            for child in value:
                walk(child)

    walk(response)

    return found


async def load_options_accounts(
    session: Dict[str, Any],
) -> None:

    response = await get_deriv_accounts(session)

    raw_accounts = _extract_accounts(response)

    demo_account = None
    real_account = None

    debug = []

    for account in raw_accounts:

        account_id = extract_account_id(account)

        if not account_id:
            continue

        account_type = detect_account_type(account)

        currency = detect_currency(account) or "USD"

        debug.append({
            "account_id": account_id,
            "type": account_type,
            "currency": currency,
        })

        if (
            account_type == "demo"
            and not demo_account
        ):
            demo_account = account_id
            session["account_currencies"]["demo"] = currency

        elif (
            account_type == "real"
            and not real_account
        ):
            real_account = account_id
            session["account_currencies"]["real"] = currency

    session["accounts"] = {
        "demo": demo_account,
        "real": real_account,
    }

    session["account_debug"] = debug

    if not demo_account and not real_account:
        raise RuntimeError(
            "OAuth succeeded, but no Deriv Options trading accounts were returned."
        )


# ============================================================
# WEBSOCKET HELPERS
# ============================================================

async def ws_request(
    url: str,
    payload: Dict[str, Any],
    timeout: float = 15,
    *,
    open_timeout: Optional[float] = None,
) -> Dict[str, Any]:

    open_timeout = open_timeout or timeout

    try:

        async with websockets.connect(
            url,
            open_timeout=open_timeout,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=3,
            max_size=2 * 1024 * 1024,
        ) as ws:

            await ws.send(
                json.dumps(payload)
            )

            deadline = (
                time.monotonic()
                + timeout
            )

            while True:

                remaining = (
                    deadline
                    - time.monotonic()
                )

                if remaining <= 0:
                    raise TimeoutError(
                        "Timed out waiting for Deriv WebSocket response."
                    )

                raw = await asyncio.wait_for(
                    ws.recv(),
                    timeout=remaining,
                )

                try:
                    response = json.loads(raw)
                except Exception:
                    continue

                if isinstance(response, dict):

                    if response.get("error"):
                        return response

                    if (
                        response.get("msg_type")
                        or "echo_req" in response
                    ):
                        return response

    except asyncio.TimeoutError:
        raise TimeoutError(
            "Timed out during Deriv WebSocket opening/response."
        )

    except TimeoutError:
        raise

    except Exception as exc:
        raise RuntimeError(
            f"Deriv WebSocket error: {exc}"
        )


@asynccontextmanager
async def public_ws_connection():

    try:

        async with websockets.connect(
            DERIV_PUBLIC_WS,
            open_timeout=PUBLIC_WS_OPEN_TIMEOUT,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=3,
            max_size=2 * 1024 * 1024,
        ) as ws:

            yield ws

    except asyncio.TimeoutError:

        raise TimeoutError(
            "Timed out during Deriv public WebSocket opening handshake."
        )

    except Exception as exc:

        raise RuntimeError(
            f"Deriv public WebSocket connection failed: {exc}"
        )


async def ws_request_existing(
    ws: Any,
    payload: Dict[str, Any],
    timeout: float = PUBLIC_WS_RESPONSE_TIMEOUT,
    expected_msg_types: Optional[set] = None,
) -> Dict[str, Any]:

    await ws.send(
        json.dumps(payload)
    )

    deadline = (
        time.monotonic()
        + timeout
    )

    while True:

        remaining = (
            deadline
            - time.monotonic()
        )

        if remaining <= 0:
            raise TimeoutError(
                "Timed out waiting for Deriv public WebSocket response."
            )

        raw = await asyncio.wait_for(
            ws.recv(),
            timeout=remaining,
        )

        try:
            response = json.loads(raw)
        except Exception:
            continue

        if not isinstance(response, dict):
            continue

        if response.get("error"):
            return response

        msg_type = response.get("msg_type")

        if (
            expected_msg_types is None
            or msg_type in expected_msg_types
        ):
            return response

        if (
            "echo_req" in response
            and not expected_msg_types
        ):
            return response


async def public_ws_request(
    payload: Dict[str, Any],
    timeout: float = PUBLIC_WS_RESPONSE_TIMEOUT,
) -> Dict[str, Any]:

    async with public_ws_connection() as ws:

        return await ws_request_existing(
            ws,
            payload,
            timeout,
        )


# ============================================================
# AUTHENTICATED OPTIONS WS
# ============================================================

async def get_deriv_otp(
    session: Dict[str, Any],
    account_id: str,
) -> str:

    response = await deriv_rest_async(
        "POST",
        f"/trading/v1/options/accounts/{account_id}/otp",
        session,
        json_body={},
    )

    data = response.get("data")

    if isinstance(data, dict):

        for key in (
            "url",
            "ws_url",
            "websocket_url",
        ):
            if data.get(key):
                return str(data[key])

    for key in (
        "url",
        "ws_url",
        "websocket_url",
    ):
        if response.get(key):
            return str(response[key])

    raise HTTPException(
        status_code=502,
        detail="Deriv did not provide an Options WebSocket URL.",
    )


async def authenticated_ws_request(
    session: Dict[str, Any],
    account_type: str,
    payload: Dict[str, Any],
    timeout: float = AUTH_WS_RESPONSE_TIMEOUT,
) -> Dict[str, Any]:

    account_type = normalize_account_type(
        account_type
    )

    account_id = session["accounts"].get(
        account_type
    )

    if not account_id:
        raise HTTPException(
            status_code=400,
            detail=f"No {account_type} Options account is available.",
        )

    ws_url = await get_deriv_otp(
        session,
        account_id,
    )

    return await ws_request(
        ws_url,
        payload,
        timeout,
        open_timeout=AUTH_WS_OPEN_TIMEOUT,
    )


@asynccontextmanager
async def authenticated_ws_connection(
    session: Dict[str, Any],
    account_type: str,
):

    account_type = normalize_account_type(
        account_type
    )

    account_id = session["accounts"].get(
        account_type
    )

    if not account_id:
        raise HTTPException(
            status_code=400,
            detail=f"No {account_type} Options account is available.",
        )

    ws_url = await get_deriv_otp(
        session,
        account_id,
    )

    try:

        async with websockets.connect(
            ws_url,
            open_timeout=AUTH_WS_OPEN_TIMEOUT,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=3,
            max_size=2 * 1024 * 1024,
        ) as ws:

            yield ws

    except asyncio.TimeoutError:

        raise TimeoutError(
            "Timed out during authenticated Deriv WebSocket opening handshake."
        )

    except Exception as exc:

        raise RuntimeError(
            f"Authenticated Deriv WebSocket failed: {exc}"
        )


async def send_and_receive(
    ws: Any,
    payload: Dict[str, Any],
    expected_msg_type: str,
    timeout: float = AUTH_WS_RESPONSE_TIMEOUT,
) -> Dict[str, Any]:

    await ws.send(
        json.dumps(payload)
    )

    deadline = (
        time.monotonic()
        + timeout
    )

    while True:

        remaining = (
            deadline
            - time.monotonic()
        )

        if remaining <= 0:
            raise TimeoutError(
                f"Timed out waiting for {expected_msg_type} response."
            )

        raw = await asyncio.wait_for(
            ws.recv(),
            timeout=remaining,
        )

        try:
            response = json.loads(raw)
        except Exception:
            continue

        if not isinstance(response, dict):
            continue

        if response.get("error"):
            return response

        if (
            response.get("msg_type")
            == expected_msg_type
        ):
            return response


# ============================================================
# BALANCES
# ============================================================

async def get_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> Optional[float]:

    account_type = normalize_account_type(
        account_type
    )

    if not session["accounts"].get(
        account_type
    ):
        return None

    response = await authenticated_ws_request(
        session,
        account_type,
        {"balance": 1},
    )

    if response.get("error"):
        raise RuntimeError(
            str(response["error"])
        )

    data = response.get("balance")

    if isinstance(data, dict):
        return safe_float(
            data.get("balance")
        )

    return None


async def refresh_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> Optional[float]:

    value = await get_account_balance(
        session,
        account_type,
    )

    session["balances"][account_type] = value

    return value


async def refresh_all_balances(
    session: Dict[str, Any],
) -> None:

    try:
        await refresh_account_balance(
            session,
            "demo",
        )

    except Exception as exc:
        print(
            f"[BALANCE] demo refresh failed: {exc}"
        )

    if session["accounts"].get("real"):

        try:
            await refresh_account_balance(
                session,
                "real",
            )

        except Exception as exc:
            print(
                f"[BALANCE] real refresh failed: {exc}"
            )


# ============================================================
# MARKET DISCOVERY
# ============================================================

def normalize_active_symbol(
    item: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    if not isinstance(item, dict):
        return None

    symbol = clean_symbol(
        item.get("underlying_symbol")
        or item.get("symbol")
        or item.get("display_symbol")
    )

    if not symbol:
        return None

    display_name = (
        item.get("underlying_symbol_name")
        or item.get("display_name")
        or item.get("display_symbol_name")
        or symbol
    )

    symbol_type = (
        item.get("underlying_symbol_type")
        or item.get("symbol_type")
        or ""
    )

    market = (
        item.get("market")
        or item.get("market_display_name")
        or ""
    )

    subgroup = (
        item.get("subgroup")
        or item.get("submarket")
        or item.get("submarket_display_name")
        or ""
    )

    submarket = (
        item.get("submarket")
        or item.get("submarket_display_name")
        or ""
    )

    pip = (
        item.get("pip_size")
        if item.get("pip_size") is not None
        else item.get("pip")
    )

    return {
        "symbol": symbol,
        "display_name": str(display_name),
        "symbol_type": str(symbol_type),
        "market": str(market),
        "subgroup": str(subgroup),
        "submarket": str(submarket),
        "pip": safe_float(pip, 0.0),
        "exchange_is_open": item.get(
            "exchange_is_open",
            True,
        ),
        "is_trading_suspended": item.get(
            "is_trading_suspended",
            False,
        ),
    }


def symbol_is_trade_candidate(
    symbol: Dict[str, Any],
) -> bool:

    if not clean_symbol(
        symbol.get("symbol")
    ):
        return False

    if symbol.get(
        "exchange_is_open"
    ) in {
        False,
        0,
        "0",
        "false",
        "False",
    }:
        return False

    if symbol.get(
        "is_trading_suspended"
    ) in {
        True,
        1,
        "1",
        "true",
        "True",
    }:
        return False

    searchable = " ".join(
        str(symbol.get(k) or "")
        for k in (
            "market",
            "symbol_type",
            "subgroup",
            "submarket",
        )
    ).lower()

    return (
        "synthetic" in searchable
        or "derived" in searchable
    )


async def get_active_symbols_on_ws(
    ws: Any,
) -> List[Dict[str, Any]]:

    response = await ws_request_existing(
        ws,
        {
            "active_symbols": "brief",
            "contract_type": [
                "DIGITOVER",
                "DIGITUNDER",
            ],
        },
        expected_msg_types={
            "active_symbols"
        },
    )

    if response.get("error"):
        raise RuntimeError(
            str(response["error"])
        )

    raw = response.get(
        "active_symbols",
        [],
    )

    if not isinstance(raw, list):
        return []

    result = []

    for item in raw:

        normalized = normalize_active_symbol(
            item
        )

        if normalized:
            result.append(normalized)

    return result


async def verify_digit_contract_support_on_ws(
    ws: Any,
    symbol: Dict[str, Any],
) -> bool:

    code = clean_symbol(
        symbol.get("symbol")
    )

    if not code:
        return False

    try:

        response = await ws_request_existing(
            ws,
            {
                "contracts_for": code
            },
            expected_msg_types={
                "contracts_for"
            },
        )

    except Exception:
        return False

    if response.get("error"):
        return False

    data = response.get(
        "contracts_for",
        {},
    )

    if not isinstance(data, dict):
        return False

    available = data.get(
        "available",
        [],
    )

    if not isinstance(available, list):
        return False

    return any(
        isinstance(c, dict)
        and str(
            c.get("contract_type")
            or c.get("type")
            or ""
        ).upper()
        in {
            "DIGITOVER",
            "DIGITUNDER",
        }
        for c in available
    )


async def get_tradeable_symbols(
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:

    now = time.monotonic()

    cached = MARKET_DISCOVERY_CACHE.get(
        "data"
    )

    if (
        not force_refresh
        and isinstance(cached, list)
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
            and isinstance(cached, list)
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

            symbols = await get_active_symbols_on_ws(
                ws
            )

            candidates_by_code: Dict[
                str,
                Dict[str, Any],
            ] = {}

            for symbol in symbols:

                if symbol_is_trade_candidate(
                    symbol
                ):
                    candidates_by_code.setdefault(
                        symbol["symbol"],
                        symbol,
                    )

            candidates = list(
                candidates_by_code.values()
            )

            tradeable: List[
                Dict[str, Any]
            ] = []

            for symbol in candidates:

                if await verify_digit_contract_support_on_ws(
                    ws,
                    symbol,
                ):
                    tradeable.append(symbol)

        MARKET_DISCOVERY_CACHE[
            "timestamp"
        ] = time.monotonic()

        MARKET_DISCOVERY_CACHE[
            "data"
        ] = tradeable

        print(
            f"[MARKETS] discovered={len(symbols)} "
            f"candidates={len(candidates)} "
            f"digit_tradeable={len(tradeable)}"
        )

        if tradeable:
            print(
                "[MARKETS] "
                + ", ".join(
                    x["symbol"]
                    for x in tradeable
                )
            )

        return tradeable


# ============================================================
# TICK HISTORY
# ============================================================

async def get_tick_history_on_ws(
    ws: Any,
    symbol: str,
    count: int = HISTORY_TICKS,
) -> List[float]:

    count = max(
        50,
        min(int(count), 5000),
    )

    response = await ws_request_existing(
        ws,
        {
            "ticks_history": symbol,
            "count": count,
            "end": "latest",
            "style": "ticks",
        },
        timeout=PUBLIC_WS_RESPONSE_TIMEOUT,
        expected_msg_types={
            "history"
        },
    )

    if response.get("error"):
        raise RuntimeError(
            str(response["error"])
        )

    history = response.get(
        "history",
        {},
    )

    if not isinstance(history, dict):
        return []

    prices = history.get(
        "prices",
        [],
    )

    times = history.get(
        "times",
        [],
    )

    if not isinstance(prices, list):
        return []

    if (
        isinstance(times, list)
        and len(times) == len(prices)
    ):

        pairs = []

        for timestamp, price in zip(
            times,
            prices,
        ):

            t = safe_int(timestamp)
            p = safe_float(price)

            if (
                t is not None
                and p is not None
                and math.isfinite(p)
            ):
                pairs.append(
                    (t, p)
                )

        pairs.sort(
            key=lambda x: x[0]
        )

        return [
            p
            for _, p in pairs
        ]

    return [
        p
        for p in (
            safe_float(x)
            for x in prices
        )
        if p is not None
        and math.isfinite(p)
    ]


async def get_tick_history(
    symbol: str,
    count: int = HISTORY_TICKS,
) -> List[float]:

    async with public_ws_connection() as ws:

        return await get_tick_history_on_ws(
            ws,
            symbol,
            count,
        )


async def get_latest_tick(
    symbol: str,
) -> Optional[float]:

    try:

        response = await public_ws_request(
            {
                "ticks": symbol
            },
            timeout=10,
        )

        tick = response.get(
            "tick"
        )

        return (
            safe_float(
                tick.get("quote")
            )
            if isinstance(tick, dict)
            else None
        )

    except Exception:
        return None


# ============================================================
# DIGIT / TECHNICAL ANALYSIS
# ============================================================

def extract_last_digit(
    price: float,
    pip: float = 0.0,
) -> int:

    if pip and pip > 0:

        decimals = max(
            0,
            int(
                round(
                    -math.log10(pip)
                )
            ),
        )

        scaled = round(
            price * (
                10 ** decimals
            )
        )

        return abs(scaled) % 10

    text = (
        f"{price:.10f}"
        .rstrip("0")
    )

    if "." in text:

        fractional = text.split(
            ".",
            1,
        )[1]

        if fractional:
            return int(
                fractional[-1]
            )

    return abs(
        int(round(price))
    ) % 10


def digit_distribution(
    digits: List[int],
) -> Dict[int, float]:

    counts = {
        d: 0
        for d in range(10)
    }

    for d in digits:

        if d in counts:
            counts[d] += 1

    total = len(digits)

    return {
        d: (
            counts[d]
            / total
            * 100
            if total
            else 0.0
        )
        for d in range(10)
    }


def predict_digit_direction(
    digits: List[int],
    barrier: int,
) -> Tuple[
    str,
    float,
    Dict[str, Any],
]:

    if len(digits) < 20:

        return (
            "NO TRADE",
            0.0,
            {
                "over_probability": 0.0,
                "under_probability": 0.0,
                "edge": 0.0,
            },
        )

    barrier = max(
        0,
        min(
            9,
            int(barrier),
        ),
    )

    over = sum(
        1
        for d in digits
        if d > barrier
    )

    under = sum(
        1
        for d in digits
        if d < barrier
    )

    total = len(digits)

    over_probability = (
        over
        / total
        * 100.0
    )

    under_probability = (
        under
        / total
        * 100.0
    )

    if (
        over_probability
        > under_probability
    ):

        direction = "OVER"
        confidence = over_probability
        baseline = (
            (9 - barrier)
            / 10
            * 100.0
        )

    else:

        direction = "UNDER"
        confidence = under_probability
        baseline = (
            barrier
            / 10
            * 100.0
        )

    edge = confidence - baseline

    if edge <= 0:
        direction = "NO TRADE"

    return (
        direction,
        round(confidence, 2),
        {
            "over_probability": round(
                over_probability,
                2,
            ),
            "under_probability": round(
                under_probability,
                2,
            ),
            "baseline_probability": round(
                baseline,
                2,
            ),
            "edge": round(
                edge,
                2,
            ),
        },
    )


def calculate_ema(
    values: List[float],
    period: int,
) -> Optional[float]:

    if len(values) < period:
        return None

    multiplier = 2 / (
        period + 1
    )

    ema = statistics.mean(
        values[:period]
    )

    for value in values[period:]:
        ema = (
            value - ema
        ) * multiplier + ema

    return ema


def calculate_rsi(
    values: List[float],
    period: int = 14,
) -> Optional[float]:

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(values),
    ):

        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
        )

    avg_gain = statistics.mean(
        gains[-period:]
    )

    avg_loss = statistics.mean(
        losses[-period:]
    )

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return 100 - 100 / (
        1 + rs
    )


def calculate_volatility(
    values: List[float],
    period: int = 50,
) -> Optional[float]:

    if len(values) < period + 1:
        return None

    recent = values[
        -(period + 1):
    ]

    returns = [
        (b - a) / a
        for a, b in zip(
            recent,
            recent[1:],
        )
        if a != 0
    ]

    return (
        statistics.pstdev(returns)
        if len(returns) >= 2
        else None
    )


def evaluate_digit_strategy(
    prices: List[float],
    pip: float,
    barrier: int,
) -> Dict[str, Any]:

    minimum = (
        MIN_BACKTEST_SAMPLES
        + 30
    )

    if len(prices) < minimum:

        return {
            "valid": False,
            "accuracy": 0.0,
            "samples": 0,
        }

    digits = [
        extract_last_digit(
            p,
            pip,
        )
        for p in prices
    ]

    correct = 0
    samples = 0

    start = max(
        30,
        len(digits) - 750,
    )

    for index in range(
        start,
        len(digits) - 1,
    ):

        training = digits[:index]

        direction, _, _ = (
            predict_digit_direction(
                training,
                barrier,
            )
        )

        if direction == "NO TRADE":
            continue

        actual = digits[index]

        success = (
            actual > barrier
            if direction == "OVER"
            else actual < barrier
        )

        samples += 1

        if success:
            correct += 1

    accuracy = (
        correct
        / samples
        * 100.0
        if samples
        else 0.0
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


# ============================================================
# MARKET ANALYZER
# ============================================================

async def analyze_symbol(
    symbol_data: Dict[str, Any],
    barrier: int = DEFAULT_BARRIER,
    ws: Any = None,
) -> Dict[str, Any]:

    symbol = clean_symbol(
        symbol_data.get("symbol")
    )

    display_name = (
        symbol_data.get("display_name")
        or symbol
    )

    pip = safe_float(
        symbol_data.get("pip"),
        0.0,
    ) or 0.0

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
            "confidence": 0,
            "historical_accuracy": 0,
            "tradeable": False,
            "samples": 0,
            "reason": (
                f"Tick history unavailable: {exc}"
            ),
        }

    if len(prices) < 50:

        return {
            "asset": symbol,
            "display_name": display_name,
            "direction": "NO TRADE",
            "confidence": 0,
            "historical_accuracy": 0,
            "tradeable": False,
            "samples": len(prices),
            "reason": "Insufficient tick history.",
        }

    digits = [
        extract_last_digit(
            p,
            pip,
        )
        for p in prices
    ]

    direction, confidence, probabilities = (
        predict_digit_direction(
            digits,
            barrier,
        )
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

    tradeable = bool(
        direction != "NO TRADE"
        and confidence
        >= MIN_CONFIDENCE_TO_TRADE
        and backtest["valid"]
        and backtest["accuracy"]
        >= MIN_EDGE_ACCURACY
    )

    reasons = []

    if direction == "NO TRADE":
        reasons.append(
            "Current digit distribution does not show sufficient edge."
        )

    if not backtest["valid"]:

        reasons.append(
            "Insufficient valid walk-forward samples."
        )

    elif (
        backtest["accuracy"]
        < MIN_EDGE_ACCURACY
    ):

        reasons.append(
            "Historical walk-forward accuracy is below the configured threshold."
        )

    if (
        confidence
        < MIN_CONFIDENCE_TO_TRADE
    ):

        reasons.append(
            "Current confidence is below the configured threshold."
        )

    if tradeable:

        reasons.append(
            "Market passed current prediction and historical validation."
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
            backtest["accuracy"],
            2,
        ),
        "backtest_samples": backtest[
            "samples"
        ],
        "tradeable": tradeable,
        "markets": [symbol],
        "latest_price": prices[-1],
        "pip": pip,
        "barrier": barrier,
        "prediction_horizon_ticks": (
            PREDICTION_HORIZON_TICKS
        ),
        "digit_probabilities": probabilities,
        "rsi": (
            round(rsi, 2)
            if rsi is not None
            else None
        ),
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "technical_bias": technical_bias,
        "volatility": volatility,
        "samples": len(prices),
        "reason": (
            " ".join(reasons)
            or "No trade signal."
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

        try:

            candidates = await get_tradeable_symbols()

        except Exception as exc:

            output = {
                "status": "ok",
                "prediction": {
                    "direction": "NO TRADE",
                    "asset": None,
                    "confidence": 0,
                    "historical_accuracy": 0,
                    "tradeable": False,
                    "markets": [],
                    "reason": (
                        f"Market discovery unavailable: {exc}"
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

        if not candidates:

            output = {
                "status": "ok",
                "prediction": {
                    "direction": "NO TRADE",
                    "asset": None,
                    "confidence": 0,
                    "historical_accuracy": 0,
                    "tradeable": False,
                    "markets": [],
                    "reason": "No usable market data was available.",
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

        cleaned: List[
            Dict[str, Any]
        ] = []

        try:

            async with public_ws_connection() as ws:

                for symbol in candidates:

                    result = await analyze_symbol(
                        symbol,
                        barrier,
                        ws=ws,
                    )

                    cleaned.append(
                        result
                    )

        except Exception as exc:

            cleaned = [
                {
                    "asset": s["symbol"],
                    "display_name": s.get(
                        "display_name",
                        s["symbol"],
                    ),
                    "direction": "NO TRADE",
                    "confidence": 0,
                    "historical_accuracy": 0,
                    "tradeable": False,
                    "samples": 0,
                    "reason": (
                        "Tick history connection unavailable: "
                        f"{exc}"
                    ),
                }
                for s in candidates
            ]

        cleaned.sort(
            key=lambda x: (
                bool(
                    x.get("tradeable")
                ),
                float(
                    x.get(
                        "confidence",
                        0,
                    )
                ),
                float(
                    x.get(
                        "historical_accuracy",
                        0,
                    )
                ),
            ),
            reverse=True,
        )

        best = (
            cleaned[0]
            if cleaned
            else None
        )

        if best:

            prediction = {
                "direction": best.get(
                    "direction",
                    "NO TRADE",
                ),
                "asset": best.get(
                    "asset"
                ),
                "confidence": best.get(
                    "confidence",
                    0,
                ),
                "historical_accuracy": best.get(
                    "historical_accuracy",
                    0,
                ),
                "tradeable": bool(
                    best.get(
                        "tradeable",
                        False,
                    )
                ),
                "markets": [
                    x.get("asset")
                    for x in cleaned
                    if x.get("asset")
                ],
                "reason": best.get(
                    "reason",
                    "",
                ),
            }

        else:

            prediction = {
                "direction": "NO TRADE",
                "asset": None,
                "confidence": 0,
                "historical_accuracy": 0,
                "tradeable": False,
                "markets": [],
                "reason": "No market analysis completed.",
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
# TRADING
# ============================================================

def validate_stake(
    amount: float,
) -> float:

    value = (
        safe_float(
            amount,
            0.0,
        )
        or 0.0
    )

    if value <= 0:
        raise HTTPException(
            status_code=400,
            detail="Stake must be greater than zero.",
        )

    if value > MAX_STAKE:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Stake exceeds MAX_STAKE={MAX_STAKE}."
            ),
        )

    return round(
        value,
        2,
    )


def validate_duration(
    duration: int,
) -> int:

    value = (
        safe_int(
            duration,
            DEFAULT_DURATION,
        )
        or DEFAULT_DURATION
    )

    if value < 1 or value > 100:
        raise HTTPException(
            status_code=400,
            detail="Duration must be between 1 and 100.",
        )

    return value


def validate_barrier_for_direction(
    direction: str,
    barrier: int,
) -> int:

    direction = str(
        direction
    ).upper()

    if direction == "NO TRADE":
        return int(barrier)

    value = safe_int(
        barrier,
        DEFAULT_BARRIER,
    )

    if (
        value is None
        or value < 0
        or value > 9
    ):
        raise HTTPException(
            status_code=400,
            detail="Barrier must be between 0 and 9.",
        )

    if (
        direction == "OVER"
        and value >= 9
    ):
        raise HTTPException(
            status_code=400,
            detail="DIGITOVER barrier must be between 0 and 8.",
        )

    if (
        direction == "UNDER"
        and value <= 0
    ):
        raise HTTPException(
            status_code=400,
            detail="DIGITUNDER barrier must be between 1 and 9.",
        )

    return int(value)


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
            detail="Real trading is disabled on this backend.",
        )

    contract_type = (
        "DIGITOVER"
        if direction.upper() == "OVER"
        else "DIGITUNDER"
    )

    payload = {
        "proposal": 1,
        "amount": amount,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": session[
            "account_currencies"
        ].get(
            account_type,
            "USD",
        ),
        "duration": duration,
        "duration_unit": session.get(
            "duration_unit",
            DEFAULT_DURATION_UNIT,
        ),
        "symbol": asset,
        "barrier": str(barrier),
    }

    response = await authenticated_ws_request(
        session,
        account_type,
        payload,
    )

    if response.get("error"):
        raise HTTPException(
            status_code=400,
            detail=str(
                response["error"]
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
            detail="Deriv did not return a proposal.",
        )

    proposal_id = (
        proposal.get("id")
        or proposal.get("proposal_id")
    )

    if not proposal_id:
        raise HTTPException(
            status_code=502,
            detail="Proposal ID was missing.",
        )

    return {
        "proposal": proposal,
        "proposal_id": str(
            proposal_id
        ),
    }


async def buy_proposal(
    session: Dict[str, Any],
    account_type: str,
    proposal_id: str,
    price: float,
) -> Dict[str, Any]:

    if (
        account_type == "real"
        and not REAL_TRADING_ENABLED
    ):
        raise HTTPException(
            status_code=403,
            detail="Real trading is disabled.",
        )

    response = await authenticated_ws_request(
        session,
        account_type,
        {
            "buy": proposal_id,
            "price": price,
        },
    )

    if response.get("error"):
        raise HTTPException(
            status_code=400,
            detail=str(
                response["error"]
            ),
        )

    buy_data = response.get(
        "buy"
    )

    if not isinstance(
        buy_data,
        dict,
    ):
        raise HTTPException(
            status_code=502,
            detail="Deriv did not return buy data.",
        )

    contract_id = (
        buy_data.get("contract_id")
        or buy_data.get("contractId")
    )

    if not contract_id:
        raise HTTPException(
            status_code=502,
            detail="Contract ID was missing.",
        )

    return {
        "buy": buy_data,
        "contract_id": str(
            contract_id
        ),
    }


async def get_open_contract(
    session: Dict[str, Any],
    account_type: str,
    contract_id: str,
) -> Dict[str, Any]:

    response = await authenticated_ws_request(
        session,
        account_type,
        {
            "proposal_open_contract": 1,
            "contract_id": contract_id,
        },
    )

    if response.get("error"):
        raise HTTPException(
            status_code=400,
            detail=str(
                response["error"]
            ),
        )

    contract = response.get(
        "proposal_open_contract"
    )

    if not isinstance(
        contract,
        dict,
    ):
        raise HTTPException(
            status_code=502,
            detail="Invalid contract response.",
        )

    return contract


def contract_is_finished(
    contract: Dict[str, Any],
) -> bool:

    if contract.get(
        "is_sold"
    ) in {
        1,
        True,
        "1",
        "true",
        "True",
    }:
        return True

    return str(
        contract.get(
            "status",
            "",
        )
    ).lower() in {
        "sold",
        "won",
        "lost",
        "expired",
        "closed",
    }


def final_profit_from_contract(
    contract: Dict[str, Any],
) -> Optional[float]:

    for key in (
        "profit",
        "sell_profit",
    ):

        value = safe_float(
            contract.get(key)
        )

        if value is not None:
            return value

    return None


def record_trade_result(
    session: Dict[str, Any],
    account_type: str,
    trade: Dict[str, Any],
    profit: float,
) -> None:

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

    stats["trades"] += 1
    stats["profit"] += profit

    if profit > 0:

        stats["wins"] += 1
        session[
            "consecutive_losses"
        ] = 0

    elif profit < 0:

        stats["losses"] += 1
        session[
            "consecutive_losses"
        ] += 1

    history_item = {
        **trade,
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

    session["session_profit"] = stats[
        "profit"
    ]

    session["session_trades"] = stats[
        "trades"
    ]

    session["session_wins"] = stats[
        "wins"
    ]

    session["session_losses"] = stats[
        "losses"
    ]


async def buy_and_monitor(
    session: Dict[str, Any],
    account_type: str,
    proposal_id: str,
    proposal_price: float,
    trade_metadata: Dict[str, Any],
) -> Dict[str, Any]:

    async with authenticated_ws_connection(
        session,
        account_type,
    ) as ws:

        bought = await send_and_receive(
            ws,
            {
                "buy": proposal_id,
                "price": proposal_price,
            },
            "buy",
        )

        if bought.get("error"):
            raise HTTPException(
                status_code=400,
                detail=str(
                    bought["error"]
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
                detail="Deriv did not return buy data.",
            )

        contract_id = (
            buy_data.get("contract_id")
            or buy_data.get("contractId")
        )

        if not contract_id:
            raise HTTPException(
                status_code=502,
                detail="Contract ID was missing.",
            )

        contract_id = str(
            contract_id
        )

        session["active_trade"] = {
            **trade_metadata,
            "account": account_type,
            "contract_id": contract_id,
            "status": "OPEN",
            "bought_at": iso_now(),
        }

        await ws.send(
            json.dumps(
                {
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 1,
                }
            )
        )

        deadline = (
            time.monotonic()
            + max(
                300,
                (
                    DEFAULT_DURATION
                    + 30
                )
                * 3,
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
                    timeout=10,
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
                        "No contract update received for 10 seconds."
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

                if session.get(
                    "active_trade"
                ):
                    session[
                        "active_trade"
                    ][
                        "monitor_error"
                    ] = str(
                        response["error"]
                    )

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

            if contract_is_finished(
                contract
            ):

                profit = (
                    final_profit_from_contract(
                        contract
                    )
                )

                if profit is None:

                    session[
                        "active_trade"
                    ][
                        "status"
                    ] = "UNKNOWN_RESULT"

                    return {
                        "status": "unknown_result",
                        "contract": contract,
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

                except Exception:
                    pass

                return {
                    "status": "completed",
                    "contract": contract,
                    "profit": profit,
                }

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
            "contract": last_contract,
            "contract_id": contract_id,
        }


async def execute_trade(
    session: Dict[str, Any],
    account_type: str,
    asset: str,
    direction: str,
    amount: float,
    duration: int,
    barrier: int,
) -> Dict[str, Any]:

    if session.get(
        "active_trade"
    ):
        raise HTTPException(
            status_code=409,
            detail="Another trade is currently active.",
        )

    if (
        session.get(
            "consecutive_losses",
            0,
        )
        >= MAX_CONSECUTIVE_LOSSES
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                f"Trading session is protected after "
                f"{MAX_CONSECUTIVE_LOSSES} consecutive losses."
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
            "message": "Prediction returned NO TRADE.",
        }

    if direction not in {
        "OVER",
        "UNDER",
    }:

        raise HTTPException(
            status_code=400,
            detail="Direction must be OVER or UNDER.",
        )

    barrier = validate_barrier_for_direction(
        direction,
        barrier,
    )

    if account_type == "real":

        if not REAL_TRADING_ENABLED:
            raise HTTPException(
                status_code=403,
                detail="Real trading is disabled.",
            )

        if not session.get(
            "real_market_mode",
            False,
        ):
            raise HTTPException(
                status_code=403,
                detail="Real market mode is not enabled for this session.",
            )

    balance = session[
        "balances"
    ].get(
        account_type
    )

    if (
        balance is not None
        and amount > balance
    ):
        raise HTTPException(
            status_code=400,
            detail="Stake exceeds available balance.",
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

    proposal_price = safe_float(
        proposal.get(
            "ask_price"
        )
        or proposal.get(
            "buy_price"
        )
    )

    if proposal_price is None:
        proposal_price = amount

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
        "started_at": iso_now(),
    }

    return await buy_and_monitor(
        session,
        account_type,
        proposal_id,
        proposal_price,
        trade_metadata,
    )


# ============================================================
# MODELS
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
# HEALTH / SESSION
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "ok",
        "app": "Fixed Risk Booster",
        "version": APP_VERSION,
        "real_trading_enabled": REAL_TRADING_ENABLED,
    }


@app.get("/health")
async def health():

    return {
        "status": "ok",
        "version": APP_VERSION,
        "timestamp": iso_now(),
        "sessions": len(USER_SESSIONS),
        "prediction_scan": "single-flight",
    }


@app.post("/api/session")
async def create_session():

    session = new_session()

    return {
        "status": "ok",
        "session_id": session[
            "session_id"
        ],
    }


# ============================================================
# IMPORTANT COMPATIBILITY ROUTE
#
# The current frontend calls:
#
#     POST /api/session/register
#
# The previous backend only exposed:
#
#     POST /api/session
#
# That mismatch caused the 404 Not Found.
#
# This route now accepts the frontend's session_id and registers
# it on the backend.
# ============================================================

@app.post("/api/session/register")
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


@app.get("/api/session/status/{session_id}")
async def session_status(
    session_id: str,
):

    session = get_session(
        session_id
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
        "account": session[
            "account"
        ],
        "accounts": {
            "demo": bool(
                session[
                    "accounts"
                ].get("demo")
            ),
            "real": bool(
                session[
                    "accounts"
                ].get("real")
            ),
        },
        "balances": session[
            "balances"
        ],
        "trading": session[
            "trading"
        ],
        "real_trading_enabled": REAL_TRADING_ENABLED,
        "active_trade": session[
            "active_trade"
        ],
    }


# ============================================================
# OAUTH
# ============================================================

@app.get("/auth/deriv/login")
async def deriv_login(
    session_id: str = Query(...),
):

    session = ensure_session(
        session_id
    )

    if not DERIV_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="DERIV_CLIENT_ID is not configured.",
        )

    if not DERIV_REDIRECT_URI:
        raise HTTPException(
            status_code=500,
            detail="DERIV_REDIRECT_URI is not configured.",
        )

    cleanup_memory()

    verifier = create_pkce_verifier()

    challenge = create_pkce_challenge(
        verifier
    )

    state = secrets.token_urlsafe(
        32
    )

    OAUTH_STATES[state] = {
        "session_id": session[
            "session_id"
        ],
        "verifier": verifier,
        "created_at": time.time(),
    }

    params = {
        "response_type": "code",
        "client_id": DERIV_CLIENT_ID,
        "redirect_uri": DERIV_REDIRECT_URI,
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


@app.get("/auth/deriv/callback")
async def deriv_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):

    if error:

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(str(error))}",
                status_code=302,
            )

        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": error,
            },
        )

    if not code or not state:

        raise HTTPException(
            status_code=400,
            detail="Missing OAuth code or state.",
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
            timeout=20
        ) as client:

            response = await client.post(
                f"{DERIV_AUTH_BASE}/oauth2/token",
                data=token_payload,
                headers={
                    "Accept": "application/json"
                },
            )

    except httpx.HTTPError as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv OAuth token exchange failed: "
                f"{exc}"
            ),
        )

    if response.status_code >= 400:

        error_text = response.text[
            :1000
        ]

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(error_text)}",
                status_code=302,
            )

        raise HTTPException(
            status_code=502,
            detail="Deriv OAuth token exchange failed.",
        )

    try:

        token_data = response.json()

    except Exception:

        raise HTTPException(
            status_code=502,
            detail="Deriv OAuth returned invalid token JSON.",
        )

    session["access_token"] = (
        token_data.get(
            "access_token"
        )
    )

    session["refresh_token"] = (
        token_data.get(
            "refresh_token"
        )
    )

    expires_in = (
        safe_int(
            token_data.get(
                "expires_in"
            ),
            3600,
        )
        or 3600
    )

    session[
        "token_expires_at"
    ] = time.time() + expires_in

    if not session[
        "access_token"
    ]:

        raise HTTPException(
            status_code=502,
            detail="Deriv did not return an access token.",
        )

    try:

        await load_options_accounts(
            session
        )

        await refresh_all_balances(
            session
        )

        session["connected"] = True

        session[
            "connection_status"
        ] = "connected"

    except Exception as exc:

        session["connected"] = False

        session[
            "connection_status"
        ] = (
            "connected_but_account_load_failed: "
            f"{exc}"
        )

        if FRONTEND_ORIGIN:

            return RedirectResponse(
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(str(exc))}",
                status_code=302,
            )

        raise

    if FRONTEND_ORIGIN:

        return RedirectResponse(
            f"{FRONTEND_ORIGIN}"
            f"?deriv=connected"
            f"&session_id="
            f"{urllib.parse.quote(session['session_id'])}",
            status_code=302,
        )

    return {
        "status": "connected",
        "session_id": session[
            "session_id"
        ],
    }


# ============================================================
# ACCOUNT / MARKETS / PREDICTION
# ============================================================

@app.get("/api/account/diagnostics/{session_id}")
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
    }


@app.get("/api/account/balance/{session_id}")
async def account_balance(
    session_id: str,
):

    session = get_session(
        session_id
    )

    errors = {}

    if session["connected"]:

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
                ] = str(exc)

    return {
        "status": "ok",
        "connected": session[
            "connected"
        ],
        "accounts": session[
            "accounts"
        ],
        "balances": {
            "demo": session[
                "balances"
            ].get("demo"),
            "real": session[
                "balances"
            ].get("real"),
        },
        "currencies": session[
            "account_currencies"
        ],
        "errors": errors,
    }


@app.get("/api/markets")
async def markets(
    force_refresh: bool = False,
):

    try:

        eligible = await get_tradeable_symbols(
            force_refresh=force_refresh
        )

    except Exception as exc:

        return {
            "status": "error",
            "markets": [],
            "count": 0,
            "generated_at": iso_now(),
            "error": str(exc),
        }

    output = []

    try:

        async with public_ws_connection() as ws:

            for item in eligible:

                symbol = item[
                    "symbol"
                ]

                latest = None

                try:

                    tick_response = (
                        await ws_request_existing(
                            ws,
                            {
                                "ticks": symbol
                            },
                            expected_msg_types={
                                "tick"
                            },
                            timeout=8,
                        )
                    )

                    tick = tick_response.get(
                        "tick"
                    )

                    if isinstance(
                        tick,
                        dict,
                    ):
                        latest = safe_float(
                            tick.get(
                                "quote"
                            )
                        )

                except Exception:
                    pass

                output.append({
                    **item,
                    "asset": symbol,
                    "latest_price": latest,
                    "tradeable": True,
                })

    except Exception:

        output = [
            {
                **item,
                "asset": item[
                    "symbol"
                ],
                "latest_price": None,
                "tradeable": True,
            }
            for item in eligible
        ]

    return {
        "status": "ok",
        "markets": output,
        "count": len(output),
        "generated_at": iso_now(),
    }


@app.get("/api/market/prediction")
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
# STATS / HISTORY / TRADING STATUS
# ============================================================

@app.get("/api/stats/{session_id}")
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

    current = session[
        "demo_stats"
        if account == "demo"
        else "real_stats"
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

    return {
        "status": "ok",
        "account": account,
        "profit": round(
            float(
                current.get(
                    "profit",
                    0,
                )
            ),
            2,
        ),
        "trades": trades,
        "wins": wins,
        "losses": int(
            current.get(
                "losses",
                0,
            )
        ),
        "win_rate": round(
            wins / trades * 100
            if trades
            else 0.0,
            2,
        ),
    }


@app.get("/api/trades/{session_id}")
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

    return {
        "status": "ok",
        "account": account,
        "trades": list(
            session[
                "demo_history"
                if account == "demo"
                else "real_history"
            ]
        ),
    }


@app.get("/api/trading/status/{session_id}")
async def trading_status(
    session_id: str,
):

    session = get_session(
        session_id
    )

    return {
        "status": "ok",
        "trading": session[
            "trading"
        ],
        "account": session[
            "account"
        ],
        "real_market_mode": session[
            "real_market_mode"
        ],
        "active_trade": session[
            "active_trade"
        ],
        "consecutive_losses": session[
            "consecutive_losses"
        ],
        "max_consecutive_losses": (
            MAX_CONSECUTIVE_LOSSES
        ),
    }


# ============================================================
# START / STOP
# ============================================================

@app.post("/api/trading/start")
async def start_trading(
    request: TradingStartRequest,
):

    session = get_session(
        request.session_id
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

    if not session[
        "connected"
    ]:

        raise HTTPException(
            status_code=401,
            detail="Connect Deriv account first.",
        )

    if account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail="Real trading is disabled on the backend.",
            )

        if not session[
            "accounts"
        ].get("real"):

            raise HTTPException(
                status_code=400,
                detail="No real Options account is available.",
            )

    elif not session[
        "accounts"
    ].get("demo"):

        raise HTTPException(
            status_code=400,
            detail="No demo Options account is available.",
        )

    session["account"] = account
    session["stake"] = stake
    session["duration"] = duration

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
        "trading"
    ] = True

    return {
        "status": "ok",
        "trading": True,
        "account": account,
        "stake": stake,
        "duration": duration,
        "real_market_mode": session[
            "real_market_mode"
        ],
    }


@app.post("/api/trading/stop")
async def stop_trading(
    request: TradingStopRequest,
):

    session = get_session(
        request.session_id
    )

    session[
        "trading"
    ] = False

    return {
        "status": "ok",
        "trading": False,
        "active_trade": session.get(
            "active_trade"
        ),
        "message": (
            "Future trades stopped. "
            "Any already-open Deriv contract "
            "continues until it finishes."
        ),
    }


# ============================================================
# MANUAL TRADE
# ============================================================

@app.post("/api/trade")
async def manual_trade(
    request: TradeRequest,
):

    session = get_session(
        request.session_id
    )

    account = normalize_account_type(
        request.account
    )

    if not session[
        "connected"
    ]:

        raise HTTPException(
            status_code=401,
            detail="Connect Deriv account first.",
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
            "message": "NO TRADE signal.",
        }

    if direction not in {
        "OVER",
        "UNDER",
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "Direction must be OVER, "
                "UNDER or NO TRADE."
            ),
        )

    amount = validate_stake(
        request.amount
    )

    duration = validate_duration(
        request.duration
    )

    barrier = validate_barrier_for_direction(
        direction,
        request.barrier,
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
        "result": result,
    }


# ============================================================
# AUTO TRADE CURRENT PREDICTION
# ============================================================

@app.post("/api/trading/execute")
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
            detail="Connect Deriv account first.",
        )

    if not session[
        "trading"
    ]:

        raise HTTPException(
            status_code=400,
            detail="Trading is stopped.",
        )

    if session.get(
        "active_trade"
    ):

        raise HTTPException(
            status_code=409,
            detail="A trade is already active.",
        )

    if (
        session.get(
            "consecutive_losses",
            0,
        )
        >= MAX_CONSECUTIVE_LOSSES
    ):

        session[
            "trading"
        ] = False

        raise HTTPException(
            status_code=403,
            detail=(
                "Trading automatically stopped after "
                f"{MAX_CONSECUTIVE_LOSSES} consecutive losses."
            ),
        )

    account = session[
        "account"
    ]

    if account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail="Real trading is disabled.",
            )

        if not session[
            "real_market_mode"
        ]:

            raise HTTPException(
                status_code=403,
                detail="Real market mode is not enabled.",
            )

    # IMPORTANT:
    # This forces a fresh current market scan.
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

    asset = prediction.get(
        "asset"
    )

    tradeable = bool(
        prediction.get(
            "tradeable",
            False,
        )
    )

    if (
        direction == "NO TRADE"
        or not asset
        or not tradeable
    ):

        return {
            "status": "no_trade",
            "prediction": prediction,
            "message": (
                "Prediction did not meet "
                "the configured trading conditions."
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
        "prediction": prediction,
        "result": result,
    }


# ============================================================
# DISCONNECT
# ============================================================

@app.post("/api/disconnect")
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

    # Stats/history intentionally remain.
    return {
        "status": "ok",
        "connected": False,
        "trading": False,
        "active_trade": session.get(
            "active_trade"
        ),
    }


# ============================================================
# ERROR HANDLER
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request,
    exc: Exception,
):

    print(
        f"[ERROR] "
        f"{request.method} "
        f"{request.url}: "
        f"{exc}"
    )

    if isinstance(
        exc,
        HTTPException,
    ):

        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "detail": exc.detail,
            },
        )

    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "detail": "Internal server error.",
        },
    )
