# ============================================================
# FIXED RISK BOOSTER
# Deriv Options Multi-User Backend
# Version: 6.0.1
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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field


# ============================================================
# ENVIRONMENT
# ============================================================

APP_VERSION = "6.0.1"

FRONTEND_ORIGIN = (
    os.getenv("FRONTEND_ORIGIN", "").strip().rstrip("/")
)

DERIV_CLIENT_ID = os.getenv("DERIV_CLIENT_ID", "").strip()
DERIV_REDIRECT_URI = os.getenv("DERIV_REDIRECT_URI", "").strip()

DERIV_REST_BASE = "https://api.derivws.com"
DERIV_AUTH_BASE = "https://auth.deriv.com"

DERIV_PUBLIC_WS = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

DERIV_OAUTH_SCOPE = "trade"

REAL_TRADING_ENABLED = (
    os.getenv("REAL_TRADING_ENABLED", "false").lower()
    in {"1", "true", "yes", "on"}
)

HISTORY_TICKS = int(os.getenv("HISTORY_TICKS", "2500"))
PREDICTION_HORIZON_TICKS = int(
    os.getenv("PREDICTION_HORIZON_TICKS", "5")
)
MIN_BACKTEST_SAMPLES = int(
    os.getenv("MIN_BACKTEST_SAMPLES", "150")
)
MIN_EDGE_ACCURACY = float(
    os.getenv("MIN_EDGE_ACCURACY", "58")
)
MIN_CONFIDENCE_TO_TRADE = float(
    os.getenv("MIN_CONFIDENCE_TO_TRADE", "62")
)
MAX_SYMBOLS_TO_ANALYZE = int(
    os.getenv("MAX_SYMBOLS_TO_ANALYZE", "10")
)
PREDICTION_CACHE_SECONDS = float(
    os.getenv("PREDICTION_CACHE_SECONDS", "5")
)

DEFAULT_DURATION = int(
    os.getenv("DEFAULT_DURATION", "5")
)

DEFAULT_DURATION_UNIT = os.getenv(
    "DEFAULT_DURATION_UNIT",
    "t",
).strip() or "t"

DEFAULT_BARRIER = int(
    os.getenv("DEFAULT_BARRIER", "5")
)

MAX_STAKE = float(
    os.getenv("MAX_STAKE", "1000")
)

MAX_HISTORY_RECORDS = 100

MAX_CONSECUTIVE_LOSSES = 3


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Fixed Risk Booster API",
    version=APP_VERSION,
    description="Multi-user Deriv Options trading backend.",
)


# ============================================================
# CORS
# ============================================================
#
# During deployment, allowing "*" makes the API reachable from
# the Vercel frontend even if FRONTEND_ORIGIN has not yet been
# updated to the exact Vercel hostname.
#
# We do NOT use credentials/cookies for this architecture.
# Authentication is handled through server-side session IDs and
# Deriv OAuth.
#
# Once everything is confirmed working, FRONTEND_ORIGIN can be
# tightened to the exact Vercel domain.
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=86400,
)


# ============================================================
# IN-MEMORY STORAGE
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}
OAUTH_STATES: Dict[str, Dict[str, Any]] = {}
PREDICTION_CACHE: Dict[str, Dict[str, Any]] = {}


# ============================================================
# MODELS
# ============================================================

class TradingStartRequest(BaseModel):
    session_id: str = Field(
        min_length=20,
        max_length=256,
    )

    account: str = Field(
        default="demo",
        pattern="^(demo|real)$",
    )

    stake: float = Field(
        default=2,
        gt=0,
        le=MAX_STAKE,
    )

    duration: int = Field(
        default=DEFAULT_DURATION,
        ge=1,
        le=100,
    )

    real_market_mode: bool = False


class TradingStopRequest(BaseModel):
    session_id: str = Field(
        min_length=20,
        max_length=256,
    )


class TradeRequest(BaseModel):
    session_id: str = Field(
        min_length=20,
        max_length=256,
    )

    asset: str = Field(
        min_length=1,
        max_length=100,
    )

    direction: str = Field(
        pattern="^(OVER|UNDER|NO TRADE)$",
    )

    amount: float = Field(
        gt=0,
        le=MAX_STAKE,
    )

    account: str = Field(
        default="demo",
        pattern="^(demo|real)$",
    )

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
    session_id: str = Field(
        min_length=20,
        max_length=256,
    )


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_session_id() -> str:
    return secrets.token_urlsafe(32)


def random_req_id() -> int:
    return secrets.randbelow(900000) + 100000


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:
    try:
        if value is None:
            return default

        if isinstance(value, bool):
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def safe_int(
    value: Any,
    default: Optional[int] = None,
) -> Optional[int]:
    try:
        if value is None:
            return default

        return int(float(value))

    except (TypeError, ValueError):
        return default


def clamp(
    value: float,
    low: float,
    high: float,
) -> float:
    return max(low, min(high, value))


def clean_symbol(value: Any) -> str:
    return str(value or "").strip()


def require_session(
    session_id: str,
) -> Dict[str, Any]:

    session = USER_SESSIONS.get(session_id)

    if not session:
        raise HTTPException(
            status_code=404,
            detail="Session not found or expired.",
        )

    return session


def is_connected(
    session: Dict[str, Any],
) -> bool:

    return bool(
        session.get("connected")
        and session.get("access_token")
    )


def account_id_for(
    session: Dict[str, Any],
    account: str,
) -> Optional[str]:

    return session.get(
        "accounts",
        {},
    ).get(account)


def account_currency(
    session: Dict[str, Any],
    account: str,
) -> str:

    return (
        session.get(
            "account_currencies",
            {},
        ).get(account)
        or "USD"
    )


def sanitize_accounts(
    session: Dict[str, Any],
) -> Dict[str, bool]:

    accounts = session.get(
        "accounts",
        {}
    )

    return {
        "demo": bool(accounts.get("demo")),
        "real": bool(accounts.get("real")),
    }


# ============================================================
# SESSION CREATION
# ============================================================

def create_session() -> Tuple[str, Dict[str, Any]]:

    session_id = create_session_id()

    session = {
        "session_id": session_id,

        "created_at": utc_now_iso(),
        "last_activity": utc_now_iso(),

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

        "demo_stats": {
            "profit": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
        },

        "real_stats": {
            "profit": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
        },

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

        "trade_lock": asyncio.Lock(),

        "last_prediction": None,
    }

    USER_SESSIONS[session_id] = session

    return session_id, session


# ============================================================
# HEALTH
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "ok",
        "service": "Fixed Risk Booster",
        "version": APP_VERSION,
        "deriv_options": True,
        "real_trading_enabled": REAL_TRADING_ENABLED,
        "time": utc_now_iso(),
    }


@app.head("/")
async def root_head():
    return None


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "service": "Fixed Risk Booster",
        "version": APP_VERSION,
        "deriv_options": True,
        "real_trading_enabled": REAL_TRADING_ENABLED,
        "time": utc_now_iso(),
    }


@app.head("/health")
async def health_head():
    return None


# ============================================================
# SESSION API
# ============================================================

@app.post("/api/session/register")
async def register_session():

    session_id, session = create_session()

    return {
        "status": "registered",
        "session_id": session_id,
        "connected": session["connected"],
        "connection_status": session["connection_status"],
        "balances": session["balances"],
        "accounts": sanitize_accounts(session),
        "message": (
            "Application session registered. "
            "Connect your own Deriv account to access "
            "your Demo and Real Options accounts."
        ),
    }


@app.get("/api/session/status/{session_id}")
async def session_status(
    session_id: str,
):

    session = require_session(session_id)

    session["last_activity"] = utc_now_iso()

    return {
        "status": "ok",
        "session_id": session_id,
        "connected": session["connected"],
        "connection_status": session["connection_status"],
        "accounts": sanitize_accounts(session),
        "balances": session["balances"],
        "account": session["account"],
        "stake": session["stake"],
        "duration": session["duration"],
        "duration_unit": session["duration_unit"],
        "real_market_mode": session["real_market_mode"],
        "trading": session["trading"],
        "consecutive_losses": session["consecutive_losses"],
        "active_trade": session["active_trade"],
    }


# ============================================================
# PKCE
# ============================================================

def base64url_encode(
    value: bytes,
) -> str:

    return (
        base64.urlsafe_b64encode(value)
        .rstrip(b"=")
        .decode("ascii")
    )


def make_pkce() -> Tuple[str, str]:

    verifier = base64url_encode(
        secrets.token_bytes(32)
    )

    digest = hashlib.sha256(
        verifier.encode("ascii")
    ).digest()

    challenge = base64url_encode(digest)

    return verifier, challenge


def require_oauth_config():

    missing = []

    if not DERIV_CLIENT_ID:
        missing.append("DERIV_CLIENT_ID")

    if not DERIV_REDIRECT_URI:
        missing.append("DERIV_REDIRECT_URI")

    if not FRONTEND_ORIGIN:
        missing.append("FRONTEND_ORIGIN")

    if missing:

        raise HTTPException(
            status_code=500,
            detail=(
                "Missing backend environment variables: "
                + ", ".join(missing)
            ),
        )


# ============================================================
# DERIV OAUTH LOGIN
# ============================================================

@app.get("/auth/deriv/login")
async def deriv_login(
    session_id: str,
):

    require_oauth_config()

    session = require_session(
        session_id
    )

    verifier, challenge = make_pkce()

    state = secrets.token_urlsafe(32)

    OAUTH_STATES[state] = {
        "session_id": session_id,
        "code_verifier": verifier,
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
        + urllib.parse.urlencode(params)
    )

    session["connection_status"] = "oauth_pending"

    return {
        "status": "ok",
        "authorization_url": authorization_url,
    }


# ============================================================
# DERIV TOKEN EXCHANGE
# ============================================================

async def exchange_oauth_code(
    code: str,
    code_verifier: str,
) -> Dict[str, Any]:

    payload = {
        "grant_type": "authorization_code",
        "client_id": DERIV_CLIENT_ID,
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": DERIV_REDIRECT_URI,
    }

    async with httpx.AsyncClient(
        timeout=20
    ) as client:

        response = await client.post(
            f"{DERIV_AUTH_BASE}/oauth2/token",
            data=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": (
                    "application/x-www-form-urlencoded"
                ),
            },
        )

    if response.status_code >= 400:

        raise RuntimeError(
            "Deriv OAuth token exchange failed: "
            f"{response.text[:500]}"
        )

    return response.json()


# ============================================================
# DERIV REST
# ============================================================

async def deriv_rest_async(
    method: str,
    path: str,
    access_token: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    url = f"{DERIV_REST_BASE}{path}"

    async with httpx.AsyncClient(
        timeout=20
    ) as client:

        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=payload,
        )

    if response.status_code >= 400:

        raise RuntimeError(
            f"Deriv REST error {response.status_code}: "
            f"{response.text[:1000]}"
        )

    return response.json()


# ============================================================
# OPTIONS ACCOUNTS
# ============================================================

def extract_account_records(
    response: Dict[str, Any],
) -> List[Dict[str, Any]]:

    data = response.get("data")

    if isinstance(data, list):

        return [
            item
            for item in data
            if isinstance(item, dict)
        ]

    if isinstance(data, dict):

        for key in (
            "accounts",
            "items",
            "results",
            "data",
        ):

            value = data.get(key)

            if isinstance(value, list):

                return [
                    item
                    for item in value
                    if isinstance(item, dict)
                ]

        return [data]

    return []


def normalize_account_type(
    account: Dict[str, Any],
) -> str:

    value = str(
        account.get("account_type")
        or account.get("type")
        or account.get("environment")
        or ""
    ).lower()

    if "real" in value:
        return "real"

    if "demo" in value:
        return "demo"

    account_id = str(
        account.get("account_id")
        or account.get("id")
        or ""
    ).upper()

    if account_id.startswith("VRTC"):
        return "demo"

    return ""


def extract_account_id(
    account: Dict[str, Any],
) -> Optional[str]:

    value = (
        account.get("account_id")
        or account.get("accountId")
        or account.get("id")
        or account.get("loginid")
    )

    if value is None:
        return None

    return str(value)


async def get_deriv_accounts(
    access_token: str,
) -> List[Dict[str, Any]]:

    response = await deriv_rest_async(
        "GET",
        "/trading/v1/options/accounts",
        access_token,
    )

    return extract_account_records(response)


async def load_options_accounts(
    session: Dict[str, Any],
):

    token = session.get("access_token")

    if not token:

        raise RuntimeError(
            "No Deriv access token is available."
        )

    accounts = await get_deriv_accounts(
        token
    )

    demo_id = None
    real_id = None

    demo_currency = "USD"
    real_currency = "USD"

    for account in accounts:

        account_type = normalize_account_type(
            account
        )

        account_id = extract_account_id(
            account
        )

        if not account_id:
            continue

        currency = (
            account.get("currency")
            or account.get("currency_code")
            or "USD"
        )

        if account_type == "demo" and not demo_id:

            demo_id = account_id
            demo_currency = str(currency)

        elif account_type == "real" and not real_id:

            real_id = account_id
            real_currency = str(currency)

    session["accounts"]["demo"] = demo_id
    session["accounts"]["real"] = real_id

    session["account_currencies"]["demo"] = (
        demo_currency
    )

    session["account_currencies"]["real"] = (
        real_currency
    )

    return {
        "demo": demo_id,
        "real": real_id,
    }


# ============================================================
# OAUTH CALLBACK
# ============================================================

@app.get("/auth/deriv/callback")
async def deriv_callback(
    code: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):

    if error:

        frontend = (
            f"{FRONTEND_ORIGIN}"
            f"?deriv=error"
            f"&message="
            f"{urllib.parse.quote(error)}"
        )

        return RedirectResponse(
            frontend
        )

    if not code or not state:

        raise HTTPException(
            status_code=400,
            detail="Missing OAuth code or state.",
        )

    oauth = OAUTH_STATES.pop(
        state,
        None,
    )

    if not oauth:

        raise HTTPException(
            status_code=400,
            detail="Invalid or expired OAuth state.",
        )

    if (
        time.time()
        - oauth["created_at"]
        > 600
    ):

        raise HTTPException(
            status_code=400,
            detail="OAuth state expired.",
        )

    session_id = oauth["session_id"]

    session = require_session(
        session_id
    )

    try:

        token_data = await exchange_oauth_code(
            code=code,
            code_verifier=oauth[
                "code_verifier"
            ],
        )

        access_token = token_data.get(
            "access_token"
        )

        if not access_token:

            raise RuntimeError(
                "Deriv did not return an access token."
            )

        session["access_token"] = (
            access_token
        )

        session["refresh_token"] = (
            token_data.get("refresh_token")
        )

        expires_in = safe_int(
            token_data.get("expires_in"),
            3600,
        )

        session["token_expires_at"] = (
            time.time()
            + (
                expires_in
                or 3600
            )
        )

        await load_options_accounts(
            session
        )

        session["connected"] = True
        session["connection_status"] = "connected"
        session["last_activity"] = utc_now_iso()

        await refresh_all_balances(
            session
        )

        frontend_url = (
            f"{FRONTEND_ORIGIN}"
            f"?deriv=connected"
            f"&session_id="
            f"{urllib.parse.quote(session_id)}"
        )

    except Exception as exc:

        session["connected"] = False
        session["connection_status"] = "error"

        frontend_url = (
            f"{FRONTEND_ORIGIN}"
            f"?deriv=error"
            f"&message="
            f"{urllib.parse.quote(str(exc)[:300])}"
        )

    return RedirectResponse(
        frontend_url
    )


# ============================================================
# OTP
# ============================================================

async def get_deriv_otp(
    access_token: str,
    account_id: str,
) -> str:

    response = await deriv_rest_async(
        "POST",
        "/trading/v1/options/accounts/"
        f"{urllib.parse.quote(account_id, safe='')}/otp",
        access_token,
    )

    data = response.get(
        "data",
        {},
    )

    if not isinstance(data, dict):

        raise RuntimeError(
            "Invalid OTP response from Deriv."
        )

    ws_url = (
        data.get("url")
        or data.get("ws_url")
        or data.get("websocket_url")
    )

    if not ws_url:

        raise RuntimeError(
            "Deriv OTP response did not contain a WebSocket URL."
        )

    return str(ws_url)


# ============================================================
# DERIV WEBSOCKET HELPERS
# ============================================================

def payload_msg_type(
    payload: Dict[str, Any],
) -> str:

    if "ticks_history" in payload:
        return "history"

    if "ticks" in payload:
        return "tick"

    if "proposal" in payload:
        return "proposal"

    if "buy" in payload:
        return "buy"

    if "proposal_open_contract" in payload:
        return "proposal_open_contract"

    if "balance" in payload:
        return "balance"

    if "active_symbols" in payload:
        return "active_symbols"

    if "contracts_for" in payload:
        return "contracts_for"

    return ""


async def ws_request(
    ws_url: str,
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

    req_id = payload.get(
        "req_id",
        random_req_id(),
    )

    payload = dict(payload)
    payload["req_id"] = req_id

    expected_type = payload_msg_type(
        payload
    )

    async with websockets.connect(
        ws_url,
        open_timeout=10,
        close_timeout=5,
        ping_interval=20,
        ping_timeout=10,
        max_size=8 * 1024 * 1024,
    ) as ws:

        await ws.send(
            json.dumps(payload)
        )

        deadline = (
            time.monotonic()
            + timeout
        )

        while time.monotonic() < deadline:

            remaining = max(
                0.5,
                deadline
                - time.monotonic(),
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            data = json.loads(raw)

            if data.get("error"):

                error = data["error"]

                raise RuntimeError(
                    error.get(
                        "message",
                        "Deriv WebSocket error.",
                    )
                )

            if (
                expected_type
                and data.get("msg_type")
                == expected_type
            ):

                return data

            if (
                data.get("req_id") == req_id
                and not expected_type
            ):

                return data

        raise TimeoutError(
            "Timed out waiting for Deriv WebSocket response."
        )


async def authenticated_ws_request(
    access_token: str,
    account_id: str,
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

    ws_url = await get_deriv_otp(
        access_token,
        account_id,
    )

    return await ws_request(
        ws_url,
        payload,
        timeout=timeout,
    )


# ============================================================
# BALANCE
# ============================================================

async def get_account_balance(
    access_token: str,
    account_id: str,
) -> Optional[float]:

    response = await authenticated_ws_request(
        access_token=access_token,
        account_id=account_id,
        payload={
            "balance": 1,
        },
        timeout=15,
    )

    balance_data = response.get(
        "balance",
        {},
    )

    if not isinstance(
        balance_data,
        dict,
    ):

        return None

    return safe_float(
        balance_data.get("balance")
    )


async def refresh_account_balance(
    session: Dict[str, Any],
    account: str,
) -> Optional[float]:

    token = session.get(
        "access_token"
    )

    account_id = account_id_for(
        session,
        account,
    )

    if not token or not account_id:
        return None

    try:

        balance = await get_account_balance(
            token,
            account_id,
        )

        session["balances"][
            account
        ] = balance

        return balance

    except Exception:

        return session[
            "balances"
        ].get(account)


async def refresh_all_balances(
    session: Dict[str, Any],
):

    await refresh_account_balance(
        session,
        "demo",
    )

    if session["accounts"].get("real"):

        await refresh_account_balance(
            session,
            "real",
        )


# ============================================================
# PUBLIC MARKET DATA
# ============================================================

async def public_ws_request(
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

    return await ws_request(
        DERIV_PUBLIC_WS,
        payload,
        timeout=timeout,
    )


async def get_active_symbols() -> List[Dict[str, Any]]:

    response = await public_ws_request(
        {
            "active_symbols": "brief",
        },
        timeout=20,
    )

    data = response.get(
        "active_symbols",
        [],
    )

    if not isinstance(data, list):
        return []

    return [
        item
        for item in data
        if isinstance(item, dict)
    ]


def symbol_is_trade_candidate(
    symbol: Dict[str, Any],
) -> bool:

    code = clean_symbol(
        symbol.get("symbol")
    )

    if not code:
        return False

    market = str(
        symbol.get("market")
        or symbol.get("market_display_name")
        or ""
    ).lower()

    symbol_type = str(
        symbol.get("symbol_type")
        or ""
    ).lower()

    exchange_open = symbol.get(
        "exchange_is_open",
        True,
    )

    if exchange_open is False:
        return False

    if (
        "synthetic" in market
        or "derived" in market
        or "synthetic" in symbol_type
        or "derived" in symbol_type
    ):

        return True

    return True


async def get_tick_history(
    symbol: str,
    count: int = HISTORY_TICKS,
) -> List[float]:

    count = int(
        clamp(
            count,
            50,
            10000,
        )
    )

    response = await public_ws_request(
        {
            "ticks_history": symbol,
            "count": count,
            "end": "latest",
            "style": "ticks",
        },
        timeout=25,
    )

    history = response.get(
        "history",
        {},
    )

    if not isinstance(
        history,
        dict,
    ):

        return []

    prices = history.get(
        "prices",
        [],
    )

    if not isinstance(
        prices,
        list,
    ):

        return []

    values = []

    for price in prices:

        number = safe_float(price)

        if number is not None:
            values.append(number)

    return values


async def get_latest_tick(
    symbol: str,
) -> Optional[float]:

    response = await public_ws_request(
        {
            "ticks": symbol,
        },
        timeout=10,
    )

    tick = response.get(
        "tick",
        {},
    )

    if not isinstance(
        tick,
        dict,
    ):

        return None

    return safe_float(
        tick.get("quote")
    )


# ============================================================
# DIGIT EXTRACTION
# ============================================================

def decimal_places(
    value: float,
) -> int:

    text = (
        f"{value:.10f}"
        .rstrip("0")
    )

    if "." not in text:
        return 0

    return len(
        text.split(".")[1]
    )


def last_digit(
    quote: float,
    pip: Optional[float] = None,
) -> int:

    if pip is not None and pip > 0:

        places = max(
            0,
            int(
                round(
                    -math.log10(pip)
                )
            ),
        )

    else:

        places = decimal_places(
            quote
        )

    if places <= 0:

        scaled = int(
            round(quote)
        )

    else:

        scaled = int(
            round(
                quote
                * (10 ** places)
            )
        )

    return abs(scaled) % 10


def extract_digits(
    prices: List[float],
    pip: Optional[float] = None,
) -> List[int]:

    return [
        last_digit(
            price,
            pip,
        )
        for price in prices
    ]


# ============================================================
# TECHNICAL HELPERS
# ============================================================

def ema(
    values: List[float],
    period: int,
) -> List[float]:

    if not values:
        return []

    period = max(
        2,
        min(
            period,
            len(values),
        ),
    )

    multiplier = 2 / (
        period + 1
    )

    result = [
        values[0]
    ]

    for value in values[1:]:

        result.append(
            (
                value
                - result[-1]
            )
            * multiplier
            + result[-1]
        )

    return result


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

        diff = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(diff, 0)
        )

        losses.append(
            max(-diff, 0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains),
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return 100 - (
        100 / (1 + rs)
    )


def calculate_volatility(
    values: List[float],
) -> Optional[float]:

    if len(values) < 2:
        return None

    returns = []

    for previous, current in zip(
        values[:-1],
        values[1:],
    ):

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
    )


# ============================================================
# DIGIT STRATEGY
# ============================================================

def classify_digit(
    digit: int,
    barrier: int,
) -> Optional[str]:

    if digit > barrier:
        return "OVER"

    if digit < barrier:
        return "UNDER"

    return None


def prediction_from_digits(
    digits: List[int],
    barrier: int,
) -> Dict[str, Any]:

    if not digits:

        return {
            "direction": "NO TRADE",
            "confidence": 0,
            "accuracy": 0,
            "samples": 0,
            "barrier": barrier,
            "reason": (
                "No digit data available."
            ),
        }

    over_count = sum(
        1
        for digit in digits
        if digit > barrier
    )

    under_count = sum(
        1
        for digit in digits
        if digit < barrier
    )

    total = len(digits)

    over_rate = (
        over_count / total
    ) * 100

    under_rate = (
        under_count / total
    ) * 100

    if over_rate > under_rate:

        direction = "OVER"
        probability = over_rate

    elif under_rate > over_rate:

        direction = "UNDER"
        probability = under_rate

    else:

        direction = "NO TRADE"
        probability = 50.0

    edge = abs(
        over_rate
        - under_rate
    )

    confidence = clamp(
        50 + edge * 2,
        0,
        99,
    )

    return {
        "direction": direction,
        "confidence": round(
            confidence,
            2,
        ),
        "accuracy": round(
            probability,
            2,
        ),
        "samples": total,
        "barrier": barrier,
        "over_rate": round(
            over_rate,
            2,
        ),
        "under_rate": round(
            under_rate,
            2,
        ),
        "reason": (
            "Historical digit distribution "
            "used for this live analysis."
        ),
    }


def walk_forward_backtest(
    digits: List[int],
    barrier: int,
    min_samples: int = MIN_BACKTEST_SAMPLES,
) -> Dict[str, Any]:

    if len(digits) < (
        min_samples + 10
    ):

        return {
            "samples": 0,
            "wins": 0,
            "losses": 0,
            "accuracy": 0.0,
            "valid": False,
        }

    wins = 0
    losses = 0
    samples = 0

    start = max(
        20,
        min_samples,
    )

    for index in range(
        start,
        len(digits) - 1,
    ):

        training = digits[:index]

        over = sum(
            1
            for digit in training
            if digit > barrier
        )

        under = sum(
            1
            for digit in training
            if digit < barrier
        )

        if over == under:
            continue

        prediction = (
            "OVER"
            if over > under
            else "UNDER"
        )

        actual = classify_digit(
            digits[index],
            barrier,
        )

        if actual is None:
            continue

        samples += 1

        if prediction == actual:
            wins += 1
        else:
            losses += 1

    accuracy = (
        wins / samples * 100
        if samples
        else 0.0
    )

    return {
        "samples": samples,
        "wins": wins,
        "losses": losses,
        "accuracy": round(
            accuracy,
            2,
        ),
        "valid": samples >= 1,
    }


# ============================================================
# MARKET ANALYSIS
# ============================================================

async def analyze_symbol(
    symbol_data: Dict[str, Any],
    barrier: int = DEFAULT_BARRIER,
) -> Optional[Dict[str, Any]]:

    symbol = clean_symbol(
        symbol_data.get("symbol")
    )

    if not symbol:
        return None

    try:

        prices = await get_tick_history(
            symbol,
            HISTORY_TICKS,
        )

        if len(prices) < 50:
            return None

        pip = safe_float(
            symbol_data.get("pip")
        )

        digits = extract_digits(
            prices,
            pip,
        )

        prediction = prediction_from_digits(
            digits,
            barrier,
        )

        backtest = walk_forward_backtest(
            digits,
            barrier,
            MIN_BACKTEST_SAMPLES,
        )

        rsi = calculate_rsi(
            prices
        )

        ema_fast = ema(
            prices,
            9,
        )

        ema_slow = ema(
            prices,
            21,
        )

        trend = "NEUTRAL"

        if ema_fast and ema_slow:

            if ema_fast[-1] > ema_slow[-1]:
                trend = "UP"

            elif ema_fast[-1] < ema_slow[-1]:
                trend = "DOWN"

        volatility = calculate_volatility(
            prices
        )

        latest = prices[-1]

        tradeable = (
            prediction["direction"]
            != "NO TRADE"
            and prediction["confidence"]
            >= MIN_CONFIDENCE_TO_TRADE
            and (
                backtest["accuracy"]
                >= MIN_EDGE_ACCURACY
                if backtest["samples"] > 0
                else False
            )
        )

        if not backtest["valid"]:
            tradeable = False

        return {
            "asset": symbol,
            "display_name": (
                symbol_data.get(
                    "display_name"
                )
                or symbol
            ),
            "direction": prediction[
                "direction"
            ],
            "confidence": prediction[
                "confidence"
            ],
            "historical_accuracy": backtest[
                "accuracy"
            ],
            "backtest_samples": backtest[
                "samples"
            ],
            "over_rate": prediction[
                "over_rate"
            ],
            "under_rate": prediction[
                "under_rate"
            ],
            "barrier": barrier,
            "latest_price": latest,
            "latest_digit": digits[-1],
            "trend": trend,
            "rsi": (
                round(
                    rsi,
                    2,
                )
                if rsi is not None
                else None
            ),
            "volatility": (
                round(
                    volatility,
                    8,
                )
                if volatility is not None
                else None
            ),
            "tradeable": tradeable,
            "sample_size": len(prices),
            "generated_at": utc_now_iso(),
        }

    except Exception:

        return None


async def analyze_live_markets(
    force_fresh: bool = False,
    barrier: int = DEFAULT_BARRIER,
) -> List[Dict[str, Any]]:

    cache_key = (
        f"all:{barrier}"
    )

    cached = PREDICTION_CACHE.get(
        cache_key
    )

    if (
        not force_fresh
        and cached
        and (
            time.time()
            - cached["timestamp"]
            < PREDICTION_CACHE_SECONDS
        )
    ):

        return cached["data"]

    symbols = await get_active_symbols()

    candidates = [
        symbol
        for symbol in symbols
        if symbol_is_trade_candidate(symbol)
    ]

    candidates = candidates[
        :MAX_SYMBOLS_TO_ANALYZE
    ]

    tasks = [
        analyze_symbol(
            symbol,
            barrier,
        )
        for symbol in candidates
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    cleaned = [
        result
        for result in results
        if isinstance(
            result,
            dict,
        )
    ]

    cleaned.sort(
        key=lambda item: (
            bool(
                item.get(
                    "tradeable"
                )
            ),
            item.get(
                "confidence",
                0,
            ),
            item.get(
                "historical_accuracy",
                0,
            ),
        ),
        reverse=True,
    )

    if not force_fresh:

        PREDICTION_CACHE[
            cache_key
        ] = {
            "timestamp": time.time(),
            "data": cleaned,
        }

    return cleaned


async def get_market_prediction(
    force_fresh: bool = False,
    barrier: int = DEFAULT_BARRIER,
) -> Dict[str, Any]:

    markets = await analyze_live_markets(
        force_fresh=force_fresh,
        barrier=barrier,
    )

    if not markets:

        return {
            "direction": "NO TRADE",
            "asset": None,
            "confidence": 0,
            "historical_accuracy": 0,
            "tradeable": False,
            "markets": [],
            "reason": (
                "No usable market data was available."
            ),
        }

    tradeable = [
        market
        for market in markets
        if market.get("tradeable")
    ]

    if not tradeable:

        best = markets[0]

        return {
            **best,
            "tradeable": False,
            "direction": (
                best.get("direction")
                if best.get(
                    "confidence",
                    0,
                ) >= 50
                else "NO TRADE"
            ),
            "markets": markets,
            "reason": (
                "No market passed all trading filters."
            ),
        }

    best = tradeable[0]

    return {
        **best,
        "markets": markets,
        "reason": (
            "Market passed the configured "
            "confidence and backtest filters."
        ),
    }


# ============================================================
# PREDICTION ENDPOINT
# ============================================================

@app.get("/api/market/prediction")
async def market_prediction(
    barrier: int = Query(
        default=DEFAULT_BARRIER,
        ge=0,
        le=9,
    ),
):

    prediction = await get_market_prediction(
        force_fresh=True,
        barrier=barrier,
    )

    return {
        "status": "ok",
        "prediction": prediction,
        "generated_at": utc_now_iso(),
    }


# ============================================================
# MARKETS ENDPOINT
# ============================================================

@app.get("/api/markets")
async def markets_endpoint(
    barrier: int = Query(
        default=DEFAULT_BARRIER,
        ge=0,
        le=9,
    ),
):

    markets = await analyze_live_markets(
        force_fresh=True,
        barrier=barrier,
    )

    return {
        "status": "ok",
        "markets": markets,
        "count": len(markets),
        "generated_at": utc_now_iso(),
    }


# ============================================================
# STATS
# ============================================================

def stats_for_account(
    session: Dict[str, Any],
    account: str,
) -> Dict[str, Any]:

    if account == "real":
        return session["real_stats"]

    return session["demo_stats"]


def history_for_account(
    session: Dict[str, Any],
    account: str,
) -> List[Dict[str, Any]]:

    if account == "real":
        return session["real_history"]

    return session["demo_history"]


def update_account_stats(
    session: Dict[str, Any],
    account: str,
    profit: float,
):

    stats = stats_for_account(
        session,
        account,
    )

    stats["profit"] += profit
    stats["trades"] += 1

    if profit > 0:

        stats["wins"] += 1

    else:

        stats["losses"] += 1

    session["session_profit"] += profit
    session["session_trades"] += 1

    if profit > 0:

        session["session_wins"] += 1
        session["consecutive_losses"] = 0

    else:

        session["session_losses"] += 1
        session["consecutive_losses"] += 1


def add_history(
    session: Dict[str, Any],
    account: str,
    record: Dict[str, Any],
):

    history = history_for_account(
        session,
        account,
    )

    history.insert(
        0,
        record,
    )

    del history[
        MAX_HISTORY_RECORDS:
    ]


# ============================================================
# TRADE CONTRACT
# ============================================================

def direction_to_contract(
    direction: str,
) -> str:

    if direction == "OVER":
        return "DIGITOVER"

    if direction == "UNDER":
        return "DIGITUNDER"

    raise ValueError(
        "Direction must be OVER or UNDER."
    )


def contract_status_is_final(
    contract: Dict[str, Any],
) -> bool:

    status = str(
        contract.get("status")
        or ""
    ).lower()

    if status in {
        "won",
        "lost",
        "sold",
        "expired",
        "cancelled",
        "canceled",
    }:

        return True

    if contract.get("is_sold"):
        return True

    if contract.get("is_expired"):
        return True

    return False


def final_profit_from_contract(
    contract: Dict[str, Any],
) -> float:

    profit = safe_float(
        contract.get("profit")
    )

    if profit is not None:
        return profit

    buy_price = safe_float(
        contract.get("buy_price")
    )

    payout = safe_float(
        contract.get("payout")
    )

    if (
        buy_price is not None
        and payout is not None
    ):

        return payout - buy_price

    return 0.0


async def buy_and_monitor(
    session: Dict[str, Any],
    account: str,
    asset: str,
    direction: str,
    amount: float,
    duration: int,
    barrier: int,
) -> Dict[str, Any]:

    token = session.get(
        "access_token"
    )

    account_id = account_id_for(
        session,
        account,
    )

    if not token:

        raise RuntimeError(
            "Deriv account is not connected."
        )

    if not account_id:

        raise RuntimeError(
            f"No {account} Options account is available."
        )

    contract_type = direction_to_contract(
        direction
    )

    currency = account_currency(
        session,
        account,
    )

    ws_url = await get_deriv_otp(
        token,
        account_id,
    )

    async with websockets.connect(
        ws_url,
        open_timeout=10,
        close_timeout=5,
        ping_interval=20,
        ping_timeout=10,
        max_size=8 * 1024 * 1024,
    ) as ws:

        proposal_request = {
            "proposal": 1,
            "amount": amount,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": currency,
            "duration": duration,
            "duration_unit": DEFAULT_DURATION_UNIT,
            "underlying_symbol": asset,
            "barrier": str(barrier),
            "req_id": random_req_id(),
        }

        await ws.send(
            json.dumps(
                proposal_request
            )
        )

        proposal_response = None

        deadline = (
            time.monotonic()
            + 15
        )

        while time.monotonic() < deadline:

            remaining = max(
                0.5,
                deadline
                - time.monotonic(),
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            data = json.loads(raw)

            if data.get("error"):

                error = data["error"]

                raise RuntimeError(
                    error.get(
                        "message",
                        "Proposal request failed.",
                    )
                )

            if (
                data.get("msg_type")
                == "proposal"
            ):

                proposal_response = data
                break

        if not proposal_response:

            raise TimeoutError(
                "Timed out waiting for trade proposal."
            )

        proposal = proposal_response.get(
            "proposal",
            {},
        )

        if not isinstance(
            proposal,
            dict,
        ):

            raise RuntimeError(
                "Invalid proposal response."
            )

        proposal_id = proposal.get(
            "id"
        )

        ask_price = safe_float(
            proposal.get(
                "ask_price"
            )
        )

        payout = safe_float(
            proposal.get(
                "payout"
            )
        )

        if not proposal_id:

            raise RuntimeError(
                "Deriv did not return a proposal ID."
            )

        if ask_price is None:

            raise RuntimeError(
                "Deriv did not return an ask price."
            )

        buy_request = {
            "buy": str(proposal_id),
            "price": ask_price,
            "req_id": random_req_id(),
        }

        await ws.send(
            json.dumps(
                buy_request
            )
        )

        buy_response = None

        deadline = (
            time.monotonic()
            + 15
        )

        while time.monotonic() < deadline:

            remaining = max(
                0.5,
                deadline
                - time.monotonic(),
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            data = json.loads(raw)

            if data.get("error"):

                error = data["error"]

                raise RuntimeError(
                    error.get(
                        "message",
                        "Buy request failed.",
                    )
                )

            if (
                data.get("msg_type")
                == "buy"
            ):

                buy_response = data
                break

        if not buy_response:

            raise TimeoutError(
                "Timed out waiting for Deriv buy confirmation."
            )

        buy_data = buy_response.get(
            "buy",
            {},
        )

        if not isinstance(
            buy_data,
            dict,
        ):

            raise RuntimeError(
                "Invalid buy response."
            )

        contract_id = buy_data.get(
            "contract_id"
        )

        if not contract_id:

            raise RuntimeError(
                "Deriv did not return a contract ID."
            )

        buy_price = safe_float(
            buy_data.get(
                "buy_price"
            ),
            ask_price,
        )

        session["active_trade"] = {
            "contract_id": str(
                contract_id
            ),
            "asset": asset,
            "direction": direction,
            "contract_type": contract_type,
            "account": account,
            "amount": amount,
            "duration": duration,
            "duration_unit": DEFAULT_DURATION_UNIT,
            "barrier": barrier,
            "buy_price": buy_price,
            "proposal_payout": payout,
            "status": "open",
            "started_at": utc_now_iso(),
        }

        monitor_request = {
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
            "req_id": random_req_id(),
        }

        await ws.send(
            json.dumps(
                monitor_request
            )
        )

        final_contract = None
        subscription_id = None

        timeout_seconds = max(
            60,
            duration * 10,
        )

        deadline = (
            time.monotonic()
            + timeout_seconds
        )

        while time.monotonic() < deadline:

            remaining = max(
                0.5,
                deadline
                - time.monotonic(),
            )

            try:

                raw = await asyncio.wait_for(
                    ws.recv(),
                    timeout=remaining,
                )

            except asyncio.TimeoutError:

                break

            data = json.loads(raw)

            if data.get("error"):

                error = data["error"]

                raise RuntimeError(
                    error.get(
                        "message",
                        "Contract monitoring failed.",
                    )
                )

            if (
                data.get("msg_type")
                != "proposal_open_contract"
            ):

                continue

            contract = data.get(
                "proposal_open_contract",
                {},
            )

            if not isinstance(
                contract,
                dict,
            ):

                continue

            subscription = data.get(
                "subscription"
            )

            if isinstance(
                subscription,
                dict,
            ):

                subscription_id = (
                    subscription.get(
                        "id"
                    )
                )

            current_profit = safe_float(
                contract.get("profit"),
                0.0,
            )

            session["active_trade"].update(
                {
                    "status": (
                        contract.get(
                            "status"
                        )
                        or "open"
                    ),
                    "profit": current_profit,
                    "current_spot": (
                        contract.get(
                            "current_spot"
                        )
                    ),
                    "payout": (
                        contract.get(
                            "payout"
                        )
                    ),
                    "is_sold": bool(
                        contract.get(
                            "is_sold"
                        )
                    ),
                    "is_expired": bool(
                        contract.get(
                            "is_expired"
                        )
                    ),
                }
            )

            if contract_status_is_final(
                contract
            ):

                final_contract = contract
                break

        if subscription_id:

            try:

                await ws.send(
                    json.dumps(
                        {
                            "forget": subscription_id,
                            "req_id": random_req_id(),
                        }
                    )
                )

            except Exception:
                pass

        if not final_contract:

            session["active_trade"][
                "status"
            ] = "monitor_timeout"

            return {
                "status": "monitor_timeout",
                "contract_id": str(
                    contract_id
                ),
                "asset": asset,
                "direction": direction,
                "account": account,
                "amount": amount,
                "buy_price": buy_price,
                "proposal_payout": payout,
                "message": (
                    "Contract was purchased, "
                    "but monitoring timed out. "
                    "The contract was not automatically "
                    "classified as a win or loss."
                ),
            }

        profit = final_profit_from_contract(
            final_contract
        )

        final_status = str(
            final_contract.get(
                "status"
            )
            or ""
        ).lower()

        won = (
            final_status == "won"
            or profit > 0
        )

        result = {
            "status": (
                "won"
                if won
                else "lost"
            ),
            "contract_id": str(
                contract_id
            ),
            "asset": asset,
            "direction": direction,
            "contract_type": contract_type,
            "account": account,
            "amount": amount,
            "duration": duration,
            "duration_unit": DEFAULT_DURATION_UNIT,
            "barrier": barrier,
            "buy_price": buy_price,
            "payout": (
                safe_float(
                    final_contract.get(
                        "payout"
                    )
                )
                or payout
            ),
            "profit": profit,
            "final_status": final_status,
            "exit_spot": (
                final_contract.get(
                    "exit_spot"
                )
            ),
            "started_at": (
                session["active_trade"]
                .get("started_at")
            ),
            "completed_at": utc_now_iso(),
        }

        return result


# ============================================================
# EXECUTE TRADE
# ============================================================

@app.post("/api/trade/execute")
async def execute_trade(
    request: TradeRequest,
):

    session = require_session(
        request.session_id
    )

    if not is_connected(session):

        raise HTTPException(
            status_code=400,
            detail="Connect your Deriv account first.",
        )

    if request.account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled on the backend."
                ),
            )

        if not session.get(
            "real_market_mode"
        ):

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real Market Mode is not enabled."
                ),
            )

    if (
        session.get(
            "consecutive_losses",
            0,
        )
        >= MAX_CONSECUTIVE_LOSSES
    ):

        session["trading"] = False

        raise HTTPException(
            status_code=403,
            detail=(
                "Trading automatically stopped after "
                f"{MAX_CONSECUTIVE_LOSSES} consecutive losses."
            ),
        )

    if session.get(
        "active_trade"
    ):

        raise HTTPException(
            status_code=409,
            detail=(
                "An Options contract is already active."
            ),
        )

    if request.direction == "NO TRADE":

        return {
            "status": "no_trade",
            "message": (
                "NO TRADE prediction. "
                "No contract was purchased."
            ),
        }

    balance = session["balances"].get(
        request.account
    )

    if balance is None:

        balance = await refresh_account_balance(
            session,
            request.account,
        )

    if (
        balance is not None
        and request.amount > balance
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Stake is greater than the "
                "available account balance."
            ),
        )

    async with session["trade_lock"]:

        if session.get(
            "active_trade"
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Another trade became active "
                    "before execution."
                ),
            )

        prediction = await get_market_prediction(
            force_fresh=True,
            barrier=request.barrier,
        )

        predicted_direction = prediction.get(
            "direction"
        )

        predicted_asset = prediction.get(
            "asset"
        )

        if not prediction.get(
            "tradeable"
        ):

            return {
                "status": "no_trade",
                "prediction": prediction,
                "message": (
                    "Current live analysis did not "
                    "pass the trading filters."
                ),
            }

        if (
            predicted_asset
            != request.asset
            or predicted_direction
            != request.direction
        ):

            return {
                "status": "no_trade",
                "prediction": prediction,
                "message": (
                    "The requested trade no longer "
                    "matches the fresh live prediction. "
                    "No contract was purchased."
                ),
            }

        session["account"] = request.account
        session["stake"] = request.amount
        session["duration"] = request.duration

        session["last_prediction"] = prediction

        try:

            result = await buy_and_monitor(
                session=session,
                account=request.account,
                asset=request.asset,
                direction=request.direction,
                amount=request.amount,
                duration=request.duration,
                barrier=request.barrier,
            )

        except Exception as exc:

            session["active_trade"] = None

            raise HTTPException(
                status_code=502,
                detail=str(exc),
            )

        if result.get(
            "status"
        ) == "monitor_timeout":

            return {
                "status": "monitor_timeout",
                "trade": result,
                "prediction": prediction,
                "message": (
                    "The contract was purchased, "
                    "but its final result was not "
                    "confirmed by the monitor."
                ),
            }

        profit = safe_float(
            result.get("profit"),
            0.0,
        ) or 0.0

        update_account_stats(
            session,
            request.account,
            profit,
        )

        history_record = {
            **result,
            "prediction_confidence": (
                prediction.get(
                    "confidence"
                )
            ),
            "historical_accuracy": (
                prediction.get(
                    "historical_accuracy"
                )
            ),
        }

        add_history(
            session,
            request.account,
            history_record,
        )

        session["active_trade"] = None

        await refresh_account_balance(
            session,
            request.account,
        )

        if (
            session["consecutive_losses"]
            >= MAX_CONSECUTIVE_LOSSES
        ):

            session["trading"] = False

        return {
            "status": "completed",
            "trade": result,
            "prediction": prediction,
            "stats": stats_for_account(
                session,
                request.account,
            ),
            "balance": session[
                "balances"
            ].get(
                request.account
            ),
            "trading": session[
                "trading"
            ],
            "consecutive_losses": session[
                "consecutive_losses"
            ],
        }


# ============================================================
# START TRADING
# ============================================================

@app.post("/api/trading/start")
async def start_trading(
    request: TradingStartRequest,
):

    session = require_session(
        request.session_id
    )

    if not is_connected(session):

        raise HTTPException(
            status_code=400,
            detail=(
                "Connect your Deriv account before "
                "starting trading."
            ),
        )

    if request.account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled by "
                    "the backend."
                ),
            )

        if not request.real_market_mode:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real Market Mode confirmation is required."
                ),
            )

        if not session["accounts"].get(
            "real"
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "No Real Options account is available."
                ),
            )

        session["real_market_mode"] = True

    else:

        session["real_market_mode"] = False

    if (
        session["consecutive_losses"]
        >= MAX_CONSECUTIVE_LOSSES
    ):

        raise HTTPException(
            status_code=403,
            detail=(
                "Trading is locked after "
                f"{MAX_CONSECUTIVE_LOSSES} consecutive losses."
            ),
        )

    session["account"] = request.account
    session["stake"] = request.stake
    session["duration"] = request.duration
    session["duration_unit"] = (
        DEFAULT_DURATION_UNIT
    )

    session["trading"] = True

    return {
        "status": "started",
        "trading": True,
        "account": session[
            "account"
        ],
        "stake": session[
            "stake"
        ],
        "duration": session[
            "duration"
        ],
        "duration_unit": session[
            "duration_unit"
        ],
        "real_market_mode": session[
            "real_market_mode"
        ],
        "message": (
            "Trading started. The frontend should "
            "request the next trade only when there "
            "is no active contract."
        ),
    }


# ============================================================
# STOP TRADING
# ============================================================

@app.post("/api/trading/stop")
async def stop_trading(
    request: TradingStopRequest,
):

    session = require_session(
        request.session_id
    )

    session["trading"] = False

    active_trade = session.get(
        "active_trade"
    )

    if active_trade:

        message = (
            "New trades have been stopped. "
            "The already-purchased contract remains "
            "active and will continue to its Deriv outcome."
        )

    else:

        message = (
            "Trading stopped. No new contracts will "
            "be opened."
        )

    return {
        "status": "stopped",
        "trading": False,
        "active_trade": active_trade,
        "message": message,
    }


# ============================================================
# TRADING STATUS
# ============================================================

@app.get("/api/trading/status/{session_id}")
async def trading_status(
    session_id: str,
):

    session = require_session(
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
        "stake": session[
            "stake"
        ],
        "duration": session[
            "duration"
        ],
        "duration_unit": session[
            "duration_unit"
        ],
        "real_market_mode": session[
            "real_market_mode"
        ],
        "consecutive_losses": session[
            "consecutive_losses"
        ],
        "active_trade": session[
            "active_trade"
        ],
        "last_prediction": session[
            "last_prediction"
        ],
    }


# ============================================================
# BALANCE
# ============================================================

@app.get("/api/account/balance/{session_id}")
async def account_balance(
    session_id: str,
):

    session = require_session(
        session_id
    )

    if not is_connected(session):

        raise HTTPException(
            status_code=400,
            detail="Deriv account is not connected.",
        )

    await refresh_all_balances(
        session
    )

    return {
        "status": "ok",
        "balances": {
            "demo": session[
                "balances"
            ].get("demo"),
            "real": session[
                "balances"
            ].get("real"),
        },
    }


# ============================================================
# STATS ENDPOINT
# ============================================================

@app.get("/api/stats/{session_id}")
async def stats_endpoint(
    session_id: str,
    account: str = Query(
        default="demo",
        pattern="^(demo|real)$",
    ),
):

    session = require_session(
        session_id
    )

    stats = stats_for_account(
        session,
        account,
    )

    total = stats[
        "trades"
    ]

    win_rate = (
        stats["wins"]
        / total
        * 100
        if total
        else 0
    )

    return {
        "status": "ok",
        "account": account,
        "stats": {
            **stats,
            "win_rate": round(
                win_rate,
                2,
            ),
        },
    }


# ============================================================
# HISTORY ENDPOINT
# ============================================================

@app.get("/api/trades/{session_id}")
async def trades_endpoint(
    session_id: str,
    account: str = Query(
        default="demo",
        pattern="^(demo|real)$",
    ),
):

    session = require_session(
        session_id
    )

    return {
        "status": "ok",
        "account": account,
        "history": history_for_account(
            session,
            account,
        ),
    }


# ============================================================
# DASHBOARD
# ============================================================

@app.get("/api/dashboard/{session_id}")
async def dashboard(
    session_id: str,
):

    session = require_session(
        session_id
    )

    demo_stats = session[
        "demo_stats"
    ]

    real_stats = session[
        "real_stats"
    ]

    demo_total = demo_stats[
        "trades"
    ]

    real_total = real_stats[
        "trades"
    ]

    demo_win_rate = (
        demo_stats["wins"]
        / demo_total
        * 100
        if demo_total
        else 0
    )

    real_win_rate = (
        real_stats["wins"]
        / real_total
        * 100
        if real_total
        else 0
    )

    return {
        "status": "ok",
        "connected": session[
            "connected"
        ],
        "connection_status": session[
            "connection_status"
        ],
        "accounts": sanitize_accounts(
            session
        ),
        "balances": {
            "demo": session[
                "balances"
            ].get("demo"),
            "real": session[
                "balances"
            ].get("real"),
        },
        "account": session[
            "account"
        ],
        "trading": session[
            "trading"
        ],
        "stake": session[
            "stake"
        ],
        "duration": session[
            "duration"
        ],
        "duration_unit": session[
            "duration_unit"
        ],
        "real_market_mode": session[
            "real_market_mode"
        ],
        "session": {
            "profit": session[
                "session_profit"
            ],
            "trades": session[
                "session_trades"
            ],
            "wins": session[
                "session_wins"
            ],
            "losses": session[
                "session_losses"
            ],
            "consecutive_losses": session[
                "consecutive_losses"
            ],
        },
        "demo": {
            "stats": {
                **demo_stats,
                "win_rate": round(
                    demo_win_rate,
                    2,
                ),
            },
            "history": session[
                "demo_history"
            ],
        },
        "real": {
            "stats": {
                **real_stats,
                "win_rate": round(
                    real_win_rate,
                    2,
                ),
            },
            "history": session[
                "real_history"
            ],
        },
        "active_trade": session[
            "active_trade"
        ],
        "last_prediction": session[
            "last_prediction"
        ],
    }


# ============================================================
# DISCONNECT
# ============================================================

@app.post("/api/session/disconnect")
async def disconnect_session(
    request: DisconnectRequest,
):

    session = require_session(
        request.session_id
    )

    if session.get(
        "active_trade"
    ):

        session["connected"] = False

        session[
            "connection_status"
        ] = "disconnected_with_active_trade"

        session["access_token"] = None
        session["refresh_token"] = None
        session["token_expires_at"] = None

        session["trading"] = False

        return {
            "status": "disconnected",
            "trading": False,
            "active_trade": session[
                "active_trade"
            ],
            "message": (
                "Deriv connection removed. "
                "The existing contract record was preserved."
            ),
        }

    session["connected"] = False

    session[
        "connection_status"
    ] = "disconnected"

    session["access_token"] = None
    session["refresh_token"] = None
    session["token_expires_at"] = None

    session["accounts"] = {
        "demo": None,
        "real": None,
    }

    session["balances"] = {
        "demo": None,
        "real": None,
    }

    session["account_currencies"] = {
        "demo": "USD",
        "real": "USD",
    }

    session["trading"] = False
    session["real_market_mode"] = False

    return {
        "status": "disconnected",
        "trading": False,
        "active_trade": None,
        "message": (
            "Deriv account disconnected."
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
        "[Fixed Risk Booster] Unhandled exception:",
        repr(exc),
    )

    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": str(exc),
        },
    )


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup_event():

    print(
        f"[Fixed Risk Booster] v{APP_VERSION} starting..."
    )

    print(
        "[Fixed Risk Booster] Deriv OAuth:",
        bool(DERIV_CLIENT_ID),
    )

    print(
        "[Fixed Risk Booster] Redirect URI:",
        bool(DERIV_REDIRECT_URI),
    )

    print(
        "[Fixed Risk Booster] Frontend origin:",
        FRONTEND_ORIGIN or "(not configured)",
    )

    print(
        "[Fixed Risk Booster] Real trading:",
        (
            "ENABLED"
            if REAL_TRADING_ENABLED
            else "DISABLED"
        ),
    )

    print(
        "[Fixed Risk Booster] Default duration:",
        DEFAULT_DURATION,
        DEFAULT_DURATION_UNIT,
    )


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    import uvicorn

    port = int(
        os.getenv(
            "PORT",
            "8000",
        )
    )

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
    )
