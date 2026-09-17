import os
import asyncio
import base64
import hashlib
import json
import math
import secrets
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from urllib.parse import urlencode

from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse
from pydantic import BaseModel, Field

try:
    import websockets
except ImportError as exc:
    raise RuntimeError(
        "The websockets package is required. Add websockets to requirements.txt."
    ) from exc


# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "5.0.0-multi-user-options"

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*").strip()

DERIV_CLIENT_ID = os.getenv("DERIV_CLIENT_ID", "").strip()
DERIV_CLIENT_SECRET = os.getenv("DERIV_CLIENT_SECRET", "").strip()
DERIV_REDIRECT_URI = os.getenv("DERIV_REDIRECT_URI", "").strip()

DERIV_REST_BASE = "https://api.derivws.com"
DERIV_AUTH_BASE = "https://auth.deriv.com"

DERIV_PUBLIC_WS = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

DERIV_OAUTH_SCOPE = "trade"

# ------------------------------------------------------------
# Prediction settings
# ------------------------------------------------------------

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

PREDICTION_CACHE_SECONDS = int(
    os.getenv("PREDICTION_CACHE_SECONDS", "5")
)

DEFAULT_DURATION = int(
    os.getenv(
        "DEFAULT_DURATION",
        str(PREDICTION_HORIZON_TICKS)
    )
)

DEFAULT_DURATION_UNIT = os.getenv(
    "DEFAULT_DURATION_UNIT",
    "t"
).strip()

# ------------------------------------------------------------
# Safety
# ------------------------------------------------------------

REAL_TRADING_ENABLED = (
    os.getenv("REAL_TRADING_ENABLED", "true").lower()
    == "true"
)

MAX_HISTORY_PER_ACCOUNT = int(
    os.getenv("MAX_HISTORY_PER_ACCOUNT", "100")
)

MAX_STAKE = float(
    os.getenv("MAX_STAKE", "1000")
)

MAX_CONSECUTIVE_LOSSES = int(
    os.getenv("MAX_CONSECUTIVE_LOSSES", "3")
)


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Fixed Risk Booster API",
    version=APP_VERSION,
    description=(
        "Multi-user Deriv Options trading backend with "
        "OAuth authentication, Demo/Real account separation, "
        "live market analysis and fixed-risk execution."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=(
        ["*"]
        if FRONTEND_ORIGIN == "*"
        else [FRONTEND_ORIGIN]
    ),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# IN-MEMORY STATE
# ============================================================
#
# This version deliberately keeps state in memory while we develop.
#
# BEFORE PUBLIC PRODUCTION:
#   - move sessions into Redis/PostgreSQL
#   - encrypt OAuth tokens at rest
#   - add proper persistent authentication
#   - add token refresh handling
#
# Each session has its own:
#   OAuth token
#   Demo account
#   Real account
#   Demo stats
#   Real stats
#   Demo history
#   Real history
#   trading state
#   active trade
#
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}

OAUTH_STATES: Dict[str, Dict[str, Any]] = {}

PREDICTION_CACHE: Dict[str, Any] = {
    "expires_at": 0.0,
    "data": None,
}


# ============================================================
# REQUEST MODELS
# ============================================================

class SessionRegisterRequest(BaseModel):
    user_id: str = Field(
        min_length=8,
        max_length=128
    )


class TradingStartRequest(BaseModel):
    user_id: str = Field(
        min_length=8,
        max_length=128
    )

    account: str = Field(
        default="demo"
    )

    stake: float = Field(
        gt=0,
        le=MAX_STAKE
    )

    real_market_mode: bool = False

    duration: int = Field(
        default=DEFAULT_DURATION,
        gt=0,
        le=60
    )


class TradingStopRequest(BaseModel):
    user_id: str = Field(
        min_length=8,
        max_length=128
    )


class TradeRequest(BaseModel):
    user_id: str = Field(
        min_length=8,
        max_length=128
    )

    asset: str = Field(
        min_length=1,
        max_length=64
    )

    amount: float = Field(
        gt=0,
        le=MAX_STAKE
    )

    account: str = Field(
        default="demo"
    )

    duration: int = Field(
        default=DEFAULT_DURATION,
        gt=0,
        le=60
    )

    direction: str = Field(
        default="CALL",
        min_length=3,
        max_length=4
    )


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_session_id() -> str:
    return secrets.token_urlsafe(32)


def get_session(user_id: str) -> Dict[str, Any]:
    """
    Create a completely isolated session for a user.

    user_id is an application session identifier.
    It is NOT a Deriv account number.
    """

    if user_id not in USER_SESSIONS:
        USER_SESSIONS[user_id] = {
            "user_id": user_id,

            # Authentication
            "connected": False,
            "connection_status": "not_connected",
            "oauth_access_token": None,
            "oauth_refresh_token": None,
            "oauth_expires_at": None,

            # Deriv Options accounts
            "accounts": {
                "demo": None,
                "real": None,
            },

            # Current balances
            "balances": {
                "demo": None,
                "real": None,
            },

            # Permanent-ish in-memory statistics
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

            # Separate histories
            "demo_history": [],
            "real_history": [],

            # Trading state
            "trading": False,
            "account": "demo",
            "stake": 2.00,
            "duration": DEFAULT_DURATION,
            "real_market_mode": False,

            # Current session statistics
            "session_profit": 0.0,
            "session_trades": 0,
            "session_wins": 0,
            "session_losses": 0,

            "consecutive_losses": 0,

            # Currently executing/bought contract
            "active_trade": None,

            "session_started_at": None,
            "last_trade_at": None,

            # Prevent two trades from being executed
            # simultaneously for one user.
            "trade_lock": asyncio.Lock(),
        }

    return USER_SESSIONS[user_id]


def require_session(user_id: str) -> Dict[str, Any]:
    session = USER_SESSIONS.get(user_id)

    if not session:
        raise HTTPException(
            status_code=404,
            detail="Application session not found. Register the Mini App session first.",
        )

    return session


def validate_account(account: str) -> None:
    if account not in {"demo", "real"}:
        raise HTTPException(
            status_code=400,
            detail="account must be either 'demo' or 'real'",
        )


def safe_float(
    value: Any,
    default: Optional[float] = None
) -> Optional[float]:

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# DERIV REST
# ============================================================

def deriv_rest(
    method: str,
    path: str,
    access_token: str,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    url = f"{DERIV_REST_BASE}{path}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": "FixedRiskBooster/5.0",
    }

    data = None

    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = Request(
        url,
        method=method.upper(),
        headers=headers,
        data=data,
    )

    try:
        with urlopen(req, timeout=20) as response:

            raw = response.read().decode("utf-8")

            return (
                json.loads(raw)
                if raw
                else {}
            )

    except HTTPError as exc:

        raw = exc.read().decode(
            "utf-8",
            errors="replace"
        )

        try:
            detail = json.loads(raw)
        except Exception:
            detail = raw

        raise RuntimeError(
            f"Deriv REST {exc.code}: {detail}"
        ) from exc

    except (URLError, TimeoutError) as exc:

        raise RuntimeError(
            f"Deriv REST connection error: {exc}"
        ) from exc


def get_deriv_accounts(
    access_token: str
) -> List[Dict[str, Any]]:

    result = deriv_rest(
        "GET",
        "/trading/v1/options/accounts",
        access_token,
    )

    data = result.get(
        "data",
        result.get("accounts", [])
    )

    if isinstance(data, dict):
        data = [data]

    if not isinstance(data, list):
        raise RuntimeError(
            "Deriv returned an unexpected accounts response."
        )

    return data


def get_account_from_list(
    accounts: List[Dict[str, Any]],
    account_type: str
) -> Optional[Dict[str, Any]]:

    for account in accounts:

        if (
            str(
                account.get(
                    "account_type",
                    ""
                )
            ).lower()
            == account_type
        ):
            return account

    return None


def get_deriv_otp(
    access_token: str,
    account_id: str
) -> str:

    result = deriv_rest(
        "POST",
        f"/trading/v1/options/accounts/{account_id}/otp",
        access_token,
        body={},
    )

    data = result.get(
        "data",
        result
    )

    if not isinstance(data, dict):
        raise RuntimeError(
            "Deriv OTP response was not an object."
        )

    ws_url = (
        data.get("url")
        or data.get("websocket_url")
        or data.get("ws_url")
    )

    if not ws_url:
        raise RuntimeError(
            "Deriv OTP response did not contain a WebSocket URL."
        )

    return ws_url


# ============================================================
# DERIV OAUTH 2.0 + PKCE
# ============================================================

def make_pkce() -> tuple[str, str]:

    verifier = secrets.token_urlsafe(64)[:128]

    challenge_bytes = hashlib.sha256(
        verifier.encode("ascii")
    ).digest()

    challenge = (
        base64.urlsafe_b64encode(
            challenge_bytes
        )
        .rstrip(b"=")
        .decode("ascii")
    )

    return verifier, challenge


def require_oauth_config() -> None:

    missing = []

    if not DERIV_CLIENT_ID:
        missing.append("DERIV_CLIENT_ID")

    if not DERIV_REDIRECT_URI:
        missing.append("DERIV_REDIRECT_URI")

    if missing:

        raise HTTPException(
            status_code=503,
            detail=(
                "Deriv OAuth is not configured. "
                f"Missing: {', '.join(missing)}"
            ),
        )


@app.get("/auth/deriv/login")
async def deriv_login(user_id: str):

    require_oauth_config()

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="Missing application session ID.",
        )

    session = require_session(user_id)

    state = secrets.token_urlsafe(32)

    verifier, challenge = make_pkce()

    OAUTH_STATES[state] = {
        "user_id": user_id,
        "code_verifier": verifier,
        "created_at": time.time(),
    }

    # Remove stale OAuth states.
    cutoff = time.time() - 600

    for key in list(OAUTH_STATES):

        if (
            OAUTH_STATES[key]["created_at"]
            < cutoff
        ):
            del OAUTH_STATES[key]

    params = {
        "response_type": "code",
        "client_id": DERIV_CLIENT_ID,
        "redirect_uri": DERIV_REDIRECT_URI,
        "scope": DERIV_OAUTH_SCOPE,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }

    login_url = (
        f"{DERIV_AUTH_BASE}/oauth2/auth?"
        f"{urlencode(params)}"
    )

    session["connection_status"] = "oauth_pending"

    return RedirectResponse(
        login_url,
        status_code=302,
    )


def exchange_oauth_code(
    code: str,
    verifier: str
) -> Dict[str, Any]:

    body = urlencode(
        {
            "grant_type": "authorization_code",
            "client_id": DERIV_CLIENT_ID,
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": DERIV_REDIRECT_URI,
        }
    ).encode("utf-8")

    req = Request(
        f"{DERIV_AUTH_BASE}/oauth2/token",
        method="POST",
        headers={
            "Content-Type":
                "application/x-www-form-urlencoded",
            "User-Agent":
                "FixedRiskBooster/5.0",
        },
        data=body,
    )

    try:

        with urlopen(
            req,
            timeout=20
        ) as response:

            return json.loads(
                response.read().decode("utf-8")
            )

    except HTTPError as exc:

        raw = exc.read().decode(
            "utf-8",
            errors="replace"
        )

        raise RuntimeError(
            f"Deriv OAuth token exchange failed "
            f"({exc.code}): {raw}"
        ) from exc


@app.get("/auth/deriv/callback")
async def deriv_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):

    if error:

        return HTMLResponse(
            "<h2>Deriv connection cancelled</h2>"
            f"<p>{error}</p>"
            "<p>You can close this tab.</p>",
            status_code=400,
        )

    if not code or not state:

        return HTMLResponse(
            "<h2>Missing Deriv OAuth response.</h2>"
            "<p>You can close this tab.</p>",
            status_code=400,
        )

    oauth = OAUTH_STATES.pop(
        state,
        None
    )

    if not oauth:

        return HTMLResponse(
            "<h2>Invalid or expired OAuth state.</h2>"
            "<p>Please connect Deriv again.</p>",
            status_code=400,
        )

    if (
        time.time()
        - oauth["created_at"]
        > 600
    ):

        return HTMLResponse(
            "<h2>OAuth state expired.</h2>"
            "<p>Please connect Deriv again.</p>",
            status_code=400,
        )

    user_id = oauth["user_id"]

    try:

        session = require_session(user_id)

        token_result = exchange_oauth_code(
            code,
            oauth["code_verifier"]
        )

        access_token = token_result.get(
            "access_token"
        )

        if not access_token:

            raise RuntimeError(
                "Deriv did not return an access token."
            )

        # Keep credentials server-side only.
        session["oauth_access_token"] = (
            access_token
        )

        session["oauth_refresh_token"] = (
            token_result.get("refresh_token")
        )

        expires_in = safe_float(
            token_result.get("expires_in")
        )

        if expires_in:
            session["oauth_expires_at"] = (
                time.time() + expires_in
            )
        else:
            session["oauth_expires_at"] = None

        session["connected"] = True

        session["connection_status"] = (
            "authorised"
        )

        accounts = get_deriv_accounts(
            access_token
        )

        demo = get_account_from_list(
            accounts,
            "demo"
        )

        real = get_account_from_list(
            accounts,
            "real"
        )

        session["accounts"]["demo"] = demo
        session["accounts"]["real"] = real

        session["balances"]["demo"] = (
            safe_float(
                demo.get("balance")
            )
            if demo
            else None
        )

        session["balances"]["real"] = (
            safe_float(
                real.get("balance")
            )
            if real
            else None
        )

        # Never automatically switch to Real.
        session["account"] = "demo"

        if (
            FRONTEND_ORIGIN
            and FRONTEND_ORIGIN != "*"
        ):

            separator = (
                "&"
                if "?" in FRONTEND_ORIGIN
                else "?"
            )

            return RedirectResponse(
                (
                    f"{FRONTEND_ORIGIN}"
                    f"{separator}"
                    f"deriv=connected"
                    f"&user_id={user_id}"
                ),
                status_code=302,
            )

        return HTMLResponse(
            "<h2>Deriv connected successfully.</h2>"
            "<p>Your Deriv Options accounts are linked.</p>"
            "<p>You can close this tab.</p>",
            status_code=200,
        )

    except Exception as exc:

        return HTMLResponse(
            "<h2>Deriv connection failed</h2>"
            f"<p>{str(exc)[:500]}</p>"
            "<p>You can close this tab.</p>",
            status_code=500,
        )


# ============================================================
# DERIV WEBSOCKET HELPERS
# ============================================================

async def ws_request(
    ws_url: str,
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

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

        expected_type = (
            payload_msg_type(payload)
        )

        while (
            time.monotonic()
            < deadline
        ):

            remaining = max(
                0.5,
                deadline
                - time.monotonic()
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            data = json.loads(raw)

            if data.get("error"):

                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Deriv WebSocket error."
                    )
                )

            if (
                data.get("msg_type")
                == expected_type
            ):
                return data

        raise TimeoutError(
            "Timed out waiting for Deriv WebSocket response."
        )


async def public_ws_request(
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

    return await ws_request(
        DERIV_PUBLIC_WS,
        payload,
        timeout,
    )


def payload_msg_type(
    payload: Dict[str, Any]
) -> str:

    mapping = {
        "balance": "balance",
        "proposal": "proposal",
        "buy": "buy",
        "proposal_open_contract":
            "proposal_open_contract",
        "portfolio": "portfolio",
        "active_symbols": "active_symbols",
        "ticks_history": "history",
        "ticks": "tick",
    }

    for key, value in mapping.items():

        if key in payload:
            return value

    return ""


async def authenticated_ws_request(
    access_token: str,
    account_id: str,
    payload: Dict[str, Any],
    timeout: float = 15.0,
) -> Dict[str, Any]:

    ws_url = await asyncio.to_thread(
        get_deriv_otp,
        access_token,
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

        await ws.send(
            json.dumps(
                {
                    "authorize": access_token,
                    "req_id": 1,
                }
            )
        )

        deadline = (
            time.monotonic()
            + timeout
        )

        while (
            time.monotonic()
            < deadline
        ):

            remaining = max(
                0.5,
                deadline
                - time.monotonic()
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            auth_data = json.loads(raw)

            if auth_data.get("error"):

                raise RuntimeError(
                    auth_data["error"].get(
                        "message",
                        "Deriv authorization failed."
                    )
                )

            if (
                auth_data.get("msg_type")
                == "authorize"
            ):
                break

        else:

            raise TimeoutError(
                "Timed out authorizing Deriv WebSocket."
            )

        await ws.send(
            json.dumps(payload)
        )

        expected_type = (
            payload_msg_type(payload)
        )

        deadline = (
            time.monotonic()
            + timeout
        )

        while (
            time.monotonic()
            < deadline
        ):

            remaining = max(
                0.5,
                deadline
                - time.monotonic()
            )

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

            data = json.loads(raw)

            if data.get("error"):

                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Deriv WebSocket error."
                    )
                )

            if (
                data.get("msg_type")
                == expected_type
            ):
                return data

        raise TimeoutError(
            "Timed out waiting for Deriv WebSocket response."
        )


# ============================================================
# MARKET DATA
# ============================================================

async def get_active_symbols() -> List[Dict[str, Any]]:

    data = await public_ws_request(
        {
            "active_symbols": "brief",
            "contract_type": [
                "CALL",
                "PUT"
            ],
            "req_id": 1001,
        }
    )

    symbols = data.get(
        "active_symbols",
        []
    )

    return (
        symbols
        if isinstance(symbols, list)
        else []
    )


def symbol_name(
    item: Dict[str, Any]
) -> str:

    return (
        item.get(
            "underlying_symbol_name"
        )
        or item.get(
            "display_name"
        )
        or item.get(
            "underlying_symbol"
        )
        or item.get(
            "symbol"
        )
        or ""
    )


def symbol_code(
    item: Dict[str, Any]
) -> str:

    return (
        item.get(
            "underlying_symbol"
        )
        or item.get(
            "symbol"
        )
        or ""
    )


def select_candidate_symbols(
    symbols: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    candidates = []

    for item in symbols:

        code = symbol_code(item)

        market = str(
            item.get(
                "market",
                ""
            )
        ).lower()

        name = symbol_name(item)

        if not code:
            continue

        if market not in {
            "forex",
            "synthetic_index",
            "synthetic",
        }:
            continue

        if int(
            item.get(
                "is_trading_suspended",
                0
            )
            or 0
        ) == 1:
            continue

        if (
            int(
                item.get(
                    "exchange_is_open",
                    1
                )
                or 1
            )
            == 0
            and market == "forex"
        ):
            continue

        candidates.append(
            {
                "asset": code,
                "symbol": name or code,
                "market": market,
                "pip_size": item.get(
                    "pip_size"
                ),
            }
        )

    candidates.sort(
        key=lambda x: (
            0
            if x["market"] == "forex"
            else 1,
            x["symbol"],
        )
    )

    return candidates[
        :MAX_SYMBOLS_TO_ANALYZE
    ]


async def get_tick_history(
    symbol: str,
    count: int = HISTORY_TICKS,
) -> List[Dict[str, Any]]:

    data = await public_ws_request(
        {
            "ticks_history": symbol,
            "count": max(
                100,
                min(
                    count,
                    10000
                )
            ),
            "end": "latest",
            "style": "ticks",
            "req_id": 2001,
        },
        timeout=20,
    )

    history = data.get(
        "history",
        {}
    )

    prices = history.get(
        "prices",
        []
    )

    times = history.get(
        "times",
        []
    )

    result = []

    for price, epoch in zip(
        prices,
        times
    ):

        try:

            result.append(
                {
                    "price": float(price),
                    "epoch": int(epoch),
                }
            )

        except (
            TypeError,
            ValueError
        ):
            continue

    if len(result) < 100:

        raise RuntimeError(
            f"Insufficient tick history for {symbol}."
        )

    return result


async def get_latest_tick(
    symbol: str
) -> Dict[str, Any]:

    data = await public_ws_request(
        {
            "ticks": symbol,
            "subscribe": 0,
            "req_id": 2002,
        },
        timeout=10,
    )

    tick = data.get(
        "tick",
        {}
    )

    if not tick:
        raise RuntimeError(
            f"No current tick returned for {symbol}."
        )

    return {
        "price": float(
            tick["quote"]
        ),
        "epoch": int(
            tick["epoch"]
        ),
    }


# ============================================================
# PREDICTION FEATURES
# ============================================================

def ema(
    values: List[float],
    period: int
) -> float:

    if not values:
        return 0.0

    period = max(
        2,
        min(
            period,
            len(values)
        )
    )

    k = 2.0 / (
        period + 1.0
    )

    result = values[0]

    for value in values[1:]:

        result = (
            value * k
            + result * (1.0 - k)
        )

    return result


def rsi(
    values: List[float],
    period: int = 14
) -> float:

    if len(values) <= period:
        return 50.0

    gains = []
    losses = []

    for i in range(
        1,
        len(values)
    ):

        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(
                change,
                0.0
            )
        )

        losses.append(
            max(
                -change,
                0.0
            )
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
        len(gains)
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

        return (
            100.0
            if avg_gain > 0
            else 50.0
        )

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100.0
        - (
            100.0
            / (1.0 + rs)
        )
    )


def feature_snapshot(
    prices: List[float]
) -> Dict[str, float]:

    medium = prices[-50:]

    if len(prices) < 50:
        medium = prices

    e9 = ema(
        medium,
        9
    )

    e21 = ema(
        medium,
        21
    )

    current = prices[-1]

    lookback = min(
        10,
        len(prices) - 1
    )

    momentum_pct = (
        (
            current
            - prices[
                -1 - lookback
            ]
        )
        / prices[
            -1 - lookback
        ]
    ) * 100.0

    micro_window = min(
        8,
        len(prices)
    )

    micro = prices[
        -micro_window:
    ]

    micro_slope = 0.0

    if len(micro) >= 2:

        micro_slope = (
            (
                micro[-1]
                - micro[0]
            )
            / micro[0]
        ) * 100.0

    rsi_value = rsi(
        prices,
        14
    )

    returns = []

    for i in range(
        max(
            1,
            len(prices) - 30
        ),
        len(prices)
    ):

        if prices[i - 1] != 0:

            returns.append(
                (
                    (
                        prices[i]
                        - prices[i - 1]
                    )
                    / prices[i - 1]
                )
                * 100.0
            )

    volatility = (
        math.sqrt(
            sum(
                x * x
                for x in returns
            )
            / len(returns)
        )
        if returns
        else 0.0
    )

    return {
        "price": current,
        "ema9": e9,
        "ema21": e21,
        "ema_gap_pct": (
            (
                (e9 - e21)
                / current
            )
            * 100.0
            if current
            else 0.0
        ),
        "momentum_pct": momentum_pct,
        "micro_slope_pct":
            micro_slope,
        "rsi": rsi_value,
        "volatility_pct":
            volatility,
    }


def rule_direction(
    features: Dict[str, float]
) -> str:

    bullish = 0.0
    bearish = 0.0

    if (
        features["ema9"]
        > features["ema21"]
    ):
        bullish += 2.0

    elif (
        features["ema9"]
        < features["ema21"]
    ):
        bearish += 2.0

    if (
        features["momentum_pct"]
        > 0
    ):
        bullish += 2.0

    elif (
        features["momentum_pct"]
        < 0
    ):
        bearish += 2.0

    if (
        features["micro_slope_pct"]
        > 0
    ):
        bullish += 1.0

    elif (
        features["micro_slope_pct"]
        < 0
    ):
        bearish += 1.0

    if (
        52
        <= features["rsi"]
        <= 68
    ):
        bullish += 1.0

    elif (
        32
        <= features["rsi"]
        <= 48
    ):
        bearish += 1.0

    if bullish >= bearish + 2.0:
        return "CALL"

    if bearish >= bullish + 2.0:
        return "PUT"

    return "NO_TRADE"


# ============================================================
# BACKTEST
# ============================================================

def backtest_prediction(
    history: List[Dict[str, Any]],
    horizon: int = PREDICTION_HORIZON_TICKS,
) -> Dict[str, Any]:

    prices = [
        x["price"]
        for x in history
    ]

    minimum = (
        70 + horizon
    )

    if len(prices) < minimum:

        return {
            "samples": 0,
            "wins": 0,
            "losses": 0,
            "accuracy_pct": 0.0,
            "skipped": 0,
            "evaluated": False,
        }

    split = int(
        len(prices) * 0.70
    )

    start = max(
        50,
        split
    )

    wins = 0
    losses = 0
    skipped = 0

    for i in range(
        start,
        len(prices) - horizon
    ):

        window = prices[
            :i + 1
        ]

        features = feature_snapshot(
            window
        )

        direction = rule_direction(
            features
        )

        if direction == "NO_TRADE":

            skipped += 1
            continue

        future = prices[
            i + horizon
        ]

        current = prices[i]

        if direction == "CALL":
            correct = (
                future > current
            )
        else:
            correct = (
                future < current
            )

        if correct:
            wins += 1
        else:
            losses += 1

    total = (
        wins + losses
    )

    accuracy = (
        wins / total * 100.0
        if total
        else 0.0
    )

    return {
        "samples": total,
        "wins": wins,
        "losses": losses,
        "accuracy_pct":
            round(
                accuracy,
                2
            ),
        "skipped": skipped,
        "evaluated":
            total
            >= MIN_BACKTEST_SAMPLES,
    }


def calculate_confidence(
    backtest: Dict[str, Any],
    features: Dict[str, float],
    direction: str,
) -> float:

    if direction == "NO_TRADE":
        return 0.0

    base = 50.0

    if (
        backtest["samples"]
        >= MIN_BACKTEST_SAMPLES
    ):

        base += max(
            -10.0,
            min(
                25.0,
                backtest[
                    "accuracy_pct"
                ] - 50.0
            )
        )

    agreement = 0

    if direction == "CALL":

        agreement += (
            features["ema9"]
            > features["ema21"]
        )

        agreement += (
            features["momentum_pct"]
            > 0
        )

        agreement += (
            features[
                "micro_slope_pct"
            ]
            > 0
        )

        agreement += (
            52
            <= features["rsi"]
            <= 68
        )

    else:

        agreement += (
            features["ema9"]
            < features["ema21"]
        )

        agreement += (
            features["momentum_pct"]
            < 0
        )

        agreement += (
            features[
                "micro_slope_pct"
            ]
            < 0
        )

        agreement += (
            32
            <= features["rsi"]
            <= 48
        )

    base += (
        agreement * 4.0
    )

    if (
        features["volatility_pct"]
        < 0.00001
    ):
        base -= 10.0

    return round(
        max(
            0.0,
            min(
                95.0,
                base
            )
        ),
        1,
    )


# ============================================================
# LIVE PREDICTION ENGINE
# ============================================================

async def analyze_symbol(
    candidate: Dict[str, Any]
) -> Dict[str, Any]:

    symbol = candidate["asset"]

    # IMPORTANT:
    # Fresh history is collected on every uncached analysis.
    history = await get_tick_history(
        symbol,
        HISTORY_TICKS
    )

    prices = [
        x["price"]
        for x in history
    ]

    features = feature_snapshot(
        prices
    )

    direction = rule_direction(
        features
    )

    backtest = backtest_prediction(
        history,
        PREDICTION_HORIZON_TICKS
    )

    confidence = calculate_confidence(
        backtest,
        features,
        direction
    )

    # Get the newest tick AFTER the history
    # was collected.
    latest = await get_latest_tick(
        symbol
    )

    price_change = (
        (
            latest["price"]
            - features["price"]
        )
        / features["price"]
        * 100.0
        if features["price"]
        else 0.0
    )

    tradeable = (
        direction != "NO_TRADE"
        and backtest["evaluated"]
        and (
            backtest[
                "accuracy_pct"
            ]
            >= MIN_EDGE_ACCURACY
        )
        and (
            confidence
            >= MIN_CONFIDENCE_TO_TRADE
        )
    )

    return {
        "asset": symbol,
        "symbol": candidate["symbol"],
        "market": candidate["market"],

        "direction": direction,

        "confidence": confidence,

        "tradeable": tradeable,

        "historical_accuracy_pct":
            backtest[
                "accuracy_pct"
            ],

        "historical_samples":
            backtest["samples"],

        "historical_wins":
            backtest["wins"],

        "historical_losses":
            backtest["losses"],

        "skipped_historical":
            backtest["skipped"],

        "rsi":
            round(
                features["rsi"],
                2
            ),

        "ema9":
            features["ema9"],

        "ema21":
            features["ema21"],

        "momentum_pct":
            round(
                features[
                    "momentum_pct"
                ],
                6
            ),

        "micro_slope_pct":
            round(
                features[
                    "micro_slope_pct"
                ],
                6
            ),

        "volatility_pct":
            round(
                features[
                    "volatility_pct"
                ],
                6
            ),

        "price":
            latest["price"],

        "analysis_price":
            features["price"],

        "latest_price_change_pct":
            round(
                price_change,
                6
            ),

        "latest_tick_time":
            datetime.fromtimestamp(
                latest["epoch"],
                tz=timezone.utc
            ).isoformat(),

        "reason": (
            "Fresh Deriv tick history was "
            "evaluated using the transparent "
            "directional rule. "
            f"Out-of-sample accuracy: "
            f"{backtest['accuracy_pct']:.2f}% "
            f"over {backtest['samples']} "
            "directional samples."
        ),
    }


async def analyze_live_markets() -> Dict[str, Any]:

    global PREDICTION_CACHE

    now = time.time()

    if (
        PREDICTION_CACHE["data"]
        is not None
        and now
        < PREDICTION_CACHE[
            "expires_at"
        ]
    ):
        return PREDICTION_CACHE[
            "data"
        ]

    symbols = await get_active_symbols()

    candidates = select_candidate_symbols(
        symbols
    )

    if not candidates:

        raise RuntimeError(
            "Deriv returned no eligible active CALL/PUT markets."
        )

    analyses = []
    errors = []

    for candidate in candidates:

        try:

            analyses.append(
                await analyze_symbol(
                    candidate
                )
            )

        except Exception as exc:

            errors.append(
                {
                    "symbol":
                        candidate["asset"],
                    "error":
                        str(exc)[:240],
                }
            )

    if not analyses:

        raise RuntimeError(
            "No market could be analyzed. "
            f"Errors: {errors[:3]}"
        )

    analyses.sort(
        key=lambda x: (
            x["tradeable"],
            x[
                "historical_accuracy_pct"
            ],
            x["confidence"],
            abs(
                x["momentum_pct"]
            ),
        ),
        reverse=True,
    )

    for i, item in enumerate(
        analyses
    ):

        item["favourable"] = (
            i == 0
            and item["tradeable"]
        )

    result = {
        "status": "live",
        "source": "deriv_live_ticks",
        "markets": analyses,
        "errors": errors,
        "generated_at": utc_now(),
        "prediction_horizon_ticks":
            PREDICTION_HORIZON_TICKS,
        "minimum_edge_accuracy":
            MIN_EDGE_ACCURACY,
        "minimum_confidence":
            MIN_CONFIDENCE_TO_TRADE,
    }

    PREDICTION_CACHE = {
        "expires_at":
            now
            + PREDICTION_CACHE_SECONDS,
        "data": result,
    }

    return result


async def get_market_prediction() -> Dict[str, Any]:

    data = await analyze_live_markets()

    if (
        data.get("status")
        != "live"
        or not data.get("markets")
    ):

        return {
            "status": "unavailable",
            "recommended_asset": None,
            "predicted_direction": None,
            "confidence_score": 0.0,
            "source": "deriv_live_ticks",
            "generated_at": utc_now(),
        }

    top = data["markets"][0]

    return {
        "status": "live",

        "recommended_asset":
            top["asset"],

        "symbol":
            top["symbol"],

        "market":
            top["market"],

        "predicted_direction":
            top["direction"],

        "confidence_score":
            top["confidence"],

        "tradeable":
            top["tradeable"],

        "historical_accuracy_pct":
            top[
                "historical_accuracy_pct"
            ],

        "historical_samples":
            top[
                "historical_samples"
            ],

        "rsi_value":
            top["rsi"],

        "price":
            top["price"],

        "momentum_pct":
            top["momentum_pct"],

        "volatility_pct":
            top["volatility_pct"],

        "reason":
            top["reason"],

        "source":
            "deriv_live_ticks",

        "latest_tick_time":
            top[
                "latest_tick_time"
            ],

        "generated_at":
            data["generated_at"],
    }


# ============================================================
# ACCOUNT SYNCHRONISATION
# ============================================================

async def sync_deriv_accounts(
    session: Dict[str, Any]
) -> None:

    token = session.get(
        "oauth_access_token"
    )

    if not token:

        raise RuntimeError(
            "Deriv account is not connected."
        )

    accounts = await asyncio.to_thread(
        get_deriv_accounts,
        token
    )

    demo = get_account_from_list(
        accounts,
        "demo"
    )

    real = get_account_from_list(
        accounts,
        "real"
    )

    session["accounts"]["demo"] = demo
    session["accounts"]["real"] = real

    session["balances"]["demo"] = (
        safe_float(
            demo.get("balance")
        )
        if demo
        else None
    )

    session["balances"]["real"] = (
        safe_float(
            real.get("balance")
        )
        if real
        else None
    )

    session["connected"] = True

    session["connection_status"] = (
        "authorised"
    )


# ============================================================
# PROPOSAL
# ============================================================

async def request_proposal(
    session: Dict[str, Any],
    account: str,
    asset: str,
    direction: str,
    amount: float,
    duration: int,
) -> Dict[str, Any]:

    validate_account(account)

    token = session.get(
        "oauth_access_token"
    )

    if not token:

        raise HTTPException(
            status_code=409,
            detail=(
                "Connect your Deriv account "
                "before requesting a proposal."
            ),
        )

    account_info = (
        session["accounts"].get(
            account
        )
    )

    if not account_info:

        await sync_deriv_accounts(
            session
        )

        account_info = (
            session["accounts"].get(
                account
            )
        )

    if not account_info:

        raise HTTPException(
            status_code=409,
            detail=(
                f"No Deriv {account} "
                "Options account is available."
            ),
        )

    account_id = account_info.get(
        "account_id"
    )

    if not account_id:

        raise HTTPException(
            status_code=409,
            detail=(
                f"Deriv {account} account ID "
                "is unavailable."
            ),
        )

    direction = direction.upper()

    if direction not in {
        "CALL",
        "PUT"
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "Direction must be CALL or PUT."
            ),
        )

    payload = {
        "proposal": 1,
        "amount": amount,
        "basis": "stake",
        "contract_type": direction,
        "currency":
            account_info.get(
                "currency",
                "USD"
            ),
        "duration": duration,
        "duration_unit":
            DEFAULT_DURATION_UNIT,
        "underlying_symbol": asset,
        "req_id": 3001,
    }

    return await authenticated_ws_request(
        token,
        account_id,
        payload,
        timeout=15,
    )


# ============================================================
# BUY + MONITOR
# ============================================================

async def buy_and_monitor(
    session: Dict[str, Any],
    account: str,
    proposal_id: str,
    proposal_price: float,
    duration: int,
) -> Dict[str, Any]:

    token = session.get(
        "oauth_access_token"
    )

    account_info = (
        session["accounts"].get(
            account
        )
    )

    if not token or not account_info:

        raise HTTPException(
            status_code=409,
            detail=(
                "Deriv account connection "
                "is unavailable."
            ),
        )

    account_id = account_info.get(
        "account_id"
    )

    ws_url = await asyncio.to_thread(
        get_deriv_otp,
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

        # ----------------------------------------------------
        # AUTHORIZE
        # ----------------------------------------------------

        await ws.send(
            json.dumps(
                {
                    "authorize":
                        token,
                    "req_id": 4001,
                }
            )
        )

        authorized = False

        deadline = (
            time.monotonic()
            + 15
        )

        while (
            time.monotonic()
            < deadline
        ):

            data = json.loads(
                await asyncio.wait_for(
                    ws.recv(),
                    timeout=15
                )
            )

            if data.get("error"):

                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Deriv authorization failed."
                    )
                )

            if (
                data.get("msg_type")
                == "authorize"
            ):

                authorized = True
                break

        if not authorized:

            raise TimeoutError(
                "Timed out authorizing Deriv."
            )

        # ----------------------------------------------------
        # BUY
        # ----------------------------------------------------

        await ws.send(
            json.dumps(
                {
                    "buy":
                        proposal_id,
                    "price":
                        proposal_price,
                    "req_id": 4002,
                }
            )
        )

        buy_data = None

        deadline = (
            time.monotonic()
            + 15
        )

        while (
            time.monotonic()
            < deadline
        ):

            data = json.loads(
                await asyncio.wait_for(
                    ws.recv(),
                    timeout=15
                )
            )

            if data.get("error"):

                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Deriv buy failed."
                    )
                )

            if (
                data.get("msg_type")
                == "buy"
            ):

                buy_data = data["buy"]
                break

        if not buy_data:

            raise RuntimeError(
                "Deriv did not return a buy response."
            )

        contract_id = buy_data.get(
            "contract_id"
        )

        if not contract_id:

            raise RuntimeError(
                "Deriv buy response did not "
                "contain a contract ID."
            )

        # ----------------------------------------------------
        # MONITOR CONTRACT
        # ----------------------------------------------------

        await ws.send(
            json.dumps(
                {
                    "proposal_open_contract":
                        1,
                    "contract_id":
                        contract_id,
                    "subscribe":
                        1,
                    "req_id":
                        4003,
                }
            )
        )

        final_contract = None

        deadline = (
            time.monotonic()
            + max(
                30,
                duration * 3 + 30
            )
        )

        while (
            time.monotonic()
            < deadline
        ):

            try:

                data = json.loads(
                    await asyncio.wait_for(
                        ws.recv(),
                        timeout=15
                    )
                )

            except asyncio.TimeoutError:

                continue

            if data.get("error"):

                raise RuntimeError(
                    data["error"].get(
                        "message",
                        "Contract monitoring failed."
                    )
                )

            if (
                data.get("msg_type")
                != "proposal_open_contract"
            ):
                continue

            contract = data.get(
                "proposal_open_contract",
                {}
            )

            final_contract = contract

            status = str(
                contract.get(
                    "status",
                    ""
                )
            ).lower()

            if (
                contract.get("is_sold")
                or status in {
                    "won",
                    "lost",
                    "sold",
                    "expired",
                }
            ):
                break

        if final_contract is None:

            raise RuntimeError(
                "No final contract status "
                "was received from Deriv."
            )

        return {
            "contract_id":
                contract_id,

            "buy":
                buy_data,

            "contract":
                final_contract,
        }


# ============================================================
# ROOT / HEALTH
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "online",
        "service":
            "Fixed Risk Booster API",
        "version":
            APP_VERSION,
        "broker":
            "Deriv",
        "product":
            "Deriv Options",
        "prediction_source":
            "Deriv live ticks",
        "time":
            utc_now(),
    }


@app.head("/")
async def root_head():
    return None


@app.get("/api/health")
async def health():

    return {
        "status": "healthy",
        "version":
            APP_VERSION,
        "broker":
            "deriv",
        "product":
            "options",
        "deriv_oauth_configured":
            bool(
                DERIV_CLIENT_ID
                and DERIV_REDIRECT_URI
            ),
        "users_in_memory":
            len(USER_SESSIONS),
        "real_trading_enabled":
            REAL_TRADING_ENABLED,
        "time":
            utc_now(),
    }


# ============================================================
# SESSION
# ============================================================

@app.post("/api/session/register")
async def register_session(
    request: SessionRegisterRequest
):

    session = get_session(
        request.user_id
    )

    return {
        "status":
            "registered",

        "user_id":
            request.user_id,

        "connected":
            session["connected"],

        "connection_status":
            session[
                "connection_status"
            ],

        "balances":
            session["balances"],

        "accounts": {
            "demo":
                bool(
                    session[
                        "accounts"
                    ]["demo"]
                ),
            "real":
                bool(
                    session[
                        "accounts"
                    ]["real"]
                ),
        },

        "message":
            (
                "Application session registered. "
                "Connect Deriv to access your "
                "own Demo/Real Options accounts."
            ),
    }


@app.get(
    "/api/session/status/{user_id}"
)
async def session_status(
    user_id: str
):

    session = require_session(
        user_id
    )

    return {
        "user_id":
            user_id,

        "connected":
            session["connected"],

        "connection_status":
            session[
                "connection_status"
            ],

        "account":
            session["account"],

        "accounts": {
            "demo":
                session[
                    "accounts"
                ]["demo"],

            "real":
                session[
                    "accounts"
                ]["real"],
        },

        "balances":
            session["balances"],
    }


@app.post(
    "/api/session/disconnect"
)
async def disconnect_session(
    request: SessionRegisterRequest
):

    session = require_session(
        request.user_id
    )

    # Stop NEW trades.
    session["trading"] = False

    # Remove server-side OAuth credentials.
    session["oauth_access_token"] = None
    session["oauth_refresh_token"] = None
    session["oauth_expires_at"] = None

    session["connected"] = False

    session["connection_status"] = (
        "not_connected"
    )

    session["accounts"] = {
        "demo": None,
        "real": None,
    }

    session["balances"] = {
        "demo": None,
        "real": None,
    }

    session["active_trade"] = None

    return {
        "status":
            "disconnected",
        "message":
            (
                "Deriv connection removed "
                "from this application session."
            ),
    }


# ============================================================
# ACCOUNT BALANCE
# ============================================================

@app.get(
    "/api/account/balance/{user_id}"
)
async def account_balance(
    user_id: str
):

    session = require_session(
        user_id
    )

    if session.get(
        "oauth_access_token"
    ):

        try:

            await sync_deriv_accounts(
                session
            )

        except Exception as exc:

            raise HTTPException(
                status_code=502,
                detail=str(exc)
            )

    return {
        "demo": {
            "balance":
                session[
                    "balances"
                ]["demo"],

            "verified":
                session[
                    "balances"
                ]["demo"] is not None,
        },

        "real": {
            "balance":
                session[
                    "balances"
                ]["real"],

            "verified":
                session[
                    "balances"
                ]["real"] is not None,
        },
    }


# ============================================================
# MARKET ENDPOINTS
# ============================================================

@app.get(
    "/api/market/prediction"
)
async def market_prediction():

    try:

        return await get_market_prediction()

    except Exception as exc:

        raise HTTPException(
            status_code=503,
            detail=str(exc)
        )


@app.get(
    "/api/market/debug"
)
async def market_debug():

    try:

        data = await analyze_live_markets()

        return {
            "status":
                data["status"],

            "market_count":
                len(
                    data["markets"]
                ),

            "errors":
                data["errors"][:10],

            "source":
                data["source"],

            "generated_at":
                data["generated_at"],

            "prediction_horizon_ticks":
                data[
                    "prediction_horizon_ticks"
                ],

            "minimum_edge_accuracy":
                data[
                    "minimum_edge_accuracy"
                ],

            "minimum_confidence":
                data[
                    "minimum_confidence"
                ],
        }

    except Exception as exc:

        return {
            "status":
                "unavailable",

            "market_count":
                0,

            "errors":
                [str(exc)],

            "source":
                "deriv_live_ticks",

            "generated_at":
                utc_now(),
        }


@app.get("/api/markets")
async def markets():

    try:

        data = await analyze_live_markets()

        return {
            "status":
                data["status"],

            "recommended_asset":
                (
                    data["markets"][0]["asset"]
                    if data["markets"]
                    else None
                ),

            "markets":
                data["markets"],

            "errors":
                data["errors"],

            "source":
                data["source"],

            "generated_at":
                data["generated_at"],
        }

    except Exception as exc:

        raise HTTPException(
            status_code=503,
            detail=str(exc)
        )


# ============================================================
# TRADING SESSION
# ============================================================

@app.post(
    "/api/trading/start"
)
async def start_trading(
    request: TradingStartRequest
):

    validate_account(
        request.account
    )

    session = require_session(
        request.user_id
    )

    if not session.get(
        "oauth_access_token"
    ):

        raise HTTPException(
            status_code=409,
            detail=(
                "Connect your Deriv account "
                "before starting a trading session."
            ),
        )

    if request.account == "real":

        if not REAL_TRADING_ENABLED:

            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled "
                    "on this server."
                ),
            )

        if not request.real_market_mode:

            raise HTTPException(
                status_code=400,
                detail=(
                    "Real account requires explicit "
                    "real_market_mode confirmation."
                ),
            )

    await sync_deriv_accounts(
        session
    )

    account_info = (
        session["accounts"].get(
            request.account
        )
    )

    if not account_info:

        raise HTTPException(
            status_code=409,
            detail=(
                f"No verified Deriv "
                f"{request.account} Options "
                "account is available."
            ),
        )

    balance = (
        session["balances"].get(
            request.account
        )
    )

    if balance is None:

        raise HTTPException(
            status_code=409,
            detail=(
                "Deriv account balance "
                "could not be verified."
            ),
        )

    if balance < request.stake:

        raise HTTPException(
            status_code=400,
            detail=(
                "Insufficient account balance."
            ),
        )

    # --------------------------------------------------------
    # Reset CURRENT SESSION statistics.
    #
    # Account-wide statistics remain untouched.
    # --------------------------------------------------------

    session["trading"] = True

    session["account"] = (
        request.account
    )

    session["stake"] = round(
        request.stake,
        2
    )

    session["duration"] = (
        request.duration
    )

    session["real_market_mode"] = (
        request.real_market_mode
    )

    session["session_profit"] = 0.0
    session["session_trades"] = 0
    session["session_wins"] = 0
    session["session_losses"] = 0
    session["consecutive_losses"] = 0
    session["active_trade"] = None

    session["session_started_at"] = (
        utc_now()
    )

    return {
        "status":
            "started",

        "account":
            request.account,

        "stake":
            session["stake"],

        "balance":
            balance,

        "duration":
            request.duration,

        "duration_unit":
            DEFAULT_DURATION_UNIT,

        "session_started_at":
            session[
                "session_started_at"
            ],
    }


@app.post(
    "/api/trading/stop"
)
async def stop_trading(
    request: TradingStopRequest
):

    session = require_session(
        request.user_id
    )

    # This stops NEW trades.
    #
    # It does NOT cancel an Options contract
    # that has already been purchased.
    session["trading"] = False

    active_trade = (
        session["active_trade"]
    )

    return {
        "status":
            "stopped",

        "account":
            session["account"],

        "new_trades_allowed":
            False,

        "active_trade":
            active_trade,

        "message":
            (
                "New trades are stopped. "
                "Any contract already purchased "
                "continues according to its Deriv "
                "contract lifecycle."
            ),

        "session_profit":
            round(
                session[
                    "session_profit"
                ],
                2
            ),

        "session_trades":
            session[
                "session_trades"
            ],

        "session_wins":
            session[
                "session_wins"
            ],

        "session_losses":
            session[
                "session_losses"
            ],
    }


@app.get(
    "/api/trading/status/{user_id}"
)
async def trading_status(
    user_id: str
):

    session = require_session(
        user_id
    )

    return {
        "trading":
            session["trading"],

        "account":
            session["account"],

        "stake":
            session["stake"],

        "duration":
            session["duration"],

        "duration_unit":
            DEFAULT_DURATION_UNIT,

        "session_profit":
            round(
                session[
                    "session_profit"
                ],
                2
            ),

        "session_trades":
            session[
                "session_trades"
            ],

        "session_wins":
            session[
                "session_wins"
            ],

        "session_losses":
            session[
                "session_losses"
            ],

        "consecutive_losses":
            session[
                "consecutive_losses"
            ],

        "active_trade":
            session[
                "active_trade"
            ],
    }


# ============================================================
# TRADE EXECUTION
# ============================================================

@app.post(
    "/api/trade/execute"
)
async def execute_trade(
    request: TradeRequest
):

    validate_account(
        request.account
    )

    session = require_session(
        request.user_id
    )

    # --------------------------------------------------------
    # One trade at a time per user.
    # --------------------------------------------------------

    async with session["trade_lock"]:

        # ----------------------------------------------------
        # CHECK BEFORE ANY TRADE ACTION
        # ----------------------------------------------------

        if not session["trading"]:

            raise HTTPException(
                status_code=409,
                detail=(
                    "Trading session is not active."
                ),
            )

        opening_stake = (
            session["stake"]
        )

        # ----------------------------------------------------
        # FIXED RISK
        # ----------------------------------------------------

        if (
            round(
                request.amount,
                2
            )
            != round(
                opening_stake,
                2
            )
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "Fixed-risk violation: "
                    "trade amount must equal "
                    f"the active stake "
                    f"({opening_stake:.2f})."
                ),
            )

        if (
            request.account
            != session["account"]
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Trade account does not "
                    "match the active session."
                ),
            )

        # ----------------------------------------------------
        # REAL MODE PROTECTION
        # ----------------------------------------------------

        if request.account == "real":

            if not REAL_TRADING_ENABLED:

                raise HTTPException(
                    status_code=403,
                    detail=(
                        "Real trading is disabled "
                        "on this server."
                    ),
                )

            if not session[
                "real_market_mode"
            ]:

                raise HTTPException(
                    status_code=403,
                    detail=(
                        "Real trading requires "
                        "explicit confirmation."
                    ),
                )

        # ----------------------------------------------------
        # CHECK CONNECTION
        # ----------------------------------------------------

        if not session.get(
            "oauth_access_token"
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Deriv account is not connected."
                ),
            )

        # ----------------------------------------------------
        # FRESH LIVE PREDICTION
        # ----------------------------------------------------
        #
        # The prediction is obtained immediately
        # before the proposal request.
        #
        # The prediction cache only prevents excessive
        # repeated requests during the short configured
        # window. It does NOT use an old historical dataset
        # as the live prediction source.
        # ----------------------------------------------------

        try:

            prediction = (
                await get_market_prediction()
            )

        except Exception as exc:

            raise HTTPException(
                status_code=503,
                detail=str(exc)
            )

        if (
            prediction.get("status")
            != "live"
        ):

            raise HTTPException(
                status_code=503,
                detail=(
                    "Live Deriv market data "
                    "is unavailable."
                ),
            )

        if not prediction.get(
            "tradeable"
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Prediction engine says "
                    "NO TRADE. The current setup "
                    "does not meet the configured "
                    "accuracy and confidence thresholds."
                ),
            )

        # ----------------------------------------------------
        # MARKET MUST MATCH CURRENT RECOMMENDATION
        # ----------------------------------------------------

        if (
            request.asset
            != prediction[
                "recommended_asset"
            ]
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Market recommendation changed. "
                    "Refresh the live signal."
                ),
            )

        # ----------------------------------------------------
        # DIRECTION MUST MATCH
        # ----------------------------------------------------

        if (
            request.direction.upper()
            != prediction[
                "predicted_direction"
            ]
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Trade direction does not "
                    "match the current live prediction."
                ),
            )

        # ----------------------------------------------------
        # REQUEST ACTUAL DERIV PROPOSAL
        # ----------------------------------------------------

        proposal_response = (
            await request_proposal(
                session=session,
                account=request.account,
                asset=request.asset,
                direction=request.direction.upper(),
                amount=opening_stake,
                duration=request.duration,
            )
        )

        proposal = proposal_response.get(
            "proposal",
            {}
        )

        proposal_id = proposal.get(
            "id"
        )

        ask_price = safe_float(
            proposal.get(
                "ask_price"
            )
        )

        if (
            not proposal_id
            or ask_price is None
        ):

            raise HTTPException(
                status_code=502,
                detail=(
                    "Deriv did not return "
                    "a valid contract proposal."
                ),
            )

        payout = safe_float(
            proposal.get(
                "payout"
            )
        )

        spot = safe_float(
            proposal.get(
                "spot"
            )
        )

        if (
            payout is None
            or payout <= 0
        ):

            raise HTTPException(
                status_code=409,
                detail=(
                    "Deriv returned no usable "
                    "payout. Trade skipped."
                ),
            )

        # ----------------------------------------------------
        # BREAK-EVEN CALCULATION
        # ----------------------------------------------------

        break_even_probability = (
            ask_price / payout
            if payout > 0
            else None
        )

        if (
            break_even_probability
            is not None
        ):

            historical_probability = (
                prediction[
                    "historical_accuracy_pct"
                ]
                / 100.0
            )

            # 3 percentage point buffer.
            required_probability = (
                break_even_probability
                + 0.03
            )

            if (
                historical_probability
                <= required_probability
            ):

                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Trade skipped: measured "
                        "historical accuracy does not "
                        "clear the current Deriv "
                        "contract break-even threshold "
                        "by the configured buffer."
                    ),
                )

        # ----------------------------------------------------
        # RECORD ACTIVE TRADE
        # ----------------------------------------------------

        session["active_trade"] = {
            "asset":
                request.asset,

            "direction":
                request.direction.upper(),

            "account":
                request.account,

            "stake":
                opening_stake,

            "duration":
                request.duration,

            "duration_unit":
                DEFAULT_DURATION_UNIT,

            "proposal_id":
                proposal_id,

            "payout":
                payout,

            "ask_price":
                ask_price,

            "spot":
                spot,

            "prediction_confidence":
                prediction[
                    "confidence_score"
                ],

            "historical_accuracy_pct":
                prediction[
                    "historical_accuracy_pct"
                ],

            "started_at":
                utc_now(),
        }

        # ----------------------------------------------------
        # BUY + MONITOR
        # ----------------------------------------------------

        try:

            execution = (
                await buy_and_monitor(
                    session=session,
                    account=request.account,
                    proposal_id=proposal_id,
                    proposal_price=ask_price,
                    duration=request.duration,
                )
            )

        except Exception as exc:

            session["active_trade"] = None

            raise HTTPException(
                status_code=502,
                detail=(
                    f"Deriv execution failed: {exc}"
                ),
            )

        contract = execution[
            "contract"
        ]

        profit = safe_float(
            contract.get(
                "profit"
            ),
            0.0
        ) or 0.0

        status = str(
            contract.get(
                "status",
                ""
            )
        ).lower()

        if (
            status == "won"
            or profit > 0
        ):

            result = "WIN"

        elif (
            status == "lost"
            or profit < 0
        ):

            result = "LOSS"

        else:

            result = "CLOSED"

        # ----------------------------------------------------
        # UPDATE SESSION STATS
        # ----------------------------------------------------

        session[
            "session_trades"
        ] += 1

        session[
            "session_profit"
        ] += profit

        # ----------------------------------------------------
        # UPDATE ACCOUNT-WIDE STATS
        # ----------------------------------------------------

        stats = session[
            f"{request.account}_stats"
        ]

        stats["profit"] += profit

        stats["trades"] += 1

        if result == "WIN":

            session[
                "session_wins"
            ] += 1

            session[
                "consecutive_losses"
            ] = 0

            stats["wins"] += 1

        elif result == "LOSS":

            session[
                "session_losses"
            ] += 1

            session[
                "consecutive_losses"
            ] += 1

            stats["losses"] += 1

        # ----------------------------------------------------
        # TRADE RECORD
        # ----------------------------------------------------

        trade = {
            "order_id":
                str(
                    execution[
                        "contract_id"
                    ]
                ),

            "contract_id":
                execution[
                    "contract_id"
                ],

            "account":
                request.account,

            "asset":
                request.asset,

            "direction":
                request.direction.upper(),

            "stake":
                opening_stake,

            "duration":
                request.duration,

            "duration_unit":
                DEFAULT_DURATION_UNIT,

            "result":
                result,

            "profit":
                round(
                    profit,
                    2
                ),

            "proposal_payout":
                payout,

            "proposal_ask_price":
                ask_price,

            "break_even_probability_pct":
                (
                    round(
                        break_even_probability
                        * 100.0,
                        2
                    )
                    if break_even_probability
                    is not None
                    else None
                ),

            "prediction_confidence":
                prediction[
                    "confidence_score"
                ],

            "historical_accuracy_pct":
                prediction[
                    "historical_accuracy_pct"
                ],

            "historical_samples":
                prediction[
                    "historical_samples"
                ],

            "timestamp":
                utc_now(),
        }

        history_key = (
            f"{request.account}_history"
        )

        session[
            history_key
        ].insert(
            0,
            trade
        )

        session[
            history_key
        ] = session[
            history_key
        ][:MAX_HISTORY_PER_ACCOUNT]

        session["last_trade_at"] = (
            trade["timestamp"]
        )

        session["active_trade"] = None

        # ----------------------------------------------------
        # LOSS PROTECTION
        # ----------------------------------------------------

        protection_message = None

        if (
            session[
                "consecutive_losses"
            ]
            >= MAX_CONSECUTIVE_LOSSES
        ):

            session["trading"] = False

            protection_message = (
                "Trading session automatically "
                f"stopped after "
                f"{MAX_CONSECUTIVE_LOSSES} "
                "consecutive losses."
            )

        # ----------------------------------------------------
        # REFRESH ACTUAL DERIV BALANCE
        # ----------------------------------------------------

        try:

            await sync_deriv_accounts(
                session
            )

        except Exception:
            pass

        return {
            "status":
                "completed",

            "order_id":
                str(
                    execution[
                        "contract_id"
                    ]
                ),

            "contract_id":
                execution[
                    "contract_id"
                ],

            "account":
                request.account,

            "asset":
                request.asset,

            "direction":
                request.direction.upper(),

            "stake":
                opening_stake,

            "result":
                result,

            "profit":
                round(
                    profit,
                    2
                ),

            "remaining_balance":
                session[
                    "balances"
                ].get(
                    request.account
                ),

            "session_profit":
                round(
                    session[
                        "session_profit"
                    ],
                    2
                ),

            "session_trades":
                session[
                    "session_trades"
                ],

            "session_wins":
                session[
                    "session_wins"
                ],

            "session_losses":
                session[
                    "session_losses"
                ],

            "consecutive_losses":
                session[
                    "consecutive_losses"
                ],

            "trading":
                session["trading"],

            "protection_message":
                protection_message,

            "deriv_contract": {
                "status":
                    contract.get(
                        "status"
                    ),

                "payout":
                    contract.get(
                        "payout"
                    ),

                "buy_price":
                    contract.get(
                        "buy_price"
                    ),

                "sell_price":
                    contract.get(
                        "sell_price"
                    ),

                "entry_spot":
                    contract.get(
                        "entry_spot"
                    ),

                "exit_tick":
                    contract.get(
                        "exit_tick"
                    ),
            },
        }


# ============================================================
# TRADE HISTORY
# ============================================================

@app.get(
    "/api/trades/{user_id}"
)
async def trade_history(
    user_id: str,
    account: str = "demo"
):

    validate_account(
        account
    )

    session = require_session(
        user_id
    )

    return {
        "account":
            account,

        "trades":
            session[
                f"{account}_history"
            ],
    }


# ============================================================
# ACCOUNT STATISTICS
# ============================================================

@app.get(
    "/api/stats/{user_id}"
)
async def statistics(
    user_id: str,
    account: str = "demo"
):

    validate_account(
        account
    )

    session = require_session(
        user_id
    )

    stats = session[
        f"{account}_stats"
    ]

    trades = stats[
        "trades"
    ]

    wins = stats[
        "wins"
    ]

    win_rate = (
        round(
            (
                wins
                / trades
            )
            * 100.0,
            2
        )
        if trades
        else 0.0
    )

    return {
        "account":
            account,

        "profit":
            round(
                stats["profit"],
                2
            ),

        "trades":
            trades,

        "wins":
            wins,

        "losses":
            stats["losses"],

        "win_rate":
            win_rate,
    }


# ============================================================
# COMBINED DASHBOARD DATA
# ============================================================

@app.get(
    "/api/dashboard/{user_id}"
)
async def dashboard(
    user_id: str
):

    session = require_session(
        user_id
    )

    demo_stats = session[
        "demo_stats"
    ]

    real_stats = session[
        "real_stats"
    ]

    def make_stats(stats):

        trades = stats[
            "trades"
        ]

        wins = stats[
            "wins"
        ]

        return {
            "profit":
                round(
                    stats[
                        "profit"
                    ],
                    2
                ),

            "trades":
                trades,

            "wins":
                wins,

            "losses":
                stats[
                    "losses"
                ],

            "win_rate":
                (
                    round(
                        wins
                        / trades
                        * 100.0,
                        2
                    )
                    if trades
                    else 0.0
                ),
        }

    return {
        "user_id":
            user_id,

        "connected":
            session["connected"],

        "connection_status":
            session[
                "connection_status"
            ],

        "selected_account":
            session["account"],

        "balances": {
            "demo":
                session[
                    "balances"
                ]["demo"],

            "real":
                session[
                    "balances"
                ]["real"],
        },

        "demo": {
            "stats":
                make_stats(
                    demo_stats
                ),

            "history":
                session[
                    "demo_history"
                ],
        },

        "real": {
            "stats":
                make_stats(
                    real_stats
                ),

            "history":
                session[
                    "real_history"
                ],
        },

        "trading": {
            "active":
                session["trading"],

            "account":
                session["account"],

            "stake":
                session["stake"],

            "duration":
                session[
                    "duration"
                ],

            "session_profit":
                round(
                    session[
                        "session_profit"
                    ],
                    2
                ),

            "session_trades":
                session[
                    "session_trades"
                ],

            "session_wins":
                session[
                    "session_wins"
                ],

            "session_losses":
                session[
                    "session_losses"
                ],

            "active_trade":
                session[
                    "active_trade"
                ],
        },
    }


# ============================================================
# SERVER
# ============================================================

if __name__ == "__main__":

    import uvicorn

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )
