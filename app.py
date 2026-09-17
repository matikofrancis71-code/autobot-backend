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
# CONFIGURATION
# ============================================================

APP_VERSION = "6.1.0"

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "").strip().rstrip("/")

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

MARKET_DISCOVERY_CACHE_SECONDS = float(
    os.getenv("MARKET_DISCOVERY_CACHE_SECONDS", "60")
)

DEFAULT_DURATION = int(
    os.getenv("DEFAULT_DURATION", "5")
)

DEFAULT_DURATION_UNIT = (
    os.getenv("DEFAULT_DURATION_UNIT", "t").strip() or "t"
)

DEFAULT_BARRIER = int(
    os.getenv("DEFAULT_BARRIER", "5")
)

MAX_STAKE = float(
    os.getenv("MAX_STAKE", "1000")
)

MAX_HISTORY_RECORDS = 100

MAX_CONSECUTIVE_LOSSES = 3


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
# IN-MEMORY STORAGE
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}

OAUTH_STATES: Dict[str, Dict[str, Any]] = {}

PREDICTION_CACHE: Dict[str, Dict[str, Any]] = {
    "timestamp": 0,
    "data": None,
}

MARKET_DISCOVERY_CACHE: Dict[str, Any] = {
    "timestamp": 0,
    "data": [],
}


# ============================================================
# HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def clean_symbol(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
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


def get_session(session_id: str) -> Dict[str, Any]:
    session = USER_SESSIONS.get(session_id)

    if not session:
        raise HTTPException(
            status_code=404,
            detail="Session not found.",
        )

    session["last_activity"] = iso_now()

    return session


def new_stats() -> Dict[str, Any]:
    return {
        "profit": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
    }


def new_session(session_id: Optional[str] = None) -> Dict[str, Any]:
    sid = session_id or secrets.token_urlsafe(32)

    session = {
        "session_id": sid,
        "created_at": iso_now(),
        "last_activity": iso_now(),

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


def ensure_session(session_id: str) -> Dict[str, Any]:
    if not session_id:
        raise HTTPException(
            status_code=400,
            detail="session_id is required.",
        )

    return get_session(session_id)


# ============================================================
# PKCE
# ============================================================

def create_pkce_verifier() -> str:
    return (
        base64.urlsafe_b64encode(
            secrets.token_bytes(48)
        )
        .rstrip(b"=")
        .decode("ascii")
    )


def create_pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(
        verifier.encode("ascii")
    ).digest()

    return (
        base64.urlsafe_b64encode(digest)
        .rstrip(b"=")
        .decode("ascii")
    )


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

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            method,
            url,
            headers=headers,
            json=json_body,
            params=params,
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text[:1000],
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
            detail="Unexpected Deriv response.",
        )

    return data


# ============================================================
# OPTIONS ACCOUNTS
# ============================================================

async def get_deriv_accounts(
    session: Dict[str, Any]
) -> Dict[str, Any]:

    return await deriv_rest_async(
        "GET",
        "/trading/v1/options/accounts",
        session,
    )


def extract_account_id(account: Dict[str, Any]) -> Optional[str]:
    possible = [
        account.get("id"),
        account.get("account_id"),
        account.get("accountId"),
        account.get("loginid"),
    ]

    for value in possible:
        if value:
            return str(value)

    return None


def detect_account_type(account: Dict[str, Any]) -> Optional[str]:
    raw_values = [
        account.get("type"),
        account.get("account_type"),
        account.get("accountType"),
        account.get("environment"),
        account.get("mode"),
        account.get("loginid"),
    ]

    text = " ".join(
        str(value).lower()
        for value in raw_values
        if value is not None
    )

    if any(
        word in text
        for word in [
            "demo",
            "virtual",
            "practice",
        ]
    ):
        return "demo"

    if any(
        word in text
        for word in [
            "real",
            "live",
        ]
    ):
        return "real"

    return None


def detect_currency(
    account: Dict[str, Any]
) -> Optional[str]:

    for key in [
        "currency",
        "currency_code",
    ]:
        value = account.get(key)

        if value:
            return str(value)

    return None


async def load_options_accounts(
    session: Dict[str, Any]
) -> None:

    response = await get_deriv_accounts(session)

    raw_accounts = (
        response.get("accounts")
        or response.get("data")
        or []
    )

    if isinstance(raw_accounts, dict):
        raw_accounts = list(raw_accounts.values())

    if not isinstance(raw_accounts, list):
        raw_accounts = []

    demo_account = None
    real_account = None

    for account in raw_accounts:
        if not isinstance(account, dict):
            continue

        account_id = extract_account_id(account)

        if not account_id:
            continue

        account_type = detect_account_type(account)

        if account_type == "demo" and not demo_account:
            demo_account = account_id

            currency = detect_currency(account)

            if currency:
                session["account_currencies"]["demo"] = currency

        elif account_type == "real" and not real_account:
            real_account = account_id

            currency = detect_currency(account)

            if currency:
                session["account_currencies"]["real"] = currency

    session["accounts"]["demo"] = demo_account
    session["accounts"]["real"] = real_account


# ============================================================
# OTP / AUTHENTICATED WEBSOCKET
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

    for key in [
        "url",
        "ws_url",
        "websocket_url",
    ]:
        value = response.get(key)

        if value:
            return str(value)

    nested = response.get("data")

    if isinstance(nested, dict):
        for key in [
            "url",
            "ws_url",
            "websocket_url",
        ]:
            value = nested.get(key)

            if value:
                return str(value)

    raise HTTPException(
        status_code=502,
        detail="Deriv did not provide an Options WebSocket URL.",
    )


async def ws_request(
    url: str,
    payload: Dict[str, Any],
    timeout: float = 20,
) -> Dict[str, Any]:

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=5 * 1024 * 1024,
    ) as ws:

        await ws.send(json.dumps(payload))

        end_time = time.monotonic() + timeout

        while True:
            remaining = end_time - time.monotonic()

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

                if response.get("msg_type"):
                    return response

                if "echo_req" in response:
                    return response

    raise RuntimeError("No Deriv response received.")


async def authenticated_ws_request(
    session: Dict[str, Any],
    account_type: str,
    payload: Dict[str, Any],
    timeout: float = 20,
) -> Dict[str, Any]:

    account_type = normalize_account_type(account_type)

    account_id = session["accounts"].get(account_type)

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
        timeout=timeout,
    )


# ============================================================
# BALANCES
# ============================================================

async def get_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> Optional[float]:

    account_type = normalize_account_type(account_type)

    if not session["accounts"].get(account_type):
        return None

    response = await authenticated_ws_request(
        session,
        account_type,
        {
            "balance": 1,
        },
    )

    balance_data = response.get("balance")

    if isinstance(balance_data, dict):
        value = balance_data.get("balance")

        if value is not None:
            return safe_float(value)

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
    session: Dict[str, Any]
) -> None:

    tasks = [
        refresh_account_balance(session, "demo")
    ]

    if session["accounts"].get("real"):
        tasks.append(
            refresh_account_balance(session, "real")
        )

    await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )


# ============================================================
# PUBLIC MARKET DATA
# ============================================================

async def public_ws_request(
    payload: Dict[str, Any],
    timeout: float = 15,
) -> Dict[str, Any]:

    return await ws_request(
        DERIV_PUBLIC_WS,
        payload,
        timeout=timeout,
    )


def normalize_active_symbol(
    item: Dict[str, Any]
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

    exchange_is_open = item.get(
        "exchange_is_open",
        True,
    )

    is_trading_suspended = item.get(
        "is_trading_suspended",
        False,
    )

    normalized = {
        "symbol": symbol,
        "display_name": str(display_name),
        "symbol_type": str(symbol_type),
        "market": str(market),
        "subgroup": str(subgroup),
        "submarket": str(submarket),
        "pip": safe_float(pip, 0.0),

        "exchange_is_open": exchange_is_open,
        "is_trading_suspended": is_trading_suspended,

        "raw": item,
    }

    return normalized


async def get_active_symbols() -> List[Dict[str, Any]]:

    response = await public_ws_request(
        {
            "active_symbols": "brief",

            # Ask Deriv for markets relevant to
            # digit over / digit under contracts.
            "contract_type": [
                "DIGITOVER",
                "DIGITUNDER",
            ],
        }
    )

    raw_symbols = response.get(
        "active_symbols",
        []
    )

    if not isinstance(raw_symbols, list):
        return []

    normalized = []

    for item in raw_symbols:
        symbol = normalize_active_symbol(item)

        if symbol:
            normalized.append(symbol)

    return normalized


def symbol_is_trade_candidate(
    symbol: Dict[str, Any]
) -> bool:

    code = clean_symbol(
        symbol.get("symbol")
    )

    if not code:
        return False

    exchange_open = symbol.get(
        "exchange_is_open",
        True,
    )

    if exchange_open in {
        False,
        0,
        "0",
        "false",
        "False",
    }:
        return False

    suspended = symbol.get(
        "is_trading_suspended",
        False,
    )

    if suspended in {
        True,
        1,
        "1",
        "true",
        "True",
    }:
        return False

    searchable = " ".join(
        str(symbol.get(key) or "")
        for key in [
            "market",
            "symbol_type",
            "subgroup",
            "submarket",
        ]
    ).lower()

    # We are intentionally restricting the prediction
    # universe to synthetic/derived markets.
    if (
        "synthetic" not in searchable
        and "derived" not in searchable
    ):
        return False

    return True


async def verify_digit_contract_support(
    symbol: Dict[str, Any]
) -> bool:

    code = clean_symbol(
        symbol.get("symbol")
    )

    if not code:
        return False

    try:
        response = await public_ws_request(
            {
                "contracts_for": code,
            },
            timeout=12,
        )
    except Exception:
        return False

    contracts_data = response.get(
        "contracts_for",
        {}
    )

    if not isinstance(contracts_data, dict):
        return False

    available = contracts_data.get(
        "available",
        []
    )

    if not isinstance(available, list):
        return False

    for contract in available:

        if not isinstance(contract, dict):
            continue

        contract_type = (
            contract.get("contract_type")
            or contract.get("type")
            or ""
        )

        contract_type = str(
            contract_type
        ).upper()

        if contract_type in {
            "DIGITOVER",
            "DIGITUNDER",
        }:
            return True

    return False


async def get_tradeable_symbols(
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:

    now = time.monotonic()

    cached_data = MARKET_DISCOVERY_CACHE.get(
        "data"
    )

    cached_timestamp = MARKET_DISCOVERY_CACHE.get(
        "timestamp",
        0,
    )

    if (
        not force_refresh
        and isinstance(cached_data, list)
        and cached_data
        and now - cached_timestamp
        < MARKET_DISCOVERY_CACHE_SECONDS
    ):
        return cached_data

    symbols = await get_active_symbols()

    candidates = [
        symbol
        for symbol in symbols
        if symbol_is_trade_candidate(symbol)
    ]

    # Deduplicate symbols.
    unique = {}

    for symbol in candidates:
        code = symbol["symbol"]

        if code not in unique:
            unique[code] = symbol

    candidates = list(unique.values())

    # Verify actual DIGITOVER/DIGITUNDER availability.
    verification_tasks = [
        verify_digit_contract_support(symbol)
        for symbol in candidates
    ]

    verification_results = await asyncio.gather(
        *verification_tasks,
        return_exceptions=True,
    )

    tradeable = []

    for symbol, result in zip(
        candidates,
        verification_results,
    ):
        if result is True:
            tradeable.append(symbol)

    MARKET_DISCOVERY_CACHE["timestamp"] = now
    MARKET_DISCOVERY_CACHE["data"] = tradeable

    print(
        f"[MARKETS] discovered={len(symbols)} "
        f"candidates={len(candidates)} "
        f"digit_tradeable={len(tradeable)}"
    )

    if tradeable:
        print(
            "[MARKETS] "
            + ", ".join(
                symbol["symbol"]
                for symbol in tradeable
            )
        )

    return tradeable


# ============================================================
# TICK HISTORY
# ============================================================

async def get_tick_history(
    symbol: str,
    count: int = HISTORY_TICKS,
) -> List[float]:

    count = max(
        50,
        min(
            int(count),
            10000,
        ),
    )

    response = await public_ws_request(
        {
            "ticks_history": symbol,
            "count": count,
            "end": "latest",
            "style": "ticks",
        },
        timeout=20,
    )

    history = response.get(
        "history",
        {}
    )

    if not isinstance(history, dict):
        return []

    prices = history.get(
        "prices",
        []
    )

    times = history.get(
        "times",
        []
    )

    if not isinstance(prices, list):
        return []

    numeric_prices = []

    for price in prices:
        value = safe_float(price)

        if value is not None and math.isfinite(value):
            numeric_prices.append(value)

    # If timestamps exist, explicitly enforce chronological
    # ordering. This protects the predictor from relying on
    # an unexpected API ordering.
    if (
        isinstance(times, list)
        and len(times) == len(prices)
    ):
        pairs = []

        for timestamp, price in zip(
            times,
            prices,
        ):
            p = safe_float(price)
            t = safe_int(timestamp)

            if (
                p is not None
                and math.isfinite(p)
                and t is not None
            ):
                pairs.append((t, p))

        pairs.sort(
            key=lambda item: item[0]
        )

        numeric_prices = [
            price
            for _, price in pairs
        ]

    return numeric_prices


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
        "tick"
    )

    if not isinstance(tick, dict):
        return None

    return safe_float(
        tick.get("quote")
    )


# ============================================================
# DIGIT ANALYSIS
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
            price * (10 ** decimals)
        )

        return abs(
            scaled
        ) % 10

    text = f"{price:.10f}".rstrip("0")

    if "." in text:
        fractional = text.split(
            ".",
            1
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
        digit: 0
        for digit in range(10)
    }

    for digit in digits:
        if digit in counts:
            counts[digit] += 1

    total = len(digits)

    if total <= 0:
        return {
            digit: 0.0
            for digit in range(10)
        }

    return {
        digit: (
            counts[digit] / total
        ) * 100.0
        for digit in range(10)
    }


def predict_digit_direction(
    digits: List[int],
    barrier: int,
) -> Tuple[str, float, Dict[str, Any]]:

    if len(digits) < 20:
        return (
            "NO TRADE",
            0.0,
            {
                "over_probability": 0.0,
                "under_probability": 0.0,
            },
        )

    barrier = max(
        0,
        min(
            9,
            int(barrier),
        ),
    )

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

    over_probability = (
        over_count / total
    ) * 100.0

    under_probability = (
        under_count / total
    ) * 100.0

    if over_probability > under_probability:
        direction = "OVER"
        confidence = over_probability
    else:
        direction = "UNDER"
        confidence = under_probability

    # Digit contracts have a theoretical base probability
    # determined by the barrier. We only call it a meaningful
    # edge when the observed probability clears the baseline.
    if direction == "OVER":
        baseline = (
            (9 - barrier) / 10
        ) * 100.0
    else:
        baseline = (
            barrier / 10
        ) * 100.0

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


# ============================================================
# TECHNICAL FEATURES
# ============================================================

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
            (value - ema)
            * multiplier
        ) + ema

    return ema


def calculate_rsi(
    values: List[float],
    period: int = 14,
) -> Optional[float]:

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = (
            values[i] - values[i - 1]
        )

        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    recent_gains = gains[-period:]
    recent_losses = losses[-period:]

    average_gain = statistics.mean(
        recent_gains
    )

    average_loss = statistics.mean(
        recent_losses
    )

    if average_loss == 0:
        return 100.0

    rs = (
        average_gain /
        average_loss
    )

    return 100 - (
        100 / (1 + rs)
    )


def calculate_volatility(
    values: List[float],
    period: int = 50,
) -> Optional[float]:

    if len(values) < period + 1:
        return None

    recent = values[-(
        period + 1
    ):]

    returns = []

    for previous, current in zip(
        recent,
        recent[1:],
    ):
        if previous == 0:
            continue

        returns.append(
            (
                current - previous
            ) / previous
        )

    if len(returns) < 2:
        return None

    return statistics.pstdev(
        returns
    )


# ============================================================
# WALK-FORWARD BACKTEST
# ============================================================

def evaluate_digit_strategy(
    prices: List[float],
    pip: float,
    barrier: int,
) -> Dict[str, Any]:

    if len(prices) < (
        MIN_BACKTEST_SAMPLES + 20
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

    # Walk-forward:
    # At every point, prediction uses only data available
    # before the point being tested.
    start = max(
        20,
        len(digits) - 750,
    )

    for index in range(
        start,
        len(digits) - 1,
    ):

        training = digits[
            :index
        ]

        if len(training) < 20:
            continue

        direction, _, _ = (
            predict_digit_direction(
                training,
                barrier,
            )
        )

        if direction == "NO TRADE":
            continue

        actual = digits[
            index
        ]

        if direction == "OVER":
            success = (
                actual > barrier
            )
        else:
            success = (
                actual < barrier
            )

        samples += 1

        if success:
            correct += 1

    if samples <= 0:
        return {
            "valid": False,
            "accuracy": 0.0,
            "samples": 0,
        }

    accuracy = (
        correct / samples
    ) * 100.0

    return {
        "valid": (
            samples >= MIN_BACKTEST_SAMPLES
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
        prices = await get_tick_history(
            symbol,
            HISTORY_TICKS,
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
            price,
            pip,
        )
        for price in prices
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

    latest_price = prices[-1]

    technical_bias = "NEUTRAL"

    if (
        ema_fast is not None
        and ema_slow is not None
    ):
        if ema_fast > ema_slow:
            technical_bias = "UP"
        elif ema_fast < ema_slow:
            technical_bias = "DOWN"

    tradeable = (
        direction != "NO TRADE"
        and confidence >= MIN_CONFIDENCE_TO_TRADE
        and backtest["valid"]
        and backtest["accuracy"]
        >= MIN_EDGE_ACCURACY
    )

    reason_parts = []

    if direction == "NO TRADE":
        reason_parts.append(
            "Current digit distribution does not show sufficient edge."
        )

    if not backtest["valid"]:
        reason_parts.append(
            "Insufficient valid walk-forward samples."
        )

    elif backtest["accuracy"] < MIN_EDGE_ACCURACY:
        reason_parts.append(
            "Historical walk-forward accuracy is below the configured threshold."
        )

    if confidence < MIN_CONFIDENCE_TO_TRADE:
        reason_parts.append(
            "Current confidence is below the configured threshold."
        )

    if tradeable:
        reason_parts.append(
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

        "markets": [
            symbol,
        ],

        "latest_price": latest_price,

        "pip": pip,

        "barrier": barrier,

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

        "reason": " ".join(
            reason_parts
        ) or "No trade signal.",
    }


async def analyze_live_markets(
    barrier: int = DEFAULT_BARRIER,
    force_fresh: bool = True,
) -> Dict[str, Any]:

    now = time.monotonic()

    if (
        not force_fresh
        and PREDICTION_CACHE.get("data")
        and (
            now
            - PREDICTION_CACHE.get(
                "timestamp",
                0,
            )
        )
        < PREDICTION_CACHE_SECONDS
    ):
        return PREDICTION_CACHE["data"]

    candidates = await get_tradeable_symbols()

    if not candidates:

        prediction = {
            "status": "ok",
            "prediction": {
                "direction": "NO TRADE",
                "asset": None,
                "confidence": 0,
                "historical_accuracy": 0,
                "tradeable": False,
                "markets": [],
                "reason": (
                    "No usable market data was available."
                ),
            },
            "generated_at": iso_now(),
        }

        PREDICTION_CACHE["timestamp"] = now
        PREDICTION_CACHE["data"] = prediction

        return prediction

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

    cleaned = []

    for result in results:
        if isinstance(result, dict):
            cleaned.append(result)

    # Tradeable markets first.
    # Then confidence.
    # Then historical accuracy.
    cleaned.sort(
        key=lambda item: (
            bool(
                item.get("tradeable")
            ),
            float(
                item.get("confidence", 0)
            ),
            float(
                item.get(
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

    if not best:
        prediction_data = {
            "direction": "NO TRADE",
            "asset": None,
            "confidence": 0,
            "historical_accuracy": 0,
            "tradeable": False,
            "markets": [],
            "reason": (
                "No market analysis completed."
            ),
        }

    else:
        prediction_data = {
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
            "tradeable": best.get(
                "tradeable",
                False,
            ),
            "markets": [
                item.get("asset")
                for item in cleaned
                if item.get("asset")
            ],
            "reason": best.get(
                "reason",
                "",
            ),
        }

    output = {
        "status": "ok",
        "prediction": prediction_data,
        "markets": cleaned,
        "generated_at": iso_now(),
    }

    PREDICTION_CACHE["timestamp"] = now
    PREDICTION_CACHE["data"] = output

    return output


# ============================================================
# TRADING VALIDATION
# ============================================================

def validate_stake(
    amount: float,
) -> float:

    amount = safe_float(
        amount,
        0.0,
    ) or 0.0

    if amount <= 0:
        raise HTTPException(
            status_code=400,
            detail="Stake must be greater than zero.",
        )

    if amount > MAX_STAKE:
        raise HTTPException(
            status_code=400,
            detail=f"Stake exceeds MAX_STAKE={MAX_STAKE}.",
        )

    return round(
        amount,
        2,
    )


def validate_duration(
    duration: int,
) -> int:

    duration = safe_int(
        duration,
        DEFAULT_DURATION,
    ) or DEFAULT_DURATION

    if duration < 1 or duration > 100:
        raise HTTPException(
            status_code=400,
            detail="Duration must be between 1 and 100.",
        )

    return duration


def validate_barrier_for_direction(
    direction: str,
    barrier: int,
) -> int:

    barrier = safe_int(
        barrier,
        DEFAULT_BARRIER,
    )

    if barrier is None:
        raise HTTPException(
            status_code=400,
            detail="Invalid barrier.",
        )

    barrier = int(barrier)

    if barrier < 0 or barrier > 9:
        raise HTTPException(
            status_code=400,
            detail="Barrier must be between 0 and 9.",
        )

    direction = direction.upper()

    # DIGITOVER barrier 9 can never win.
    if direction == "OVER" and barrier >= 9:
        raise HTTPException(
            status_code=400,
            detail="DIGITOVER barrier must be between 0 and 8.",
        )

    # DIGITUNDER barrier 0 can never win.
    if direction == "UNDER" and barrier <= 0:
        raise HTTPException(
            status_code=400,
            detail="DIGITUNDER barrier must be between 1 and 9.",
        )

    return barrier


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

    if account_type == "real" and not REAL_TRADING_ENABLED:
        raise HTTPException(
            status_code=403,
            detail=(
                "Real trading is disabled on this backend."
            ),
        )

    direction = direction.upper()

    if direction not in {
        "OVER",
        "UNDER",
    }:
        raise HTTPException(
            status_code=400,
            detail="Direction must be OVER or UNDER.",
        )

    contract_type = (
        "DIGITOVER"
        if direction == "OVER"
        else "DIGITUNDER"
    )

    proposal_payload = {
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
        proposal_payload,
        timeout=20,
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

    if not isinstance(proposal, dict):
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


# ============================================================
# BUY
# ============================================================

async def buy_proposal(
    session: Dict[str, Any],
    account_type: str,
    proposal_id: str,
    price: float,
) -> Dict[str, Any]:

    if account_type == "real" and not REAL_TRADING_ENABLED:
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
        timeout=20,
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

    if not isinstance(buy_data, dict):
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


# ============================================================
# OPEN CONTRACT
# ============================================================

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
        timeout=20,
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

    if not isinstance(contract, dict):
        raise HTTPException(
            status_code=502,
            detail="Invalid contract response.",
        )

    return contract


def contract_is_finished(
    contract: Dict[str, Any]
) -> bool:

    is_sold = contract.get(
        "is_sold"
    )

    status = str(
        contract.get(
            "status",
            ""
        )
    ).lower()

    if is_sold in {
        1,
        True,
        "1",
        "true",
        "True",
    }:
        return True

    if status in {
        "sold",
        "won",
        "lost",
        "expired",
        "closed",
    }:
        return True

    return False


def final_profit_from_contract(
    contract: Dict[str, Any]
) -> Optional[float]:

    for key in [
        "profit",
        "sell_profit",
    ]:

        value = safe_float(
            contract.get(key)
        )

        if value is not None:
            return value

    return None


# ============================================================
# RECORD TRADE
# ============================================================

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
            else (
                "LOSS"
                if profit < 0
                else "BREAK_EVEN"
            )
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

    # These session totals are retained for compatibility
    # with the existing frontend.
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


# ============================================================
# MONITOR CONTRACT
# ============================================================

async def buy_and_monitor(
    session: Dict[str, Any],
    account_type: str,
    proposal_id: str,
    proposal_price: float,
    trade_metadata: Dict[str, Any],
) -> Dict[str, Any]:

    bought = await buy_proposal(
        session,
        account_type,
        proposal_id,
        proposal_price,
    )

    contract_id = bought[
        "contract_id"
    ]

    # IMPORTANT:
    # Once Deriv has accepted the buy, we preserve
    # active_trade immediately. This prevents an open
    # contract from disappearing from the dashboard if
    # monitoring later encounters a temporary error.
    session["active_trade"] = {
        **trade_metadata,
        "account": account_type,
        "contract_id": contract_id,
        "status": "OPEN",
        "bought_at": iso_now(),
    }

    last_contract = None

    for _ in range(180):

        try:
            contract = await get_open_contract(
                session,
                account_type,
                contract_id,
            )

            last_contract = contract

            session["active_trade"][
                "contract"
            ] = contract

            if contract_is_finished(
                contract
            ):
                profit = final_profit_from_contract(
                    contract
                )

                if profit is None:
                    # Do not invent a loss when Deriv hasn't
                    # supplied the final result.
                    session["active_trade"][
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

                session["active_trade"] = None

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

        except Exception as exc:

            # Preserve the open trade.
            if session.get(
                "active_trade"
            ):
                session["active_trade"][
                    "monitor_error"
                ] = str(exc)

            await asyncio.sleep(2)

            continue

        await asyncio.sleep(2)

    # Contract may still be open.
    if session.get(
        "active_trade"
    ):
        session["active_trade"][
            "status"
        ] = "MONITORING_TIMEOUT"

    return {
        "status": "monitoring_timeout",
        "contract": last_contract,
        "contract_id": contract_id,
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
                "Trading session is protected after "
                f"{MAX_CONSECUTIVE_LOSSES} consecutive losses."
            ),
        )

    amount = validate_stake(
        amount
    )

    duration = validate_duration(
        duration
    )

    direction = direction.upper()

    barrier = validate_barrier_for_direction(
        direction,
        barrier,
    )

    if direction == "NO TRADE":
        return {
            "status": "no_trade",
            "message": "Prediction returned NO TRADE.",
        }

    if account_type == "real":
        if not REAL_TRADING_ENABLED:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled. "
                    "Set REAL_TRADING_ENABLED=true "
                    "on the backend to enable it."
                ),
            )

        if not session.get(
            "real_market_mode",
            False,
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Real market mode is not enabled "
                    "for this session."
                ),
            )

    balance = session[
        "balances"
    ].get(
        account_type
    )

    if balance is not None:
        if amount > balance:
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
            "display_value"
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
# PYDANTIC MODELS
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
# HEALTH
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


@app.get("/api/session/status/{session_id}")
async def session_status(
    session_id: str
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
                session["accounts"].get(
                    "demo"
                )
            ),
            "real": bool(
                session["accounts"].get(
                    "real"
                )
            ),
        },
        "trading": session[
            "trading"
        ],
        "real_trading_enabled": REAL_TRADING_ENABLED,
        "active_trade": session[
            "active_trade"
        ],
    }


# ============================================================
# OAUTH LOGIN
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

    verifier = create_pkce_verifier()
    challenge = create_pkce_challenge(
        verifier
    )

    state = secrets.token_urlsafe(
        32
    )

    OAUTH_STATES[state] = {
        "session_id": session_id,
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
        f"{DERIV_AUTH_BASE}/oauth2/authorize?"
        + urllib.parse.urlencode(
            params
        )
    )

    return {
        "status": "ok",
        "authorization_url": authorization_url,
    }


# ============================================================
# OAUTH CALLBACK
# ============================================================

@app.get("/auth/deriv/callback")
async def deriv_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):

    if error:
        if FRONTEND_ORIGIN:
            url = (
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(str(error))}"
            )

            return RedirectResponse(
                url=url,
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
            detail="Invalid or expired OAuth state.",
        )

    if (
        time.time()
        - oauth_data["created_at"]
        > 600
    ):
        raise HTTPException(
            status_code=400,
            detail="OAuth state expired.",
        )

    session_id = oauth_data[
        "session_id"
    ]

    session = ensure_session(
        session_id
    )

    verifier = oauth_data[
        "verifier"
    ]

    token_payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": DERIV_CLIENT_ID,
        "redirect_uri": DERIV_REDIRECT_URI,
        "code_verifier": verifier,
    }

    token_url = (
        f"{DERIV_AUTH_BASE}/oauth2/token"
    )

    async with httpx.AsyncClient(
        timeout=20
    ) as client:

        response = await client.post(
            token_url,
            data=token_payload,
            headers={
                "Accept": "application/json",
            },
        )

    if response.status_code >= 400:

        error_text = response.text[:1000]

        if FRONTEND_ORIGIN:
            url = (
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(error_text)}"
            )

            return RedirectResponse(
                url=url,
                status_code=302,
            )

        raise HTTPException(
            status_code=502,
            detail="Deriv OAuth token exchange failed.",
        )

    token_data = response.json()

    session["access_token"] = token_data.get(
        "access_token"
    )

    session["refresh_token"] = token_data.get(
        "refresh_token"
    )

    expires_in = safe_int(
        token_data.get(
            "expires_in"
        ),
        3600,
    )

    session["token_expires_at"] = (
        time.time()
        + (
            expires_in
            if expires_in
            else 3600
        )
    )

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

        session[
            "connected"
        ] = True

        session[
            "connection_status"
        ] = "connected"

    except Exception as exc:

        session[
            "connection_status"
        ] = "error"

        if FRONTEND_ORIGIN:
            url = (
                f"{FRONTEND_ORIGIN}"
                f"?deriv=error"
                f"&error="
                f"{urllib.parse.quote(str(exc))}"
            )

            return RedirectResponse(
                url=url,
                status_code=302,
            )

        raise

    if FRONTEND_ORIGIN:

        url = (
            f"{FRONTEND_ORIGIN}"
            f"?deriv=connected"
            f"&session_id="
            f"{urllib.parse.quote(session_id)}"
        )

        return RedirectResponse(
            url=url,
            status_code=302,
        )

    return {
        "status": "connected",
        "session_id": session_id,
    }


# ============================================================
# BALANCE
# ============================================================

@app.get("/api/account/balance/{session_id}")
async def account_balance(
    session_id: str
):

    session = get_session(
        session_id
    )

    if session["connected"]:

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
        "currencies": session[
            "account_currencies"
        ],
    }


# ============================================================
# MARKETS
# ============================================================

@app.get("/api/markets")
async def markets():

    result = await analyze_live_markets(
        barrier=DEFAULT_BARRIER,
        force_fresh=False,
    )

    return {
        "status": "ok",
        "markets": result.get(
            "markets",
            [],
        ),
        "count": len(
            result.get(
                "markets",
                [],
            )
        ),
        "generated_at": result.get(
            "generated_at"
        ),
    }


# ============================================================
# PREDICTION
# ============================================================

@app.get("/api/market/prediction")
async def market_prediction(
    barrier: int = Query(
        DEFAULT_BARRIER,
        ge=0,
        le=9,
    ),
):

    result = await analyze_live_markets(
        barrier=barrier,
        force_fresh=True,
    )

    return result


# ============================================================
# STATS
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

    win_rate = (
        (
            wins / trades
        ) * 100.0
        if trades > 0
        else 0.0
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
            win_rate,
            2,
        ),
    }


# ============================================================
# TRADES / HISTORY
# ============================================================

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

    history_key = (
        "demo_history"
        if account == "demo"
        else "real_history"
    )

    return {
        "status": "ok",
        "account": account,
        "trades": session[
            history_key
        ],
    }


# ============================================================
# TRADING STATUS
# ============================================================

@app.get("/api/trading/status/{session_id}")
async def trading_status(
    session_id: str
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
        "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
    }


# ============================================================
# START TRADING
# ============================================================

@app.post("/api/trading/start")
async def start_trading(
    request: TradingStartRequest
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

    if not session["connected"]:
        raise HTTPException(
            status_code=401,
            detail="Connect Deriv account first.",
        )

    if account == "real":

        if not REAL_TRADING_ENABLED:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Real trading is disabled on the backend."
                ),
            )

        if not session["accounts"].get(
            "real"
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "No real Options account is available."
                ),
            )

    else:

        if not session["accounts"].get(
            "demo"
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "No demo Options account is available."
                ),
            )

    session["account"] = account
    session["stake"] = stake
    session["duration"] = duration
    session["real_market_mode"] = (
        bool(
            request.real_market_mode
        )
        if account == "real"
        else False
    )

    session["trading"] = True

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


# ============================================================
# STOP TRADING
# ============================================================

@app.post("/api/trading/stop")
async def stop_trading(
    request: TradingStopRequest
):

    session = get_session(
        request.session_id
    )

    session["trading"] = False

    # IMPORTANT:
    # We do not cancel an already purchased contract.
    # Stop Trading only prevents future trades.
    active_trade = session.get(
        "active_trade"
    )

    return {
        "status": "ok",
        "trading": False,
        "active_trade": active_trade,
        "message": (
            "Future trades stopped. "
            "Any already-open Deriv contract continues "
            "until it finishes."
        ),
    }


# ============================================================
# MANUAL TRADE
# ============================================================

@app.post("/api/trade")
async def manual_trade(
    request: TradeRequest
):

    session = get_session(
        request.session_id
    )

    account = normalize_account_type(
        request.account
    )

    if not session["connected"]:
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
            detail="Direction must be OVER, UNDER or NO TRADE.",
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

    async with session["lock"]:

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
# AUTO TRADE
# ============================================================

@app.post("/api/trading/execute")
async def execute_current_prediction(
    session_id: str = Query(...),
):

    session = get_session(
        session_id
    )

    if not session["connected"]:
        raise HTTPException(
            status_code=401,
            detail="Connect Deriv account first.",
        )

    if not session["trading"]:
        raise HTTPException(
            status_code=400,
            detail="Trading is stopped.",
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
                detail=(
                    "Real market mode is not enabled."
                ),
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

    prediction_result = await analyze_live_markets(
        barrier=DEFAULT_BARRIER,
        force_fresh=True,
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

    async with session["lock"]:

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
    request: DisconnectRequest
):

    session = get_session(
        request.session_id
    )

    # Stop future trades.
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

    # We intentionally do not delete historical stats here.
    # The session can be reconnected later.
    #
    # An already purchased contract is not cancelled here.
    # Deriv controls its lifecycle.

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
        f"[ERROR] {request.method} "
        f"{request.url}: {exc}"
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
