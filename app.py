import asyncio
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

APP_VERSION = "8.0.0"
APP_NAME = "Fixed Risk Booster"

DERIV_CLIENT_ID = os.getenv("DERIV_CLIENT_ID", "").strip()
DERIV_REDIRECT_URI = os.getenv("DERIV_REDIRECT_URI", "").strip()

DERIV_AUTH_BASE = "https://auth.deriv.com"
DERIV_REST_BASE = "https://api.derivws.com"
DERIV_PUBLIC_WS = (
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

DERIV_OAUTH_SCOPE = "trade"

FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "",
).strip().rstrip("/")

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

DEFAULT_STAKE = 2.0
MAX_STAKE = float(
    os.getenv(
        "MAX_STAKE",
        "1000",
    )
)

DEFAULT_DURATION = 5
DEFAULT_DURATION_UNIT = "t"
DEFAULT_BARRIER = 5

MAX_CONSECUTIVE_LOSSES = 3
MAX_HISTORY_RECORDS = 100

PREDICTION_CACHE_SECONDS = 3

SELECTION_ACCURACY_WEIGHT = 0.50
SELECTION_CONFIDENCE_WEIGHT = 0.50

OAUTH_STATE_TTL_SECONDS = 600


# ============================================================
# APP
# ============================================================

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# GLOBAL MEMORY
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}
OAUTH_STATES: Dict[str, Dict[str, Any]] = {}

PREDICTION_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "data": None,
}

MARKET_CACHE: Dict[str, Any] = {
    "timestamp": 0.0,
    "markets": [],
}

MARKET_CACHE_LOCK = asyncio.Lock()
PREDICTION_LOCK = asyncio.Lock()


# ============================================================
# BASIC HELPERS
# ============================================================

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:
    try:
        if value is None:
            return default

        if isinstance(value, bool):
            return default

        number = float(value)

        if not math.isfinite(number):
            return default

        return number

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


def clean_symbol(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip().upper()


def normalize_account_type(
    value: Any,
) -> str:
    value = str(
        value or "demo"
    ).strip().lower()

    if value in {
        "real",
        "live",
    }:
        return "real"

    return "demo"


def readable_api_error(
    value: Any,
    fallback: str = "Unknown error.",
) -> str:

    if value is None:
        return fallback

    if isinstance(value, str):
        return value

    if isinstance(value, dict):

        # Direct common fields.
        for key in (
            "message",
            "detail",
            "reason",
            "error_description",
            "description",
        ):
            candidate = value.get(key)

            if candidate:
                text = readable_api_error(
                    candidate,
                    fallback,
                )

                code = value.get("code")

                if (
                    code
                    and text != fallback
                ):
                    return f"{code}: {text}"

                return text

        # REST responses can contain errors arrays.
        errors = value.get("errors")

        if isinstance(
            errors,
            list,
        ) and errors:

            first = errors[0]

            return readable_api_error(
                first,
                fallback,
            )

        # Nested error.
        if "error" in value:

            return readable_api_error(
                value.get("error"),
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


def error_response_message(
    exc: Exception,
) -> str:
    return readable_api_error(
        exc,
        "Request failed.",
    )


# ============================================================
# SESSION MANAGEMENT
# ============================================================

def new_session(
    session_id: Optional[str] = None,
) -> Dict[str, Any]:

    if session_id:

        session_id = str(
            session_id
        ).strip()

        # IMPORTANT:
        # Never reset an existing session.
        if session_id in USER_SESSIONS:
            return USER_SESSIONS[
                session_id
            ]

    else:
        session_id = secrets.token_urlsafe(
            24
        )

    session = {
        "session_id": session_id,

        "connected": False,

        "connection_status": "disconnected",

        "access_token": None,

        "refresh_token": None,

        "token_expires_at": None,

        "accounts": {
            "demo": None,
            "real": None,
        },

        "balances": {
            "demo": None,
            "real": None,
        },

        "account_currencies": {
            "demo": "USD",
            "real": "USD",
        },

        "account_debug": [],

        "account": "demo",

        "stake": DEFAULT_STAKE,

        "duration": DEFAULT_DURATION,

        "duration_unit": DEFAULT_DURATION_UNIT,

        "real_market_mode": False,

        "trading": False,

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

        # Compatibility fields.
        "session_profit": 0.0,
        "session_trades": 0,
        "session_wins": 0,
        "session_losses": 0,
        "consecutive_losses": 0,

        "consecutive_losses_by_account": {
            "demo": 0,
            "real": 0,
        },

        "active_trade": None,

        "last_prediction": None,

        "session_recreated_after_restart": False,

        "lock": asyncio.Lock(),
    }

    USER_SESSIONS[
        session_id
    ] = session

    return session


def ensure_session(
    session_id: str,
) -> Dict[str, Any]:

    session_id = str(
        session_id
    ).strip()

    if not session_id:
        return new_session()

    if session_id in USER_SESSIONS:
        return USER_SESSIONS[
            session_id
        ]

    return new_session(
        session_id
    )


def get_session(
    session_id: str,
) -> Dict[str, Any]:

    session_id = str(
        session_id
    ).strip()

    if session_id in USER_SESSIONS:
        return USER_SESSIONS[
            session_id
        ]

    session = new_session(
        session_id
    )

    session[
        "session_recreated_after_restart"
    ] = True

    return session


def cleanup_memory() -> None:

    now = time.time()

    expired = []

    for state, data in list(
        OAUTH_STATES.items()
    ):

        created = safe_float(
            data.get(
                "created_at"
            ),
            0,
        ) or 0

        if (
            now - created
            > OAUTH_STATE_TTL_SECONDS
        ):
            expired.append(state)

    for state in expired:
        OAUTH_STATES.pop(
            state,
            None,
        )


# ============================================================
# LOSS PROTECTION
# ============================================================

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

    session[
        "consecutive_losses_by_account"
    ][
        account_type
    ] = max(
        0,
        int(value),
    )


# ============================================================
# PKCE
# ============================================================

def create_pkce_verifier() -> str:
    return secrets.token_urlsafe(
        64
    )


def create_pkce_challenge(
    verifier: str,
) -> str:

    digest = hashlib.sha256(
        verifier.encode()
    ).digest()

    return (
        __import__(
            "base64"
        )
        .urlsafe_b64encode(
            digest
        )
        .decode()
        .rstrip("=")
    )


# ============================================================
# DERIV REST
# ============================================================

async def deriv_rest_request(
    session: Dict[str, Any],
    method: str,
    path: str,
    *,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    token = session.get(
        "access_token"
    )

    if not token:
        raise HTTPException(
            status_code=401,
            detail=(
                "Deriv account is not connected."
            ),
        )

    url = (
        f"{DERIV_REST_BASE}"
        f"{path}"
    )

    headers = {
        "Authorization": (
            f"Bearer {token}"
        ),
        "Accept": "application/json",
    }

    try:

        async with httpx.AsyncClient(
            timeout=30
        ) as client:

            response = await client.request(
                method.upper(),
                url,
                headers=headers,
                json=data,
                params=params,
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
        payload = response.json()

    except Exception:
        payload = response.text

    if response.status_code >= 400:

        raise HTTPException(
            status_code=(
                response.status_code
                if response.status_code < 600
                else 502
            ),
            detail=readable_api_error(
                payload,
                "Deriv REST request failed.",
            ),
        )

    if not isinstance(
        payload,
        dict,
    ):

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv returned an invalid "
                "REST response."
            ),
        )

    return payload


# ============================================================
# ACCOUNT DISCOVERY
# ============================================================

def _extract_accounts(
    value: Any,
) -> List[Dict[str, Any]]:

    found: List[
        Dict[str, Any]
    ] = []

    def walk(
        node: Any,
    ) -> None:

        if isinstance(
            node,
            dict,
        ):

            # Looks like an account object.
            if (
                node.get("account_id")
                or node.get("accountId")
                or (
                    node.get("id")
                    and (
                        node.get(
                            "account_type"
                        )
                        or node.get(
                            "type"
                        )
                    )
                )
            ):

                found.append(
                    node
                )

            for child in node.values():
                walk(child)

        elif isinstance(
            node,
            list,
        ):

            for item in node:
                walk(item)

    walk(value)

    unique = []
    seen = set()

    for item in found:

        account_id = (
            item.get("account_id")
            or item.get("accountId")
            or item.get("id")
        )

        if account_id is None:
            continue

        key = str(
            account_id
        )

        if key in seen:
            continue

        seen.add(key)
        unique.append(item)

    return unique


def detect_account_type(
    account: Dict[str, Any],
) -> Optional[str]:

    raw = " ".join(
        str(
            account.get(
                key,
                "",
            )
        )
        for key in (
            "account_type",
            "type",
            "environment",
            "mode",
            "account_category",
        )
    ).lower()

    if "demo" in raw:
        return "demo"

    if "real" in raw:
        return "real"

    account_id = str(
        account.get(
            "account_id",
            account.get(
                "accountId",
                account.get(
                    "id",
                    "",
                ),
            ),
        )
    ).lower()

    if (
        account_id.startswith("vrtc")
        or "demo" in account_id
    ):
        return "demo"

    if (
        account_id.startswith("cr")
        or "real" in account_id
    ):
        return "real"

    return None


async def load_options_accounts(
    session: Dict[str, Any],
) -> Dict[str, Any]:

    response = await deriv_rest_request(
        session,
        "GET",
        "/trading/v1/options/accounts",
    )

    accounts = _extract_accounts(
        response
    )

    session[
        "account_debug"
    ] = []

    detected = {
        "demo": None,
        "real": None,
    }

    for item in accounts:

        account_type = detect_account_type(
            item
        )

        account_id = (
            item.get("account_id")
            or item.get("accountId")
            or item.get("id")
        )

        currency = (
            item.get("currency")
            or "USD"
        )

        balance = safe_float(
            item.get("balance")
        )

        debug_item = {
            "account_type": account_type,
            "account_id": (
                str(account_id)
                if account_id
                else None
            ),
            "currency": str(
                currency
            ),
            "balance": balance,
        }

        session[
            "account_debug"
        ].append(
            debug_item
        )

        if (
            account_type in detected
            and account_id
            and not detected[
                account_type
            ]
        ):

            detected[
                account_type
            ] = str(account_id)

            session[
                "account_currencies"
            ][
                account_type
            ] = str(currency)

            if balance is not None:

                session[
                    "balances"
                ][
                    account_type
                ] = balance

    session[
        "accounts"
    ] = detected

    if not detected["demo"] and not detected["real"]:

        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv returned Options account "
                "data, but no Demo or Real Options "
                "account could be identified."
            ),
        )

    return response


# ============================================================
# PUBLIC WEBSOCKET
# ============================================================

async def public_ws_connection():

    return websockets.connect(
        DERIV_PUBLIC_WS,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    )


async def send_ws_request(
    ws,
    payload: Dict[str, Any],
    label: str,
    timeout: float = 20,
) -> Dict[str, Any]:

    request = dict(
        payload
    )

    req_id = secrets.randbelow(
        2_000_000_000
    )

    request[
        "req_id"
    ] = req_id

    await ws.send(
        json.dumps(
            request
        )
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

        try:

            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=remaining,
            )

        except asyncio.TimeoutError:

            raise HTTPException(
                status_code=504,
                detail=(
                    f"Timed out waiting for "
                    f"Deriv {label} response."
                ),
            )

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

        response_req_id = response.get(
            "req_id"
        )

        if (
            response_req_id is not None
            and int(response_req_id)
            != int(req_id)
        ):
            continue

        return response

    raise HTTPException(
        status_code=504,
        detail=(
            f"Timed out waiting for "
            f"Deriv {label} response."
        ),
    )


async def get_active_symbols_on_ws(
    ws,
) -> List[Dict[str, Any]]:

    response = await send_ws_request(
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
            status_code=502,
            detail=readable_api_error(
                response["error"],
                "Unable to load active symbols.",
            ),
        )

    symbols = response.get(
        "active_symbols",
        [],
    )

    if not isinstance(
        symbols,
        list,
    ):
        return []

    return symbols


def normalize_active_symbol(
    item: Dict[str, Any],
) -> Dict[str, Any]:

    symbol = clean_symbol(
        item.get("symbol")
    )

    return {
        "symbol": symbol,
        "display_name": (
            item.get(
                "display_name"
            )
            or symbol
        ),
        "symbol_type": item.get(
            "symbol_type"
        ),
        "market": item.get(
            "market"
        ),
        "subgroup": item.get(
            "subgroup"
        ),
        "submarket": item.get(
            "submarket"
        ),
        "pip": safe_float(
            item.get("pip")
        ),
        "exchange_is_open": item.get(
            "exchange_is_open"
        ),
        "is_trading_suspended": item.get(
            "is_trading_suspended"
        ),
        "asset": item.get(
            "asset"
        ),
        "latest_price": safe_float(
            item.get(
                "latest_price"
            )
        ),
    }


def market_is_candidate(
    item: Dict[str, Any],
) -> bool:

    symbol = clean_symbol(
        item.get("symbol")
    )

    if not symbol:
        return False

    market = str(
        item.get(
            "market",
            "",
        )
    ).lower()

    symbol_type = str(
        item.get(
            "symbol_type",
            "",
        )
    ).lower()

    if market not in {
        "synthetic_index",
        "derived",
        "synthetic",
    } and symbol_type not in {
        "stockindex",
        "synthetic_index",
        "derived",
    }:
        return False

    if item.get(
        "exchange_is_open"
    ) in {
        0,
        False,
        "0",
        "false",
        "False",
    }:
        return False

    if item.get(
        "is_trading_suspended"
    ) in {
        1,
        True,
        "1",
        "true",
        "True",
    }:
        return False

    return True


async def verify_digit_contract_support_on_ws(
    ws,
    symbol: str,
) -> bool:

    try:

        response = await send_ws_request(
            ws,
            {
                "contracts_for": symbol,
            },
            f"contracts_for {symbol}",
            timeout=15,
        )

    except Exception:
        return False

    if response.get(
        "error"
    ):
        return False

    data = response.get(
        "contracts_for"
    )

    if not isinstance(
        data,
        dict,
    ):
        return False

    available = data.get(
        "available",
        [],
    )

    if not isinstance(
        available,
        list,
    ):
        return False

    for contract in available:

        if not isinstance(
            contract,
            dict,
        ):
            continue

        contract_type = str(
            contract.get(
                "contract_type",
                "",
            )
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

    if (
        not force_refresh
        and MARKET_CACHE["markets"]
        and (
            now
            - MARKET_CACHE["timestamp"]
            < 30
        )
    ):
        return MARKET_CACHE[
            "markets"
        ]

    async with MARKET_CACHE_LOCK:

        now = time.monotonic()

        if (
            not force_refresh
            and MARKET_CACHE["markets"]
            and (
                now
                - MARKET_CACHE["timestamp"]
                < 30
            )
        ):
            return MARKET_CACHE[
                "markets"
            ]

        async with public_ws_connection() as ws:

            active = (
                await get_active_symbols_on_ws(
                    ws
                )
            )

            candidates = []

            for raw in active:

                item = normalize_active_symbol(
                    raw
                )

                if not market_is_candidate(
                    item
                ):
                    continue

                symbol = item[
                    "symbol"
                ]

                if await verify_digit_contract_support_on_ws(
                    ws,
                    symbol,
                ):

                    item[
                        "digit_contracts"
                    ] = True

                    candidates.append(
                        item
                    )

            MARKET_CACHE[
                "markets"
            ] = candidates

            MARKET_CACHE[
                "timestamp"
            ] = time.monotonic()

            return candidates


# ============================================================
# TICKS
# ============================================================

async def get_tick_history_on_ws(
    ws,
    symbol: str,
    count: int = 2000,
) -> List[float]:

    response = await send_ws_request(
        ws,
        {
            "ticks_history": symbol,
            "count": count,
            "end": "latest",
            "style": "ticks",
        },
        f"tick history {symbol}",
        timeout=30,
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=502,
            detail=readable_api_error(
                response["error"],
                f"Unable to load ticks for {symbol}.",
            ),
        )

    history = response.get(
        "history",
        {}
    )

    if not isinstance(
        history,
        dict,
    ):
        return []

    prices = history.get(
        "prices",
        []
    )

    if not isinstance(
        prices,
        list,
    ):
        return []

    output = []

    for price in prices:

        value = safe_float(
            price
        )

        if (
            value is not None
            and value > 0
        ):
            output.append(
                value
            )

    return output


async def get_latest_tick_on_ws(
    ws,
    symbol: str,
) -> Optional[float]:

    response = await send_ws_request(
        ws,
        {
            "ticks": symbol,
        },
        f"latest tick {symbol}",
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
# AUTHENTICATED OPTIONS WEBSOCKET
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

    response = await deriv_rest_request(
        session,
        "POST",
        (
            "/trading/v1/options/accounts/"
            f"{urllib.parse.quote(str(account_id), safe='')}"
            "/otp"
        ),
    )

    data = response.get(
        "data"
    )

    if not isinstance(
        data,
        dict,
    ):
        data = response

    # IMPORTANT:
    # Deriv returns a ready-to-use WebSocket URL.
    websocket_url = (
        data.get("url")
        or data.get("websocket_url")
        or data.get("ws_url")
    )

    if not websocket_url:
        raise HTTPException(
            status_code=502,
            detail=(
                "Deriv OTP response did not "
                "contain a WebSocket URL."
            ),
        )

    return str(
        websocket_url
    )


async def authenticated_ws_connection(
    session: Dict[str, Any],
    account_type: str,
):

    websocket_url = await get_deriv_otp(
        session,
        account_type,
    )

    return websockets.connect(
        websocket_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    )


async def authenticated_ws_request(
    session: Dict[str, Any],
    account_type: str,
    payload: Dict[str, Any],
    label: str,
    timeout: float = 20,
) -> Dict[str, Any]:

    async with authenticated_ws_connection(
        session,
        account_type,
    ) as ws:

        return await send_ws_request(
            ws,
            payload,
            label,
            timeout,
        )


# ============================================================
# BALANCES
# ============================================================

async def get_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> Optional[float]:

    response = await authenticated_ws_request(
        session,
        account_type,
        {
            "balance": 1,
        },
        f"{account_type} balance",
        timeout=20,
    )

    if response.get(
        "error"
    ):

        raise HTTPException(
            status_code=502,
            detail=readable_api_error(
                response["error"],
                (
                    f"Unable to retrieve "
                    f"{account_type} balance."
                ),
            ),
        )

    balance_data = response.get(
        "balance"
    )

    if isinstance(
        balance_data,
        dict,
    ):

        balance = safe_float(
            balance_data.get(
                "balance"
            )
        )

        if balance is not None:
            return balance

    balance = safe_float(
        response.get(
            "balance"
        )
    )

    return balance


async def refresh_account_balance(
    session: Dict[str, Any],
    account_type: str,
) -> Optional[float]:

    account_type = normalize_account_type(
        account_type
    )

    balance = await get_account_balance(
        session,
        account_type,
    )

    session[
        "balances"
    ][
        account_type
    ] = balance

    return balance


async def refresh_all_balances(
    session: Dict[str, Any],
) -> None:

    errors = []

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

            errors.append(
                {
                    "account": account_type,
                    "error": readable_api_error(
                        exc
                    ),
                }
            )

    if errors:

        session[
            "account_debug"
        ].append(
            {
                "balance_refresh_errors": errors,
                "timestamp": iso_now(),
            }
        )


# ============================================================
# DIGIT ANALYSIS
# ============================================================

def extract_last_digit(
    price: float,
) -> Optional[int]:

    try:

        text = (
            f"{price:.8f}"
            .rstrip("0")
            .rstrip(".")
        )

        digits = [
            c
            for c in text
            if c.isdigit()
        ]

        if not digits:
            return None

        return int(
            digits[-1]
        )

    except Exception:
        return None


def digit_distribution(
    prices: List[float],
) -> Dict[int, int]:

    distribution = {
        digit: 0
        for digit in range(10)
    }

    for price in prices:

        digit = extract_last_digit(
            price
        )

        if digit is not None:
            distribution[
                digit
            ] += 1

    return distribution


def predict_digit_direction(
    prices: List[float],
    barrier: int,
) -> Tuple[str, float, int]:

    if not prices:
        return (
            "NO TRADE",
            0.0,
            0,
        )

    recent = prices[
        -500:
    ]

    digits = []

    for price in recent:

        digit = extract_last_digit(
            price
        )

        if digit is not None:
            digits.append(
                digit
            )

    if len(digits) < 30:
        return (
            "NO TRADE",
            0.0,
            0,
        )

    over_count = sum(
        1
        for digit in digits
        if digit > barrier
    )

    under_count = len(
        digits
    ) - over_count

    over_probability = (
        over_count
        / len(digits)
    )

    under_probability = (
        under_count
        / len(digits)
    )

    if (
        over_probability
        >= under_probability
    ):

        direction = "OVER"
        probability = (
            over_probability
        )

    else:

        direction = "UNDER"
        probability = (
            under_probability
        )

    confidence = (
        probability * 100
    )

    return (
        direction,
        round(
            confidence,
            2,
        ),
        len(digits),
    )


def calculate_ema(
    values: List[float],
    period: int,
) -> Optional[float]:

    if len(values) < period:
        return None

    multiplier = (
        2
        / (period + 1)
    )

    ema = statistics.mean(
        values[
            :period
        ]
    )

    for value in values[
        period:
    ]:

        ema = (
            (
                value - ema
            )
            * multiplier
            + ema
        )

    return ema


def calculate_rsi(
    values: List[float],
    period: int = 14,
) -> Optional[float]:

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for index in range(
        1,
        len(values),
    ):

        change = (
            values[index]
            - values[index - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    recent_gains = gains[
        -period:
    ]

    recent_losses = losses[
        -period:
    ]

    average_gain = (
        statistics.mean(
            recent_gains
        )
    )

    average_loss = (
        statistics.mean(
            recent_losses
        )
    )

    if average_loss == 0:
        return 100.0

    rs = (
        average_gain
        / average_loss
    )

    return (
        100
        - (
            100
            / (
                1 + rs
            )
        )
    )


def calculate_volatility(
    values: List[float],
) -> float:

    if len(values) < 2:
        return 0.0

    returns = []

    for old, new in zip(
        values[:-1],
        values[1:],
    ):

        if old <= 0:
            continue

        returns.append(
            (
                new - old
            )
            / old
        )

    if len(returns) < 2:
        return 0.0

    return float(
        statistics.pstdev(
            returns
        )
    )


def evaluate_digit_strategy(
    prices: List[float],
    barrier: int,
) -> Dict[str, Any]:

    if len(prices) < 50:

        return {
            "valid": False,
            "accuracy": 0.0,
            "samples": 0,
        }

    # Walk-forward:
    # every prediction uses only data that
    # existed before the evaluated tick.
    correct = 0
    total = 0

    window = min(
        300,
        len(prices) - 1,
    )

    start = len(prices) - window

    for index in range(
        start,
        len(prices),
    ):

        training = prices[
            :index
        ]

        actual_digit = (
            extract_last_digit(
                prices[index]
            )
        )

        if actual_digit is None:
            continue

        direction, _, _ = (
            predict_digit_direction(
                training,
                barrier,
            )
        )

        if direction == "NO TRADE":
            continue

        actual_direction = (
            "OVER"
            if actual_digit > barrier
            else "UNDER"
        )

        total += 1

        if direction == actual_direction:
            correct += 1

    accuracy = (
        correct / total
        if total
        else 0.0
    )

    return {
        "valid": total >= 20,
        "accuracy": round(
            accuracy * 100,
            2,
        ),
        "correct": correct,
        "samples": total,
    }


def calculate_selection_score(
    accuracy: float,
    confidence: float,
) -> float:

    return round(
        (
            (
                accuracy
                * SELECTION_ACCURACY_WEIGHT
            )
            + (
                confidence
                * SELECTION_CONFIDENCE_WEIGHT
            )
        ),
        2,
    )


# ============================================================
# MARKET ANALYSIS
# ============================================================

async def analyze_symbol(
    ws,
    market: Dict[str, Any],
    barrier: int,
) -> Dict[str, Any]:

    symbol = clean_symbol(
        market.get("symbol")
    )

    try:

        prices = await get_tick_history_on_ws(
            ws,
            symbol,
            2000,
        )

        if len(prices) < 50:

            return {
                **market,
                "asset": symbol,
                "valid": False,
                "tradeable": False,
                "reason": (
                    "Insufficient fresh "
                    "tick data."
                ),
            }

        live_price = (
            await get_latest_tick_on_ws(
                ws,
                symbol,
            )
        )

        if (
            live_price is None
            or live_price <= 0
        ):
            live_price = prices[-1]

        direction, confidence, digit_samples = (
            predict_digit_direction(
                prices,
                barrier,
            )
        )

        backtest = (
            evaluate_digit_strategy(
                prices,
                barrier,
            )
        )

        accuracy = safe_float(
            backtest.get(
                "accuracy"
            ),
            0.0,
        ) or 0.0

        ema_fast = calculate_ema(
            prices,
            20,
        )

        ema_slow = calculate_ema(
            prices,
            50,
        )

        rsi = calculate_rsi(
            prices,
            14,
        )

        volatility = calculate_volatility(
            prices[
                -200:
            ]
        )

        valid = (
            direction
            in {
                "OVER",
                "UNDER",
            }
            and backtest.get(
                "valid",
                False,
            )
            and live_price is not None
            and live_price > 0
        )

        score = calculate_selection_score(
            accuracy,
            confidence,
        )

        return {
            **market,

            "asset": symbol,

            "valid": valid,

            "tradeable": False,

            "reason": (
                "Valid fresh analysis."
                if valid
                else "Analysis did not produce "
                "a valid trade."
            ),

            "prediction": direction,

            "direction": direction,

            "barrier": barrier,

            "latest_price": round(
                live_price,
                8,
            ),

            "live_price": round(
                live_price,
                8,
            ),

            "historical_accuracy": accuracy,

            "accuracy": accuracy,

            "confidence": round(
                confidence,
                2,
            ),

            "selection_score": score,

            "samples": digit_samples,

            "backtest_samples": backtest.get(
                "samples",
                0,
            ),

            "ema_fast": ema_fast,

            "ema_slow": ema_slow,

            "rsi": (
                round(rsi, 2)
                if rsi is not None
                else None
            ),

            "volatility": volatility,

            "data_points": len(
                prices
            ),
        }

    except Exception as exc:

        return {
            **market,
            "asset": symbol,
            "valid": False,
            "tradeable": False,
            "reason": readable_api_error(
                exc,
                "Market analysis failed.",
            ),
        }


async def analyze_live_markets(
    barrier: int = DEFAULT_BARRIER,
    force_fresh: bool = False,
) -> Dict[str, Any]:

    barrier = validate_barrier_for_direction(
        "OVER",
        barrier,
    )

    if (
        not force_fresh
        and PREDICTION_CACHE.get(
            "data"
        )
        and (
            time.monotonic()
            - PREDICTION_CACHE.get(
                "timestamp",
                0,
            )
            < PREDICTION_CACHE_SECONDS
        )
    ):

        return PREDICTION_CACHE[
            "data"
        ]

    async with PREDICTION_LOCK:

        if (
            not force_fresh
            and PREDICTION_CACHE.get(
                "data"
            )
            and (
                time.monotonic()
                - PREDICTION_CACHE.get(
                    "timestamp",
                    0,
                )
                < PREDICTION_CACHE_SECONDS
            )
        ):

            return PREDICTION_CACHE[
                "data"
            ]

        eligible = (
            await get_tradeable_symbols(
                force_refresh=force_fresh
            )
        )

        # Analyze a reasonable number of markets.
        selected_markets = eligible[
            :10
        ]

        results = []

        async with public_ws_connection() as ws:

            for market in selected_markets:

                result = await analyze_symbol(
                    ws,
                    market,
                    barrier,
                )

                results.append(
                    result
                )

        valid = [
            item
            for item in results
            if item.get(
                "valid",
                False,
            )
            and item.get(
                "latest_price"
            ) is not None
        ]

        valid.sort(
            key=lambda item: (
                safe_float(
                    item.get(
                        "selection_score"
                    ),
                    0,
                )
                or 0,
                safe_float(
                    item.get(
                        "accuracy"
                    ),
                    0,
                )
                or 0,
                safe_float(
                    item.get(
                        "confidence"
                    ),
                    0,
                )
                or 0,
                safe_int(
                    item.get(
                        "samples"
                    ),
                    0,
                )
                or 0,
            ),
            reverse=True,
        )

        for item in results:
            item[
                "tradeable"
            ] = False

        prediction = {
            "asset": None,
            "direction": "NO TRADE",
            "prediction": "NO TRADE",
            "tradeable": False,
            "latest_price": None,
            "historical_accuracy": 0.0,
            "confidence": 0.0,
            "selection_score": 0.0,
            "barrier": barrier,
            "reason": (
                "No valid best-available "
                "market was found."
            ),
        }

        if valid:

            best = valid[0]

            best[
                "tradeable"
            ] = True

            best[
                "reason"
            ] = (
                "BEST AVAILABLE SIGNAL "
                "from fresh current market data."
            )

            prediction = {
                "asset": best.get(
                    "asset"
                ),

                "direction": best.get(
                    "direction",
                    best.get(
                        "prediction",
                        "NO TRADE",
                    ),
                ),

                "prediction": best.get(
                    "prediction",
                    best.get(
                        "direction",
                        "NO TRADE",
                    ),
                ),

                "tradeable": True,

                "latest_price": best.get(
                    "latest_price"
                ),

                "historical_accuracy": best.get(
                    "historical_accuracy",
                    best.get(
                        "accuracy",
                        0,
                    ),
                ),

                "confidence": best.get(
                    "confidence",
                    0,
                ),

                "selection_score": best.get(
                    "selection_score",
                    0,
                ),

                "barrier": barrier,

                "market": best.get(
                    "display_name",
                    best.get(
                        "asset"
                    ),
                ),

                "reason": best.get(
                    "reason"
                ),
            }

        cleaned = []

        for item in results:

            cleaned.append(
                {
                    "asset": item.get(
                        "asset"
                    ),

                    "symbol": item.get(
                        "symbol"
                    ),

                    "display_name": item.get(
                        "display_name"
                    ),

                    "market": item.get(
                        "market"
                    ),

                    "subgroup": item.get(
                        "subgroup"
                    ),

                    "submarket": item.get(
                        "submarket"
                    ),

                    "latest_price": item.get(
                        "latest_price"
                    ),

                    "live_price": item.get(
                        "live_price"
                    ),

                    "prediction": item.get(
                        "prediction",
                        "NO TRADE",
                    ),

                    "direction": item.get(
                        "direction",
                        "NO TRADE",
                    ),

                    "tradeable": bool(
                        item.get(
                            "tradeable",
                            False,
                        )
                    ),

                    "valid": bool(
                        item.get(
                            "valid",
                            False,
                        )
                    ),

                    "historical_accuracy": item.get(
                        "historical_accuracy",
                        item.get(
                            "accuracy",
                            0,
                        ),
                    ),

                    "accuracy": item.get(
                        "accuracy",
                        0,
                    ),

                    "confidence": item.get(
                        "confidence",
                        0,
                    ),

                    "selection_score": item.get(
                        "selection_score",
                        0,
                    ),

                    "samples": item.get(
                        "samples",
                        0,
                    ),

                    "backtest_samples": item.get(
                        "backtest_samples",
                        0,
                    ),

                    "rsi": item.get(
                        "rsi"
                    ),

                    "volatility": item.get(
                        "volatility"
                    ),

                    "data_points": item.get(
                        "data_points",
                        0,
                    ),

                    "reason": item.get(
                        "reason"
                    ),
                }
            )

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

    if value < 1 or value > 100:

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

    if value < 0 or value > 9:

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

    return int(value)


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
            DEFAULT_DURATION_UNIT,
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
        timeout=25,
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

        # ----------------------------------------------------
        # BUY
        # ----------------------------------------------------

        bought = await send_ws_request(
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

        # ----------------------------------------------------
        # CONTRACT MONITOR
        # ----------------------------------------------------

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

        deadline = (
            time.monotonic()
            + max(
                120,
                duration * 10 + 90,
            )
        )

        last_contract = None

        while time.monotonic() < deadline:

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

            returned_contract_id = str(
                contract.get(
                    "contract_id",
                    contract_id,
                )
            )

            if (
                returned_contract_id
                != contract_id
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

                buy_price = safe_float(
                    contract.get(
                        "buy_price"
                    )
                )

                payout = safe_float(
                    contract.get(
                        "payout"
                    )
                )

                if (
                    buy_price is not None
                    and payout is not None
                ):

                    profit = (
                        payout
                        - buy_price
                    )

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

    barrier = validate_barrier_for_direction(
        direction,
        barrier,
    )

    # --------------------------------------------------------
    # Refresh balance immediately before purchase.
    # --------------------------------------------------------

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

        "app": APP_NAME,

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

    # IMPORTANT:
    # Existing sessions are reused.
    session = ensure_session(
        session_id
        or secrets.token_urlsafe(24)
    )

    return {
        "status": "ok",

        "session_id": session[
            "session_id"
        ],

        "connected": bool(
            session.get(
                "connected",
                False,
            )
        ),

        "account": normalize_account_type(
            session.get(
                "account",
                "demo",
            )
        ),

        "trading": bool(
            session.get(
                "trading",
                False,
            )
        ),
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

        "trading": bool(
            session[
                "trading"
            ]
        ),

        "real_market_mode": bool(
            session[
                "real_market_mode"
            ]
        ),

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

        "session_recreated_after_restart": (
            session.get(
                "session_recreated_after_restart",
                False,
            )
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
                    "Accept": "application/json",

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

        # Always reconnect in Demo UI mode.
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

        "balance": session[
            "balances"
        ].get(
            account
        ),

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

    losses = get_consecutive_losses(
        session,
        account,
    )

    if (
        losses
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
    ] = losses

    session[
        "trading"
    ] = True

    # Return the current authoritative state.
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

        "consecutive_losses": losses,

        "should_execute_trade": True,
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

    result_status = (
        result.get(
            "status"
        )
        if isinstance(
            result,
            dict,
        )
        else None
    )

    return {
        "status": "ok",

        "account": account,

        "message": (
            result.get(
                "message"
            )
            if isinstance(
                result,
                dict,
            )
            else (
                f"{account.capitalize()} "
                "manual trade completed."
            )
        ),

        "trade_status": result_status,

        "profit": (
            result.get(
                "profit"
            )
            if isinstance(
                result,
                dict,
            )
            else None
        ),

        "contract_id": (
            result.get(
                "contract_id"
            )
            if isinstance(
                result,
                dict,
            )
            else None
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
            DEFAULT_STAKE,
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

    result_status = (
        result.get(
            "status"
        )
        if isinstance(
            result,
            dict,
        )
        else None
    )

    return {
        "status": "ok",

        "account": account,

        "message": (
            result.get(
                "message"
            )
            if isinstance(
                result,
                dict,
            )
            else (
                f"{account.capitalize()} "
                "automatic trade completed."
            )
        ),

        "trade_status": result_status,

        "profit": (
            result.get(
                "profit"
            )
            if isinstance(
                result,
                dict,
            )
            else None
        ),

        "contract_id": (
            result.get(
                "contract_id"
            )
            if isinstance(
                result,
                dict,
            )
            else None
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

    # Stats/history intentionally remain.
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
# HTTP ERROR HANDLER
# ============================================================

@app.exception_handler(
    HTTPException
)
async def http_exception_handler(
    request: Request,
    exc: HTTPException,
):

    detail = readable_api_error(
        exc.detail,
        f"HTTP {exc.status_code} error.",
    )

    print(
        "[HTTP ERROR] "
        f"{request.method} "
        f"{request.url}: "
        f"{detail}"
    )

    return JSONResponse(
        status_code=exc.status_code,

        content={
            "status": "error",

            "detail": detail,

            "message": detail,
        },
    )


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

    return JSONResponse(
        status_code=500,

        content={
            "status": "error",

            "detail": message,

            "message": message,
        },
    )
