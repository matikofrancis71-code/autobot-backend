import os
import asyncio
import uuid
import json
import math
from datetime import timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "3.2.0-live-market"

# Your Vercel frontend
FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "*"
)


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Fixed Risk Booster API",
    version=APP_VERSION,
    description=(
        "Backend for Fixed Risk Booster. "
        "Demo and real-account state are isolated."
    )
)


# ============================================================
# CORS
# ============================================================

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
# IN-MEMORY STORAGE
#
# This is intentionally the first backend stage.
#
# IMPORTANT:
# Render instance memory is NOT permanent storage.
# Later we will move account/trade history into a database.
# ============================================================

USER_SESSIONS: Dict[str, Dict[str, Any]] = {}


# ============================================================
# MODELS
# ============================================================

class SessionRegisterRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class TradingStartRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)

    account: str = Field(
        default="demo"
    )

    stake: float = Field(
        gt=0
    )

    real_market_mode: bool = False


class TradingStopRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class TradeRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)

    asset: str = Field(
        min_length=1,
        max_length=64
    )

    amount: float = Field(
        gt=0
    )

    account: str = Field(
        default="demo"
    )

    duration: int = Field(
        default=60,
        gt=0,
        le=3600
    )

    direction: str = Field(default="CALL", min_length=3, max_length=4)


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def create_user_session(
    user_id: str
) -> Dict[str, Any]:

    return {

        "user_id": user_id,

        "connected": False,

        "connection_status": "not_connected",

        # ----------------------------------------------------
        # DEMO ACCOUNT
        # ----------------------------------------------------

        "demo_balance": 10000.00,

        "demo_stats": {
            "profit": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
        },

        "demo_history": [],

        # ----------------------------------------------------
        # REAL ACCOUNT
        #
        # None means:
        # "We have NOT received a verified real balance."
        # ----------------------------------------------------

        "real_balance": None,

        "real_stats": {
            "profit": None,
            "trades": None,
            "wins": None,
            "losses": None,
        },

        "real_history": [],

        # ----------------------------------------------------
        # CURRENT TRADING SESSION
        # ----------------------------------------------------

        "trading": False,

        "account": "demo",

        "stake": 2.00,

        "real_market_mode": False,

        "session_profit": 0.0,

        "session_trades": 0,

        "session_wins": 0,

        "session_losses": 0,

        "consecutive_losses": 0,

        "active_trade": None,

        "session_started_at": None,

        "last_trade_at": None,

    }


def get_session(
    user_id: str
) -> Dict[str, Any]:

    if user_id not in USER_SESSIONS:

        USER_SESSIONS[user_id] = \
            create_user_session(user_id)

    return USER_SESSIONS[user_id]


def validate_account(
    account: str
):

    if account not in {
        "demo",
        "real"
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "account must be either "
                "'demo' or 'real'"
            )
        )


def validate_trade_amount(
    session: Dict[str, Any],
    account: str,
    amount: float
):

    if amount <= 0:

        raise HTTPException(
            status_code=400,
            detail="Stake must be greater than zero."
        )


    if account == "demo":

        if session["demo_balance"] < amount:

            raise HTTPException(
                status_code=400,
                detail="Insufficient demo balance."
            )


    elif account == "real":

        real_balance = \
            session["real_balance"]

        if real_balance is None:

            raise HTTPException(
                status_code=409,
                detail=(
                    "Real account balance has "
                    "not been verified."
                )
            )

        if real_balance < amount:

            raise HTTPException(
                status_code=400,
                detail="Insufficient real balance."
            )


# ============================================================
# LIVE MARKET ENGINE — TWELVE DATA
# ============================================================

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
TWELVE_DATA_BASE = "https://api.twelvedata.com/time_series"

# Physical forex pairs. These are deliberately NOT Pocket Option OTC symbols.
MARKET_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "EUR/GBP",
    "AUD/USD", "USD/CAD", "USD/CHF", "NZD/USD"
]

LIVE_CANDLE_INTERVAL = "1min"
CANDLE_OUTPUTSIZE = 60
STALE_AFTER_SECONDS = 150
LIVE_CACHE_SECONDS = 20
LIVE_CACHE = {"expires_at": 0.0, "data": None}


def _fetch_json(url: str) -> Dict[str, Any]:
    req = Request(url, headers={"User-Agent": "FixedRiskBooster/3.2"})
    with urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_pair(symbol: str) -> Dict[str, Any]:
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is not configured on the server.")

    params = urlencode({
        "symbol": symbol,
        "interval": LIVE_CANDLE_INTERVAL,
        "outputsize": CANDLE_OUTPUTSIZE,
        # Twelve Data defaults forex timestamps to Australia/Sydney.
        # Request UTC explicitly so freshness checks compare like-for-like.
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON",
    })
    data = _fetch_json(f"{TWELVE_DATA_BASE}?{params}")

    if data.get("status") == "error" or "values" not in data:
        raise RuntimeError(data.get("message", "Twelve Data returned no candle data."))

    values = []
    for row in reversed(data["values"]):
        try:
            values.append({
                "datetime": row["datetime"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
        except (KeyError, TypeError, ValueError):
            continue

    if len(values) < 30:
        raise RuntimeError(f"Insufficient candles for {symbol}.")

    return {"symbol": symbol, "values": values}


def _ema(values: List[float], period: int) -> float:
    k = 2.0 / (period + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = value * k + ema * (1.0 - k)
    return ema


def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _analyze_candles(symbol: str, values: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [x["close"] for x in values]
    highs = [x["high"] for x in values]
    lows = [x["low"] for x in values]
    last = values[-1]

    rsi = _rsi(closes, 14)
    ema9 = _ema(closes[-40:], 9)
    ema21 = _ema(closes[-40:], 21)
    momentum = ((closes[-1] - closes[-6]) / closes[-6]) * 100.0
    range_pct = ((max(highs[-14:]) - min(lows[-14:])) / closes[-1]) * 100.0

    score = 50.0
    reasons = []

    if ema9 > ema21:
        score += 15
        reasons.append("short EMA is above long EMA")
    else:
        score -= 15
        reasons.append("short EMA is below long EMA")

    if momentum > 0.002:
        score += 15
        reasons.append("1-minute momentum is positive")
    elif momentum < -0.002:
        score -= 15
        reasons.append("1-minute momentum is negative")

    if rsi < 30:
        score += 10
        reasons.append(f"RSI is oversold ({rsi:.1f})")
    elif rsi > 70:
        score -= 10
        reasons.append(f"RSI is overbought ({rsi:.1f})")

    candle_change = ((last["close"] - last["open"]) / last["open"]) * 100.0
    if candle_change > 0:
        score += 5
    elif candle_change < 0:
        score -= 5

    score = max(0.0, min(100.0, score))
    direction = "CALL" if score >= 50 else "PUT"
    confidence = 50.0 + abs(score - 50.0)

    # Penalize very quiet/unclear setups rather than pretending they are strong.
    if abs(momentum) < 0.002 and 40 <= rsi <= 60:
        confidence = min(confidence, 55.0)

    return {
        "asset": symbol.replace("/", ""),
        "symbol": symbol,
        "direction": direction,
        "confidence": round(confidence, 1),
        "rsi": round(rsi, 2),
        "ema9": ema9,
        "ema21": ema21,
        "momentum_pct": round(momentum, 5),
        "volatility_pct": round(range_pct, 5),
        "price": last["close"],
        "candle_time": last["datetime"],
        "reason": "; ".join(reasons),
    }


def _is_fresh_candle(candle_time: str) -> bool:
    try:
        raw = candle_time.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
        return age >= -5 and age <= STALE_AFTER_SECONDS
    except (TypeError, ValueError):
        return False


def _analyze_live_markets() -> Dict[str, Any]:
    global LIVE_CACHE

    now_ts = datetime.now(timezone.utc).timestamp()
    if LIVE_CACHE["data"] is not None and now_ts < LIVE_CACHE["expires_at"]:
        return LIVE_CACHE["data"]

    if not TWELVE_DATA_API_KEY:
        result = {
            "status": "unavailable",
            "error": "Live market data is not configured. Add TWELVE_DATA_API_KEY to the Render environment.",
            "markets": [],
        }
        LIVE_CACHE = {"expires_at": now_ts + LIVE_CACHE_SECONDS, "data": result}
        return result

    analyses = []
    errors = []
    for symbol in MARKET_PAIRS:
        try:
            payload = _fetch_pair(symbol)
            analysis = _analyze_candles(symbol, payload["values"])
            if not _is_fresh_candle(analysis["candle_time"]):
                raise RuntimeError(f"Latest candle for {symbol} is stale ({analysis['candle_time']}).")
            analyses.append(analysis)
        except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError) as exc:
            errors.append({"symbol": symbol, "error": str(exc)[:180]})
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)[:180]})

    if not analyses:
        result = {
            "status": "unavailable",
            "error": "No live market data could be retrieved from Twelve Data.",
            "markets": [],
            "errors": errors,
        }
        LIVE_CACHE = {"expires_at": now_ts + 5, "data": result}
        return result

    # Favor stronger confidence first, then stronger absolute momentum.
    analyses.sort(key=lambda x: (x["confidence"], abs(x["momentum_pct"])), reverse=True)
    for index, item in enumerate(analyses):
        item["favourable"] = index == 0
        item["payout"] = None  # Twelve Data is not a Pocket Option payout feed.

    result = {
        "status": "live",
        "markets": analyses,
        "errors": errors,
        "generated_at": utc_now(),
    }
    LIVE_CACHE = {"expires_at": now_ts + LIVE_CACHE_SECONDS, "data": result}
    return result


def get_market_prediction() -> Dict[str, Any]:
    data = _analyze_live_markets()
    if data.get("status") != "live" or not data.get("markets"):
        return {
            "status": "unavailable",
            "recommended_asset": None,
            "payout": None,
            "predicted_direction": None,
            "confidence_score": 0.0,
            "rsi_value": None,
            "price": None,
            "reason": data.get("error", "Live market data unavailable."),
            "source": "twelve_data_live",
            "generated_at": utc_now(),
        }

    top = data["markets"][0]
    return {
        "status": "live",
        "recommended_asset": top["asset"],
        "symbol": top["symbol"],
        "payout": None,
        "predicted_direction": top["direction"],
        "confidence_score": top["confidence"],
        "rsi_value": top["rsi"],
        "price": top["price"],
        "momentum_pct": top["momentum_pct"],
        "volatility_pct": top["volatility_pct"],
        "reason": top["reason"],
        "source": "twelve_data_live",
        "candle_time": top["candle_time"],
        "generated_at": data["generated_at"],
    }


def get_markets() -> Dict[str, Any]:
    data = _analyze_live_markets()
    if data.get("status") != "live":
        return {
            "status": "unavailable",
            "recommended_asset": None,
            "markets": [],
            "errors": data.get("errors", []),
            "message": data.get("error", "Live market data unavailable."),
            "generated_at": utc_now(),
        }

    return {
        "status": "live",
        "recommended_asset": data["markets"][0]["asset"],
        "markets": data["markets"],
        "errors": data.get("errors", []),
        "generated_at": data["generated_at"],
    }


# ============================================================
# ROOT / HEALTH
# ============================================================

@app.get("/")
async def root():

    return {

        "status": "online",

        "service": "Fixed Risk Booster API",

        "version": APP_VERSION,

        "time": utc_now(),

    }


@app.head("/")
async def root_head():
    return None


@app.get("/api/health")
async def health():

    return {

        "status": "healthy",

        "version": APP_VERSION,

        "users_in_memory":
            len(USER_SESSIONS),

        "time": utc_now(),

    }


# ============================================================
# MARKET ENDPOINTS
# ============================================================

@app.get(
    "/api/market/prediction"
)
async def market_prediction():

    return get_market_prediction()


@app.get(
    "/api/markets"
)
async def markets():

    return get_markets()


# ============================================================
# SESSION REGISTER
#
# IMPORTANT:
# This endpoint currently registers the Mini App user.
#
# It does NOT pretend that a Pocket Option account
# has been authenticated.
# ============================================================

@app.post(
    "/api/session/register"
)
async def register_session(
    request: SessionRegisterRequest
):

    session = get_session(request.user_id)

    session["connected"] = False

    session["connection_status"] = \
        "not_connected"

    return {

        "status": "registered",

        "user_id": request.user_id,

        "connected": False,

        "connection_status":
            "not_connected",

        "balances": {

            "demo":
                session["demo_balance"],

            "real":
                session["real_balance"],

        },

        "message": (
            "Mini App session registered. "
            "Pocket Option account has not "
            "been verified."
        ),

    }


# ============================================================
# SESSION STATUS
# ============================================================

@app.get(
    "/api/session/status/{user_id}"
)
async def session_status(
    user_id: str
):

    session = get_session(user_id)

    return {

        "user_id": user_id,

        "connected":
            session["connected"],

        "connection_status":
            session["connection_status"],

        "account":
            session["account"],

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

    session = get_session(user_id)

    return {

        "demo": {

            "balance":
                session["demo_balance"],

        },

        "real": {

            "balance":
                session["real_balance"],

            "verified":
                (
                    session["real_balance"]
                    is not None
                ),

        },

    }


# ============================================================
# START TRADING SESSION
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


    session = get_session(request.user_id)


    # --------------------------------------------------------
    # Validate real account
    # --------------------------------------------------------

    if request.account == "real":

        if not session["connected"]:

            raise HTTPException(
                status_code=409,
                detail=(
                    "Real account is not "
                    "connected and verified."
                )
            )


        if session["real_balance"] is None:

            raise HTTPException(
                status_code=409,
                detail=(
                    "Real account balance "
                    "has not been verified."
                )
            )


    # --------------------------------------------------------
    # Demo balance validation
    # --------------------------------------------------------

    if request.account == "demo":

        if (
            session["demo_balance"]
            < request.stake
        ):

            raise HTTPException(
                status_code=400,
                detail=(
                    "Insufficient demo balance."
                )
            )


    # --------------------------------------------------------
    # Start fresh session
    # --------------------------------------------------------

    session["trading"] = True

    session["account"] = request.account

    session["stake"] = round(request.stake, 2)

    session["real_market_mode"] = request.real_market_mode

    session["session_profit"] = 0.0

    session["session_trades"] = 0

    session["session_wins"] = 0

    session["session_losses"] = 0

    session["consecutive_losses"] = 0

    session["active_trade"] = None

    session["session_started_at"] = utc_now()

    return {

        "status": "started",

        "account":
            session["account"],

        "stake":
            session["stake"],

        "real_market_mode":
            session["real_market_mode"],

        "session_started_at":
            session["session_started_at"],

    }


# ============================================================
# STOP TRADING SESSION
# ============================================================

@app.post(
    "/api/trading/stop"
)
async def stop_trading(
    request: TradingStopRequest
):

    session = get_session(request.user_id)

    session["trading"] = False

    session["active_trade"] = None

    return {

        "status": "stopped",

        "account":
            session["account"],

        "session_profit":
            session["session_profit"],

        "session_trades":
            session["session_trades"],

        "session_wins":
            session["session_wins"],

        "session_losses":
            session["session_losses"],

    }


# ============================================================
# TRADING STATUS
# ============================================================

@app.get(
    "/api/trading/status/{user_id}"
)
async def trading_status(
    user_id: str
):

    session = get_session(user_id)

    return {

        "trading":
            session["trading"],

        "account":
            session["account"],

        "stake":
            session["stake"],

        "session_profit":
            session["session_profit"],

        "session_trades":
            session["session_trades"],

        "session_wins":
            session["session_wins"],

        "session_losses":
            session["session_losses"],

        "consecutive_losses":
            session["consecutive_losses"],

        "active_trade":
            session["active_trade"],

    }


# ============================================================
# DEMO TRADE
#
# This is the ONLY place where the backend simulates a
# result at this stage.
#
# Real trades are intentionally blocked until the broker
# adapter is verified.
# ============================================================

async def execute_demo_trade(
    session: Dict[str, Any],
    asset: str,
    amount: float,
    duration: int
):

    validate_trade_amount(
        session,
        "demo",
        amount
    )


    # --------------------------------------------------------
    # Fixed-risk result
    #
    # 92% payout:
    # WIN  = +amount * 0.92
    # LOSS = -amount
    #
    # This first backend stage uses a deterministic alternating
    # demo result for testing the accounting API.
    #
    # We can replace this with the actual demo market engine
    # later.
    # --------------------------------------------------------

    next_trade_number = session["demo_stats"]["trades"] + 1


    is_win = next_trade_number % 2 == 1


    profit = (
            amount * 0.92
            if is_win
            else -amount
        )


    # --------------------------------------------------------
    # Balance
    # --------------------------------------------------------

    session["demo_balance"] += profit


    # --------------------------------------------------------
    # Session statistics
    # --------------------------------------------------------

    session["session_profit"] += profit

    session["session_trades"] += 1


    # --------------------------------------------------------
    # All-time Demo statistics
    # --------------------------------------------------------

    session["demo_stats"]["profit"] += \
        profit

    session["demo_stats"]["trades"] += 1


    if is_win:

        session["session_wins"] += 1

        session["demo_stats"]["wins"] += 1

        session["consecutive_losses"] = 0

        result = "WIN"

    else:

        session["session_losses"] += 1

        session["demo_stats"]["losses"] += 1

        session["consecutive_losses"] += 1

        result = "LOSS"


    order_id = "DEMO-" + uuid.uuid4().hex[:12].upper()


    trade = {

        "order_id": order_id,

        "account": "demo",

        "asset": asset,

        "stake": amount,

        "duration": duration,

        "result": result,

        "profit": round(
            profit,
            2
        ),

        "timestamp": utc_now(),

    }


    session["demo_history"].insert(
        0,
        trade
    )


    session["demo_history"] = session["demo_history"][:100]


    session["last_trade_at"] = trade["timestamp"]


    # --------------------------------------------------------
    # Three-loss protection
    # --------------------------------------------------------

    if (
        session["consecutive_losses"]
        >= 3
    ):

        session["trading"] = False

        protection_message = (
            "Session closed after 3 "
            "consecutive losses to "
            "protect balance"
        )

    else:

        protection_message = None


    return {

        "status": "completed",

        "order_id": order_id,

        "account": "demo",

        "asset": asset,

        "stake": amount,

        "result": result,

        "profit": round(
            profit,
            2
        ),

        "remaining_balance":
            round(
                session["demo_balance"],
                2
            ),

        "session_profit":
            round(
                session["session_profit"],
                2
            ),

        "session_trades":
            session["session_trades"],

        "session_wins":
            session["session_wins"],

        "session_losses":
            session["session_losses"],

        "consecutive_losses":
            session["consecutive_losses"],

        "trading":
            session["trading"],

        "protection_message":
            protection_message,

    }


# ============================================================
# REAL TRADE PLACEHOLDER
#
# We deliberately DO NOT fake a real order.
# ============================================================

async def execute_real_trade(
    session: Dict[str, Any],
    asset: str,
    amount: float,
    duration: int
):

    raise HTTPException(
        status_code=501,
        detail=(
            "Real broker execution is not "
            "enabled yet. The broker adapter "
            "must be verified before real "
            "orders are allowed."
        )
    )


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


    session = get_session(request.user_id)


    # --------------------------------------------------------
    # Trading must have been started
    # --------------------------------------------------------

    if not session["trading"]:

        raise HTTPException(
            status_code=409,
            detail=(
                "Trading session is not active."
            )
        )


    # --------------------------------------------------------
    # Fixed stake enforcement
    #
    # Once a session starts, every trade must use exactly
    # the opening stake.
    # --------------------------------------------------------

    opening_stake = session["stake"]


    if round(
        request.amount,
        2
    ) != round(
        opening_stake,
        2
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Fixed-risk violation: "
                "trade amount must equal "
                f"the opening stake "
                f"({opening_stake:.2f})."
            )
        )


    # --------------------------------------------------------
    # Require a fresh live recommendation for every trade.
    # This prevents stale/hardcoded symbols or directions.
    # --------------------------------------------------------

    prediction = get_market_prediction()
    if prediction.get("status") != "live":
        raise HTTPException(503, detail=prediction.get("reason", "Live market data unavailable."))

    if request.asset != prediction["recommended_asset"]:
        raise HTTPException(409, detail="Market recommendation changed. Refresh the live market signal before trading.")

    if request.direction.upper() != prediction["predicted_direction"]:
        raise HTTPException(409, detail="Trade direction does not match the current live market recommendation.")

    # --------------------------------------------------------
    # DEMO
    # --------------------------------------------------------

    if request.account == "demo":

        return await execute_demo_trade(

            session=session,

            asset=request.asset,

            amount=opening_stake,

            duration=request.duration,

        )


    # --------------------------------------------------------
    # REAL
    # --------------------------------------------------------

    return await execute_real_trade(

        session=session,

        asset=request.asset,

        amount=opening_stake,

        duration=request.duration,

    )


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

    validate_account(account)

    session = get_session(user_id)


    if account == "demo":

        history = session["demo_history"]

    else:

        history = session["real_history"]


    return {

        "account": account,

        "trades": history,

    }


# ============================================================
# STATISTICS
# ============================================================

@app.get(
    "/api/stats/{user_id}"
)
async def statistics(
    user_id: str,
    account: str = "demo"
):

    validate_account(account)

    session = get_session(user_id)


    if account == "demo":

        stats = session["demo_stats"]

        trades = stats["trades"]

        wins = stats["wins"]

        profit = stats["profit"]

    else:

        stats = session["real_stats"]

        trades = stats["trades"]

        wins = stats["wins"]

        profit = stats["profit"]


    if (
        trades is None
        or wins is None
    ):

        win_rate = None

    elif trades == 0:

        win_rate = 0.0

    else:

        win_rate = round(
                wins /
                trades *
                100,
                2
            )


    return {

        "account": account,

        "profit": profit,

        "trades": trades,

        "wins": wins,

        "losses":
            (
                stats["losses"]
                if stats["losses"] is not None
                else None
            ),

        "win_rate":
            win_rate,

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
        port=port
    )
