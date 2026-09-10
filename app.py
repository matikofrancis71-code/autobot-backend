import os
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "3.0.0-fixed-risk"

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
# MARKET ENGINE
# ============================================================

def get_market_prediction():

    return {

        "recommended_asset": "EURUSD_otc",

        "payout": "92%",

        "predicted_direction": "CALL",

        "confidence_score": 89.2,

        "rsi_value": 28.4,

        "reason": (
            "RSI indicates oversold conditions "
            "(28.4) with high payout."
        ),

        "generated_at": utc_now(),

    }


def get_markets():

    prediction = \
        get_market_prediction()

    recommended = \
        prediction["recommended_asset"]

    markets = [

        {
            "asset": recommended,
            "payout": 92,
            "confidence": 89.2,
            "favourable": True,
        },

        {
            "asset": "GBPUSD_otc",
            "payout": 88,
            "confidence": 74.0,
            "favourable": False,
        },

        {
            "asset": "USDJPY_otc",
            "payout": 85,
            "confidence": 69.0,
            "favourable": False,
        },

        {
            "asset": "BTCUSD",
            "payout": 80,
            "confidence": 66.0,
            "favourable": False,
        },

    ]

    return {

        "recommended_asset": recommended,

        "markets": markets,

        "generated_at": utc_now(),

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
