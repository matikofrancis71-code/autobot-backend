import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


APP_VERSION = "3.1.0-dynamic-market"

FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "*"
)


app = FastAPI(
    title="Fixed Risk Booster API",
    version=APP_VERSION,
    description=(
        "Backend for Fixed Risk Booster. "
        "Demo and real-account state are isolated."
    )
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


USER_SESSIONS: Dict[str, Dict[str, Any]] = {}


# ============================================================
# MODELS
# ============================================================

class SessionRegisterRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class TradingStartRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)

    account: str = Field(default="demo")

    stake: float = Field(gt=0)

    real_market_mode: bool = False


class TradingStopRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class TradeRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)

    asset: str = Field(
        min_length=1,
        max_length=64
    )

    direction: str = Field(
        min_length=1,
        max_length=16
    )

    amount: float = Field(gt=0)

    account: str = Field(default="demo")

    duration: int = Field(
        default=60,
        gt=0,
        le=3600
    )


# ============================================================
# UTILITIES
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

        # DEMO
        "demo_balance": 10000.00,

        "demo_stats": {
            "profit": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
        },

        "demo_history": [],

        # REAL
        "real_balance": None,

        "real_stats": {
            "profit": None,
            "trades": None,
            "wins": None,
            "losses": None,
        },

        "real_history": [],

        # SESSION
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

        USER_SESSIONS[user_id] = (
            create_user_session(user_id)
        )

    return USER_SESSIONS[user_id]


def validate_account(account: str):

    if account not in {"demo", "real"}:

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

        real_balance = session["real_balance"]

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
#
# IMPORTANT:
# The frontend does NOT select a hard-coded market.
# The backend is the source of the recommendation.
#
# This is currently an ANALYSIS PLACEHOLDER.
# It is NOT a live Pocket Option signal.
# ============================================================

def get_market_prediction():

    return {
        "recommended_asset": "EURUSD_otc",

        "payout": "92%",

        "predicted_direction": "CALL",

        "confidence_score": 89.2,

        "rsi_value": 28.4,

        "reason": (
            "Analysis placeholder: live market-data "
            "provider is not connected yet."
        ),

        "source": "backend_analysis_placeholder",

        "generated_at": utc_now(),
    }


def get_markets():

    prediction = get_market_prediction()

    recommended = prediction["recommended_asset"]

    markets = [
        {
            "asset": recommended,

            "payout": float(
                str(
                    prediction["payout"]
                ).replace("%", "")
            ),

            "confidence": float(
                prediction["confidence_score"]
            ),

            "favourable": True,

            "predicted_direction":
                prediction[
                    "predicted_direction"
                ],
        }
    ]

    return {
        "recommended_asset": recommended,

        "predicted_direction":
            prediction[
                "predicted_direction"
            ],

        "source":
            prediction.get("source"),

        "markets": markets,

        "generated_at":
            prediction["generated_at"],
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

        "time":
            utc_now(),
    }


@app.head("/")
async def root_head():

    return Response(
        status_code=200
    )


@app.get("/api/health")
async def health():

    return {
        "status": "healthy",

        "version":
            APP_VERSION,

        "users_in_memory":
            len(USER_SESSIONS),

        "time":
            utc_now(),
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
# ============================================================

@app.post(
    "/api/session/register"
)
async def register_session(
    request: SessionRegisterRequest
):

    session = get_session(
        request.user_id
    )

    session["connected"] = False

    session["connection_status"] = (
        "not_connected"
    )

    return {
        "status": "registered",

        "user_id":
            request.user_id,

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
        "user_id":
            user_id,

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
# START TRADING
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

    session = get_session(
        request.user_id
    )

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

    session["trading"] = True

    session["account"] = (
        request.account
    )

    session["stake"] = round(
        request.stake,
        2
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
# STOP TRADING
# ============================================================

@app.post(
    "/api/trading/stop"
)
async def stop_trading(
    request: TradingStopRequest
):

    session = get_session(
        request.user_id
    )

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
# ============================================================

async def execute_demo_trade(
    session: Dict[str, Any],
    asset: str,
    direction: str,
    amount: float,
    duration: int
):

    validate_trade_amount(
        session,
        "demo",
        amount
    )

    # Deterministic result for backend testing.
    # Odd trade = WIN.
    # Even trade = LOSS.
    #
    # This will later be replaced by the actual
    # demo market result.

    next_trade_number = (
        session["demo_stats"]["trades"]
        + 1
    )

    is_win = (
        next_trade_number % 2 == 1
    )

    if is_win:

        profit = (
            amount * 0.92
        )

    else:

        profit = -amount

    session["demo_balance"] += (
        profit
    )

    session["session_profit"] += (
        profit
    )

    session["session_trades"] += 1

    session["demo_stats"]["profit"] += (
        profit
    )

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

    order_id = (
        "DEMO-"
        + uuid.uuid4().hex[:12].upper()
    )

    timestamp = utc_now()

    trade = {
        "order_id":
            order_id,

        "account":
            "demo",

        "asset":
            asset,

        "direction":
            direction,

        "stake":
            amount,

        "duration":
            duration,

        "result":
            result,

        "profit":
            round(
                profit,
                2
            ),

        "timestamp":
            timestamp,
    }

    session["demo_history"].insert(
        0,
        trade
    )

    session["demo_history"] = (
        session["demo_history"][:100]
    )

    session["last_trade_at"] = (
        timestamp
    )

    protection_message = None

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

    return {
        "status":
            "completed",

        "order_id":
            order_id,

        "account":
            "demo",

        "asset":
            asset,

        "direction":
            direction,

        "stake":
            amount,

        "result":
            result,

        "profit":
            round(
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
# REAL TRADE
#
# INTENTIONALLY BLOCKED.
# ============================================================

async def execute_real_trade(
    session: Dict[str, Any],
    asset: str,
    direction: str,
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

    session = get_session(
        request.user_id
    )

    if not session["trading"]:

        raise HTTPException(
            status_code=409,
            detail=(
                "Trading session is not active."
            )
        )

    # Ensure the request account matches
    # the account used to start the session.

    if request.account != session["account"]:

        raise HTTPException(
            status_code=409,
            detail=(
                "Account mismatch: the trade "
                "account must match the active "
                "trading session."
            )
        )

    # Fixed-risk enforcement.

    opening_stake = (
        session["stake"]
    )

    if (
        round(
            request.amount,
            2
        )
        !=
        round(
            opening_stake,
            2
        )
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
    # CURRENT BACKEND MARKET RECOMMENDATION
    # --------------------------------------------------------

    prediction = (
        get_market_prediction()
    )

    recommended_asset = (
        prediction[
            "recommended_asset"
        ]
    )

    predicted_direction = (
        prediction[
            "predicted_direction"
        ]
    )

    # Reject stale/wrong market.

    if (
        request.asset
        !=
        recommended_asset
    ):

        raise HTTPException(
            status_code=409,
            detail={
                "message":
                    "Market recommendation "
                    "is stale or invalid.",

                "recommended_asset":
                    recommended_asset,

                "predicted_direction":
                    predicted_direction,

                "generated_at":
                    prediction[
                        "generated_at"
                    ],
            },
        )

    # Reject stale/wrong direction.

    if (
        request.direction
        !=
        predicted_direction
    ):

        raise HTTPException(
            status_code=409,
            detail={
                "message":
                    "Prediction direction "
                    "is stale or invalid.",

                "recommended_asset":
                    recommended_asset,

                "predicted_direction":
                    predicted_direction,

                "generated_at":
                    prediction[
                        "generated_at"
                    ],
            },
        )

    # DEMO

    if request.account == "demo":

        return await execute_demo_trade(
            session=session,

            asset=request.asset,

            direction=request.direction,

            amount=opening_stake,

            duration=request.duration,
        )

    # REAL

    return await execute_real_trade(
        session=session,

        asset=request.asset,

        direction=request.direction,

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

    session = get_session(
        user_id
    )

    if account == "demo":

        history = (
            session["demo_history"]
        )

    else:

        history = (
            session["real_history"]
        )

    return {
        "account":
            account,

        "trades":
            history,
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

    session = get_session(
        user_id
    )

    if account == "demo":

        stats = (
            session["demo_stats"]
        )

    else:

        stats = (
            session["real_stats"]
        )

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
        "account":
            account,

        "profit":
            profit,

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
