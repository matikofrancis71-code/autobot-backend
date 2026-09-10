import os
import time
import logging
import secrets
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fixed-risk-booster")

app = FastAPI(
    title="Fixed Risk Booster API",
    version="3.0.0"
)

# ---------------------------------------------------------
# CORS
# ---------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# IN-MEMORY SESSIONS
# ---------------------------------------------------------

USER_SESSIONS = {}

DEFAULT_DEMO_BALANCE = 10000.00


# ---------------------------------------------------------
# MODELS
# ---------------------------------------------------------

class SessionAuthRequest(BaseModel):
    user_id: str
    ssid: str


class TradeExecutionRequest(BaseModel):
    user_id: str
    asset: str
    amount: float = Field(gt=0)
    direction: str
    duration: int = Field(default=60, ge=5, le=3600)
    is_demo: bool = True


class BalanceRequest(BaseModel):
    user_id: str
    is_demo: bool = True


# ---------------------------------------------------------
# BASIC MARKET INFORMATION
# ---------------------------------------------------------

def calculate_favorable_market():
    """
    This is still the existing placeholder market-analysis
    logic from the previous backend.

    IMPORTANT:
    These numbers are NOT claimed to be live AI predictions.
    """

    markets = [
        {
            "asset": "EURUSD_otc",
            "payout": 92,
            "rsi": 28.4,
            "signal": "BULLISH",
            "direction": "CALL",
        },
        {
            "asset": "GBPUSD_otc",
            "payout": 88,
            "rsi": 52.1,
            "signal": "HOLD",
            "direction": None,
        },
        {
            "asset": "USDJPY_otc",
            "payout": 85,
            "rsi": 74.8,
            "signal": "BEARISH",
            "direction": "PUT",
        },
        {
            "asset": "BTCUSD",
            "payout": 80,
            "rsi": 31.2,
            "signal": "BULLISH",
            "direction": "CALL",
        },
    ]

    actionable = [
        m for m in markets
        if m["direction"] is not None
    ]

    best = max(
        actionable,
        key=lambda x: x["payout"]
    )

    return {
        "recommended_asset": best["asset"],
        "payout": f'{best["payout"]}%',
        "predicted_direction": best["direction"],
        "confidence_score": 89.2,
        "rsi_value": best["rsi"],
        "reason": (
            f'RSI indicates '
            f'{"oversold" if best["rsi"] < 30 else "overbought" if best["rsi"] > 70 else "neutral"} '
            f'conditions ({best["rsi"]}) with high payout.'
        )
    }


# ---------------------------------------------------------
# POCKET OPTION CLIENT
# ---------------------------------------------------------

def load_pocket_client():
    """
    Loads the Pocket Option client already used by the project.

    We deliberately do NOT silently create a fake client.
    """

    try:
        from pocketoptionapi_async import (
            AsyncPocketOptionClient,
            OrderDirection
        )

        return AsyncPocketOptionClient, OrderDirection

    except ImportError as exc:
        logger.error(
            "pocketoptionapi_async is not installed: %s",
            exc
        )

        raise HTTPException(
            status_code=503,
            detail=(
                "Pocket Option connector is not installed on the "
                "backend."
            )
        )


async def create_connected_client(ssid: str):
    """
    Creates a real Pocket Option connection.

    The exact SSID/session format must be supplied by the
    supported Pocket Option connector being used by the server.
    """

    if not ssid:
        raise HTTPException(
            status_code=400,
            detail="No Pocket Option session token supplied."
        )

    if not ssid.startswith('42["auth"'):
        raise HTTPException(
            status_code=400,
            detail="Invalid Pocket Option session token format."
        )

    AsyncPocketOptionClient, _ = load_pocket_client()

    client = AsyncPocketOptionClient(ssid)

    try:
        await client.connect()

    except Exception as exc:
        logger.exception(
            "Pocket Option connection failed"
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Pocket Option connection failed. "
                "No real-account balance was returned."
            )
        ) from exc

    return client


# ---------------------------------------------------------
# ROOT
# ---------------------------------------------------------

@app.get("/")
@app.head("/")
@app.options("/")
async def root():
    return {
        "status": "online",
        "service": "Fixed Risk Booster API",
        "version": "3.0.0"
    }


# ---------------------------------------------------------
# HEALTH
# ---------------------------------------------------------

@app.get("/api/health")
async def health():
    return {
        "status": "healthy",
        "service": "fixed-risk-booster"
    }


# ---------------------------------------------------------
# MARKET PREDICTION
# ---------------------------------------------------------

@app.get("/api/market/prediction")
async def market_prediction():

    return calculate_favorable_market()


# ---------------------------------------------------------
# REGISTER / CONNECT POCKET OPTION SESSION
# ---------------------------------------------------------

@app.post("/api/session/register")
async def register_session(request: SessionAuthRequest):

    user_id = request.user_id.strip()
    ssid = request.ssid.strip()

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="Missing user_id."
        )

    if not ssid.startswith('42["auth"'):
        raise HTTPException(
            status_code=400,
            detail="Invalid Pocket Option session token."
        )

    # Try to connect to the actual broker.
    client = await create_connected_client(ssid)

    # IMPORTANT:
    # We only call the session connected when the actual
    # Pocket Option connection succeeds.
    try:
        balance = await client.get_balance()

    except Exception as exc:
        logger.exception(
            "Unable to retrieve Pocket Option balance."
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Pocket Option connected, but the real balance "
                "could not be retrieved."
            )
        ) from exc

    # Normalize the returned balance.
    try:
        real_balance = float(balance)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=502,
            detail="Pocket Option returned an invalid balance."
        )

    session_id = secrets.token_urlsafe(32)

    USER_SESSIONS[user_id] = {
        "session_id": session_id,
        "ssid": ssid,
        "client": client,
        "connected": True,
        "connected_at": time.time(),

        # Demo account is local to our application.
        "demo_balance": DEFAULT_DEMO_BALANCE,

        # This is ALWAYS obtained from Pocket Option.
        "real_balance": real_balance,

        "real_balance_updated_at": time.time(),

        "total_trades": 0,
        "winning_trades": 0,
        "session_profit": 0.0,
        "consecutive_losses": 0,
    }

    logger.info(
        "Pocket Option session connected for user %s",
        user_id
    )

    return {
        "status": "connected",
        "user_id": user_id,
        "session_id": session_id,
        "real_balance": real_balance,
        "demo_balance": DEFAULT_DEMO_BALANCE
    }


# ---------------------------------------------------------
# ACCOUNT STATUS
# ---------------------------------------------------------

@app.get("/api/session/status/{user_id}")
async def session_status(user_id: str):

    session = USER_SESSIONS.get(user_id)

    if not session:
        return {
            "connected": False,
            "demo_balance": DEFAULT_DEMO_BALANCE,
            "real_balance": None
        }

    return {
        "connected": bool(session.get("connected")),
        "demo_balance": round(
            float(session.get("demo_balance", DEFAULT_DEMO_BALANCE)),
            2
        ),
        "real_balance": round(
            float(session["real_balance"]),
            2
        ) if session.get("real_balance") is not None else None,
        "total_trades": session.get("total_trades", 0),
        "winning_trades": session.get("winning_trades", 0),
        "session_profit": round(
            float(session.get("session_profit", 0)),
            2
        ),
        "consecutive_losses": session.get(
            "consecutive_losses",
            0
        )
    }


# ---------------------------------------------------------
# REFRESH REAL BALANCE
# ---------------------------------------------------------

@app.post("/api/account/balance")
async def refresh_balance(request: BalanceRequest):

    session = USER_SESSIONS.get(request.user_id)

    if not session:
        raise HTTPException(
            status_code=401,
            detail="Pocket Option account is not connected."
        )

    if request.is_demo:
        return {
            "account": "demo",
            "balance": round(
                float(session["demo_balance"]),
                2
            )
        }

    client = session.get("client")

    if client is None:
        raise HTTPException(
            status_code=401,
            detail="No active Pocket Option connection."
        )

    try:
        balance = await client.get_balance()

    except Exception as exc:
        logger.exception(
            "Real balance refresh failed."
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Could not retrieve the real Pocket Option "
                "balance."
            )
        ) from exc

    try:
        real_balance = float(balance)

    except (TypeError, ValueError):
        raise HTTPException(
            status_code=502,
            detail="Pocket Option returned an invalid balance."
        )

    session["real_balance"] = real_balance
    session["real_balance_updated_at"] = time.time()

    return {
        "account": "real",
        "balance": round(real_balance, 2)
    }


# ---------------------------------------------------------
# TRADE EXECUTION
# ---------------------------------------------------------

@app.post("/api/trade/execute")
async def execute_trade(request: TradeExecutionRequest):

    direction = request.direction.upper()

    if direction not in ("CALL", "PUT"):
        raise HTTPException(
            status_code=400,
            detail="Direction must be CALL or PUT."
        )

    session = USER_SESSIONS.get(request.user_id)

    # -----------------------------------------------------
    # DEMO
    # -----------------------------------------------------

    if request.is_demo:

        demo_balance = float(
            session["demo_balance"]
            if session
            else DEFAULT_DEMO_BALANCE
        )

        if request.amount > demo_balance:
            raise HTTPException(
                status_code=400,
                detail="Insufficient demo balance."
            )

        # -------------------------------------------------
        # DEMO RESULT
        #
        # This intentionally remains a simulation.
        # Replace this section with your strategy result
        # later.
        # -------------------------------------------------

        import random

        won = random.random() >= 0.5

        if won:

            profit = request.amount * 0.92

            demo_balance += profit

            result = "win"

            if session:
                session["winning_trades"] += 1
                session["session_profit"] += profit
                session["consecutive_losses"] = 0

        else:

            demo_balance -= request.amount

            result = "loss"

            if session:
                session["session_profit"] -= request.amount
                session["consecutive_losses"] += 1

        if session:
            session["demo_balance"] = demo_balance
            session["total_trades"] += 1

        return {
            "status": "success",
            "account": "demo",
            "result": result,
            "amount": request.amount,
            "balance": round(demo_balance, 2)
        }

    # -----------------------------------------------------
    # REAL ACCOUNT
    # -----------------------------------------------------

    if not session:
        raise HTTPException(
            status_code=401,
            detail=(
                "Connect the Pocket Option account before "
                "placing a real trade."
            )
        )

    client = session.get("client")

    if client is None:
        raise HTTPException(
            status_code=401,
            detail="No active real Pocket Option connection."
        )

    # Refresh balance BEFORE placing the order.
    try:
        current_balance = await client.get_balance()

    except Exception as exc:
        logger.exception(
            "Unable to verify real balance."
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to verify the real Pocket Option "
                "balance. Trade was NOT sent."
            )
        ) from exc

    try:
        current_balance = float(current_balance)

    except (TypeError, ValueError):
        raise HTTPException(
            status_code=502,
            detail="Invalid balance returned by Pocket Option."
        )

    if request.amount > current_balance:
        raise HTTPException(
            status_code=400,
            detail="Insufficient real Pocket Option balance."
        )

    AsyncPocketOptionClient, OrderDirection = (
        load_pocket_client()
    )

    # Convert our direction to the connector direction.
    if direction == "CALL":
        broker_direction = OrderDirection.CALL
    else:
        broker_direction = OrderDirection.PUT

    # -----------------------------------------------------
    # REAL ORDER
    # -----------------------------------------------------

    try:

        order = await client.place_order(
            asset=request.asset,
            amount=request.amount,
            direction=broker_direction,
            duration=request.duration
        )

    except Exception as exc:

        logger.exception(
            "Pocket Option real order failed."
        )

        # VERY IMPORTANT:
        # We do NOT return success.
        # We do NOT create a fake order ID.
        # We do NOT subtract the balance locally.
        raise HTTPException(
            status_code=502,
            detail=(
                "Pocket Option rejected or failed to execute "
                "the real trade."
            )
        ) from exc

    session["total_trades"] += 1

    # Refresh the REAL broker balance after execution.
    try:
        updated_balance = await client.get_balance()
        updated_balance = float(updated_balance)

    except Exception:
        # The order was genuinely submitted, but balance
        # refresh failed. Do not invent a balance.
        updated_balance = None

    session["real_balance"] = updated_balance

    return {
        "status": "success",
        "account": "real",
        "asset": request.asset,
        "direction": direction,
        "amount": request.amount,
        "duration": request.duration,
        "order": str(order),
        "real_balance": (
            round(updated_balance, 2)
            if updated_balance is not None
            else None
        ),
        "balance_refresh": (
            "success"
            if updated_balance is not None
            else "failed"
        )
    }


# ---------------------------------------------------------
# LOGOUT
# ---------------------------------------------------------

@app.post("/api/session/logout/{user_id}")
async def logout(user_id: str):

    session = USER_SESSIONS.pop(user_id, None)

    if not session:
        return {
            "status": "logged_out"
        }

    client = session.get("client")

    # Try to close the broker connection if the installed
    # connector supports it.
    if client is not None:

        try:

            close_method = getattr(
                client,
                "disconnect",
                None
            )

            if close_method:
                result = close_method()

                if hasattr(result, "__await__"):
                    await result

        except Exception:
            logger.warning(
                "Could not cleanly disconnect broker client."
            )

    return {
        "status": "logged_out"
    }


# ---------------------------------------------------------
# START SERVER
# ---------------------------------------------------------

if __name__ == "__main__":

    import uvicorn

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
