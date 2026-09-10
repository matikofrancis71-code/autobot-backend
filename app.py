import os
import asyncio
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pocketoptionapi_async import AsyncPocketOptionClient, OrderDirection

app = FastAPI(title="Pocket Option Backend Service")

# 1. Enable CORS for Vercel, Telegram Web App, and external connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active connections map: { user_id: AsyncPocketOptionClient }
active_sessions = {}

# Input models
class LoginRequest(BaseModel):
    user_id: str
    ssid: str = None
    email: str = None
    password: str = None
    is_demo: bool = True

class TradeRequest(BaseModel):
    user_id: str
    asset: str
    amount: float
    direction: str  # "CALL" or "PUT"
    duration: int = 60

@app.get("/")
async def health_check():
    """Health endpoint pinged by UptimeRobot to keep Render service awake."""
    return {"status": "online", "active_users": len(active_sessions)}

@app.post("/api/connect")
async def connect_user(req: LoginRequest):
    try:
        auth_ssid = req.ssid

        # If full SSID string isn't sent directly, exchange credentials
        if not auth_ssid:
            if not req.email or not req.password:
                raise HTTPException(
                    status_code=400, 
                    detail="Please provide either a full SSID string or both email and password."
                )

            session = requests.Session()
            login_url = "https://pocketoption.com/login"
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            
            res = session.post(login_url, data={"email": req.email, "password": req.password, "remember": "1"}, headers=headers)
            cookies = session.cookies.get_dict()
            session_id = cookies.get("PHPSESSID") or cookies.get("session")
            
            if not session_id:
                raise Exception("Authentication failed. Invalid credentials or CAPTCHA required.")

            demo_flag = 1 if req.is_demo else 0
            auth_ssid = f'42["auth",{{\"session\":\"{session_id}\",\"isDemo\":{demo_flag},\"platform\":1}}]'

        # Initialize persistent WebSocket client for the user
        client = AsyncPocketOptionClient(ssid=auth_ssid, is_demo=req.is_demo, enable_logging=False)
        await client.connect()

        # Verify connection by retrieving balance
        balance_info = await client.get_balance()
        active_sessions[req.user_id] = client

        return {
            "status": "connected",
            "user_id": req.user_id,
            "balance": balance_info.balance,
            "currency": getattr(balance_info, 'currency', 'USD')
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/trade")
async def execute_trade(req: TradeRequest):
    if req.user_id not in active_sessions:
        raise HTTPException(status_code=401, detail="User session not found. Please log in first.")

    client = active_sessions[req.user_id]
    direction_enum = OrderDirection.CALL if req.direction.upper() == "CALL" else OrderDirection.PUT

    try:
        order = await client.place_order(
            asset=req.asset,
            amount=req.amount,
            direction=direction_enum,
            duration=req.duration
        )
        return {"status": "success", "order": order}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Trade execution failed: {str(e)}")

@app.get("/api/balance/{user_id}")
async def get_user_balance(user_id: str):
    if user_id not in active_sessions:
        raise HTTPException(status_code=401, detail="User session not found.")
    
    client = active_sessions[user_id]
    try:
        balance_info = await client.get_balance()
        return {"user_id": user_id, "balance": balance_info.balance}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Bind dynamically to PORT environment variable for Render deployment
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
