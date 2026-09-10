import os
import re
import time
import asyncio
from bs4 import BeautifulSoup
from curl_cffi import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pocketoptionapi_async import AsyncPocketOptionClient, OrderDirection

app = FastAPI(title="Pocket Option Automated Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "YOUR_CAPSOLVER_API_KEY")
active_sessions = {}

class LoginRequest(BaseModel):
    user_id: str
    email: str = None
    password: str = None
    ssid: str = None
    is_demo: bool = True

class TradeRequest(BaseModel):
    user_id: str
    asset: str
    amount: float
    direction: str
    duration: int = 60

# --- AUTOMATED CAPTCHA & AUTH HELPER FUNCTIONS ---

def extract_sitekey(html_content: str) -> str | None:
    """Parses page HTML and inline scripts for Turnstile/reCAPTCHA sitekeys."""
    soup = BeautifulSoup(html_content, "html.parser")

    element = soup.find(attrs={"data-sitekey": True})
    if element and element.get("data-sitekey"):
        return element["data-sitekey"]

    patterns = [
        r'0x4[A-Za-z0-9_-]{21}',
        r'sitekey[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]',
        r'data-sitekey[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]'
    ]
    for script in soup.find_all("script"):
        if script.string:
            for pattern in patterns:
                match = re.search(pattern, script.string, re.IGNORECASE)
                if match:
                    return match.group(0) if "0x4" in match.group(0) else match.group(1)
    return None

def solve_with_capsolver(website_url: str, sitekey: str) -> str | None:
    """Submits task to CapSolver JSON API and polls for token."""
    session = requests.Session()
    create_payload = {
        "clientKey": CAPSOLVER_API_KEY,
        "task": {
            "type": "AntiTurnstileTaskProxyLess",
            "websiteURL": website_url,
            "websiteKey": sitekey
        }
    }
    
    res = session.post("https://api.capsolver.com/createTask", json=create_payload)
    data = res.json()
    if data.get("errorId") != 0:
        return None
        
    task_id = data.get("taskId")
    poll_payload = {"clientKey": CAPSOLVER_API_KEY, "taskId": task_id}
    
    for _ in range(24):
        time.sleep(5)
        poll_res = session.post("https://api.capsolver.com/getTaskResult", json=poll_payload)
        result = poll_res.json()
        if result.get("status") == "ready":
            return result.get("solution", {}).get("token")
        elif result.get("status") == "failed":
            return None
    return None

def perform_automated_login(email: str, password: str, is_demo: bool) -> str:
    """Logs into Pocket Option via TLS impersonation and builds full auth SSID token."""
    login_url = "https://pocketoption.com/en/login/"
    auth_api_url = "https://pocketoption.com/api/auth/login"

    session = requests.Session(impersonate="chrome120")
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://pocketoption.com",
        "Referer": login_url
    })

    # Step 1: Load page and retrieve cookies/HTML
    page_res = session.get(login_url)
    if page_res.status_code != 200:
        raise Exception("Failed to reach Pocket Option login page.")

    # Step 2: Check for CAPTCHA sitekey and solve if present
    captcha_token = None
    sitekey = extract_sitekey(page_res.text)
    if sitekey:
        captcha_token = solve_with_capsolver(login_url, sitekey)

    # Step 3: Post credentials
    payload = {
        "email": email,
        "password": password,
        "remember": True
    }
    if captcha_token:
        payload["cf-turnstile-response"] = captcha_token
        payload["g-recaptcha-response"] = captcha_token

    auth_res = session.post(auth_api_url, json=payload)
    if auth_res.status_code != 200:
        raise Exception(f"Authentication rejected by platform (HTTP {auth_res.status_code}). Check email/password.")

    # Step 4: Extract session cookie and format WebSocket SSID string
    cookies = session.cookies.get_dict()
    session_id = cookies.get("PHPSESSID") or cookies.get("session")
    
    if not session_id:
        raise Exception("Login response succeeded but session cookie was missing.")

    demo_flag = 1 if is_demo else 0
    return f'42["auth",{{\"session\":\"{session_id}\",\"isDemo\":{demo_flag},\"platform\":1}}]'


# --- FASTAPI API ROUTES ---

@app.get("/")
async def health_check():
    return {"status": "online", "active_users": len(active_sessions)}

@app.post("/api/connect")
async def connect_user(req: LoginRequest):
    try:
        auth_ssid = req.ssid

        # If no explicit SSID provided, execute automated background login
        if not auth_ssid:
            if not req.email or not req.password:
                raise HTTPException(status_code=400, detail="Must provide email and password.")
            
            # Offload synchronous scraping/solving task to a separate thread
            auth_ssid = await asyncio.to_thread(
                perform_automated_login, 
                req.email, 
                req.password, 
                req.is_demo
            )

        # Initialize persistent WebSocket client using captured SSID
        client = AsyncPocketOptionClient(ssid=auth_ssid, is_demo=req.is_demo, enable_logging=False)
        await client.connect()

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
        raise HTTPException(status_code=401, detail="User session not found. Log in first.")

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
        raise HTTPException(status_code=400, detail=f"Trade failed: {str(e)}")

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
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
