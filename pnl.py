import os
import json
import tempfile
import time
import hmac
import hashlib
import base64
from urllib.parse import urlencode
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials


# =========================
# CONFIG / SECRETS (ENV)
# =========================
def require_env(name: str) -> str:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


# Google Sheets config (SHEET_URL from env, TAB_NAME hardcoded to avoid env spacing issues)
SHEET_URL = require_env("SHEET_URL")
TAB_NAME = "CeFi Data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# Google service account:
# - Prefer GOOGLE_SERVICE_ACCOUNT_FILE (path to JSON file)
# - Or provide GOOGLE_SERVICE_ACCOUNT_JSON (full JSON string) and we'll write a temp file
def get_service_account_file_path() -> str:
    # Read from env at call time (important for cron/sourcing)
    google_service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    google_service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if google_service_account_file and google_service_account_file.strip():
        return google_service_account_file.strip()

    if google_service_account_json and google_service_account_json.strip():
        # Must be valid JSON content (not a filepath)
        try:
            json.loads(google_service_account_json)
        except json.JSONDecodeError as e:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from e

        fd, path = tempfile.mkstemp(prefix="gcp-sa-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            f.write(google_service_account_json)
        return path

    raise RuntimeError(
        "Set either GOOGLE_SERVICE_ACCOUNT_FILE (path) or GOOGLE_SERVICE_ACCOUNT_JSON (full JSON)."
    )


# =========================
# BYBIT CONFIG
# =========================
BYBIT_BASE_URL = "https://api.bybit.com"
BYBIT_RECV_WINDOW = "5000"  # ms


def _bybit_hmac_sign(message: str, secret: str) -> str:
    """Bybit: HMAC-SHA256 hex digest."""
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def bybit_get_positions(
    api_key: str,
    api_secret: str,
    symbol: Optional[str] = None,
    base_coin: Optional[str] = None,
    settle_coin: Optional[str] = None,
    limit: Optional[int] = None,
    cursor: Optional[str] = None,
    timeout_s: int = 10,
) -> Dict[str, Any]:
    """
    Bybit: GET /v5/position/list (category fixed to 'linear')
    """
    timestamp = str(int(time.time() * 1000))

    params: Dict[str, Any] = {"category": "linear"}
    if symbol is not None:
        params["symbol"] = symbol
    if base_coin is not None:
        params["baseCoin"] = base_coin
    if settle_coin is not None:
        params["settleCoin"] = settle_coin
    if limit is not None:
        params["limit"] = str(limit)
    if cursor is not None:
        params["cursor"] = cursor

    query_string = urlencode(params)
    pre_sign = timestamp + api_key + BYBIT_RECV_WINDOW + query_string
    signature = _bybit_hmac_sign(pre_sign, api_secret)

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-SIGN": signature,
        "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
        "Content-Type": "application/json",
    }

    url = f"{BYBIT_BASE_URL}/v5/position/list"
    resp = requests.get(url, params=params, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


# =========================
# BINANCE CONFIG (USD-M Futures)
# =========================
BINANCE_BASE_URL = "https://fapi.binance.com"
BINANCE_INCOME_START_MS = int(
    datetime(2025, 12, 15, tzinfo=timezone.utc).timestamp() * 1000
)


def _binance_sign_params(params: Dict[str, Any], api_secret: str) -> str:
    qs = urlencode(params, doseq=True)
    return hmac.new(
        api_secret.encode("utf-8"),
        qs.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _binance_signed_get(
    path: str,
    api_key: str,
    api_secret: str,
    params: Dict[str, Any],
    timeout_s: int = 20,
) -> Any:
    params = dict(params)
    params.setdefault("timestamp", int(time.time() * 1000))
    params["signature"] = _binance_sign_params(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url = f"{BINANCE_BASE_URL}{path}"
    r = requests.get(url, params=params, headers=headers, timeout=timeout_s)

    try:
        data = r.json()
    except ValueError:
        r.raise_for_status()
        raise RuntimeError(f"Non-JSON response: {r.text}")

    if r.status_code != 200:
        raise RuntimeError(f"Binance error ({r.status_code}): {data}")

    return data


def binance_get_futures_income(
    api_key: str,
    api_secret: str,
    symbol: Optional[str] = None,
    income_type: Optional[str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    page: Optional[int] = None,
    limit: int = 100,
    recv_window: int = 5000,
    timeout_s: int = 20,
) -> List[Dict[str, Any]]:
    if not (1 <= limit <= 1000):
        raise ValueError("limit must be between 1 and 1000")

    params: Dict[str, Any] = {"recvWindow": recv_window, "limit": limit}
    if symbol:
        params["symbol"] = symbol
    if income_type:
        params["incomeType"] = income_type
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)
    if page is not None:
        params["page"] = int(page)

    data = _binance_signed_get(
        path="/fapi/v1/income",
        api_key=api_key,
        api_secret=api_secret,
        params=params,
        timeout_s=timeout_s,
    )

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response shape: {data}")

    return data


def binance_get_futures_account(
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
    timeout_s: int = 20,
) -> Dict[str, Any]:
    data = _binance_signed_get(
        path="/fapi/v3/account",
        api_key=api_key,
        api_secret=api_secret,
        params={"recvWindow": recv_window},
        timeout_s=timeout_s,
    )

    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response shape: {data}")

    return data


# =========================
# OKX CONFIG
# =========================
OKX_BASE_URL = "https://www.okx.com"


def _okx_timestamp() -> str:
    # Millisecond ISO format, e.g. 2020-12-08T09:08:57.715Z
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    prehash = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def okx_get_positions(
    api_key: str,
    api_secret: str,
    passphrase: str,
    inst_type: Optional[str] = None,   # "SWAP", "FUTURES", "MARGIN", "OPTION"
    inst_id: Optional[str] = None,     # e.g. "BTC-USDT-SWAP"
    pos_id: Optional[str] = None,      # optional
    timeout_s: int = 20,
) -> Dict[str, Any]:
    """
    Calls: GET /api/v5/account/positions
    Returns: {"code":"0","msg":"","data":[ ... ]}
    """
    path = "/api/v5/account/positions"

    params: Dict[str, str] = {}
    if inst_type:
        params["instType"] = inst_type
    if inst_id:
        params["instId"] = inst_id
    if pos_id:
        params["posId"] = pos_id

    query = urlencode(params)
    request_path = f"{path}?{query}" if query else path

    timestamp = _okx_timestamp()
    method = "GET"
    body = ""  # GET has no body

    sign = _okx_sign(timestamp, method, request_path, body, api_secret)

    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }

    url = f"{OKX_BASE_URL}{request_path}"
    r = requests.get(url, headers=headers, timeout=timeout_s)

    try:
        data = r.json()
    except ValueError:
        r.raise_for_status()
        raise RuntimeError(f"Non-JSON response: {r.text}")

    if r.status_code != 200:
        raise RuntimeError(f"OKX HTTP error ({r.status_code}): {data}")

    # OKX uses code != "0" to indicate API-level errors even with HTTP 200
    if isinstance(data, dict) and data.get("code") not in (None, "0"):
        raise RuntimeError(f"OKX API error: {data}")

    return data


def _pick_okx_position(resp: Dict[str, Any], inst_id: Optional[str]) -> Optional[Dict[str, Any]]:
    data = resp.get("data", [])
    if not isinstance(data, list) or not data:
        return None
    if not inst_id:
        return data[0]
    for p in data:
        if p.get("instId") == inst_id:
            return p
    return data[0]


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Read exchange env vars at runtime (important for cron/sourcing)
    BYBIT_API_KEY = require_env("BYBIT_API_KEY")
    BYBIT_API_SECRET = require_env("BYBIT_API_SECRET")
    BINANCE_API_KEY = require_env("BINANCE_API_KEY")
    BINANCE_API_SECRET = require_env("BINANCE_API_SECRET")

    OKX_API_KEY = require_env("OKX_API_KEY")
    OKX_API_SECRET = require_env("OKX_API_SECRET")
    OKX_PASSPHRASE = require_env("OKX_PASSPHRASE")

    # Optional OKX instrument configuration (defaults align with your example)
    OKX_INST_TYPE = "SWAP"
    OKX_INST_ID = "NIGHT-USDT-SWAP"

    # ---- BYBIT ----
    bybit_resp = bybit_get_positions(
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        symbol="NIGHTUSDT",
        settle_coin="USDT",
    )

    bybit_list = bybit_resp.get("result", {}).get("list", [])
    # Don’t assume list[0] is the one you want
    bybit_pos = next((p for p in bybit_list if p.get("symbol") == "NIGHTUSDT"), None)

    bybit_cum_realised_pnl = None
    bybit_size = None
    if bybit_pos:
        bybit_cum_realised_pnl = bybit_pos.get("cumRealisedPnl")
        bybit_size = bybit_pos.get("size")

    # ---- BINANCE ----
    income = binance_get_futures_income(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET,
        income_type="FUNDING_FEE",
        start_time_ms=BINANCE_INCOME_START_MS,
        limit=1000,
    )

    print(f"Rows: {len(income)}")
    print(income[:2])

    total_income = sum(float(row["income"]) for row in income if "income" in row)
    print("Total income (float):", total_income)

    account = binance_get_futures_account(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET,
    )

    positions = account.get("positions", [])
    open_positions = [p for p in positions if float(p.get("positionAmt", "0") or "0") != 0]
    print(f"Open positions: {len(open_positions)}")

    binance_size = None
    if positions:
        binance_size = positions[0].get("positionAmt")

    # ---- OKX ----
    okx_resp = okx_get_positions(
        api_key=OKX_API_KEY,
        api_secret=OKX_API_SECRET,
        passphrase=OKX_PASSPHRASE,
        inst_type=OKX_INST_TYPE,
        inst_id=OKX_INST_ID,
    )

    okx_pos = _pick_okx_position(okx_resp, OKX_INST_ID)
    okx_deltaPA = None
    okx_realizedPnl = None
    okx_size = None  # OKX commonly uses "pos" as position size
    if okx_pos:
        okx_deltaPA = okx_pos.get("deltaPA")
        okx_realizedPnl = okx_pos.get("realizedPnl")
        okx_size = okx_pos.get("pos")

    # ---- BUILD DATAFRAME FOR SHEET (cefi pnl) ----
    now_ms = int(time.time() * 1000)
    df = pd.DataFrame(
        [
            {
                "timestamp_ms": now_ms,
                "exchange": "bybit",
                "symbol": "NIGHTUSDT",
                "metric": "cumRealisedPnl",
                "value": bybit_cum_realised_pnl,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "bybit",
                "symbol": "NIGHTUSDT",
                "metric": "size",
                "value": bybit_size,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "binance",
                "symbol": "",
                "metric": "total_income_funding_fees",
                "value": total_income,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "binance",
                "symbol": "",
                "metric": "positions_0_positionAmt",
                "value": binance_size,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "okx",
                "symbol": OKX_INST_ID,
                "metric": "deltaPA",
                "value": okx_deltaPA,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "okx",
                "symbol": OKX_INST_ID,
                "metric": "realizedPnl",
                "value": okx_realizedPnl,
            },
            {
                "timestamp_ms": now_ms,
                "exchange": "okx",
                "symbol": OKX_INST_ID,
                "metric": "pos",
                "value": okx_size,
            },
        ]
    )

    # ---- REVISION: add a "last updated" meta row ----
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    meta_df = pd.DataFrame(
        [
            {
                "timestamp_ms": now_ms,
                "last updated": run_ts,
                "exchange": "",
                "symbol": "",
                "metric": "script_run_time",
                "value": "",
            }
        ]
    )
    df = pd.concat([df, meta_df], ignore_index=True)

    # ---- GOOGLE SHEETS WRITE ----
    sa_path = get_service_account_file_path()
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sh = gc.open_by_url(SHEET_URL)

    try:
        ws = sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=TAB_NAME,
            rows=max(len(df) + 10, 1000),
            cols=len(df.columns) + 5,
        )

    ws.clear()
    set_with_dataframe(
        ws,
        df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    print(f"Wrote {len(df)} rows to tab '{TAB_NAME}'")
