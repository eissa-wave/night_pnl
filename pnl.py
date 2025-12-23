import os
import json
import tempfile
import time
import hmac
import hashlib
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
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


# Exchange keys (set in GitHub Actions/Jenkins/AWS env)
BYBIT_API_KEY = require_env("BYBIT_API_KEY")
BYBIT_API_SECRET = require_env("BYBIT_API_SECRET")
BINANCE_API_KEY = require_env("BINANCE_API_KEY")
BINANCE_API_SECRET = require_env("BINANCE_API_SECRET")

# Google Sheets config
SHEET_URL = require_env("SHEET_URL")
TAB_NAME = "CeFi Data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Google service account:
# - Prefer GOOGLE_SERVICE_ACCOUNT_FILE (path to JSON file)
# - Or provide GOOGLE_SERVICE_ACCOUNT_JSON (full JSON string) and we'll write a temp file
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


def get_service_account_file_path() -> str:
    if GOOGLE_SERVICE_ACCOUNT_FILE:
        return GOOGLE_SERVICE_ACCOUNT_FILE

    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as e:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from e

        fd, path = tempfile.mkstemp(prefix="gcp-sa-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            f.write(GOOGLE_SERVICE_ACCOUNT_JSON)
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
# MAIN
# =========================
if __name__ == "__main__":
    # ---- BYBIT ----
    bybit_resp = bybit_get_positions(
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        symbol="NIGHTUSDT",
        settle_coin="USDT",
    )

    bybit_list = bybit_resp.get("result", {}).get("list", [])
    bybit_cum_realised_pnl = None
    bybit_size = None
    if bybit_list:
        bybit_cum_realised_pnl = bybit_list[0].get("cumRealisedPnl")
        bybit_size = bybit_list[0].get("size")

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

    # This line in your sample code didn't print; leaving behavior the same:
    positions[0]["positionAmt"] if positions else None

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
