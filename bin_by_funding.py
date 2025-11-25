# funding binance + bybit

import os
import time
import csv
import hmac
import hashlib
from decimal import Decimal
from urllib.parse import urlencode

import requests

from config import BINANCE_API_KEY, BINANCE_API_SECRET, BYBIT_API_KEY, BYBIT_API_SECRET
BINANCE_BASE = "https://fapi.binance.com"
BYBIT_BASE = "https://api.bybit.com"

# =========================
OUTPUT_DIR = #your folder dir in ""
# =========================

# =========================
#  Binance (all from specific timestamp)
# =========================

def binance_signed_request(endpoint: str, params: dict) -> list:
    api_key = BINANCE_API_KEY
    api_secret = BINANCE_API_SECRET

    if not api_key or not api_secret:
        raise RuntimeError("Set BINANCE_API_KEY and BINANCE_API_SECRET env vars")

    params = dict(params)
    params.setdefault("recvWindow", 5000)
    params["timestamp"] = int(time.time() * 1000)

    qs = urlencode(sorted(params.items()))
    signature = hmac.new(
        api_secret.encode("utf-8"),
        qs.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = f"{BINANCE_BASE}{endpoint}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_binance_funding(start_ms: int | None = None) -> list:
    """
    Collecting FUNDING_FEE via /fapi/v1/income, time pagination.
    """
    params = {
        "incomeType": "FUNDING_FEE",
        "limit": 1000,
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)

    all_rows = []
    while True:
        data = binance_signed_request("/fapi/v1/income", params)
        if not data:
            break

        all_rows.extend(data)

        if len(data) < params["limit"]:
            break

        last_time = int(data[-1]["time"])
        params["startTime"] = last_time + 1

    return all_rows

def normalize_binance(rows: list) -> list:
    norm = []
    for r in rows:
        if r.get("incomeType") != "FUNDING_FEE":
            continue
        sym = r.get("symbol")
        asset = r.get("asset", "USDT")
        amt = Decimal(str(r.get("income", "0")))
        t = int(r.get("time", 0))

        norm.append(
            {
                "exchange": "Binance",
                "symbol": sym,
                "asset": asset,
                "amount": amt,
                "time": t,
                "type": "funding",
            }
        )
    return norm

# =========================
#  Bybit (realized PnL for opened pos)
# =========================

def bybit_signed_request(path: str, params: dict | None = None) -> dict:
    api_key = BYBIT_API_KEY
    api_secret = BYBIT_API_SECRET

    if not api_key or not api_secret:
        raise RuntimeError("Set BYBIT_API_KEY and BYBIT_API_SECRET in config.py")

    params = params or {}
    recv_window = "5000"
    ts = str(int(time.time() * 1000))

    items = sorted(params.items())
    query_str = urlencode(items)

    sign_payload = f"{ts}{api_key}{recv_window}{query_str}"
    signature = hmac.new(
        api_secret.encode(),
        sign_payload.encode(),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }

    url = BYBIT_BASE + path
    if query_str:
        url += "?" + query_str

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_bybit_realized_raw() -> list:
    all_positions = []

    for settle in ["USDT", "USDC"]:
        params = {
            "category": "linear",
            "settleCoin": settle,
        }

        try:
            data = bybit_signed_request("/v5/position/list", params)
        except Exception as e:
            print(f"WARNING: Skipping Bybit {settle} because of error: {e}")
            continue

        if data.get("retCode") != 0:
            print(
                f"WARNING: Bybit {settle} retCode={data.get('retCode')}, "
                f"retMsg={data.get('retMsg')}"
            )
            continue

        result = data.get("result") or {}
        pos_list = result.get("list") or []
        all_positions.extend(pos_list)

    return all_positions

def normalize_bybit(positions: list) -> list:
    norm = []
    for pos in positions:
        size = pos.get("size")
        if not size or size in ("0", "0.0"):
            continue  # только открытые позиции

        symbol = pos.get("symbol")
        if not symbol:
            continue

        asset = symbol.replace("USDT", "").replace("USDC", "")

        # take PnL of opened pos only
        cur_realised_str = pos.get("curRealisedPnl") or "0"

        try:
            amt = Decimal(cur_realised_str)
        except Exception:
            amt = Decimal("0")

        t = int(pos.get("updatedTime", 0) or 0)

        norm.append(
            {
                "exchange": "Bybit",
                "symbol": symbol,
                "asset": asset,
                "amount": amt,
                "time": t,
                "type": "realized_pnl",
            }
        )

    print(f"INFO: Bybit realized PnL rows: {len(norm)}")
    return norm

# =========================
#  CSV Writer with OUTPUT_DIR
# =========================

def write_csv(rows, summary_name="funding_summary.csv", details_name="funding_details.csv"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary_path = os.path.join(OUTPUT_DIR, summary_name)
    details_path = os.path.join(OUTPUT_DIR, details_name)

    by_key = {}
    for r in rows:
        key = (r["exchange"], r["symbol"], r["asset"], r["type"])
        if key not in by_key:
            by_key[key] = {
                "total": Decimal("0"),
                "first": r["time"],
                "last": r["time"],
                "count": 0,
            }
        by_key[key]["total"] += r["amount"]
        by_key[key]["count"] += 1
        by_key[key]["first"] = min(by_key[key]["first"], r["time"])
        by_key[key]["last"] = max(by_key[key]["last"], r["time"])

    # SUMMARY CSV
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["exchange", "symbol", "asset", "type", "total_amount", "periods", "first_time", "last_time"])
        for (ex, sym, asset, typ), d in sorted(by_key.items()):
            w.writerow([
                ex, sym, asset, typ,
                f"{d['total']:.10f}",
                d["count"],
                d["first"],
                d["last"],
            ])

    # DETAILS CSV
    with open(details_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["exchange", "symbol", "asset", "type", "amount", "time"])
        for r in rows:
            w.writerow([
                r["exchange"],
                r["symbol"],
                r["asset"],
                r["type"],
                f"{r['amount']:.10f}",
                r["time"],
            ])

    print(f"Wrote:\n - {summary_path}\n - {details_path}")


# =========================
#  Main
# =========================

def main():
    start_ms = 1762732800000  # your date

    # Binance
    raw_binance = get_binance_funding(start_ms)
    print(f"Binance raw rows: {len(raw_binance)}")
    norm_binance = normalize_binance(raw_binance)

    # Bybit
    try:
        raw_bybit = get_bybit_realized_raw()
        norm_bybit = normalize_bybit(raw_bybit)
    except RuntimeError as e:
        print(f"Bybit not configured: {e}")
        norm_bybit = []

    rows = norm_binance + norm_bybit
    print(f"Total normalized rows: {len(rows)}")

    write_csv(rows)


if __name__ == "__main__":
    main()
