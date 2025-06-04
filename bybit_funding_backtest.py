#!/usr/bin/env python3
"""
bybit_funding_backtest.py
─────────────────────────
8h Funding を収益源にするデルタ0キャリーバスケット戦略を
「過去30日（日次リバランス）」でシミュレートし、

- 純損益 (USD)
- 年率換算リターン
- Sharpe Ratio (√365)
- IC (Spearman；スコア vs 翌日収益)

を出力する簡易バックテスター。
--------------------------------------------------------------
依存: python 3.10+,  httpx==0.27.*,  pandas,  scipy,  tqdm
"""

import math, asyncio, warnings
from typing import List, Dict, Any
import httpx
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from tqdm.asyncio import tqdm as tqdm_async

import bt_config as C

warnings.filterwarnings("ignore", category=FutureWarning)

BASE = "https://api.bybit.com"

ahttp = httpx.AsyncClient(base_url=BASE, timeout=15.0,
                          headers={"User-Agent": "funding-bt"})

async def _get(path: str, params: Dict[str, Any]) -> Any:
    r = await ahttp.get(path, params=params)
    r.raise_for_status()
    js = r.json()
    if js["retCode"] != 0:
        raise RuntimeError(js)
    return js["result"]

async def list_linear_usdt() -> List[str]:
    res = await _get("/v5/market/instruments-info",
                     {"category": "linear", "limit": 1000})
    return [x["symbol"] for x in res["list"]
            if x["status"] == "Trading" and x["quoteCoin"] == "USDT"]

async def funding_df(sym: str, start: int, end: int) -> pd.DataFrame:
    dfs = []
    cursor = end
    while cursor > start:
        chunk = await _get("/v5/market/funding/history",
                           {"symbol": sym, "startTime": start,
                            "endTime": cursor, "limit": 200})
        if not chunk["list"]:
            break
        df = pd.DataFrame(chunk["list"])
        df["ts"] = pd.to_datetime(df["fundingRateTimestamp"], unit="ms")
        df["rate"] = df["fundingRate"].astype(float)
        dfs.append(df[["ts", "rate"]])
        cursor = df["ts"].min().value // 1_000_000 - 1
    if not dfs:
        return pd.DataFrame(columns=["ts", "rate"])
    out = pd.concat(dfs).drop_duplicates("ts").sort_values("ts")
    return out[(out["ts"] >= pd.to_datetime(start, unit="ms")) &
               (out["ts"] <= pd.to_datetime(end, unit="ms"))]

async def oi_latest(sym: str) -> float:
    res = await _get("/v5/market/open-interest",
                     {"category": "linear", "symbol": sym,
                      "intervalTime": "5min", "limit": 1})
    return float(res["list"][0]["openInterest"]) * float(res["list"][0]["markPrice"])

async def basis_latest(sym: str) -> float:
    res = await _get("/v5/market/tickers",
                     {"category": "linear", "symbol": sym})
    d = res["list"][0]
    return ((float(d["markPrice"]) - float(d["indexPrice"]))
            / float(d["indexPrice"]))

async def collect_all():
    print("▼ downloading symbol list ...")
    syms = await list_linear_usdt()

    end_ts = int(pd.Timestamp(C.END_DATE).timestamp() * 1000)
    start_ts = int(pd.Timestamp(C.START_DATE).timestamp() * 1000)
    days = (pd.Timestamp(C.END_DATE) - pd.Timestamp(C.START_DATE)).days + 1
    date_index = pd.date_range(C.START_DATE, periods=days, freq="D")

    funding_tables = {}
    for s in tqdm_async(syms, desc="funding"):
        funding_tables[s] = await funding_df(s, start_ts, end_ts)

    ois = {s: await oi_latest(s) for s in tqdm_async(syms, desc="oi")}
    bas = {s: await basis_latest(s) for s in tqdm_async(syms, desc="basis")}

    universe = [s for s in syms
                if ois[s] >= C.MIN_OI_USD and abs(bas[s]) <= C.MAX_BASIS_PCT]

    return universe, funding_tables, date_index

def daily_funding(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    df = df.set_index("ts").resample("1D")["rate"].sum()
    return df * 3 * 365

def run_backtest(univ, f_tables, idx):
    cap = C.CAPITAL_USD
    lev = C.LEVERAGE_X
    fee = (C.FEE_BPS + C.SLIPPAGE_BPS) / 10_000
    pnl_daily = []
    ic_daily = []

    score_prev = pd.Series(0.0, index=univ)

    for d in idx:
        scores = {}
        for s in univ:
            df = f_tables[s]
            day_val = df[(df["ts"].dt.date == d.date())]["rate"].sum() * 3 * 365
            scores[s] = day_val
        score_series = pd.Series(scores)

        mask = (score_series >= C.FUND_MIN_ANN * 100) & (score_series <= C.FUND_MAX_ANN * 100)
        score_series = score_series[mask].sort_values(ascending=False)
        basket = score_series.head(C.TOP_K).index.tolist()
        if len(basket) == 0:
            pnl_daily.append(0.0)
            ic_daily.append(np.nan)
            continue

        ret_sum = 0.0
        for s in basket:
            df = f_tables[s]
            next_day = d + pd.Timedelta(days=1)
            day_ret = df[(df["ts"] >= d) & (df["ts"] < next_day)]["rate"].sum()
            pnl = cap / C.TOP_K * lev * day_ret - cap / C.TOP_K * fee * 2
            ret_sum += pnl
        pnl_daily.append(ret_sum / cap)

        if not score_prev.empty:
            realized = {}
            for s in score_prev.index:
                df = f_tables[s]
                realized[s] = df[(df["ts"] >= d) & (df["ts"] < d + pd.Timedelta(days=1))]["rate"].sum()
            x, y = score_prev.align(pd.Series(realized), join="inner")
            ic_daily.append(spearmanr(x, y).correlation if len(x) > 2 else np.nan)
        score_prev = score_series

    ret = pd.Series(pnl_daily, index=idx).fillna(0)
    ic = pd.Series(ic_daily, index=idx)
    return ret, ic

async def main():
    universe, tables, dates = await collect_all()
    ret, ic = run_backtest(universe, tables, dates)

    ann_ret = (1 + ret).prod() ** (365 / len(ret)) - 1
    sharpe = ret.mean() / ret.std(ddof=0) * math.sqrt(365) if ret.std(ddof=0) > 0 else np.nan
    ic_mean = ic.mean()
    ic_ir = ic_mean / ic.std(ddof=0) if ic.std(ddof=0) > 0 else np.nan

    print("\n=========  Backtest Result  =========")
    print(f"Period          : {C.START_DATE} → {C.END_DATE}")
    print(f"Capital (USD)   : {C.CAPITAL_USD:,.0f}")
    print(f"Leverage        : {C.LEVERAGE_X}×")
    print(f"Top-K basket    : {C.TOP_K}")
    print("-------------------------------------")
    print(f"Total Return    : {ret.add(1).prod() - 1:6.2%}")
    print(f"Annual Return   : {ann_ret:6.2%}")
    print(f"Annual Vol      : {ret.std(ddof=0) * math.sqrt(365):6.2%}")
    print(f"Sharpe Ratio    : {sharpe:5.2f}")
    print("-------------------------------------")
    print(f"Mean IC         : {ic_mean:6.3f}")
    print(f"IC IR           : {ic_ir:6.3f}")
    print("=====================================")

    await ahttp.aclose()

if __name__ == "__main__":
    asyncio.run(main())
