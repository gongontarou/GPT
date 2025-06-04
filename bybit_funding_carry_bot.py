#!/usr/bin/env python3
"""
bybit_funding_carry_bot.py
──────────────────────────
■ 機能
1. 直近 24 h の Funding 年率・Basis・Open Interest を取得し
   ・Funding 年率 20–40 %
   ・OI ≥ 5 M USD
   ・|Basis| ≤ 0.30 %
   を満たす銘柄上位 k をバスケット化
2. Spot BUY／Perp SELL でデルタ ≈ 0 に建玉（レバは sellLeverage に反映）
3. 5 min ごとに
   ・スコア再計算 → バスケット入替
   ・Δ 乖離 > 閾値ならヘッジ
4. Webhook 通知・ファイルロギング
5. ^C で全建玉をクローズして安全停止
──────────────────────────
依存:
    python 3.10+   pip install pybit==3.5.2 httpx[http2] pydantic apscheduler
環境変数:
    BYBIT_KEY / BYBIT_SECRET
    BYBIT_LIVE          ← "true" で本番, 省略で testnet
    CAPITAL_USD         ← 運用資本 (既定 5 000)
    TOP_K               ← バスケット銘柄数 (既定 6)
    LEVERAGE_X          ← Perp 側レバ (既定 3.0)
    DELTA_THRESHOLD_BP  ← Δ 再ヘッジ閾値 (既定 25 = 0.25 %)
    WEBHOOK_URL         ← Discord/TG など任意
"""

import os, math, time, asyncio, logging, logging.handlers
from datetime import timezone, timedelta
from typing   import Dict, Any, List

import httpx
from pydantic import BaseModel, Field, validator
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval  import IntervalTrigger
from pybit.unified_trading          import HTTP   # Bybit V5 SDK

### ───── 0. 共通設定 ──────────────────────────────────────────
TZ_UTC = timezone.utc
API_HOST = ("https://api.bybit.com"
            if os.getenv("BYBIT_LIVE", "false").lower() == "true"
            else "https://api-testnet.bybit.com")

CAPITAL_USD = float(os.getenv("CAPITAL_USD", 5_000))
TOP_K       = int  (os.getenv("TOP_K", 6))
LEV_X       = float(os.getenv("LEVERAGE_X", 3.0))
DELTA_BP    = float(os.getenv("DELTA_THRESHOLD_BP", 25))   # 0.25 %
MIN_FUND    = 0.20   # 年率 20 %
MAX_FUND    = 0.40
MIN_OI      = 5_000_000     # USD
MAX_BASIS   = 0.003         # 0.30 %

KEY, SEC    = os.getenv("BYBIT_KEY"), os.getenv("BYBIT_SECRET")
assert KEY and SEC, "BYBIT_KEY / BYBIT_SECRET を export してください"

WEBHOOK     = os.getenv("WEBHOOK_URL")

logger = logging.getLogger("bybit_funding_bot")
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(
            "bybit_funding_bot.log", maxBytes=10_485_760, backupCount=5)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

### ───── 1. Pydantic データモデル ─────────────────────────────
class Stat(BaseModel):
    symbol: str
    funding_ann: float
    basis: float
    oi_usd: float
    score: float = 0.0
    @validator("score", always=True)
    def _score(v, values):
        f = values["funding_ann"]
        b = max(1e-4, abs(values["basis"]))
        return f / math.sqrt(b)

### ───── 2. REST クライアント ────────────────────────────────
client  = HTTP(api_key=KEY, api_secret=SEC, testnet=API_HOST.endswith("testnet"))
async_http = httpx.AsyncClient(base_url=API_HOST, timeout=10.0)

async def _get(path: str, params: Dict[str, Any]) -> Any:
    r = await async_http.get(path, params=params)
    r.raise_for_status()
    js = r.json()
    if js["retCode"] != 0:
        raise RuntimeError(js)
    return js["result"]

### ───── 3. データ取得関数群 ────────────────────────────────
async def list_linear_usdt() -> List[str]:
    res = await _get("/v5/market/instruments-info",
                     {"category":"linear","limit":1000})
    return [x["symbol"] for x in res["list"]
            if x["status"]=="Trading" and x["quoteCoin"]=="USDT"]

async def has_spot(sym: str) -> bool:
    spot = sym.replace("USDT", "/USDT")
    res = await _get("/v5/market/instruments-info",
                     {"category":"spot","symbol":spot})
    return bool(res["list"])

async def funding_ann(sym: str) -> float:
    end   = int(time.time()*1000)
    start = end - 86_400_000        # 24 h
    res = await _get("/v5/market/funding/history",
                     {"symbol":sym,"category":"linear",
                      "startTime":start,"endTime":end})
    total = sum(float(x["fundingRate"]) for x in res["list"])
    return total * 3 * 365        # 8 h settle → 年率化

async def basis_pct(sym: str) -> float:
    res = await _get("/v5/market/tickers",
                     {"category":"linear","symbol":sym})
    d = res["list"][0]
    return ((float(d["markPrice"]) - float(d["indexPrice"]))
            /  float(d["indexPrice"]))

async def open_interest_usd(sym: str) -> float:
    res = await _get("/v5/market/open-interest",
                     {"category":"linear","symbol":sym,
                      "intervalTime":"5min","limit":1})
    oi_cnt = float(res["list"][0]["openInterest"])
    price  = float(res["list"][0]["markPrice"])
    return oi_cnt * price

async def collect_stats() -> List[Stat]:
    symbols = await list_linear_usdt()
    # Spot が無いペアを除外
    symbols = [s for s in symbols if await has_spot(s)]
    tasks = [asyncio.gather(funding_ann(s), basis_pct(s), open_interest_usd(s))
             for s in symbols]
    out=[]
    for s, (f, b, oi) in zip(symbols, await asyncio.gather(*tasks)):
        if not (MIN_FUND <= f <= MAX_FUND): continue
        if abs(b) > MAX_BASIS:               continue
        if oi < MIN_OI:                      continue
        out.append(Stat(symbol=s, funding_ann=f, basis=b, oi_usd=oi))
    out.sort(key=lambda x: x.score, reverse=True)
    return out[:TOP_K]

### ───── 4. 取引ラッパ ────────────────────────────────────

def set_leverage(sym:str):
    client.set_leverage(category="linear", symbol=sym,
                        buyLeverage=str(LEV_X), sellLeverage=str(LEV_X))

def place_market(category:str, symbol:str, side:str, qty:float):
    client.place_order(category=category, symbol=symbol,
                       side=side, orderType="Market", qty=qty)

def spot_symbol(linear_sym:str) -> str:
    return linear_sym.replace("USDT", "/USDT")

def last_price(sym:str) -> float:
    return float(client.get_tickers(category="linear", symbol=sym)["list"][0]["lastPrice"])

def delta_usd() -> float:
    pos = client.get_positions(category="linear")["list"]
    return sum(float(p["size"])*float(p["markPrice"])*
               (1 if p["side"]=="Buy" else -1) for p in pos)

async def enter_pair(sym:str, usd_alloc:float):
    set_leverage(sym)
    px   = last_price(sym)
    qty  = round(usd_alloc / px, 3)
    logger.info(f"ENTRY {sym}: Spot BUY {qty}, Perp SELL {qty}")
    place_market("spot",   spot_symbol(sym), "Buy",  qty)
    place_market("linear", sym,             "Sell", qty)
    await notify(f"✅ Entered {sym}")

async def exit_pair(sym:str):
    pos  = client.get_positions(category="linear", symbol=sym)["list"][0]
    qty  = abs(float(pos["size"]))
    if qty==0: return
    side = "Buy" if pos["side"]=="Sell" else "Sell"
    logger.info(f"EXIT {sym}: close {qty}")
    place_market("linear", sym, side, qty)
    await notify(f"🚪 Exited {sym}")

### ───── 5. 主要ループ ───────────────────────────────────
async def heartbeat():
    try:
        top = await collect_stats()
        basket = {s.symbol for s in top}
        logger.info("Basket: " + ", ".join(f"{x.symbol}:{x.funding_ann:.1f}%" for x in top))
        usd_each = CAPITAL_USD / TOP_K

        active = {p["symbol"] for p in client.get_positions(category="linear")["list"]
                  if float(p["size"])!=0}

        # 新規エントリ
        for sym in basket - active:
            await enter_pair(sym, usd_each)

        # 撤退
        for sym in active - basket:
            await exit_pair(sym)

        # Δ ヘッジ
        d = delta_usd()
        if abs(d) > CAPITAL_USD * DELTA_BP / 10_000:
            sym = list(basket)[0]       # 代表銘柄
            px  = last_price(sym)
            qty = round(abs(d)/px, 3)
            side= "Buy" if d<0 else "Sell"
            logger.info(f"HEDGE {sym} {side} {qty}  (Δ={d:.0f}$)")
            place_market("linear", sym, side, qty)
    except Exception as e:
        logger.exception(e)

async def notify(msg:str):
    if not WEBHOOK: return
    try:
        await async_http.post(WEBHOOK, json={"content":msg})
    except Exception:
        pass   # 通知失敗は無視

async def main():
    sched = AsyncIOScheduler(timezone=TZ_UTC)
    sched.add_job(heartbeat, IntervalTrigger(minutes=5))
    sched.start()
    logger.info("Bot started")
    await notify("🚀 Bybit Funding-Carry Bot started")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Stopping …")
        await notify("🛑 Stopping bot – closing all positions")
        for p in client.get_positions(category="linear")["list"]:
            if float(p["size"])!=0:
                await exit_pair(p["symbol"])
        await async_http.aclose()

if __name__ == "__main__":
    asyncio.run(main())
