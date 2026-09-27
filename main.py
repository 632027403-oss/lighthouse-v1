import asyncio
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import ccxt
import ccxt.pro as ccxtpro
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent

# The three exchanges are intentionally isolated: one failure must not blank the page.
MARKETS = {
    "binance": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
    "okx": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
    "coinbase": ["BTC/USD", "ETH/USD", "SOL/USD"],
}

state: dict[str, Any] = {
    "ts": 0,
    "sources": {
        ex: {"status": "starting", "last_update": None, "error": None}
        for ex in MARKETS
    },
    "quotes": {},
    "stocks": {
        s: {"status": "provider_required", "last": None, "timestamp": None}
        for s in ("AMD", "MRVL", "NVDA")
    },
}

tasks: list[asyncio.Task] = []


def ms() -> int:
    return int(time.time() * 1000)


def key(exchange: str, symbol: str) -> str:
    return f"{exchange}:{symbol}"


def normalize(exchange: str, symbol: str, ticker: dict) -> dict:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "base": symbol.split("/")[0],
        "quote": symbol.split("/")[1],
        "last": ticker.get("last"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
        "baseVolume": ticker.get("baseVolume"),
        "quoteVolume": ticker.get("quoteVolume"),
        "change24h": ticker.get("percentage"),
        "timestamp": ticker.get("timestamp") or ms(),
        "transport": "websocket",
    }


async def rest_fallback(exchange_name: str) -> None:
    ex = None
    try:
        ex = getattr(ccxt, exchange_name)({"enableRateLimit": True})
        await ex.load_markets()
        for symbol in MARKETS[exchange_name]:
            if symbol not in ex.markets:
                continue
            try:
                ticker = await ex.fetch_ticker(symbol)
                q = normalize(exchange_name, symbol, ticker)
                q["transport"] = "rest-fallback"
                state["quotes"][key(exchange_name, symbol)] = q
                state["sources"][exchange_name]["last_update"] = q["timestamp"]
            except Exception:
                continue
        state["sources"][exchange_name]["status"] = "fallback_rest"
    finally:
        if ex is not None:
            await ex.close()


async def run_exchange(exchange_name: str) -> None:
    backoff = 2

    while True:
        ex = None
        try:
            state["sources"][exchange_name].update(
                {"status": "connecting", "error": None}
            )

            ex = getattr(ccxtpro, exchange_name)({
                "enableRateLimit": True,
                "newUpdates": True,
            })
            await ex.load_markets()

            symbols = [s for s in MARKETS[exchange_name] if s in ex.markets]
            if not symbols:
                raise RuntimeError("No requested public symbols available")

            async def stream(symbol: str) -> None:
                while True:
                    ticker = await ex.watch_ticker(symbol)
                    q = normalize(exchange_name, symbol, ticker)
                    state["quotes"][key(exchange_name, symbol)] = q
                    state["sources"][exchange_name]["last_update"] = q["timestamp"]
                    state["sources"][exchange_name]["status"] = "live"

            state["sources"][exchange_name]["status"] = "live"
            backoff = 2
            await asyncio.gather(*(stream(s) for s in symbols))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state["sources"][exchange_name].update({
                "status": "offline",
                "error": f"{type(exc).__name__}: {str(exc)[:240]}",
            })

            try:
                await rest_fallback(exchange_name)
            except Exception as fallback_exc:
                state["sources"][exchange_name]["error"] += (
                    f" | REST: {type(fallback_exc).__name__}: "
                    f"{str(fallback_exc)[:160]}"
                )

            if ex is not None:
                try:
                    await ex.close()
                except Exception:
                    pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def clock() -> None:
    while True:
        state["ts"] = ms()
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tasks
    tasks = [asyncio.create_task(run_exchange(ex)) for ex in MARKETS]
    tasks.append(asyncio.create_task(clock()))
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(
    title="Lighthouse V1",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(ROOT / "static")), name="static")


@app.get("/")
async def home():
    return FileResponse(ROOT / "static" / "index.html")


@app.get("/health")
async def health():
    return {"ok": True, "service": "lighthouse-v1", "ts": ms()}


@app.get("/api/market")
async def market():
    return {
        "ts": state["ts"],
        "sources": state["sources"],
        "quotes": state["quotes"],
        "stocks": state["stocks"],
    }
