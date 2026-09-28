from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import asyncio, time, statistics
import httpx

app = FastAPI(title="Lighthouse V1")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://api.binance.com",
]
OKX = "https://www.okx.com"
COINBASE = "https://api.exchange.coinbase.com"

state = {
    "updated": 0,
    "sources": {"binance": False, "okx": False, "coinbase": False},
    "assets": {},
    "history": {s: [] for s in SYMBOLS},
}

def now():
    return int(time.time() * 1000)

async def get(c, u, p=None):
    r = await c.get(u, params=p, timeout=8)
    r.raise_for_status()
    return r.json()

async def bget(c, path, p=None):
    last_error = None
    for base in BINANCE_BASES:
        try:
            return await get(c, base + path, p), base
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Binance unavailable: {last_error}")

async def binance(c, s):
    t, base = await bget(c, "/api/v3/ticker/24hr", {"symbol": s})
    d, _ = await bget(c, "/api/v3/depth", {"symbol": s, "limit": 20})
    tr, _ = await bget(c, "/api/v3/aggTrades", {"symbol": s, "limit": 300})
    bids = sum(float(x[0]) * float(x[1]) for x in d.get("bids", []))
    asks = sum(float(x[0]) * float(x[1]) for x in d.get("asks", []))
    buy = sum(float(x.get("q", 0)) for x in tr if not x.get("m"))
    sell = sum(float(x.get("q", 0)) for x in tr if x.get("m"))
    return {
        "price": float(t["lastPrice"]),
        "change24h": float(t["priceChangePercent"]),
        "book_buy": bids / (bids + asks) if bids + asks else .5,
        "trade_buy": buy / (buy + sell) if buy + sell else .5,
        "endpoint": base,
    }

async def okx(c, s):
    inst = s.replace("USDT", "-USDT")
    t = (await get(c, f"{OKX}/api/v5/market/ticker", {"instId": inst}))["data"][0]
    d = (await get(c, f"{OKX}/api/v5/market/books", {"instId": inst, "sz": "20"}))["data"][0]
    bids = sum(float(x[0]) * float(x[1]) for x in d["bids"])
    asks = sum(float(x[0]) * float(x[1]) for x in d["asks"])
    p = float(t["last"])
    o = float(t["open24h"])
    return {
        "price": p,
        "change24h": (p - o) / o * 100 if o else 0,
        "book_buy": bids / (bids + asks) if bids + asks else .5,
    }

async def coinbase(c, s):
    product = s.replace("USDT", "-USD")
    t = await get(c, f"{COINBASE}/products/{product}/ticker")
    st = await get(c, f"{COINBASE}/products/{product}/stats")
    p = float(t["price"])
    o = float(st["open"])
    return {"price": p, "change24h": (p - o) / o * 100 if o else 0}

async def asset(c, s):
    src = {}
    for n, fn in (("binance", binance), ("okx", okx), ("coinbase", coinbase)):
        try:
            src[n] = await fn(c, s)
        except Exception:
            src[n] = None

    ps = [v["price"] for v in src.values() if v]
    out = {"sources": src, "evidence": {}, "trend": {}, "stats": {}, "probability_advantage": 0}
    if not ps:
        return out

    p = statistics.median(ps)
    h = state["history"][s]
    h.append(p)
    del h[:-2000]

    def ret(n):
        return ((p - h[-n-1]) / h[-n-1] * 100) if len(h) > n and h[-n-1] else None

    short = ret(10)
    mid = ret(100)
    changes = [v["change24h"] for v in src.values() if v]

    if short is None:
        short = statistics.mean(changes) if changes else 0
    if mid is None:
        mid = ret(30) if ret(30) is not None else short

    ratios = []
    for n in ("binance", "okx"):
        if src.get(n):
            ratios.append(src[n]["book_buy"])
    if src.get("binance"):
        ratios.append(src["binance"]["trade_buy"])
    buy = statistics.mean(ratios) if ratios else .5

    score = max(0, min(100,
        50
        + max(-15, min(15, short * 2))
        + max(-20, min(20, mid * 1.2))
        + max(-10, min(10, (buy - .5) * 100))
    ))

    trend = (
        "中期偏多" if mid > 1.5 and short >= -.5
        else "中期偏空" if mid < -1.5 and short <= .5
        else "中期震荡 / 观察"
    )
    conflict = short * mid < 0

    samples = []
    if len(h) >= 80:
        for i in range(20, len(h) - 10, max(5, len(h) // 300)):
            if h[i]:
                samples.append((h[i+10] - h[i]) / h[i] * 100)

    out["evidence"] = {
        "sources": len(ps),
        "buy_power": round(buy * 100, 1),
        "change24h": round(statistics.mean(changes), 3) if changes else 0,
        "dispersion": round((max(ps) - min(ps)) / p * 100, 4),
        "binance_endpoint": src.get("binance", {}).get("endpoint") if src.get("binance") else None,
        "conflict": conflict,
    }
    out["trend"] = {
        "short": "短期偏强" if short > .5 else ("短期偏弱" if short < -.5 else "短期震荡"),
        "mid": trend,
        "stage": "趋势形成/延续" if abs(mid) > 1.5 else "整理/待确认",
        "reversal_risk": "较高" if conflict else ("中等" if abs(short) > 1.2 else "中低"),
    }
    out["stats"] = {
        "sample_count": len(samples),
        "up_rate": round(sum(x > 0 for x in samples) / len(samples) * 100, 1) if samples else None,
        "down_rate": round(sum(x < 0 for x in samples) / len(samples) * 100, 1) if samples else None,
        "median_return": round(statistics.median(samples), 3) if samples else None,
    }
    out["probability_advantage"] = round(score, 1)
    return out

async def loop():
    async with httpx.AsyncClient(headers={"User-Agent": "Lighthouse-V1"}) as c:
        while True:
            a = {}
            for s in SYMBOLS:
                try:
                    a[s] = await asset(c, s)
                except Exception:
                    a[s] = {"sources": {}, "evidence": {}, "trend": {}, "stats": {}, "probability_advantage": 0}
            state["assets"] = a
            state["sources"] = {
                n: any(a[s].get("sources", {}).get(n) for s in SYMBOLS)
                for n in ("binance", "okx", "coinbase")
            }
            state["updated"] = now()
            await asyncio.sleep(10)

@app.on_event("startup")
async def start():
    asyncio.create_task(loop())

@app.get("/health")
async def health():
    return {"status": "ok", "sources": state["sources"], "updated": state["updated"]}

@app.get("/api/state")
async def api_state():
    return state

HTML = '''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lighthouse V1</title>
<style>
body{margin:0;background:#07111f;color:#edf5ff;font-family:system-ui,-apple-system,"Segoe UI","Noto Sans SC",sans-serif}
main{max-width:900px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:center}
.status{padding:7px 10px;border:1px solid #29405d;border-radius:999px;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.card{background:#0d1a2b;border:1px solid #213650;border-radius:16px;padding:14px;margin-top:10px}
.price{font-size:26px;font-weight:800}.small{font-size:12px;color:#91a6bf}
.row{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid #20324a;font-size:12px}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:6px;background:#ff687b}
.on{background:#43df91}.ok{color:#43df91}.bad{color:#ff687b}
@media(max-width:650px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<main>
<div class="top"><div><h1>🔦 Lighthouse V1</h1><div class="small">证据先行 · 中期为主 · 自动融合</div></div><div id="st" class="status">连接中</div></div>
<div class="card"><b>数据中枢</b><div id="src"></div><div class="small">Binance 自动尝试官方备用入口；单源失败不拖垮系统。</div></div>
<div id="a" class="grid"></div>
<div class="card"><b>五层引擎</b><div class="small">①实时数据 ②证据 ③统计样本 ④趋势状态 ⑤概率优势/反方证据。样本不足时不冒充长期概率。</div></div>
</main>
<script>
const $=x=>document.getElementById(x);
function m(x){return x==null?'—':'$'+Number(x).toLocaleString(undefined,{maximumFractionDigits:2})}
function s(n,on){return '<div class="row"><span><i class="dot '+(on?'on':'')+'"></i>'+n+'</span><b class="'+(on?'ok':'bad')+'">'+(on?'可用':'不可用')+'</b></div>'}
async function f(){
 try{
  let x=await fetch('/api/state',{cache:'no-store'}).then(r=>r.json());
  let n=Object.values(x.sources).filter(Boolean).length;
  $("st").textContent=n+'/3 数据源可用';
  $("src").innerHTML=s('Binance',x.sources.binance)+s('OKX',x.sources.okx)+s('Coinbase',x.sources.coinbase);
  $("a").innerHTML=Object.entries(x.assets).map(([k,v])=>{
   let q=v.sources||{},e=v.evidence||{},t=v.trend||{},z=v.stats||{};
   let p=q.binance?.price||q.okx?.price||q.coinbase?.price;
   return '<div class="card"><h2>'+k+'</h2><div class="price">'+m(p)+'</div><div class="small">'+(t.mid||'等待')+' · '+(t.short||'等待')+' · 概率优势 '+(v.probability_advantage??'—')+'</div><div class="row"><span>趋势阶段</span><b>'+ (t.stage||'—')+'</b></div><div class="row"><span>买方力量</span><b>'+(e.buy_power??'—')+'%</b></div><div class="row"><span>24h</span><b>'+(e.change24h??'—')+'%</b></div><div class="row"><span>反转风险</span><b>'+ (t.reversal_risk||'—')+'</b></div><div class="row"><span>统计样本</span><b>'+ (z.sample_count||0)+'</b></div><div class="row"><span>上涨频率</span><b>'+ (z.up_rate==null?'样本不足':z.up_rate+'%')+'</b></div><div class="row"><span>反方证据</span><b>'+ (e.conflict?'增强':'未见明显冲突')+'</b></div></div>'
  }).join('')
 }catch(e){$("st").textContent='服务器连接异常'}
}
f();setInterval(f,5000);
</script>
</body>
</html>'''

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(HTML)
