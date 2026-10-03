
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import asyncio, csv, io, json, math, re, statistics, time
from datetime import datetime, timezone
from email.utils import formatdate
import xml.etree.ElementTree as ET
import httpx

app = FastAPI(title="Lighthouse Final")

CRYPTO = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","DOGEUSDT",
          "ADAUSDT","AVAXUSDT","LINKUSDT","SUIUSDT","LTCUSDT","BCHUSDT",
          "DOTUSDT","TRXUSDT","TONUSDT"]
STOCKS = [
    "NVDA","MSFT","AAPL","AMZN","GOOGL","META","AVGO","TSLA","AMD","MRVL",
    "NFLX","PLTR","MU","TSM","QCOM","AMAT","INTC","ORCL","CRM","COST",
    "JPM","LLY","V","MA","WMT","XOM","CVX","GE","CAT","UBER"
]
BINANCE_SPOT = [
    "https://data-api.binance.vision","https://api-gcp.binance.com",
    "https://api1.binance.com","https://api2.binance.com",
    "https://api3.binance.com","https://api4.binance.com","https://api.binance.com"
]
BINANCE_FUT = [
    "https://fapi.binance.com","https://fapi1.binance.com","https://fapi2.binance.com",
    "https://fapi3.binance.com","https://fapi4.binance.com"
]
OKX = "https://www.okx.com"
COINBASE = "https://api.exchange.coinbase.com"
FARSIDE = "https://farside.co.uk/btc/"
SEC = "https://data.sec.gov"
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"
STOOQ = "https://stooq.com/q/d/l/"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
TREASURY = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"

UA = "LighthouseFinal/1.0 contact=lighthouse-data-project"
state = {
    "updated": 0, "sources": {}, "assets": {}, "crypto_rank": [], "stock_rank": [],
    "macro": {}, "etf": {}, "search": {}, "notes": [],
    "history": {"crypto": {}, "stocks": {}},
}
cache = {}
locks = {}

def ts(): return int(time.time()*1000)
def iso(ms): return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

async def get(c, url, params=None, headers=None, timeout=12):
    r = await c.get(url, params=params, headers=headers, timeout=timeout, follow_redirects=True)
    r.raise_for_status()
    return r

async def json_get(c, url, params=None, headers=None, timeout=12):
    return (await get(c,url,params,headers,timeout)).json()

async def text_get(c, url, params=None, headers=None, timeout=15):
    return (await get(c,url,params,headers,timeout)).text

async def first_json(c, bases, path, params=None):
    last = None
    for base in bases:
        try:
            r = await json_get(c, base + path, params)
            return r, base, None
        except Exception as e: last = str(e)
    return None, None, last

def pct(a,b):
    return ((a-b)/b*100) if b else None

def safe_mean(xs):
    return statistics.mean(xs) if xs else None

def daily_stats(closes, horizon):
    vals = []
    for i in range(len(closes)-horizon):
        if closes[i]:
            vals.append((closes[i+horizon]-closes[i])/closes[i]*100)
    if not vals: return {"count":0}
    return {
        "count":len(vals), "up_rate":round(sum(x>0 for x in vals)/len(vals)*100,1),
        "down_rate":round(sum(x<0 for x in vals)/len(vals)*100,1),
        "avg_return":round(statistics.mean(vals),2),
        "median_return":round(statistics.median(vals),2),
        "worst":round(min(vals),2),
        "best":round(max(vals),2),
    }

def max_drawdown(closes):
    peak = -float("inf"); dd = 0
    for x in closes:
        peak=max(peak,x)
        if peak: dd=min(dd,(x-peak)/peak*100)
    return round(dd,2)

def similar_stats(rows, feature_fn, target_index=0):
    # rows: chronological close records with feature tuple and close
    if len(rows)<80: return {"sample_count":0,"status":"样本不足"}
    current = feature_fn(rows[-1])
    # distance on normalized discrete features; require same regime bucket.
    sims=[]
    for i in range(30,len(rows)-91):
        f=feature_fn(rows[i])
        dist=sum(abs(a-b) for a,b in zip(current,f))
        if dist <= 1.5:
            base=rows[i]["close"]
            fut=[(rows[i+h]["close"]-base)/base*100 for h in (1,3,7,30,90)]
            sims.append(fut)
    out={"sample_count":len(sims),"condition":list(current)}
    if not sims:
        out["status"]="历史相似样本不足"
        return out
    for j,h in enumerate((1,3,7,30,90)):
        x=[s[j] for s in sims]
        out[str(h)+"d"]={
            "up_rate":round(sum(v>0 for v in x)/len(x)*100,1),
            "down_rate":round(sum(v<0 for v in x)/len(x)*100,1),
            "avg":round(statistics.mean(x),2),"median":round(statistics.median(x),2),
            "worst":round(min(x),2),"best":round(max(x),2)
        }
    # conservative risk estimate from 30/90-day outcomes
    all90=[s[4] for s in sims]
    out["max_forward_drawdown_proxy"]=round(min(all90),2)
    out["status"]="有效"
    return out

async def crypto_spot(c,s):
    t,b,e=await first_json(c,BINANCE_SPOT,"/api/v3/ticker/24hr",{"symbol":s})
    if not t: return None
    d,_,_=await first_json(c,BINANCE_SPOT,"/api/v3/depth",{"symbol":s,"limit":50})
    tr,_,_=await first_json(c,BINANCE_SPOT,"/api/v3/aggTrades",{"symbol":s,"limit":1000})
    if not d or not tr: return None
    bid=sum(float(x[0])*float(x[1]) for x in d.get("bids",[]))
    ask=sum(float(x[0])*float(x[1]) for x in d.get("asks",[]))
    buy=sum(float(x.get("q",0)) for x in tr if not x.get("m"))
    sell=sum(float(x.get("q",0)) for x in tr if x.get("m"))
    return {"price":float(t["lastPrice"]),"change24h":float(t["priceChangePercent"]),
            "volume":float(t["quoteVolume"]),"book_buy":bid/(bid+ask) if bid+ask else .5,
            "aggressive_buy":buy/(buy+sell) if buy+sell else .5,"endpoint":b,
            "updated":ts()}

async def okx_spot(c,s):
    inst=s.replace("USDT","-USDT")
    t=(await json_get(c,f"{OKX}/api/v5/market/ticker",{"instId":inst}))["data"][0]
    d=(await json_get(c,f"{OKX}/api/v5/market/books",{"instId":inst,"sz":"50"}))["data"][0]
    bid=sum(float(x[0])*float(x[1]) for x in d["bids"]); ask=sum(float(x[0])*float(x[1]) for x in d["asks"])
    p=float(t["last"]); o=float(t["open24h"])
    return {"price":p,"change24h":pct(p,o),"volume":float(t.get("volCcy24h",0)),
            "book_buy":bid/(bid+ask) if bid+ask else .5,"updated":ts()}

async def coinbase_spot(c,s):
    product=s.replace("USDT","-USD")
    t=await json_get(c,f"{COINBASE}/products/{product}/ticker")
    st=await json_get(c,f"{COINBASE}/products/{product}/stats")
    p=float(t["price"]); o=float(st["open"])
    return {"price":p,"change24h":pct(p,o),"volume":float(st.get("volume",0)),
            "updated":ts()}

async def derivatives(c,s):
    oi,ob,_=await first_json(c,BINANCE_FUT,"/fapi/v1/openInterest",{"symbol":s})
    fu,fb,_=await first_json(c,BINANCE_FUT,"/fapi/v1/premiumIndex",{"symbol":s})
    kl,kb,_=await first_json(c,BINANCE_FUT,"/fapi/v1/klines",{"symbol":s,"interval":"1d","limit":1000})
    li,lb,_=await first_json(c,BINANCE_FUT,"/fapi/v1/forceOrders",{"symbol":s,"limit":100})
    if not oi and not fu: return {"available":False}
    return {"available":True,"oi":float(oi["openInterest"]) if oi else None,
            "funding":float(fu["lastFundingRate"]) if fu else None,
            "mark":float(fu["markPrice"]) if fu else None,
            "liq_recent":len(li) if isinstance(li,list) else None,
            "klines":kl,"endpoint":ob or fb or kb,"updated":ts()}

async def historical_crypto(c,s):
    k,b,e=await first_json(c,BINANCE_SPOT,"/api/v3/klines",{"symbol":s,"interval":"1d","limit":1000})
    if not k: return None
    rows=[{"ts":int(x[0]),"close":float(x[4]),"volume":float(x[7])} for x in k]
    closes=[r["close"] for r in rows]
    def feat(r):
        i=rows.index(r)
        r1=pct(closes[i],closes[i-7]) if i>=7 else 0
        r30=pct(closes[i],closes[i-30]) if i>=30 else 0
        vol=r["volume"]
        med=statistics.median([x["volume"] for x in rows[max(0,i-20):i]]) if i else vol
        return (1 if r1>2 else -1 if r1<-2 else 0,
                1 if r30>8 else -1 if r30<-8 else 0,
                1 if vol>med*1.5 else 0)
    # attach index for speed
    def feature_at(i):
        r1=pct(closes[i],closes[i-7]) if i>=7 else 0
        r30=pct(closes[i],closes[i-30]) if i>=30 else 0
        med=statistics.median([x["volume"] for x in rows[max(0,i-20):i]]) if i else rows[i]["volume"]
        return (1 if r1>2 else -1 if r1<-2 else 0,1 if r30>8 else -1 if r30<-8 else 0,
                1 if rows[i]["volume"]>med*1.5 else 0)
    def sim():
        if len(rows)<120:return {"sample_count":0,"status":"样本不足"}
        cur=feature_at(len(rows)-1); sims=[]
        for i in range(30,len(rows)-90):
            f=feature_at(i)
            if sum(abs(a-b) for a,b in zip(cur,f))<=1:
                base=closes[i]; sims.append([(closes[i+h]-base)/base*100 for h in (1,3,7,30,90)])
        out={"sample_count":len(sims),"condition":list(cur)}
        for j,h in enumerate((1,3,7,30,90)):
            x=[z[j] for z in sims]
            if x: out[str(h)+"d"]={"up_rate":round(sum(v>0 for v in x)/len(x)*100,1),
              "down_rate":round(sum(v<0 for v in x)/len(x)*100,1),"avg":round(statistics.mean(x),2),
              "median":round(statistics.median(x),2),"worst":round(min(x),2),"best":round(max(x),2)}
        out["status"]="有效" if sims else "历史相似样本不足"; return out
    return {"rows":rows,"stats":sim(),"max_drawdown":max_drawdown(closes),"source":b}

def regime_from_asset(src,deriv,hist,macro):
    ps=[x["price"] for x in src.values() if x]
    if not ps:return {}
    p=statistics.median(ps)
    changes=[x["change24h"] for x in src.values() if x]
    buy=[]
    for x in src.values():
        if x and "book_buy" in x: buy.append(x["book_buy"])
        if x and "aggressive_buy" in x: buy.append(x["aggressive_buy"])
    buy=safe_mean(buy) or .5
    ret7=pct(p,hist["rows"][-8]["close"]) if hist and len(hist["rows"])>=8 else safe_mean(changes) or 0
    ret30=pct(p,hist["rows"][-31]["close"]) if hist and len(hist["rows"])>=31 else ret7
    oi=deriv.get("oi"); funding=deriv.get("funding")
    # directional evidence score, not a probability.
    score=0
    if ret7>2:score+=1
    if ret7<-2:score-=1
    if ret30>8:score+=2
    if ret30<-8:score-=2
    if buy>.53:score+=1
    if buy<.47:score-=1
    if funding is not None:
        if funding>.0005: score-=1
        elif funding<-.0005: score+=1
    if macro.get("10Y",{}).get("value") is not None and macro["10Y"]["value"]<4.5: score+=0.5
    mid="中期偏多" if score>=2 else "中期偏空" if score<=-2 else "中期震荡/观察"
    short="短期偏强" if safe_mean(changes)>0.5 else "短期偏弱" if safe_mean(changes)<-0.5 else "短期震荡"
    combo=("价格↑ + OI↑" if ret7>0 and oi is not None else "价格↓ + OI↑" if ret7<0 and oi is not None else "价格行为与OI待验证")
    return {"price":p,"change24h":round(safe_mean(changes) or 0,2),"buy_power":round(buy*100,1),
            "ret7":round(ret7,2),"ret30":round(ret30,2),"short":short,"mid":mid,
            "stage":"趋势形成/延续" if abs(ret30)>8 else "整理/待确认",
            "score":score,"derivatives_combo":combo,
            "funding":funding,"oi":oi,
            "reversal_risk":"较高" if (ret7*ret30<0) else "中等" if abs(ret7)>4 else "中低"}

async def fetch_farside(c):
    try:
        html=await text_get(c,FARSIDE,timeout=20)
        rows=[]
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>",html,re.S|re.I):
            cells=[re.sub(r"<[^>]+>","",x).strip() for x in re.findall(r"<td[^>]*>(.*?)</td>",tr,re.S|re.I)]
            if len(cells)>=3 and re.match(r"\d{2} \w{3} \d{4}",cells[0]):
                total=cells[-1].replace(",","").replace("(","-").replace(")","")
                try: rows.append({"date":cells[0],"total":float(total)})
                except: pass
        rows=rows[-90:]
        if not rows:return {"available":False,"status":"暂缺","source":"Farside"}
        return {"available":True,"latest":rows[-1],"history":rows,"source":"Farside"}
    except Exception as e:return {"available":False,"status":"异常","error":str(e),"source":"Farside"}

async def fred(c,series):
    try:
        txt=await text_get(c,FRED,{"id":series},timeout=15)
        rd=csv.DictReader(io.StringIO(txt))
        vals=[r for r in rd if r.get(series) not in (None,".","")]
        if not vals:return None
        v=float(vals[-1][series])
        return {"value":v,"date":vals[-1]["observation_date"],"source":"FRED"}
    except:return None

async def treasury_yield(c):
    try:
        y=str(datetime.now(timezone.utc).year)
        txt=await text_get(c,TREASURY,{"data":"daily_treasury_yield_curve","field_tdr_date_value":y})
        root=ET.fromstring(txt)
        row=None
        for el in root.iter():
            if el.tag.lower().endswith("entry"):
                row=el
        if row is None:return {"available":False}
        d={}
        for child in row.iter():
            tag=child.tag.split("}")[-1].lower()
            if tag in ("dtyear","dtmonth","dtday","tenthirtyyear","twoyear"): d[tag]=child.text
        return {"available":True,"date":f"{d.get('dtyear')}-{d.get('dtmonth')}-{d.get('dtday')}",
                "2Y":d.get("twoyear"),"10Y":d.get("tenthirtyyear"),"source":"US Treasury XML"}
    except Exception as e:return {"available":False,"error":str(e)}

async def macro(c):
    specs={"FEDFUNDS":"Fed Funds","DGS2":"2Y","DGS10":"10Y","CPIAUCSL":"CPI",
           "PCEPILFE":"Core PCE","UNRATE":"Unemployment","PAYEMS":"Payrolls","DTWEXBGS":"Dollar"}
    vals=await asyncio.gather(*(fred(c,sid) for sid in specs), return_exceptions=True)
    out={}
    for (sid,name),v in zip(specs.items(),vals):
        if isinstance(v,dict): out[name]=v
    if "2Y" in out and "10Y" in out:
        out["10Y-2Y"]={"value":round(out["10Y"]["value"]-out["2Y"]["value"],3),"source":"FRED"}
    out["_treasury"]=await treasury_yield(c)
    return out

async def sec_ticker_map(c):
    if cache.get("sec_tickers"):return cache["sec_tickers"]
    try:
        x=await json_get(c,SEC_TICKERS,headers={"User-Agent":UA},timeout=20)
        mp={v["ticker"].upper():str(v["cik_str"]).zfill(10) for v in x.values()}
        cache["sec_tickers"]=mp;return mp
    except:return {}

async def sec_fundamentals(c,ticker):
    mp=await sec_ticker_map(c); cik=mp.get(ticker)
    if not cik:return {"available":False,"status":"暂缺"}
    try:
        x=await json_get(c,f"{SEC}/api/xbrl/companyfacts/CIK{cik}.json",headers={"User-Agent":UA},timeout=20)
        facts=x.get("facts",{}).get("us-gaap",{})
        def last(tags):
            for tag in tags:
                if tag in facts:
                    units=facts[tag].get("units",{})
                    arr=next(iter(units.values()),[])
                    if arr:
                        return arr[-1].get("val"),arr[-1].get("fy"),arr[-1].get("fp")
            return None,None,None
        rev=last(["RevenueFromContractWithCustomerExcludingAssessedTax","Revenues"])
        net=last(["NetIncomeLoss"])
        gross=last(["GrossProfit"])
        cash=last(["CashAndCashEquivalentsAtCarryingValue"])
        debt=last(["LongTermDebtNoncurrent","LongTermDebtCurrent"])
        return {"available":True,"source":"SEC Company Facts",
                "revenue":rev[0],"net_income":net[0],"gross_profit":gross[0],
                "cash":cash[0],"debt":debt[0],"last_fy":rev[1]}
    except Exception as e:return {"available":False,"status":"异常","error":str(e)}

async def stock_history(c,ticker):
    # Stooq is a public historical-price fallback. No claim of real-time.
    try:
        txt=await text_get(c,STOOQ,{"s":ticker.lower()+".us","d1":"20100101","d2":datetime.now(timezone.utc).strftime("%Y%m%d"),"i":"d"},timeout=20)
        rd=csv.DictReader(io.StringIO(txt)); rows=[]
        for r in rd:
            try: rows.append({"date":r["Date"],"close":float(r["Close"]),"volume":float(r["Volume"])})
            except: pass
        if len(rows)<100:return None
        return {"rows":rows,"source":"Stooq"}
    except:return None

def stock_stat(rows):
    closes=[r["close"] for r in rows]
    def sim():
        if len(rows)<180:return {"sample_count":0,"status":"样本不足"}
        def feat(i):
            r7=pct(closes[i],closes[i-7]);r30=pct(closes[i],closes[i-30])
            med=statistics.median([r["volume"] for r in rows[i-20:i]]) if i>=20 else rows[i]["volume"]
            return (1 if r7>2 else -1 if r7<-2 else 0,1 if r30>8 else -1 if r30<-8 else 0,
                    1 if rows[i]["volume"]>med*1.5 else 0)
        cur=feat(len(rows)-1); sims=[]
        for i in range(30,len(rows)-90):
            if sum(abs(a-b) for a,b in zip(cur,feat(i)))<=1:
                base=closes[i];sims.append([(closes[i+h]-base)/base*100 for h in (1,3,7,30,90)])
        out={"sample_count":len(sims),"condition":list(cur)}
        for j,h in enumerate((1,3,7,30,90)):
            x=[s[j] for s in sims]
            if x:out[str(h)+"d"]={"up_rate":round(sum(v>0 for v in x)/len(x)*100,1),
                "down_rate":round(sum(v<0 for v in x)/len(x)*100,1),"avg":round(statistics.mean(x),2),
                "median":round(statistics.median(x),2),"worst":round(min(x),2),"best":round(max(x),2)}
        out["status"]="有效" if sims else "历史相似样本不足";return out
    return {"stats":sim(),"max_drawdown":max_drawdown(closes),"last":closes[-1],
            "ret7":pct(closes[-1],closes[-8]),"ret30":pct(closes[-1],closes[-31])}

async def scan_stocks(c,macro):
    sem=asyncio.Semaphore(8)
    async def one(ticker):
        async with sem:
            try:
                h=await stock_history(c,ticker)
                if not h:return None
                st=stock_stat(h["rows"])
                score=(st["ret30"] or 0)*0.7+(st["ret7"] or 0)*0.3
                sim=st["stats"]
                if sim.get("7d"):score+=(sim["7d"]["up_rate"]-50)*0.08
                return {"ticker":ticker,"price":st["last"],"ret7":round(st["ret7"],2),
                        "ret30":round(st["ret30"],2),"score":round(score,2),
                        "stats":sim,"max_drawdown":st["max_drawdown"]}
            except Exception:
                return None
    out=[x for x in await asyncio.gather(*(one(t) for t in STOCKS)) if x]
    out.sort(key=lambda x:x["score"],reverse=True)
    top=out[:3]
    secvals=await asyncio.gather(*(sec_fundamentals(c,a["ticker"]) for a in top), return_exceptions=True)
    for a,s in zip(top,secvals): a["sec"]=s if isinstance(s,dict) else {"available":False,"status":"异常"}
    return top

async def scan_asset(c,s):
    src={}
    for name,fn in (("binance",crypto_spot),("okx",okx_spot),("coinbase",coinbase_spot)):
        try:src[name]=await fn(c,s)
        except Exception as e:src[name]=None
    der=await derivatives(c,s)
    hist=await historical_crypto(c,s)
    macro_short={"10Y":macro.get("10Y",{})}
    regime=regime_from_asset(src,der,hist or {"rows":[]},macro_short)
    return {"symbol":s,"sources":src,"derivatives":{k:v for k,v in der.items() if k!="klines"},
            "history":hist or {},"regime":regime,
            "evidence":build_crypto_evidence(src,der,macro, hist),
            "completeness":sum(bool(v) for v in src.values())}

def build_crypto_evidence(src,der,macro,hist):
    e=[]; counter=[]
    vals=[v for v in src.values() if v]
    if vals:
        bp=safe_mean([v.get("aggressive_buy",v.get("book_buy",.5)) for v in vals])
        e.append({"name":"多交易所现货行为","direction":"支持买方" if bp>.52 else "支持卖方" if bp<.48 else "中性",
                  "detail":f"综合主动成交/盘口买方占比 {bp*100:.1f}%","independence":"交易所市场层"})
        disp=(max(v["price"] for v in vals)-min(v["price"] for v in vals))/statistics.median([v["price"] for v in vals])*100
        e.append({"name":"跨所价格一致性","direction":"一致" if disp<0.15 else "分歧","detail":f"价格离散 {disp:.3f}%","independence":"跨交易所"})
    if der.get("funding") is not None:
        f=der["funding"]; direction="反方压力" if f>.0005 else "支持多方" if f<-.0005 else "中性"
        (counter if f>.0005 else e).append({"name":"Funding","direction":direction,"detail":f"{f*100:.4f}%","independence":"衍生品"})
    if der.get("oi") is not None:
        e.append({"name":"OI","direction":"已接入","detail":str(der["oi"]),"independence":"衍生品"})
    if macro.get("10Y",{}).get("value") is not None:
        e.append({"name":"10Y","direction":"宏观证据","detail":str(macro["10Y"]["value"]),"independence":"宏观"})
    return {"pro":e,"counter":counter}

def fuse(asset,etf):
    r=asset.get("regime",{}); pro=asset.get("evidence",{}).get("pro",[]); counter=asset.get("evidence",{}).get("counter",[])
    if asset.get("symbol")=="BTCUSDT" and etf.get("available"):
        x=etf["latest"]["total"]
        item={"name":"BTC ETF净流量","direction":"支持买方" if x>0 else "反方压力" if x<0 else "中性",
              "detail":f"{x:+.1f} US$m","independence":"机构需求"}
        (pro if x>0 else counter).append(item)
    return {"pro":pro,"counter":counter,"net":len(pro)-len(counter)}

async def full_refresh():
    async with httpx.AsyncClient(headers={"User-Agent":UA}) as c:
        m=await macro(c)
        etf=await fetch_farside(c)
        sem=asyncio.Semaphore(5)
        async def one_crypto(s):
            async with sem:
                try:return s,await scan_asset(c,s)
                except Exception as e:return s,{"symbol":s,"error":str(e),"completeness":0}
        pairs=await asyncio.gather(*(one_crypto(s) for s in CRYPTO))
        crypto=dict(pairs)
        stocks=await scan_stocks(c,m)
        for a in crypto.values():
            if "error" not in a:a["fusion"]=fuse(a,etf)
        crypto_list=sorted([a for a in crypto.values() if "regime" in a],
                           key=lambda a:a["regime"].get("score",0),reverse=True)
        state["assets"]={a["symbol"]:a for a in crypto_list}
        state["crypto_rank"]=[a["symbol"] for a in crypto_list]
        state["stock_rank"]=stocks
        state["macro"]=m;state["etf"]=etf;state["updated"]=ts()
        state["sources"]={
            "Binance":any(a.get("sources",{}).get("binance") for a in crypto.values()),
            "OKX":any(a.get("sources",{}).get("okx") for a in crypto.values()),
            "Coinbase":any(a.get("sources",{}).get("coinbase") for a in crypto.values()),
            "BTC ETF / Farside":bool(etf.get("available")),
            "FRED/Fed":bool(m.get("Fed Funds")),
            "Treasury":bool(m.get("_treasury",{}).get("available")),
            "SEC":any(a.get("sec",{}).get("available") for a in stocks),
            "US historical prices":bool(stocks),
        }

async def loop():
    while True:
        try:await full_refresh()
        except Exception as e:state["notes"].append({"time":ts(),"error":str(e)})
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    asyncio.create_task(loop())

@app.get("/health")
async def health():
    return {"status":"ok","updated":state["updated"],"sources":state["sources"]}

@app.get("/api/state")
async def api_state():
    return JSONResponse(state)

@app.get("/api/search")
async def search(symbol: str = Query(..., min_length=1)):
    q=symbol.upper().replace("-","").replace("/","")
    if q in state["assets"]:return state["assets"][q]
    ticker=q if q.endswith("USDT") else q+"USDT"
    if ticker in CRYPTO:return state["assets"].get(ticker,{"status":"等待下一次扫描"})
    stock=q
    for a in state["stock_rank"]:
        if a["ticker"]==stock:return a
    return {"status":"当前扫描池尚未覆盖该资产","requested":symbol}

HTML = r'''<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🔦 Lighthouse 最终版</title>
<style>
:root{--bg:#07111f;--card:#0d1a2b;--line:#223650;--txt:#edf5ff;--muted:#91a6bf;--good:#43df91;--warn:#ffd166;--bad:#ff687b;--blue:#5aa9ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:system-ui,-apple-system,"Segoe UI","Noto Sans SC",sans-serif}
main{max-width:920px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;gap:10px;align-items:center}.brand{font-size:23px;font-weight:850}.sub{font-size:11px;color:var(--muted)}
button,input{border:1px solid var(--line);background:#10243b;color:var(--txt);border-radius:11px;padding:9px}input{width:62%}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px;margin-top:10px}.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.wide{grid-column:1/-1}
.status{padding:7px 10px;border:1px solid var(--line);border-radius:999px;font-size:11px}.muted{color:var(--muted)}.good{color:var(--good)}.warn{color:var(--warn)}.bad{color:var(--bad)}
.price{font-size:27px;font-weight:850}.row{display:flex;justify-content:space-between;gap:12px;padding:7px 0;border-bottom:1px solid #20324a;font-size:12px}.row:last-child{border:0}
.rank{padding:10px 0;border-bottom:1px solid #20324a}.rank:last-child{border:0}.tag{font-size:11px;background:#172941;border-radius:8px;padding:3px 6px;color:#bfd1e8}
h2{font-size:16px;margin:0 0 8px}.small{font-size:11px;color:var(--muted);line-height:1.5}ul{margin:6px 0;padding-left:18px}li{margin:5px 0;font-size:12px;line-height:1.45}
@media(max-width:650px){.grid{grid-template-columns:1fr}.wide{grid-column:auto}}
</style></head><body><main>
<div class="top"><div><div class="brand">🔦 灯塔 Lighthouse</div><div class="sub">证据先行 · 中期为主 · 统计验证 · 不造数据</div></div><div id="status" class="status">启动扫描</div></div>
<div class="card"><div style="display:flex;gap:7px"><input id="q" placeholder="输入 BTC / ETH / NVDA / AMD…"><button onclick="searchA()">搜索</button></div><div class="small">搜索接口只返回已有公开数据；未覆盖资产不会假装已经分析。</div></div>
<div class="card"><b>数据完整度</b><div id="sources"></div><div class="small">3/3交易所正常才进行完整多所融合；1/3仅事实观察；0/3停止方向判断。</div></div>
<div class="grid"><section class="card"><h2>🪙 加密 Top 3</h2><div id="crypto"></div></section>
<section class="card"><h2>📈 美股 Top 3</h2><div id="stocks"></div></section></div>
<div id="detail"></div>
<div class="card"><h2>宏观环境</h2><div id="macro"></div></div>
<div class="card"><h2>BTC ETF 机构需求</h2><div id="etf"></div></div>
<div class="card"><h2>证据规则</h2><div class="small">原始数据 → 证据砖 → 反方证据 → 独立性/持续性 → 历史相似条件 → 1/3/7/30/90日统计 → 融合。专业MVRV/LTH、机构订单流、暗池等拿不到的项目不冒充原始数据。</div></div>
</main>
<script>
const $=id=>document.getElementById(id), esc=s=>String(s??"—").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
function row(a,b){return `<div class="row"><span>${esc(a)}</span><b>${esc(b)}</b></div>`}
function statBlock(s){if(!s||!s.sample_count)return '<div class="small">历史相似样本不足</div>';return ['1d','3d','7d','30d','90d'].map(k=>s[k]?row(k.toUpperCase(),`↑ ${s[k].up_rate}% / ↓ ${s[k].down_rate}% · 中位 ${s[k].median}%`):'').join('')}
function render(x){
 const n=Object.values(x.sources||{}).filter(Boolean).length;
 $('status').textContent='最近更新 '+(x.updated?new Date(x.updated).toLocaleTimeString():'—');
 $('sources').innerHTML=Object.entries(x.sources||{}).map(([k,v])=>row(k,v?'可用':'暂缺')).join('');
 $('crypto').innerHTML=(x.crypto_rank||[]).slice(0,3).map((s,i)=>{let a=x.assets[s],r=a.regime||{},st=a.history?.stats||{};return `<div class="rank"><b>${i+1}. ${esc(s.replace('USDT',''))}</b> <span class="tag">${esc(r.mid)}</span>${row('价格','$'+Number(r.price||0).toLocaleString())}${row('短期',r.short)}${row('阶段',r.stage)}${row('证据优势',r.score)}${row('历史样本',st.sample_count||0)}</div>`}).join('');
 $('stocks').innerHTML=(x.stock_rank||[]).map((a,i)=>`<div class="rank"><b>${i+1}. ${esc(a.ticker)}</b> <span class="tag">历史证据排序</span>${row('价格','$'+Number(a.price||0).toLocaleString())}${row('7日',a.ret7+'%')}${row('30日',a.ret30+'%')}${row('历史样本',a.stats?.sample_count||0)}${a.sec?.available?row('SEC','已接入'):row('SEC','暂缺')}</div>`).join('');
 $('macro').innerHTML=Object.entries(x.macro||{}).filter(([k])=>k!=='_treasury').map(([k,v])=>row(k,v?.value??'暂缺')).join('');
 const e=x.etf||{};$('etf').innerHTML=e.available?row('最近日',`${e.latest.date} · ${e.latest.total>0?'+':''}${e.latest.total} US$m`)+row('数据源','Farside'):row('状态','暂缺/异常');
}
async function load(){try{render(await fetch('/api/state',{cache:'no-store'}).then(r=>r.json()))}catch(e){$('status').textContent='连接异常'}}
async function searchA(){let q=$('q').value.trim();if(!q)return;let d=await fetch('/api/search?symbol='+encodeURIComponent(q)).then(r=>r.json());$('detail').innerHTML='<div class="card"><h2>'+esc(q.toUpperCase())+'</h2>'+row('状态',d.status||'已找到')+(d.regime?row('中期',d.regime.mid)+row('短期',d.regime.short)+row('数据完整度',d.completeness+'/3')+row('统计样本',d.history?.stats?.sample_count||0)+row('最大回撤',d.history?.max_drawdown+'%'):'')+(d.stats?statBlock(d.stats):'')+(d.fusion?'<h3>正方证据</h3><ul>'+d.fusion.pro.map(z=>'<li>'+esc(z.name)+'：'+esc(z.detail)+'</li>').join('')+'</ul><h3>反方证据</h3><ul>'+d.fusion.counter.map(z=>'<li>'+esc(z.name)+'：'+esc(z.detail)+'</li>').join('')+'</ul>':'')+'</div>'}
load();setInterval(load,30000);
</script></body></html>'''

@app.get("/",response_class=HTMLResponse)
async def root(): return HTMLResponse(HTML)
