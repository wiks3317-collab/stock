"""Cloud Run 轉接服務：/quote?keys=TW2330,USNVDA（盤中報價）、/stock?key=TW2330&name=台積電（K線+股利+公司資訊+新聞）"""
import os, re, time
import pandas as pd, yfinance as yf
from flask import Flask, jsonify, request
from fetch_data import download, gnews, r, vol

app = Flask(__name__)
ALLOW = os.environ.get("ALLOW_ORIGIN", "*")   # 例如 https://帳號.github.io
KEY = re.compile(r"^(TW|US)[0-9A-Za-z\-]{1,10}$")
CACHE, SUF = {}, {}

def cached(key, ttl, fn):
    hit = CACHE.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    v = fn()
    if len(CACHE) > 1500:
        CACHE.clear()
    CACHE[key] = (time.time(), v)
    return v

@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = ALLOW
    return resp

def live(keys):
    ys = {k: k[2:] + SUF.get(k, ".TW") if k[:2] == "TW" else k[2:] for k in keys}
    got = download(list(ys.values()), "5d")
    miss = [k for k in keys if k[:2] == "TW" and ys[k] not in got]
    if miss:  # 上櫃改試 .TWO
        got.update(download([k[2:] + ".TWO" for k in miss], "5d"))
        for k in miss:
            SUF[k] = ".TWO"; ys[k] = k[2:] + ".TWO"
    data = {}
    for k in keys:
        d = got.get(ys[k])
        if d is None:
            continue
        x = d.iloc[-1]
        data[k] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High), "l": r(x.Low),
                   "p": r(x.Close), "v": vol(k[:2], x.Volume)}
    return {"data": data}

@app.get("/quote")
def quote():
    keys = sorted({k for k in request.args.get("keys", "").split(",") if KEY.match(k)})[:30]
    if not keys:
        return jsonify(error="no keys"), 400
    resp = jsonify(cached("q" + ",".join(keys), 15, lambda: live(keys)))
    resp.headers["Cache-Control"] = "public, max-age=10"
    return resp

def detail(key, name):
    m, c = key[:2], key[2:]
    for suf in ([".TW", ".TWO"] if m == "TW" else [""]):
        t = yf.Ticker(c + suf)
        h = t.history(period="8mo", interval="1d", auto_adjust=False).dropna(subset=["Open", "High", "Low", "Close"])
        if len(h):
            break
    else:
        return {"k": []}
    out = {"k": [[i.strftime("%m-%d"), r(x.Open), r(x.High), r(x.Low), r(x.Close), vol(m, x.Volume)]
                 for i, x in h.tail(130).iterrows()], "div": [], "ttm": 0, "info": {}}
    try:
        dv = t.dividends
        if len(dv):
            out["div"] = [[i.strftime("%Y-%m-%d"), float(x)] for i, x in dv.items()][::-1][:8]
            now = pd.Timestamp.now(tz=dv.index.tz) if dv.index.tz else pd.Timestamp.now()
            out["ttm"] = round(float(dv[dv.index >= now - pd.Timedelta(days=365)].sum()), 4)
    except Exception:
        pass
    try:
        i = t.info or {}
        keys = {"sector": "sector", "industry": "industry", "cap": "marketCap", "pe": "trailingPE", "eps": "trailingEps",
                "hi": "fiftyTwoWeekHigh", "lo": "fiftyTwoWeekLow", "emp": "fullTimeEmployees", "web": "website"}
        out["info"] = {k: i[y] for k, y in keys.items() if i.get(y) is not None}
        if i.get("longBusinessSummary"):
            out["info"]["sum"] = i["longBusinessSummary"][:240] + "…"
    except Exception:
        pass
    out["news"] = gnews(m, c, name or c)
    return out

@app.get("/stock")
def stock():
    key, name = request.args.get("key", ""), request.args.get("name", "")[:40]
    if not KEY.match(key):
        return jsonify(error="bad key"), 400
    return jsonify(cached("s" + key, 600, lambda: detail(key, name)))

@app.get("/")
def health():
    return "ok"
