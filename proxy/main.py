"""Cloud Run 轉接服務：/quote?keys=TW2330,USNVDA（盤中報價）、/stock?key=TW2330&name=台積電（K線+股利+公司資訊+新聞）"""
import gc, json, os, re, signal, threading, time, datetime as dt, urllib.parse, urllib.request, xml.etree.ElementTree as ET, difflib, email.utils
import pandas as pd, yfinance as yf
from flask import Flask, jsonify, request
from fetch_data import download, extra_symbols, gnews, r, vol

app = Flask(__name__)
ALLOW = os.environ.get("ALLOW_ORIGIN", "*")   # 例如 https://帳號.github.io
KEY = re.compile(r"^(TW|US)[0-9A-Za-z\-]{1,10}$")
CACHE, SUF = {}, {}

def cached(key, ttl, fn):
    hit = CACHE.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    v = fn()
    if len(CACHE) >= 300:   # 個股詳情每筆可達數十 KB：先清掉過期的，還是太多就丟掉最舊的一半
        now = time.time()
        for k in [k for k, h in CACHE.items() if now - h[0] >= h[2]]:
            del CACHE[k]
        if len(CACHE) >= 300:
            for k in sorted(CACHE, key=lambda k: CACHE[k][0])[:150]:
                del CACHE[k]
    CACHE[key] = (time.time(), v, ttl)
    return v

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36"}
TZ = dt.timezone(dt.timedelta(hours=8))

def now_tw():
    return dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def json_url(url, params=None, timeout=10):
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={**UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        import json
        return json.loads(r.read().decode("utf-8"))

def compare_quote(primary, secondary, primary_name="Yahoo Finance", secondary_name="第二來源"):
    if not primary or not secondary:
        return {"status": "無法比對", "message": f"{secondary_name} 暫無可用資料"}
    pp, sp = primary.get("p"), secondary.get("p")
    if pp in (None, 0) or sp in (None, 0):
        return {"status": "無法比對", "message": "缺少價格"}
    pdte, sdte = primary.get("date"), secondary.get("date")
    if pdte and sdte and pdte != sdte:
        return {"status": "時間不同", "message": f"{primary_name}={pdte}；{secondary_name}={sdte}，不直接判定差異"}
    diff = abs(float(pp) - float(sp)) / abs(float(sp)) * 100
    status = "一致" if diff <= 0.30 else "輕微差異" if diff <= 1.00 else "明顯差異"
    return {"status": status, "diff_pct": round(diff, 3),
            "message": f"{primary_name} {pp}；{secondary_name} {sp}；差異 {diff:.3f}%"}

def official_tw_quote(code):
    endpoints = [
        ("TWSE 官方", "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
         lambda d: str(d.get("Code")) == str(code),
         lambda d: (d.get("ClosingPrice"), d.get("TradeVolume"))),
        ("TPEx 官方", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
         lambda d: str(d.get("SecuritiesCompanyCode")) == str(code),
         lambda d: (d.get("Close"), d.get("TradingShares"))),
    ]
    for name, url, match, vals in endpoints:
        try:
            rows = json_url(url, timeout=12)
            row = next((x for x in rows if match(x)), None)
            if row:
                p, v = vals(row)
                if p not in (None, ""):
                    p = float(str(p).replace(",", ""))
                    return {"p": p, "v": float(str(v).replace(",", "")) if v not in (None, "") else None,
                            "date": dt.datetime.now(TZ).strftime("%Y-%m-%d"),
                            "source": name, "time": now_tw()}
        except Exception:
            continue
    return None

def stooq_quote(symbol):
    try:
        u = "https://stooq.com/q/d/l/?" + urllib.parse.urlencode({"s": symbol.lower()+".us", "i": "d"})
        req = urllib.request.Request(u, headers=UA)
        text = urllib.request.urlopen(req, timeout=10).read().decode("utf-8")
        lines = [x.strip() for x in text.splitlines() if x.strip()]
        if len(lines) < 2: return None
        h, row = lines[0].split(","), lines[-1].split(",")
        d = dict(zip(h, row))
        if not d.get("Close"): return None
        return {"p": float(d["Close"]), "v": float(d.get("Volume") or 0), "date": d.get("Date"),
                "source": "Stooq", "time": now_tw()}
    except Exception:
        return None

def yahoo_news(symbol, region="US", lang="en-US"):
    url = "https://feeds.finance.yahoo.com/rss/2.0/headline?" + urllib.parse.urlencode(
        {"s": symbol, "region": region, "lang": lang})
    try:
        req = urllib.request.Request(url, headers=UA)
        root = ET.fromstring(urllib.request.urlopen(req, timeout=15).read())
    except Exception:
        return []
    out = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        if not title: continue
        try:
            d = email.utils.parsedate_to_datetime(it.findtext("pubDate"))
            ts, disp = int(d.timestamp()), d.astimezone(TZ).strftime("%m-%d %H:%M")
        except Exception:
            ts, disp = 0, ""
        out.append({"t": title, "s": it.findtext("source") or "Yahoo Finance",
                    "u": it.findtext("link") or "", "ts": ts, "d": disp,
                    "provider": "Yahoo Finance"})
    out.sort(key=lambda x: -x["ts"])
    return out[:8]

def merge_news(*groups):
    all_items = [x for g in groups for x in (g or [])]
    all_items.sort(key=lambda x: -x.get("ts", 0))
    merged = []
    for x in all_items:
        found = None
        for y in merged:
            sim = difflib.SequenceMatcher(None, x.get("t","").lower(), y.get("t","").lower()).ratio()
            if sim >= 0.78 and abs(x.get("ts",0)-y.get("ts",0)) <= 48*3600:
                found = y; break
        if found:
            p = x.get("provider") or x.get("s") or "未知"
            srcs = found.setdefault("srcs", [found.get("provider") or found.get("s") or "未知"])
            if p not in srcs: srcs.append(p)
        else:
            z = dict(x); z["srcs"] = [x.get("provider") or x.get("s") or "未知"]; merged.append(z)
    return merged[:10]

START = time.time()
RSS_LIMIT_MB = int(os.environ.get("RSS_LIMIT_MB", 300))            # 常駐記憶體超過這個值，就讓 worker 優雅重啟（Cloud Run 預設上限 512 MiB）
RECYCLE_MIN_UPTIME = int(os.environ.get("RECYCLE_MIN_UPTIME", 600))  # 啟動後至少這麼久才允許重啟，避免設定太低造成連續重啟
WD = {"t": 0.0, "rss": 0.0}

def rss_mb():
    try:
        for line in open("/proc/self/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0

def trim_memory():
    """把 Python 已經不用、但 glibc 還沒還給系統的記憶體釋放掉（多執行緒服務常見的「只增不減」）。"""
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass

@app.after_request
def memory_watchdog(resp):
    now = time.time()
    if now - WD["t"] >= 60:   # 最多每分鐘整理 / 檢查一次，成本很低
        WD["t"] = now
        trim_memory()
        WD["rss"] = rss_mb()
        if (RSS_LIMIT_MB > 0 and WD["rss"] > RSS_LIMIT_MB and now - START > RECYCLE_MIN_UPTIME
                and request.environ.get("SERVER_SOFTWARE", "").startswith("gunicorn")):
            print(f"記憶體 {WD['rss']:.0f} MB 超過 {RSS_LIMIT_MB} MB，worker 優雅重啟（先處理完進行中的請求）", flush=True)
            WD["t"] = now + 300
            threading.Timer(2, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()   # 等這次回應送出後再重啟
    return resp

@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = ALLOW
    return resp

def yahoo_live(keys):
    ys = {k: k[2:] + SUF.get(k, ".TW") if k[:2] == "TW" else k[2:] for k in keys}
    got = download(list(ys.values()), "5d")
    miss = [k for k in keys if k[:2] == "TW" and ys[k] not in got]
    if miss:
        got.update(download([k[2:] + ".TWO" for k in miss], "5d"))
        for k in miss:
            if k[2:] + ".TWO" in got:
                SUF[k] = ".TWO"; ys[k] = k[2:] + ".TWO"
    data = {}
    for k in keys:
        d = got.get(ys[k])
        if d is None: continue
        x = d.iloc[-1]; date = d.index[-1].strftime("%Y-%m-%d"); pc = r(d.iloc[-2].Close) if len(d) > 1 else None
        data[k] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High),
                   "l": r(x.Low), "p": r(x.Close), "v": vol(k[:2], x.Volume), "date": date, "pc": pc}
    return data

def live(keys, cmp=()):
    checked = now_tw()
    ydata = yahoo_live(keys)
    data = {}
    for k, y in ydata.items():
        secondary = (official_tw_quote(k[2:]) if k[:2] == "TW" else stooq_quote(k[2:])) if k in cmp else None  # 第二來源很慢，只在開啟個股時才查
        y["sources"] = [{"name": "Yahoo Finance", "role": "主要", "time": checked}]
        if secondary:
            y["sources"].append({"name": secondary["source"], "role": "比對", "time": secondary["time"],
                                 "date": secondary.get("date")})
        if k not in cmp:
            data[k] = y; continue
        y["comparison"] = compare_quote({"p": y["p"], "date": y["date"]}, secondary,
                                        "Yahoo Finance", secondary.get("source", "第二來源") if secondary else "第二來源")
        data[k] = y
    return {"data": data, "updated": checked,
            "source_policy": "Yahoo Finance 為主要來源；第二來源僅作交叉檢查"}


QC = {}  # 逐檔報價快取：開盤中 15 秒、休市 10 分鐘，不同畫面重複查同一檔不會再打 Yahoo

def mkt_open(m):
    try:
        from zoneinfo import ZoneInfo
        n = dt.datetime.now(ZoneInfo("Asia/Taipei" if m == "TW" else "America/New_York"))
        a, b = (535, 815) if m == "TW" else (565, 965)
        return n.weekday() < 5 and a <= n.hour * 60 + n.minute <= b
    except Exception:
        return True

def live_cached(keys, cmp=(), maxage=None):
    now, out, need = time.time(), {}, []
    for k in keys:
        h = QC.get(k)
        if h and k not in cmp and now - h[0] < (maxage if maxage is not None else 15 if mkt_open(k[:2]) else 600):
            out[k] = h[1]
        else:
            need.append(k)
    if need:
        res = live(need, cmp)
        if len(QC) > 3000:
            QC.clear()
        for k, v in res["data"].items():
            QC[k] = (now, v); out[k] = v
    times = [v["sources"][0]["time"] for v in out.values() if v.get("sources")]
    return {"data": out, "updated": max(times) if times else now_tw(),
            "source_policy": "Yahoo Finance 為主要來源；第二來源僅作交叉檢查"}

LISTS = {"t": 0, "keys": {}}

def prefetch_keys(m):
    """預抓名單 = 精選名單（history.json 的鍵）+ 各分類成交金額前幾名（universe.json）。資料來自 GitHub 的 data 分支。"""
    if time.time() - LISTS["t"] > 1800:
        base = os.environ["DATA_BASE"].rstrip("/")
        get = lambda f: json.load(urllib.request.urlopen(urllib.request.Request(f"{base}/{f}", headers={"User-Agent": "stock-proxy"}), timeout=30))["data"]
        h = get("history.json")
        keys = {mm: {k for k in h if k.startswith(mm)} for mm in ("TW", "US")}
        del h   # 兩個大檔不要同時留在記憶體
        u = get("universe.json")
        for mm, c, _ in extra_symbols(u):
            keys[mm].add(mm + c)
        del u
        LISTS["keys"], LISTS["t"] = keys, time.time()
    return sorted(LISTS["keys"][m])

SNAP = {"t": 0, "ts": {}}   # GitHub 排程資料（live.json）每檔報價的抓取時間（epoch 秒），每 2 分鐘重新載入一次

def snap_times():
    """讀 GitHub 上 live.json 每檔的抓取時間，讓預抓可以跳過 GitHub 已經夠新的股票（資源優先放在 GitHub，Cloud Run 只補缺口）。
    載入失敗就沿用舊資料，不影響預抓（最差只是退回「全部都當作過期」）。"""
    if time.time() - SNAP["t"] > 120:
        SNAP["t"] = time.time()   # 失敗也先記時間，避免每次請求都重試而拖慢
        try:
            base = os.environ["DATA_BASE"].rstrip("/")
            req = urllib.request.Request(f"{base}/live.json?t={int(time.time() // 60)}", headers={"User-Agent": "stock-proxy"})
            d = json.load(urllib.request.urlopen(req, timeout=10))["data"]
            tz = dt.timezone(dt.timedelta(hours=8))
            SNAP["ts"] = {k: dt.datetime.strptime(v["ft"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz).timestamp()
                          for k, v in d.items() if v.get("ft")}
        except Exception as e:
            print("live.json 載入失敗：", repr(e), flush=True)
    return SNAP["ts"]

PF_LOG = []   # (時間, 檔數)：預抓最近 1 小時向 Yahoo 抓了幾檔
PF_MAX_PER_HOUR = int(os.environ.get("PREFETCH_MAX_KEYS_PER_HOUR", 1500))   # 每小時上限：GitHub 長時間掛掉時，避免預抓把 Cloud Run 免費額度吃光

PF_LOCK = {"TW": threading.Lock(), "US": threading.Lock()}  # 同一市場同時只跑一個預抓，避免排程重疊互相搶資源

@app.route("/prefetch", methods=["GET", "POST"])
def prefetch():
    """給外部排程（cron-job.org 等）定時呼叫：開盤中把常見股票的報價先抓進快取。
    預設是「限時滾動」模式：每次呼叫只抓最久沒更新的那幾批，超過時間預算（budget 秒）就不再開新的一批並立刻回應，
    所以單次請求一定在排程服務的 30 秒逾時內結束；名單很長也沒關係，排程每分鐘呼叫一次，會一批接一批輪流更新。
      /prefetch?m=TW                    → 預設：預算 15 秒、每批 20 檔、8 分鐘內（Cloud Run 或 GitHub 任一邊）抓過的先跳過
      /prefetch?m=TW&budget=12&batch=15 → 自訂（budget 5~25 秒、batch 5~60 檔、fresh 為「幾秒內抓過就跳過」）
      /prefetch?m=TW&part=0&parts=3     → 舊的固定分份模式（一次抓完該份，不限時）
    不使用背景執行緒，所以 Cloud Run 不需要開「CPU 一律分配」，維持請求計費。"""
    if request.headers.get("X-Prefetch-Token", "") != os.environ.get("PREFETCH_TOKEN", "\0"):
        return "forbidden", 403
    m = request.args.get("m", "")
    if m not in ("TW", "US") or not mkt_open(m):
        return jsonify(skipped=True, reason="休市或參數錯誤")
    t0 = time.time()
    try:
        keys = prefetch_keys(m)
    except Exception as e:
        return jsonify(error="名單載入失敗：" + str(e)[:150]), 502
    if "parts" in request.args:  # 舊模式
        parts = max(1, min(request.args.get("parts", 1, type=int), 50))
        part = request.args.get("part", 0, type=int)
        if not 0 <= part < parts:
            return jsonify(error="part 必須介於 0 到 parts-1"), 400
        keys = keys[part::parts]
        for i in range(0, len(keys), 60):
            live_cached(keys[i:i + 60], maxage=0)
        return jsonify(market=m, part=part, parts=parts, count=len(keys), seconds=round(time.time() - t0, 1), at=now_tw())
    budget = min(max(request.args.get("budget", 15, type=float), 5), 25)
    batch = min(max(request.args.get("batch", 20, type=int), 5), 60)
    fresh = max(request.args.get("fresh", 480, type=float), 0)
    if not PF_LOCK[m].acquire(blocking=False):
        return jsonify(busy=True, reason="上一輪還在執行")
    try:
        now = time.time()
        snap = snap_times()
        # 一檔股票的「新鮮度」= Cloud Run 快取與 GitHub 快照中較新的那個；夠新就不抓
        age = lambda k: min(now - QC[k][0] if k in QC else 1e9, now - snap[k] if k in snap else 1e9)
        todo = sorted((k for k in keys if age(k) >= fresh), key=age, reverse=True)  # 最久沒更新的先抓
        stale = len(todo)
        PF_LOG[:] = [x for x in PF_LOG if x[0] > now - 3600]
        room = PF_MAX_PER_HOUR - sum(n for _, n in PF_LOG)
        if stale and room <= 0:
            return jsonify(market=m, capped=True, stale=stale, cap_per_hour=PF_MAX_PER_HOUR, reason="已達每小時上限，等 GitHub 資料恢復")
        todo = todo[:max(room, 0)]
        done, err = 0, None
        for i in range(0, len(todo), batch):
            if time.time() - t0 > budget:   # 時間預算用完：不再開新的一批（正在跑的那批最多再花幾秒）
                break
            chunk = todo[i:i + batch]
            try:
                live_cached(chunk, maxage=0)
                done += len(chunk); PF_LOG.append((time.time(), len(chunk)))
            except Exception as e:           # Yahoo 限流或逾時：這輪先停，下一分鐘再試
                err = str(e)[:150]; break
        out = dict(market=m, total=len(keys), stale=stale, done=done, remaining=stale - done,
                   seconds=round(time.time() - t0, 1), at=now_tw(),
                   rss_mb=round(WD["rss"] or rss_mb()), uptime_min=round((time.time() - START) / 60))
        if err: out["error"] = err
        return jsonify(out)
    finally:
        PF_LOCK[m].release()

@app.get("/quote")
def quote():
    keys = sorted({k for k in request.args.get("keys", "").split(",") if KEY.match(k)})[:60]
    if not keys:
        return jsonify(error="no keys"), 400
    cmp = {k for k in request.args.get("cmp", "").split(",") if KEY.match(k)}
    maxage = request.args.get("maxage", type=int)
    resp = jsonify(live_cached(keys, cmp, None if maxage is None else max(0, min(maxage, 600))))
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
    checked = now_tw()
    google = gnews(m, c, name or c)
    ysym = c + (".TW" if m == "TW" and SUF.get(key, ".TW") == ".TW" else ".TWO" if m == "TW" else "")
    yahoo = yahoo_news(ysym, "TW" if m == "TW" else "US", "zh-TW" if m == "TW" else "en-US")
    out["news"] = merge_news(google, yahoo)
    out["sources"] = {
        "quote": {"name": "Yahoo Finance", "time": checked},
        "kline": {"name": "Yahoo Finance", "time": checked},
        "company": {"name": "Yahoo Finance", "time": checked},
        "news": [{"name": "Google 新聞", "time": checked}, {"name": "Yahoo Finance RSS", "time": checked}]
    }
    return out

@app.get("/search")
def search():
    q = request.args.get("q", "").strip()[:80]
    if not q:
        return jsonify(data=[])
    try:
        d = json_url("https://query1.finance.yahoo.com/v1/finance/search",
                     {"q": q, "quotesCount": 12, "newsCount": 0, "lang": "zh-TW", "region": "TW"}, 10)
        out = []
        for x in d.get("quotes", []):
            if x.get("quoteType") not in ("EQUITY", "ETF", "MUTUALFUND", "INDEX"): continue
            sy = str(x.get("symbol", ""))
            if sy.endswith(".TW") or sy.endswith(".TWO"):
                m, code = "TW", sy.rsplit(".", 1)[0]
            else:
                m, code = "US", sy
            out.append({"key": m+code, "m": m, "code": code,
                        "name": x.get("longname") or x.get("shortname") or code,
                        "exchange": x.get("exchange") or "", "type": x.get("quoteType")})
        return jsonify(data=out, updated=now_tw(), source="Yahoo Finance Search")
    except Exception as e:
        return jsonify(data=[], error=str(e), updated=now_tw()), 200

@app.get("/stock")
def stock():
    key, name = request.args.get("key", ""), request.args.get("name", "")[:40]
    if not KEY.match(key):
        return jsonify(error="bad key"), 400
    d = cached("s" + key, 600, lambda: detail(key, name))
    d["fetched_at"] = now_tw()
    return jsonify(d)

@app.get("/")
def health():
    return "ok"
