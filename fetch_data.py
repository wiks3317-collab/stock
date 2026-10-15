"""用法：python fetch_data.py history | live | daily | universe | session <TW|US>
  session ：盤中常駐模式（取代 Cloud Run）。一次 workflow 跑完整個盤：每 LIVE_INTERVAL 秒（預設 60）抓一次即時價量，
            每次直接推到 data 分支的 live.json；休市、假日或快到 6 小時上限時自動結束（必要時由 workflow 接力重啟）
  live    ：只更新即時價量（GitHub Actions 排程約每 5 分鐘；實際可能受 GitHub 排程延遲影響）
  history ：只更新 K 線
  daily   ：K 線 + 股利/公司資訊 + 新聞 + 全市場名單（收盤後每日）
  universe：只更新全市場名單與當日行情（台股上市/上櫃/興櫃/ETF、美股 S&P500+Nasdaq100+ETF）
股票清單直接從 stock.html 的 RAW 區塊讀取；輸出資料夾由環境變數 OUT_DIR 指定（預設 data）。"""
import datetime as dt, email.utils, io, json, os, re, sys, time
import shutil, subprocess, tempfile
import urllib.parse, urllib.request, xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import yfinance as yf

HTML, OUT = "stock.html", os.environ.get("OUT_DIR", "data")
TZ = dt.timezone(dt.timedelta(hours=8))

def symbols():
    src = open(HTML, encoding="utf-8").read()
    a = src.index("const RAW="); mid = src.index("US:{", a); end = src.index("const $=", a)
    out = []
    for mkt, seg in (("TW", src[a:mid]), ("US", src[mid:end])):
        for val in re.findall(r'":"([^"]+)"', seg):
            for item in val.split(","):
                parts = item.strip().split(" ", 1)
                if parts[0]:
                    out.append((mkt, parts[0], parts[1] if len(parts) > 1 else parts[0]))
    seen, res = set(), []
    for m, c, n in out:
        if (m, c) not in seen:
            seen.add((m, c)); res.append((m, c, n))
    return res

def _download(tickers, period):
    df = yf.download(tickers, period=period, interval="1d", group_by="ticker",
                     auto_adjust=False, threads=True, progress=False)
    res = {}
    for t in tickers:
        try:
            d = df[t].dropna(subset=["Open", "High", "Low", "Close"])
        except KeyError:
            continue
        if len(d):
            res[t] = d
    return res

def download(tickers, period):  # 一次最多 100 檔，避免一次要太多被 Yahoo 擋
    res = {}
    for i in range(0, len(tickers), 100):
        res.update(_download(tickers[i:i + 100], period))
    return res

def fetch(period, extra=()):
    syms = list({(m, c): (m, c, n) for m, c, n in symbols() + list(extra)}.values())
    ysym = {(m, c): (c + ".TW" if m == "TW" else c) for m, c, _ in syms}
    got = download(list(ysym.values()), period)
    miss = [(m, c) for (m, c), y in ysym.items() if m == "TW" and y not in got]
    if miss:  # 上櫃股票代號後綴是 .TWO
        alt = {k: k[1] + ".TWO" for k in miss}
        got.update(download(list(alt.values()), period)); ysym.update(alt)
    out = {m + c: {"m": m, "df": got[ysym[(m, c)]], "y": ysym[(m, c)]}
           for m, c, _ in syms if ysym[(m, c)] in got}
    print(f"價量：取得 {len(out)}/{len(syms)} 檔")
    if not out:
        sys.exit("沒有抓到任何資料，不更新檔案")
    return out

r = lambda x: round(float(x), 2)
vol = lambda m, v: round(float(v) / 1000) if m == "TW" else int(v)  # 台股換算成「張」
good = lambda v: bool(v) and (not isinstance(v, dict) or any(v.values()))

def write(name, data, merge=True):
    os.makedirs(OUT, exist_ok=True)
    path = f"{OUT}/{name}"
    if merge and os.path.exists(path):  # 個別股票這次失敗時，保留上次的資料
        try:
            old = json.load(open(path, encoding="utf-8"))["data"]
            old.update({k: v for k, v in data.items() if good(v)}); data = old
        except Exception:
            pass
    payload = {"updated": dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M"), "data": data}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print(f"寫入 {path}（{len(data)} 檔）")

def pmap(fn, items, workers=3):
    with ThreadPoolExecutor(workers) as ex:
        return list(ex.map(fn, items))

def save_history(res):
    data = {}
    for key, v in res.items():
        d = v["df"].tail(130)
        data[key] = [[i.strftime("%m-%d"), r(x.Open), r(x.High), r(x.Low), r(x.Close), vol(v["m"], x.Volume)]
                     for i, x in d.iterrows()]
    write("history.json", data)

TW_TOP, US_TOP = int(os.environ.get("TW_TOP", 12)), int(os.environ.get("US_TOP", 4))  # 每個分類預抓的檔數

def extra_symbols(u=None):
    """從 universe.json 挑出各分類畫面最常出現的股票（成交金額前幾名），盤中一併抓報價"""
    if u is None:
        try:
            u = json.load(open(f"{OUT}/universe.json", encoding="utf-8"))["data"]
        except Exception:
            return []
        have = {(m, c) for m, c, _ in symbols()}
    else:  # 在 Cloud Run 上呼叫：名單由呼叫端提供，沒有 stock.html
        have = set()
    out = []
    for m, n_top in (("TW", TW_TOP), ("US", US_TOP)):  # 台股每個產業前 N、美股每個 GICS 子產業前 N
        groups = {}
        for x in u.get(m, []):
            if m == "TW" and x[2] == "R":  # 興櫃 Yahoo 沒有資料
                continue
            groups.setdefault(x[3], []).append(x)
        for rows in groups.values():
            rows.sort(key=lambda x: x[7] * x[9], reverse=True)  # 成交金額
            out += [(m, x[0], x[1]) for x in rows[:n_top] if (m, x[0]) not in have]
    return out

LIVE_KEEP_DAYS = 2  # live.json 逐檔保留天數：這次沒抓的股票（例如另一個市場）沿用上次的報價，超過就剔除

def live(mk=None, keep_min=0):
    """keep_min > 0（session 模式用）：這次抓失敗的股票，若上次報價在 keep_min 分鐘內就沿用（ft 保留原本的抓取時間，不會被當成新價格）"""
    mk = mk or ("TW" if dt.datetime.now(dt.timezone.utc).hour < 8 else "US")  # 台股時段抓台股、美股時段抓美股
    extra = [e for e in extra_symbols() if e[0] == mk]
    print(f"精選名單 + 額外 {len(extra)} 檔（{mk}）")
    now = dt.datetime.now(TZ)
    ft = now.strftime("%Y-%m-%d %H:%M:%S")
    attempted = {m + c for m, c, _ in symbols() + extra}  # 這次有嘗試抓的股票
    data = {}
    for key, v in fetch("5d", extra).items():
        d = v["df"]; x = d.iloc[-1]
        data[key] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High), "l": r(x.Low),
                     "p": r(x.Close), "v": vol(v["m"], x.Volume),
                     "pc": r(d.iloc[-2].Close) if len(d) > 1 else None, "ft": ft}
    # 沒有嘗試抓的股票（另一個市場的額外名單）保留上次的報價，超過保留天數的剔除；
    # 這次嘗試抓卻失敗的不保留，避免把舊價格當成新的。
    try:
        old = json.load(open(f"{OUT}/live.json", encoding="utf-8"))["data"]
    except Exception:
        old = {}
    cutoff = (now - dt.timedelta(days=LIVE_KEEP_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    keep_cut = (now - dt.timedelta(minutes=keep_min)).strftime("%Y-%m-%d %H:%M:%S") if keep_min else "~"
    kept = 0
    for key, v in old.items():
        oft = str(v.get("ft", ""))
        if key not in data and oft >= cutoff and (key not in attempted or oft >= keep_cut):
            data[key] = v; kept += 1
    print(f"live.json：本次 {len(data) - kept} 檔 + 保留舊資料 {kept} 檔")
    write("live.json", data, merge=False)
    today = dt.datetime.now(ZoneInfo(MKT[mk][0])).strftime("%m-%d")   # 該市場當地日期，用來判斷今天有沒有開盤
    mine = [v for k, v in data.items() if k.startswith(mk) and v.get("ft") == ft]
    return {"n": len(mine), "today": sum(1 for v in mine if v.get("t") == today)}

def details(res):
    def one(kv):
        key, v = kv
        out = {"div": [], "ttm": 0, "info": {}}
        t = yf.Ticker(v["y"])
        try:
            dv = t.dividends
            if len(dv):
                out["div"] = [[i.strftime("%Y-%m-%d"), float(x)] for i, x in dv.items()][::-1][:8]
                now = pd.Timestamp.now(tz=dv.index.tz) if dv.index.tz else pd.Timestamp.now()
                out["ttm"] = round(float(dv[dv.index >= now - pd.Timedelta(days=365)].sum()), 4)
        except Exception as e:
            print("股利失敗", key, e)
        try:
            i = t.info or {}
            keys = {"sector": "sector", "industry": "industry", "cap": "marketCap", "pe": "trailingPE",
                    "eps": "trailingEps", "hi": "fiftyTwoWeekHigh", "lo": "fiftyTwoWeekLow",
                    "emp": "fullTimeEmployees", "web": "website"}
            out["info"] = {k: i[y] for k, y in keys.items() if i.get(y) is not None}
            if i.get("longBusinessSummary"):
                out["info"]["sum"] = i["longBusinessSummary"][:240] + "…"
        except Exception as e:
            print("公司資訊失敗", key, e)
        time.sleep(0.3)
        return key, out
    write("details.json", dict(pmap(one, res.items())))

def gnews(m, code, name):
    q = f"{name} 股票" if m == "TW" else f"{name} {code} stock"
    loc = "hl=zh-TW&gl=TW&ceid=TW:zh-Hant" if m == "TW" else "hl=en-US&gl=US&ceid=US:en"
    for when in ("2d", "7d"):
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q + ' when:' + when)}&{loc}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            root = ET.fromstring(urllib.request.urlopen(req, timeout=20).read())
        except Exception as e:
            print("新聞失敗", code, e); continue
        items = []
        for it in root.iter("item"):
            title, src = it.findtext("title") or "", it.findtext("source") or ""
            if src and title.endswith(" - " + src):
                title = title[: -len(src) - 3]
            try:
                d = email.utils.parsedate_to_datetime(it.findtext("pubDate"))
            except Exception:
                continue
            items.append({"t": title, "s": src, "u": it.findtext("link") or "", "ts": int(d.timestamp()),
                          "d": d.astimezone(TZ).strftime("%m-%d %H:%M")})
        items.sort(key=lambda x: -x["ts"])
        if items:
            return items[:6]
    return []

def news():
    def one(x):
        time.sleep(0.3)
        return x[0] + x[1], gnews(*x)
    write("news.json", dict(pmap(one, symbols())))

IND = {"01":"水泥工業","02":"食品工業","03":"塑膠工業","04":"紡織纖維","05":"電機機械","06":"電器電纜","08":"玻璃陶瓷","09":"造紙工業","10":"鋼鐵工業","11":"橡膠工業","12":"汽車工業","14":"建材營造","15":"航運業","16":"觀光餐旅","17":"金融保險","18":"貿易百貨","19":"綜合","20":"其他","21":"化學工業","22":"生技醫療","23":"油電燃氣","24":"半導體業","25":"電腦及週邊","26":"光電業","27":"通信網路","28":"電子零組件","29":"電子通路","30":"資訊服務","31":"其他電子","32":"文化創意","33":"農業科技","34":"電子商務","35":"綠能環保","36":"數位雲端","37":"運動休閒","38":"居家生活"}
ETFS = {"SPY":"SPDR S&P 500","QQQ":"Invesco QQQ","VOO":"Vanguard S&P 500","VTI":"Vanguard Total Market","IWM":"iShares Russell 2000","DIA":"SPDR Dow Jones","SMH":"VanEck Semiconductor","SOXX":"iShares Semiconductor","XLK":"Tech Select SPDR","XLF":"Financial Select SPDR","XLE":"Energy Select SPDR","XLV":"Health Care Select SPDR","ARKK":"ARK Innovation","TLT":"iShares 20+Y Treasury","GLD":"SPDR Gold","SCHD":"Schwab US Dividend","VIG":"Vanguard Div Appreciation","IBIT":"iShares Bitcoin Trust"}
UA = {"User-Agent": "Mozilla/5.0"}
jget = lambda url: json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60))

def pick(d, *ks):
    for k in ks:
        if d.get(k) not in (None, ""):
            return d[k]

def num(x):
    try:
        return float(str(x).replace(",", "").replace("+", ""))
    except Exception:
        return None

def tw_universe():
    info = {}
    for mk, url in (("L", "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"),
                    ("O", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"),
                    ("R", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R")):
        try:
            data = jget(url)
            for d in data:
                c = pick(d, "公司代號", "SecuritiesCompanyCode")
                if c:
                    v = str(pick(d, "產業別", "SecuritiesIndustryCode", "IndustryCode") or "")
                    info[str(c)] = IND.get(v.zfill(2), v or "其他")
            print(f"基本資料 {mk}：{len(data)} 筆；欄位", list(data[0])[:6] if data else "")
        except Exception as e:
            print("基本資料失敗", mk, e)
    q = {}
    def add(c, name, o, h, l, cl, ch, v, mk):
        if c and cl:
            q[str(c)] = [str(c), name, mk, o or cl, h or cl, l or cl, cl, ch or 0, round((v or 0) / 1000)]
    try:
        for d in jget("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"):
            add(d.get("Code"), d.get("Name"), num(d.get("OpeningPrice")), num(d.get("HighestPrice")), num(d.get("LowestPrice")),
                num(d.get("ClosingPrice")), num(d.get("Change")), num(d.get("TradeVolume")), "L")
    except Exception as e:
        print("上市行情失敗", e)
    try:
        for d in jget("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"):
            add(d.get("SecuritiesCompanyCode"), d.get("CompanyName"), num(d.get("Open")), num(d.get("High")), num(d.get("Low")),
                num(d.get("Close")), num(d.get("Change")), num(d.get("TradingShares")), "O")
    except Exception as e:
        print("上櫃行情失敗", e)
    try:  # 興櫃：欄位名稱未經驗證，失敗會略過
        for d in jget("https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics"):
            cl, pv = num(pick(d, "LatestPrice", "Close")), num(pick(d, "PreviousAveragePrice"))
            add(d.get("SecuritiesCompanyCode"), d.get("CompanyName"), None, None, None, cl, (cl - pv) if cl and pv else 0,
                num(pick(d, "TradingVolume", "TransactionVolume", "Volume")), "R")
    except Exception as e:
        print("興櫃行情失敗", e)
    print(f"台股行情：{len(q)} 檔")
    return [[c, x[1], x[2], "興櫃" if x[2] == "R" and c not in info else info.get(c, "ETF" if c.startswith("00") else "其他")] + x[3:]
            for c, x in q.items()]

def us_universe():
    names = {}
    for url in ("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "https://en.wikipedia.org/wiki/Nasdaq-100"):
        try:
            html = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read().decode("utf-8")
            for t in pd.read_html(io.StringIO(html)):
                cols = [str(c) for c in t.columns]
                sym = next((c for c in ("Symbol", "Ticker") if c in cols), None)
                if sym and "GICS Sector" in cols:
                    nm = next((c for c in ("Security", "Company") if c in cols), sym)
                    sub = "GICS Sub-Industry" if "GICS Sub-Industry" in cols else "GICS Sector"
                    for _, row in t.iterrows():
                        names.setdefault(str(row[sym]).strip().replace(".", "-"), (str(row[nm]), str(row["GICS Sector"]), str(row[sub])))
        except Exception as e:
            print("美股名單失敗", url, e)
    for sy, n in ETFS.items():
        names[sy] = (n, "ETF", "ETF")
    tk, rows = list(names), []
    for i in range(0, len(tk), 100):
        for t, d in download(tk[i:i + 100], "5d").items():
            if len(d) < 2:
                continue
            x, p = d.iloc[-1], d.iloc[-2]
            n, sec, sub = names[t]
            rows.append([t, n, sec, sub, r(x.Open), r(x.High), r(x.Low), r(x.Close), r(x.Close - p.Close), int(x.Volume)])
    print(f"美股：{len(rows)}/{len(tk)} 檔")
    return rows

def universe():
    data = {}
    for k, fn in (("TW", tw_universe), ("US", us_universe)):
        try:
            data[k] = fn()
        except Exception as e:
            print(k, "名單失敗", e); data[k] = []
    try:  # 某市場這次抓不到時，保留上次的資料
        old = json.load(open(f"{OUT}/universe.json", encoding="utf-8"))["data"]
        for k in data:
            if not data[k]:
                data[k] = old.get(k, [])
    except Exception:
        pass
    if not any(data.values()):
        sys.exit("全市場名單全部失敗，不更新檔案")
    write("universe.json", data, merge=False)

def daily():
    res = fetch("8mo")
    save_history(res)
    for step in (details, news, universe):  # 其中一項失敗不影響其他
        try:
            step(res) if step is details else step()
        except Exception as e:
            print(step.__name__, "失敗：", e)


# ───────────── session 模式：盤中常駐（取代 Cloud Run 的 /prefetch 與 /quote） ─────────────
# 時區, 開始（含開盤前 5 分）, 結束（含收盤後 5 分）, 正式開盤（分鐘，當地時間）
MKT = {"TW": ("Asia/Taipei", 535, 815, 540), "US": ("America/New_York", 565, 965, 570)}

def market_state(mk):
    tz, a, b, _ = MKT[mk]
    n = dt.datetime.now(ZoneInfo(tz)); m = n.hour * 60 + n.minute
    if n.weekday() >= 5: return "closed", 0
    if m < a: return "before", a - m
    return ("open", 0) if m <= b else ("closed", 0)

PUB = {}

def _git(d, *a, check=True):
    p = subprocess.run(["git", *a], cwd=d, capture_output=True, text=True)
    if check and p.returncode:
        tok = os.environ.get("GH_TOKEN", "\0")
        raise RuntimeError(f"git {a[0]} 失敗：{p.stderr.strip()[:200]}".replace(tok, "***"))
    return p

def publish():
    """把 OUT/live.json 推到 data 分支（只換這一個檔，分支維持 1 個 commit）。
    每次先抓最新的 data 分支再換檔，並用 --force-with-lease：如果同時有 daily/universe 更新了其他檔案，不會被蓋掉。"""
    d = PUB.get("dir")
    if not d:
        url = os.environ.get("PUB_REMOTE") or f"https://x-access-token:{os.environ['GH_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
        d = PUB["dir"] = tempfile.mkdtemp(prefix="livepub_")
        for a in (("init", "-q", "-b", "data"), ("remote", "add", "origin", url),
                  ("config", "user.name", "stock-bot"), ("config", "user.email", "stock-bot@users.noreply.github.com")):
            _git(d, *a)
    msg = "data live " + dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for _ in range(3):
        base = None
        if _git(d, "fetch", "-q", "--depth=1", "origin", "data", check=False).returncode == 0:
            base = _git(d, "rev-parse", "FETCH_HEAD").stdout.strip()
            _git(d, "checkout", "-q", "-f", "-B", "data", "FETCH_HEAD")
        else:  # data 分支還不存在：第一次發佈，整個輸出資料夾一起放上去
            shutil.copytree(OUT, d, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".git"))
        shutil.copyfile(f"{OUT}/live.json", f"{d}/live.json")
        _git(d, "add", "-A")
        if base and _git(d, "diff", "--cached", "--quiet", check=False).returncode == 0:
            return
        _git(d, "commit", "-q", *(["--amend"] if base else []), "-m", msg)
        if _git(d, "push", "-q", f"--force-with-lease=data:{base}" if base else "--force", "origin", "data", check=False).returncode == 0:
            return
        time.sleep(2)   # 被別的流程搶先更新了 data 分支：重新抓一次再來
    raise RuntimeError("push 失敗：data 分支被同時更新，重試 3 次仍失敗")

def _out(again):
    f = os.environ.get("GITHUB_OUTPUT")
    if f:
        with open(f, "a") as fh: fh.write(f"again={'true' if again else 'false'}\n")
    print("接力下一個 session" if again else "session 結束")

def session(mk):
    interval = int(os.environ.get("LIVE_INTERVAL", 60))
    max_sec = int(os.environ.get("SESSION_MAX_MIN", 335)) * 60   # GitHub 單一 job 上限 6 小時：超過就交棒給下一個 session
    t_start, fails, holiday = time.time(), 0, 0
    print(f"session 開始：{mk}，每 {interval} 秒一輪，單次最長 {max_sec // 60} 分鐘")
    while True:
        st, wait = market_state(mk)
        if st == "before" and wait <= 10:
            print(f"距離開盤前緩衝還有 {wait} 分鐘，等待"); time.sleep(30); continue
        if st != "open":
            print("目前休市"); return _out(False)
        t0, ok = time.time(), False
        try:
            stat = live(mk, keep_min=15)
            if stat["n"]:
                publish(); ok = True; fails = 0
                print(f"發佈完成：{stat['n']} 檔，其中今天有新 K 棒 {stat['today']} 檔，耗時 {time.time() - t0:.0f} 秒", flush=True)
                now = dt.datetime.now(ZoneInfo(MKT[mk][0]))
                if now.hour * 60 + now.minute - MKT[mk][3] >= 10 and stat["n"] >= 20:   # 開盤 10 分鐘後，幾乎沒有股票有今天的資料 → 疑似國定假日
                    holiday = holiday + 1 if stat["today"] / stat["n"] < 0.05 else 0
                    if holiday >= 3:
                        print("連續 3 輪都沒有今天的資料，判斷為休市日，結束"); return _out(False)
            else:
                print("這一輪沒有抓到任何資料", flush=True)
        except Exception as e:
            print("這一輪失敗：", repr(e), flush=True)
        if not ok:
            fails += 1
        wait_s = interval * (2 ** min(fails, 3)) if fails else interval   # 連續失敗（例如被 Yahoo 限流）就拉長間隔
        wait_s = max(wait_s - (time.time() - t0), 0)
        if time.time() + wait_s - t_start > max_sec:
            print("已達單次 session 上限"); return _out(market_state(mk)[0] == "open")
        time.sleep(wait_s)

if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "session":
        sys.exit(session(sys.argv[2] if len(sys.argv) > 2 else "TW"))
    {"history": lambda: save_history(fetch("8mo")), "live": live, "daily": daily, "universe": universe}[mode]()
