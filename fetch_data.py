"""用法：python fetch_data.py history | live | daily
  live    ：只更新即時價量（GitHub Actions 排程約每 5 分鐘；實際可能受 GitHub 排程延遲影響）
  history ：只更新 K 線
  daily   ：K 線 + 股利/公司資訊 + 新聞 + 全市場名單（收盤後每日）
  universe：只更新全市場名單與當日行情（台股上市/上櫃/興櫃/ETF、美股 S&P500+Nasdaq100+ETF）
股票清單直接從 stock.html 的 RAW 區塊讀取；輸出資料夾由環境變數 OUT_DIR 指定（預設 data）。"""
import datetime as dt, email.utils, io, json, os, re, sys, time
import urllib.parse, urllib.request, xml.etree.ElementTree as ET
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

def live():
    mk = "TW" if dt.datetime.now(dt.timezone.utc).hour < 8 else "US"  # 台股時段抓台股、美股時段抓美股
    extra = [e for e in extra_symbols() if e[0] == mk]
    print(f"精選名單 + 額外 {len(extra)} 檔（{mk}）")
    ft = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    data = {}
    for key, v in fetch("5d", extra).items():
        d = v["df"]; x = d.iloc[-1]
        data[key] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High),
                     "l": r(x.Low), "p": r(x.Close), "v": vol(v["m"], x.Volume),
                     "pc": r(d.iloc[-2].Close) if len(d) > 1 else None, "ft": ft}
    write("live.json", data, merge=False)

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

if __name__ == "__main__":
    mode = sys.argv[1]
    {"history": lambda: save_history(fetch("8mo")), "live": live, "daily": daily, "universe": universe}[mode]()
