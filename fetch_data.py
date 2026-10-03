"""用法：python fetch_data.py history | live | daily
  live    ：只更新即時價量（盤中每 10 分鐘）
  history ：只更新 K 線
  daily   ：K 線 + 股利/公司資訊 + 新聞（收盤後每日）
股票清單直接從 stock.html 的 RAW 區塊讀取；輸出資料夾由環境變數 OUT_DIR 指定（預設 data）。"""
import datetime as dt, email.utils, json, os, re, sys, time
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

def download(tickers, period):
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

def fetch(period):
    syms = symbols()
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

def live():
    data = {}
    for key, v in fetch("5d").items():
        d = v["df"]; x = d.iloc[-1]
        data[key] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High),
                     "l": r(x.Low), "p": r(x.Close), "v": vol(v["m"], x.Volume)}
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

def daily():
    res = fetch("8mo")
    save_history(res)
    for step in (details, news):  # 其中一項失敗不影響其他
        try:
            step(res) if step is details else step()
        except Exception as e:
            print(step.__name__, "失敗：", e)

if __name__ == "__main__":
    mode = sys.argv[1]
    {"history": lambda: save_history(fetch("8mo")), "live": live, "daily": daily}[mode]()
