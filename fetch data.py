"""用法：python fetch_data.py history | live
股票清單直接從 stock.html 的 RAW 區塊讀取，改清單只要改 stock.html。"""
import datetime as dt, json, os, re, sys
import yfinance as yf

HTML, OUT = "stock.html", "data"

def symbols():
    src = open(HTML, encoding="utf-8").read()
    a = src.index("const RAW="); mid = src.index("US:{", a); end = src.index("const $=", a)
    out = []
    for mkt, seg in (("TW", src[a:mid]), ("US", src[mid:end])):
        for val in re.findall(r'":"([^"]+)"', seg):
            for item in val.split(","):
                code = item.strip().split(" ")[0]
                if code:
                    out.append((mkt, code))
    return list(dict.fromkeys(out))

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
    ysym = {(m, c): (c + ".TW" if m == "TW" else c) for m, c in syms}
    got = download(list(ysym.values()), period)
    miss = [(m, c) for (m, c), y in ysym.items() if m == "TW" and y not in got]
    if miss:  # 上櫃股票代號後綴是 .TWO
        alt = {k: k[1] + ".TWO" for k in miss}
        got.update(download(list(alt.values()), period)); ysym.update(alt)
    out = {m + c: (m, got[y]) for (m, c), y in ysym.items() if y in got}
    print(f"取得 {len(out)}/{len(syms)} 檔")
    if not out:
        sys.exit("沒有抓到任何資料，不更新檔案")
    return out

r = lambda x: round(float(x), 2)
vol = lambda m, v: round(float(v) / 1000) if m == "TW" else int(v)  # 台股換算成「張」

def write(name, data):
    os.makedirs(OUT, exist_ok=True)
    tz = dt.timezone(dt.timedelta(hours=8))
    payload = {"updated": dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M"), "data": data}
    with open(f"{OUT}/{name}", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

def history():
    data = {}
    for key, (m, d) in fetch("8mo").items():
        d = d.tail(130)
        data[key] = [[i.strftime("%m-%d"), r(x.Open), r(x.High), r(x.Low), r(x.Close), vol(m, x.Volume)]
                     for i, x in d.iterrows()]
    write("history.json", data)

def live():
    data = {}
    for key, (m, d) in fetch("5d").items():
        x = d.iloc[-1]
        data[key] = {"t": d.index[-1].strftime("%m-%d"), "o": r(x.Open), "h": r(x.High),
                     "l": r(x.Low), "p": r(x.Close), "v": vol(m, x.Volume)}
    write("live.json", data)

if __name__ == "__main__":
    {"history": history, "live": live}[sys.argv[1]]()
