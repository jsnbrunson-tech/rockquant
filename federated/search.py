import json, datetime as dt, requests

CFG = json.load(open("config.json"))

def _recency(pub_ts):
    try: t = dt.datetime.fromisoformat(pub_ts.replace("Z","+00:00"))
    except Exception: return 0
    days = max(0.0, (dt.datetime.utcnow() - t).total_seconds()/86400.0)
    tau = CFG.get("time_window_days", 60)
    return max(0.0, 1.0 - min(1.0, days/tau))

def _score(x):
    return 0.5*x.get("relevance",0) + 0.3*x.get("recency",0) + 0.2*x.get("citation",0)

def fetch_proofchain(q):
    out=[]; src=CFG["sources"].get("ProofChain",{})
    if not src.get("enabled"): return out
    url = src.get("sheet_csv_url"); 
    if not url: return out
    try:
        text = requests.get(url, timeout=15).text
        lines = text.splitlines(); headers = [h.strip() for h in lines[0].split(",")]
        for line in lines[1:]:
            vals = [v.strip() for v in line.split(",")]
            row = dict(zip(headers, vals))
            blob = " ".join([row.get("headline",""),row.get("summary",""),row.get("company_name",""),row.get("ticker","")])
            if q.lower() in blob.lower():
                item = {
                    "source":"ProofChain","source_type":row.get("source_type",""),
                    "headline":row.get("headline",""),"summary":row.get("summary",""),
                    "url":row.get("source_url",""),"pub_ts":row.get("pub_ts",""),
                    "relevance":1.0,"recency":_recency(row.get("pub_ts","")),
                    "citation": float(row.get("citation_score") or 0.6)
                }
                item["final_score"]=_score(item); out.append(item)
    except Exception: pass
    return out

def fetch_sec(q):
    if not CFG["sources"]["SEC"]["enabled"]: return []
    url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=&dateb=&owner=exclude&start=0&count=100&output=atom"
    out=[]
    try:
        txt=requests.get(url, headers={"User-Agent":"BullyWiz/1.0"}, timeout=15).text
        for entry in txt.split("<entry>")[1:]:
            title=entry.split("<title>")[1].split("</title>")[0]
            link=entry.split('link href="')[1].split('"')[0] if 'link href="' in entry else ""
            if q.lower() in title.lower():
                it={"source":"SEC","source_type":"EDGAR","headline":title,"summary":"EDGAR current filing",
                    "url":link,"pub_ts":dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "relevance":0.9,"recency":1.0,"citation":1.0}
                it["final_score"]=_score(it); out.append(it)
    except Exception: pass
    return out

def search(query):
    pool=[]
    pool+=fetch_proofchain(query)
    pool+=fetch_sec(query)
    pool.sort(key=lambda r:r["final_score"], reverse=True)
    return pool[:25]

if __name__=="__main__":
    import sys
    q=" ".join(sys.argv[1:]) or "fluorspar Utah permit"
    print(json.dumps(search(q), indent=2))
