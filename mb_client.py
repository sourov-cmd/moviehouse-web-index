"""MovieBox mobile-BFF client, a faithful port of the app's MovieBoxMobile.kt.

Recipe: review/mb-recipe-v4.json (gateway /v1/mb-recipe fetched with the app UA).
Usage as a library:  from mb_client import MB, load_recipe
CLI:  uv run python bots/moviehouse/scripts/mb_client.py samples   -> review/mb-samples/*.json
"""
import base64, hashlib, hmac, json, os, sys, time, uuid
from urllib.parse import quote
import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
CT = "application/json"
PROXY = os.environ.get("MB_PROXY") or None  # optional residential proxy, never committed


def load_recipe(path=os.path.join(ROOT, "review", "mb-recipe-v4.json")):
    return json.load(open(path))


def md5hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def sorted_target(pq: str) -> str:
    if "?" not in pq:
        return pq
    path, q = pq.split("?", 1)
    pairs = []
    for p in q.split("&"):
        if not p:
            continue
        k, _, v = p.partition("=")
        pairs.append((k, v))
    pairs.sort(key=lambda x: x[0])
    return path + "?" + "&".join(f"{k}={v}" for k, v in pairs)


def canonical(method: str, ts: int, body: bytes | None, pq: str) -> str:
    blen = str(len(body)) if body is not None else ""
    bmd5 = md5hex(body[:102400]) if body is not None else ""
    return "\n".join([method.upper(), CT, CT, blen, str(ts), bmd5, sorted_target(pq)])


def signature(sign_key: str, canon: str, ver: int, ts: int) -> str:
    key = base64.b64decode(sign_key + "=" * (-len(sign_key) % 4))
    mac = hmac.new(key, canon.encode(), hashlib.md5).digest()
    return f"{ts}|{ver}|{base64.b64encode(mac).decode()}"


def client_token(ts: int) -> str:
    return f"{ts},{md5hex(str(ts)[::-1].encode())}"


class MB:
    """One MovieBox identity (device id + visitor token) over one host pool."""

    def __init__(self, cfg: dict, proxy: str | None = None, device_id: str | None = None, timeout=25):
        self.cfg = cfg
        self.client = httpx.Client(proxy=proxy, timeout=timeout)
        self.device_id = device_id or md5hex(("mb:" + uuid.uuid4().hex).encode())
        self.token: str | None = None
        self.good_host: str | None = None
        self.last = (0, None)

    # ---- wire
    def headers(self, ts, sig):
        ci = dict(self.cfg["clientInfo"])
        ci["device_id"] = self.device_id
        h = {
            "User-Agent": self.cfg["userAgent"],
            "Accept": CT,
            "Content-Type": CT,
            "x-client-token": client_token(ts),
            "x-tr-signature": sig,
            "x-client-info": json.dumps(ci, separators=(",", ":")),
            "x-client-status": "0",
        }
        if self.token:
            h["Authorization"] = "Bearer " + self.token
        return h

    def call(self, host: str, pq: str, method="GET", body=None):
        """pq is signed RAW (unencoded query values) and sent percent-encoded, like OkHttp does."""
        ts = int(time.time() * 1000)
        b = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode() if body is not None else None
        sig = signature(self.cfg["signKey"], canonical(method, ts, b, pq), self.cfg["sigVersion"], ts)
        if "?" in pq:
            path, q = pq.split("?", 1)
            enc = "&".join(f"{k}={quote(v, safe='')}" for k, _, v in (p.partition("=") for p in q.split("&") if p))
            url = host + path + "?" + enc
        else:
            url = host + pq
        try:
            r = self.client.request(method, url, headers=self.headers(ts, sig), content=b)
        except Exception as e:
            self.last = (-1, str(e))
            return -1, None
        try:
            j = r.json()
        except Exception:
            j = {"_text": r.text[:300]}
        self.last = (r.status_code, j)
        return r.status_code, j

    def rotate(self, pq, method="GET", body=None, heal=True):
        hosts = ([self.good_host] if self.good_host else []) + [h for h in self.cfg["hosts"] if h != self.good_host]
        rotate_on = set(self.cfg.get("rotateOn") or [403, 406, 407, 429, 500, 502, 503, 504])
        for host in hosts:
            code, j = self.call(host, pq, method, body)
            if code == 200 and isinstance(j, dict) and j.get("code", 0) == 0:
                self.good_host = host
                return j
            if code == 401 and heal:
                self.token = None
                if self.mint():
                    return self.rotate(pq, method, body, heal=False)
                return None
            if code not in rotate_on and code != -1:
                return None
        return None

    def mint(self) -> bool:
        j = self.rotate(self.cfg["paths"]["login"], "POST", {}, heal=False)
        t = ((j or {}).get("data") or {}).get("token")
        if not t:
            return False
        self.token = t
        return True

    def ensure(self):
        return bool(self.token) or self.mint()

    def path(self, name, fallback=""):
        return (self.cfg.get("paths") or {}).get(name) or fallback

    # ---- queries (shapes as in MovieBoxMobile.kt)
    def bottom_tab(self):
        self.ensure(); return self.rotate(self.path("bottomTab"))

    def tab_operating(self, tab_id: int):
        self.ensure(); return self.rotate(self.path("tabOperating", "/wefeed-mobile-bff/tab-operating") + f"?tabId={tab_id}")

    def list(self, subject_type: int, page=1, per_page=20, genre="", country="", classify="", sort="", year=""):
        self.ensure()
        body = {"page": max(1, page), "perPage": min(20, max(1, per_page)), "subjectType": subject_type}
        for k, v in (("genre", genre), ("country", country), ("classify", classify), ("sort", sort), ("year", year)):
            if v:
                body[k] = v
        return self.rotate(self.path("list"), "POST", body)

    def search(self, keyword: str, page=1, per_page=20, tab="All"):
        self.ensure()
        return self.rotate(self.path("search"), "POST", {"keyword": keyword, "page": page, "perPage": min(20, per_page), "subjectType": "All", "tabId": tab or "All"})

    def search_suggest(self, keyword: str):
        self.ensure(); return self.rotate(self.path("searchSuggest") + "?keyword=" + keyword)

    def search_rank(self):
        self.ensure(); return self.rotate(self.path("searchRank"))

    def filter_items(self, tab_id: int, subject_type: int):
        self.ensure(); return self.rotate(self.path("filterItems") + f"?tabId={tab_id}&subjectType={subject_type}")

    def subject(self, subject_id: str):
        self.ensure(); return self.rotate(self.path("subject") + f"?subjectId={subject_id}")

    def season_info(self, subject_id: str):
        self.ensure(); return self.rotate(self.path("seasonInfo") + f"?subjectId={subject_id}")

    def play_info(self, subject_id: str, se=0, ep=0):
        self.ensure(); return self.rotate(self.path("playInfo") + f"?subjectId={subject_id}&se={se}&ep={ep}")

    def resource(self, subject_id: str, se=0, ep=0, resolution=""):
        self.ensure()
        q = f"?subjectId={subject_id}&se={se}&ep={ep}" + (f"&resolution={resolution}" if resolution else "")
        return self.rotate(self.path("resource") + q)

    def captions(self, subject_id: str, se=0, ep=0):
        self.ensure(); return self.rotate(self.path("captions") + f"?subjectId={subject_id}&se={se}&ep={ep}")

    def shorts(self, subject_id: str, page=1, per_page=10):
        self.ensure(); return self.rotate(self.path("shortsMini") + f"?subjectId={subject_id}&page={page}&perPage={per_page}")

    def ranking(self, category: str, page=1, per_page=20):
        """h5 host, unsigned, tokenless (as in the app)."""
        base = self.path("ranking")
        r = self.client.get(f"{base}?id={category}&page={page}&perPage={per_page}")
        try:
            return r.json()
        except Exception:
            return None


def dump(name, obj, outdir):
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, name + ".json"), "w") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    size = len(json.dumps(obj or {}))
    print(f"  {name}: {'ok' if obj else 'NONE'} ({size} B) http={None}")


def samples(proxy=None):
    rec = load_recipe()
    mb = MB(rec["mobile"], proxy=proxy)
    out = os.path.join(ROOT, "review", "mb-samples")
    print("device", mb.device_id, "mint", mb.mint(), mb.last[0])
    dump("bottom-tab", mb.bottom_tab(), out)
    for t in rec["tabs"]:
        if t.get("srcTab"):
            dump(f"tab-operating-{t['srcTab']}-{t['id']}", mb.tab_operating(t["srcTab"]), out)
            time.sleep(0.3)
    dump("list-movie-p1", mb.list(1), out)
    dump("list-tv-hindi-dub", mb.list(2, classify="Hindi dub"), out)
    dump("list-movie-bengali-latest", mb.list(1, classify="Bengali dub", sort="Latest"), out)
    dump("list-movie-india-foryou", mb.list(1, country="India", sort="ForYou"), out)
    dump("search-hokum", mb.search("hokum"), out)
    dump("search-arijit-music", mb.search("arijit singh", tab="Music"), out)
    dump("search-suggest-toxi", mb.search_suggest("toxi"), out)
    dump("search-rank", mb.search_rank(), out)
    dump("filter-items-tab1-movie", mb.filter_items(1, 1), out)
    dump("filter-items-tab1-tv", mb.filter_items(1, 2), out)
    # a movie and a series from the list
    lm = json.load(open(os.path.join(out, "list-movie-p1.json")))
    mid = lm["data"]["items"][0]["subjectId"]
    dump("subject-movie", mb.subject(mid), out)
    dump("play-info-movie", mb.play_info(mid), out)
    dump("resource-movie", mb.resource(mid), out)
    dump("captions-movie", mb.captions(mid), out)
    lt = json.load(open(os.path.join(out, "list-tv-hindi-dub.json")))
    tid = lt["data"]["items"][0]["subjectId"]
    dump("subject-tv", mb.subject(tid), out)
    dump("season-info-tv", mb.season_info(tid), out)
    dump("play-info-tv-s1e1", mb.play_info(tid, 1, 1), out)
    dump("captions-tv-s1e1", mb.captions(tid, 1, 1), out)
    dump("list-shorts", mb.list(7), out)
    ls = json.load(open(os.path.join(out, "list-shorts.json")))
    sid = ls["data"]["items"][0]["subjectId"]
    dump("shorts-mini", mb.shorts(sid), out)
    dump("list-music", mb.list(6), out)
    dump("tab-operating-4-music", mb.tab_operating(4), out)


if __name__ == "__main__":
    if sys.argv[1:2] == ["samples"]:
        samples(PROXY if "--proxy" in sys.argv else None)
