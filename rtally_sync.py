"""Second content source for moviehouse.cyou: rtally (public Sanity catalog, no auth). MovieBox stays primary.

Reads rtally's catalog through its public GROQ API and writes the titles MovieBox does NOT have into the same
index (`subjects` rows with source='rtally', their own `plays` rows for playback, the "New on MovieHouse" shelves).
A title MovieBox also has (same name, same kind, same year ±1) is never written twice: it is skipped, and one that
was written earlier and later appeared on MovieBox is removed with an alias (its old URL → the MovieBox page).

  python rtally_sync.py --push https://moviehouse.cyou/ingest --max-new 600      # GitHub Actions (OIDC bearer)
  python rtally_sync.py --db catalog.sqlite --max-new 50                          # local

Two passes per run: changes since the last run (_updatedAt, incremental), then the backfill — Bangla titles first,
then everything else, newest first — up to --max-new new titles per run (D1 free plan: 100k rows written a day,
a title costs ~20 with its indexes, genres, season and plays rows).
"""
from __future__ import annotations

import argparse, datetime, hashlib, json, os, re, sqlite3, sys, time, unicodedata
from urllib.parse import quote

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from sync import Out, apply_sqlite, ingest_token, push_worker, slugify  # noqa: E402

SANITY = "https://o38ds5mw.api.sanity.io/v2021-10-21/data/query/rtally?query="
SOURCE = "rtally"
# their servers, in the order their own site offers them (the first group is what they show first)
MOVIE_SERVERS = [("abyss", "abyssUrl", "Abyss"), ("turbo", "turboUrl", "Turbo"), ("filemoon", "filemoonUrl", "FileMoon"), ("vidhide", "vidhideUrl", "VidHide"),
                 ("lulustream", "lulustreamUrl", "Lulu"), ("streamwish", "streamwishUrl", "StreamWish"), ("vidara", "vidaraUrl", "Vidara"),
                 ("seekstream", "seekstreamUrl", "Seek"), ("vidmoly", "vidmolyUrl", "Vidmoly")]
SERIES_SERVERS = [("abyss", "abyssMultiUrl", "Abyss"), ("turbo", "turboMultiUrl", "Turbo"), ("filemoon", "multiLinksDl", "FileMoon"), ("vidhide", "multiLinksSl", "VidHide"),
                  ("lulustream", "lulustreamMultiUrl", "Lulu"), ("streamwish", "streamwishMultiUrl", "StreamWish"), ("vidara", "vidaraMultiUrl", "Vidara"),
                  ("seekstream", "seekstreamMultiUrl", "Seek"), ("vidmoly", "vidmolyMultiUrl", "Vidmoly")]
MULTICLOUD = [("small", "Cloud 480p"), ("medium", "Cloud 720p"), ("large", "Cloud 1080p"), ("extraLarge", "Cloud 4K")]
DOWNLOAD_Q = [("small", "480p"), ("medium", "720p"), ("large", "1080p"), ("extraLarge", "4K")]
FIELDS = ("_id,_createdAt,_updatedAt,title,\"slug\":slug.current,description,duration,rating,country,casts,director,banglaSub,englishSub,"
          "\"genres\":genres[]->title,\"language\":language[]->title,\"year\":year[]->title,\"type\":type[]->title,\"categories\":categories[]->title,"
          "\"image\":mainImage.asset->{url,\"w\":metadata.dimensions.width,\"h\":metadata.dimensions.height},"
          + ",".join(sorted({f for _, f, _ in MOVIE_SERVERS} | {f for _, f, _ in SERIES_SERVERS})) + ","
          + ",".join(f for f, _ in DOWNLOAD_Q) + "," + ",".join(f"{f}_single" for f, _ in DOWNLOAD_Q))
BANGLA = "\"Bangla\" in language[]->title || \"Bangla-Sub\" in language[]->title || \"Bangla (Dual-Aud)\" in language[]->title || \"Ban, Eng (Sub)\" in language[]->title || \"Bangla (Unofficial)\" in language[]->title"
PHASES = [("bangla", BANGLA), ("all", "true")]
SHELVES = [  # id, tab, title, pos, filter on the entry
    ("src-rtally-home", "home", "New on MovieHouse", 2, lambda e: True),
    ("src-rtally-bengali", "bengali", "New Bangla Releases", 0, lambda e: e["bangla"]),
    ("src-rtally-hindi", "hindi", "New Hindi Releases", 0, lambda e: e["hindi"]),
]
SHELF_N = 40
ADULT_CATS = {"adult 18+"}


def now() -> int:
    return int(time.time())


def groq(q: str):
    r = httpx.get(SANITY + quote(q), timeout=60)
    r.raise_for_status()
    return r.json()["result"]


def key(title: str) -> str:
    """One key for one title across sources: lower case, no tags, no punctuation, no 'season n'."""
    t = re.sub(r"\s*\[[^\]]*\]", "", title or "")
    t = re.sub(r"\(\s*\d{4}\s*\)", "", t)
    t = re.sub(r"\b(season|series)\s*\d+\b", "", t, flags=re.I)
    t = re.sub(r"\bS\d{1,2}\b", "", t)
    t = unicodedata.normalize("NFKD", t)
    return re.sub(r"[^a-z0-9\u0980-\u09ff]", "", t.lower())


def ts(iso: str | None) -> int:
    if not iso:
        return now()
    try:
        return int(datetime.datetime.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
    except ValueError:
        return now()


def sid(kind: str, seed: str) -> str:
    """A 20-digit id (MovieBox ids are 16–19 digits; the site reads 20+ digits as a second-source title)."""
    return "9" + str(int(hashlib.sha1(f"{SOURCE}:{kind}:{seed}".encode()).hexdigest(), 16))[:19].rjust(19, "0")


def ids_of(v: str | None) -> list[str]:
    return [x.strip() for x in (v or "").split(",")] if v and v.strip() else []


def multicloud_id(url: str) -> str | None:
    m = re.search(r"https://new\d*\.multicloudlinks\.com/(?:player\.php/\?v=([A-Za-z0-9_-]+)|view/([A-Za-z0-9_-]+))", url or "")
    return (m.group(1) or m.group(2)) if m else None


def corner_of(langs: list[str]) -> str:
    """Their audio label as the card's ribbon ('Hindi (Dual-Aud)' → 'Hindi + Eng')."""
    l = (langs or [""])[0] or ""
    base = re.sub(r"\s*\(.*?\)\s*", "", l).replace(" Dubbed", "").replace("-Sub", " Sub").strip()
    if "Dual" in l:
        return f"{base} + Eng" if base != "English" else "Eng + Hindi"
    if l.startswith("Ban, Eng"):
        return "Ban/Eng Sub"
    return base


def entry_of(d: dict) -> dict | None:
    """One rtally document → what the index needs. None when it carries nothing playable or is adult."""
    title = re.sub(r"\s+", " ", (d.get("title") or "")).strip()
    if not title:
        return None
    cats = [c.lower() for c in (d.get("categories") or []) if c]
    if any(c in ADULT_CATS for c in cats):
        return None
    series = "Web Series" in (d.get("type") or [])
    servers: list[dict] = []
    if series:
        for k, f, name in SERIES_SERVERS:
            ids = ids_of(d.get(f))
            if ids:
                servers.append({"name": name, "key": k, "ids": ids})
    else:
        for k, f, name in MOVIE_SERVERS:
            v = (d.get(f) or "").strip()
            if v:
                servers.append({"name": name, "key": k, "ids": [v]})
        for f, name in MULTICLOUD:
            for u in (d.get(f) or "").split("`"):
                mid = multicloud_id(u)
                if mid:
                    servers.append({"name": name, "key": "multicloud", "ids": [mid]})
                    break
        # Abyss and the Cloud players first, like their site
        order = {"abyss": 0, "multicloud": 1, "turbo": 2}
        servers.sort(key=lambda s: order.get(s["key"], 9))
    if not servers:
        return None
    episodes = max(len(s["ids"]) for s in servers) if series else 1
    m = re.search(r"\bseason\s*(\d{1,3})\b", title, re.I) or re.search(r"\bS(\d{1,2})\b", title)
    se = int(m.group(1)) if (series and m) else (1 if series else 0)
    base = re.sub(r"\s*[-–:]?\s*\b(season|series)\s*\d{1,3}\b.*$", "", title, flags=re.I).strip(" -–:") if series else title
    base = base or title
    langs = [x for x in (d.get("language") or []) if x]
    years = [int(y) for y in (d.get("year") or []) if str(y).strip().isdigit()]
    year = max(years) if years else None
    genres = [g.strip() for g in (d.get("genres") or []) if g and g.strip()]
    downloads: dict[str, list[str]] = {}
    if series:
        for f, q in DOWNLOAD_Q:
            lst = ids_of(d.get(f"{f}_single"))
            if lst:
                downloads[q] = lst
    else:
        for f, q in DOWNLOAD_Q:
            lst = [u.strip() for u in (d.get(f) or "").split("`") if u.strip().startswith("http")]
            if lst:
                downloads[q] = lst
    play = {"src": SOURCE, "slug": d.get("slug") or "", "servers": servers, "downloads": downloads, "perEpisode": series}
    img = d.get("image") or {}
    cover = f"{img['url']}?w=480&auto=format&q=80" if img.get("url") else ""
    dur = int(d["duration"]) * 60 if str(d.get("duration") or "").strip().isdigit() else 0
    try:
        imdb = float(d.get("rating") or 0) or None
    except ValueError:
        imdb = None
    subs = ",".join(s for s, on in (("Bangla", d.get("banglaSub")), ("English", d.get("englishSub"))) if on)
    lang_key = key((langs or ["x"])[0])
    subject_id = sid("series", f"{key(base)}|{lang_key}") if series else sid("movie", d["_id"])
    src_id = f"series:{key(base)}|{lang_key}" if series else d["_id"]
    return {
        "doc": d["_id"], "id": subject_id, "src_id": src_id, "series": series, "se": se, "episodes": episodes, "title": base, "key": key(base),
        "year": year, "type": 2 if series else 1, "slug": slugify(base), "description": (d.get("description") or "").strip(),
        "duration_s": dur, "genre": ", ".join(genres), "genres": genres, "country": (d.get("country") or "").strip(), "language": langs[0] if langs else "",
        "imdb": imdb, "cover": cover, "cover_w": int(img.get("w") or 0), "cover_h": int(img.get("h") or 0), "corner": corner_of(langs),
        "subtitles": subs, "created": ts(d.get("_createdAt")), "updated": d.get("_updatedAt") or "", "play": play,
        "bangla": any(l.lower().startswith("ban") for l in langs), "hindi": any(l.lower().startswith("hindi") for l in langs),
    }


LANG_FAMILY = {"bangla": "bengali", "ban": "bengali", "bengali": "bengali", "eng": "english", "english": "english", "hindi": "hindi", "tamil": "tamil",
               "telugu": "telugu", "malayalam": "malayalam", "kannada": "kannada", "marathi": "marathi", "punjabi": "punjabi", "gujarati": "gujarati",
               "korean": "korean", "japanese": "japanese", "chinese": "chinese", "turkish": "turkish"}


def families(label: str) -> set[str]:
    """Audio families in a language label: 'Hindi (Dual-Aud)' → {hindi, english}; 'Bangla-Sub' → {bengali}; '' → {}."""
    out = set()
    for w in re.split(r"[^a-z]+", (label or "").lower()):
        if w in LANG_FAMILY:
            out.add(LANG_FAMILY[w])
    if "dual" in (label or "").lower():
        out.add("english")
    return out


def same_title(e: dict, mb: list[dict]) -> dict | None:
    """The MovieBox row this entry duplicates: same key, same kind, the same year (±1) for a film, and the same audio —
    MovieBox keeps one row per dub ('Agadha [Telugu]'), so a Bangla copy of a title MovieBox has only in Telugu is a
    new edition, not a duplicate. An untagged MovieBox title counts as every audio (nothing says which)."""
    fam = families(e["language"])
    for m in mb:
        kind_ok = (m["type"] in (2, 4, 5)) if e["series"] else (m["type"] in (1, 4, 5))
        if not kind_ok:
            continue
        if not (e["series"] or e["year"] is None or m.get("year") is None or abs(int(m["year"]) - e["year"]) <= 1):
            continue
        tags = re.findall(r"\[([^\]]+)\]", m["title"] or "")
        mb_fam = set().union(*(families(t) for t in tags)) if tags else set()
        if not tags or not fam or (fam & mb_fam):
            return m
    return None


class RtallySync:
    def __init__(self, out: Out, keys: list[dict], meta: dict, max_new: int):
        self.out, self.meta, self.max_new = out, meta, max_new
        self.mb: dict[str, list[dict]] = {}
        self.mine: dict[str, dict] = {}        # src_id -> existing rtally row
        for r in keys:
            if r.get("source") == SOURCE:
                if r.get("src_id"):
                    self.mine[r["src_id"]] = r
            else:
                self.mb.setdefault(key(r["title"]), []).append(r)
        self.written: dict[str, dict] = {}     # subject id -> entry (this run)
        self.removed: set[str] = set()
        self.new = 0
        self.n = {"new": 0, "changed": 0, "unchanged": 0, "skipped_dup": 0, "removed_dup": 0, "no_play": 0}

    def hash_of(self, e: dict) -> str:
        body = {k: e[k] for k in ("title", "year", "type", "description", "duration_s", "genre", "country", "language", "imdb", "cover", "corner", "subtitles", "se", "episodes")}
        body["play"] = e["play"]
        return hashlib.md5(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]

    def take(self, d: dict, backfill: bool) -> bool:
        """Writes one document. Returns False when the run's new-title budget is spent (backfill stops there)."""
        e = entry_of(d)
        if not e:
            self.n["no_play"] += 1
            return True
        dup = same_title(e, self.mb.get(e["key"], []))
        if dup:
            self.n["skipped_dup"] += 1
            old = self.mine.get(e["src_id"])
            if old:
                self.remove(old["id"], dup["id"])
            return True
        old = self.mine.get(e["src_id"])
        # a film's hash is its own; a series keeps one "se:hash" per season it has (several documents feed one row)
        h = f"{e['se']}:{self.hash_of(e)}" if e["series"] else self.hash_of(e)
        parts = (old.get("sync_hash") or "").split(";") if old else []
        if h in parts:
            self.n["unchanged"] += 1
            return True
        if not old and e["id"] not in self.written:
            if self.new >= self.max_new:
                return False
            self.new += 1
            self.n["new"] += 1
        elif old:
            self.n["changed"] += 1
        stored = ";".join(sorted({p for p in parts if p and not p.startswith(f"{e['se']}:")} | {h})) if e["series"] else h
        self.write(e, stored, old)
        return True

    def write(self, e: dict, h: str, old: dict | None):
        t = now()
        first = e["created"] if not old else int(old.get("first_seen") or e["created"])
        self.out.add(
            """INSERT INTO subjects (id,type,title,slug,description,release_date,year,duration_s,genre,country,language,imdb,
                 cover_url,cover_w,cover_h,cover_blur,still_url,still_w,still_h,content_rating,restrict_kid,corner,detail_url,
                 has_resource,is_cam,se_num,subtitles,aka,viewers,adult,mature,resolutions,codecs,detail_at,first_seen,updated_at,sync_hash,source,src_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET title=excluded.title, slug=excluded.slug, description=excluded.description, year=excluded.year,
                 duration_s=excluded.duration_s, genre=excluded.genre, country=excluded.country, language=excluded.language, imdb=excluded.imdb,
                 cover_url=excluded.cover_url, cover_w=excluded.cover_w, cover_h=excluded.cover_h, corner=excluded.corner, subtitles=excluded.subtitles,
                 se_num=MAX(excluded.se_num, subjects.se_num), updated_at=excluded.updated_at, sync_hash=excluded.sync_hash, source=excluded.source, src_id=excluded.src_id""",
            e["id"], e["type"], e["title"], e["slug"], e["description"], f"{e['year']}-01-01" if e["year"] else None, e["year"], e["duration_s"],
            e["genre"], e["country"] or None, e["language"] or None, e["imdb"], e["cover"], e["cover_w"], e["cover_h"], "", "", 0, 0, None, 0, e["corner"] or None, None,
            1, 0, max(e["se"], 1) if e["series"] else 0, e["subtitles"] or None, None, 0, 0, 0, None, None, t, first, t, h, SOURCE, e["src_id"])
        for g in e["genres"]:
            self.out.add("INSERT OR IGNORE INTO subject_genres (subject_id, genre) VALUES (?,?)", e["id"], g)
        if e["series"]:
            self.out.add("INSERT INTO seasons (subject_id,se,max_ep,resolutions,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(subject_id,se) DO UPDATE SET max_ep=excluded.max_ep, updated_at=excluded.updated_at",
                         e["id"], e["se"], e["episodes"], "", t)
        self.out.add("INSERT INTO plays (subject_id,se,source,play_json,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(subject_id,se) DO UPDATE SET play_json=excluded.play_json, updated_at=excluded.updated_at",
                     e["id"], e["se"] if e["series"] else 0, SOURCE, json.dumps(e["play"], ensure_ascii=False), t)
        self.written[e["id"]] = e
        self.mine[e["src_id"]] = {"id": e["id"], "src_id": e["src_id"], "sync_hash": h, "first_seen": first, "title": e["title"], "type": e["type"], "year": e["year"]}

    def remove(self, rid: str, target: str):
        """A source title MovieBox turned out to have: gone from the index, its URL kept as a 301."""
        for sql in ("DELETE FROM shelf_items WHERE subject_id=?", "DELETE FROM subject_genres WHERE subject_id=?", "DELETE FROM seasons WHERE subject_id=?",
                    "DELETE FROM plays WHERE subject_id=?", "DELETE FROM subjects WHERE id=? AND source='rtally'"):
            self.out.add(sql, rid)
        self.out.add("INSERT OR REPLACE INTO aliases (id,target,created_at) VALUES (?,?,?)", rid, target, now())
        self.n["removed_dup"] += 1
        self.removed.add(rid)
        for k, v in list(self.mine.items()):
            if v["id"] == rid:
                del self.mine[k]

    def shelves(self, state: dict | None, keys: list[dict]):
        """The 'New on MovieHouse' shelves: the newest source titles, by when rtally published them; written as a diff."""
        rows: dict[str, dict] = {}
        for r in keys:
            if r.get("source") == SOURCE:
                c = str(r.get("corner") or "")
                rows[r["id"]] = {"id": r["id"], "first_seen": int(r.get("first_seen") or 0), "bangla": c.startswith("Ban"), "hindi": c.startswith("Hindi")}
        for e in self.written.values():
            rows[e["id"]] = {"id": e["id"], "first_seen": e["created"], "bangla": e["bangla"], "hindi": e["hindi"]}
        cur_items: dict[str, list[str]] = {}
        cur_shelves: dict[str, dict] = {}
        if state:
            for r in state.get("items") or []:
                cur_items.setdefault(r["shelf_id"], []).append(str(r["subject_id"]))
            cur_shelves = {r["id"]: r for r in state.get("shelves") or []}
        t = now()
        ordered = sorted(rows.values(), key=lambda r: -r["first_seen"])
        for shelf_id, tab, title, pos, flt in SHELVES:
            want = [r["id"] for r in ordered if r["id"] not in self.removed and flt(r)][:SHELF_N]
            if not want:
                continue
            cur = cur_shelves.get(shelf_id)
            if state is None or not cur or (cur.get("tab_id"), cur.get("title"), int(cur.get("pos") or 0)) != (tab, title, pos):
                self.out.add("INSERT INTO shelves (id,tab_id,title,slug,kind,stype,pos,category_id,rail_json,adult,shorts,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                             "ON CONFLICT(id) DO UPDATE SET tab_id=excluded.tab_id,title=excluded.title,slug=excluded.slug,pos=excluded.pos,updated_at=excluded.updated_at",
                             shelf_id, tab, title, slugify(title) + ("" if tab == "home" else f"-{tab}"), "their", "SOURCE", pos, None, None, 0, 0, t)
            have = cur_items.get(shelf_id, []) if state is not None else []
            if have != want:
                for p, sid_ in enumerate(want):
                    if p >= len(have) or have[p] != sid_:
                        self.out.add("INSERT OR REPLACE INTO shelf_items (shelf_id,gpos,pos,subject_id) VALUES (?,?,?,?)", shelf_id, 0, p, sid_)
                if len(have) > len(want):
                    self.out.add("DELETE FROM shelf_items WHERE shelf_id=? AND gpos=? AND pos>=?", shelf_id, 0, len(want))


def fetch_docs(filter_expr: str, order: str, start: int, n: int) -> list[dict]:
    return groq(f"*[_type==\"movie\" && ({filter_expr})] | order({order}) [{start}...{start + n}] {{{FIELDS}}}")


def layout_state_from_db(db: sqlite3.Connection) -> dict:
    from sync import layout_state_from_db as lsd
    return lsd(db, False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--push")
    ap.add_argument("--token", default=os.environ.get("INGEST_TOKEN", ""))
    ap.add_argument("--max-new", type=int, default=600, help="new titles this run may add (each costs ~20 D1 row writes)")
    ap.add_argument("--page", type=int, default=200)
    args = ap.parse_args()
    if not args.db and not args.push:
        raise SystemExit("--db or --push")

    db = None
    if args.db:
        db = sqlite3.connect(args.db)
        db.executescript(open(os.path.join(HERE, "schema.sql")).read())
        keys = [dict(zip(("id", "type", "title", "year", "source", "src_id", "first_seen", "sync_hash", "corner"), r)) for r in db.execute("SELECT id,type,title,year,source,src_id,first_seen,sync_hash,corner FROM subjects")]
        meta = dict(db.execute("SELECT key,value FROM meta").fetchall())
        state = layout_state_from_db(db)
    else:
        plan = httpx.get(args.push + "?plan=1&keys=1&layout=their&details=0&seasons=0&rankings=0", headers={"Authorization": f"Bearer {ingest_token(args.token)}"}, timeout=120)
        if plan.status_code != 200:
            raise SystemExit(f"plan {plan.status_code}: {plan.text[:300]}")
        p = plan.json()
        keys = p.get("keys") or []
        meta = {r["key"]: r["value"] for r in (p.get("meta") or [])}
        state = p.get("state")
    print(f"index: {sum(1 for k in keys if k.get('source') != SOURCE)} MovieBox titles, {sum(1 for k in keys if k.get('source') == SOURCE)} rtally titles; since={meta.get('rtally_since', '-')} backfill={meta.get('rtally_backfill', '-')}")

    out = Out()
    s = RtallySync(out, keys, meta, args.max_new)
    t0 = time.time()
    # 1. changes since the last run (new uploads, edited links, deletions are handled by the daily reconcile below)
    since = meta.get("rtally_since")
    docs = []
    if since:
        docs = groq(f"*[_type==\"movie\" && _updatedAt > \"{since}\"] | order(_updatedAt asc) [0...400] {{{FIELDS}}}")
        for d in docs:
            s.take(d, backfill=False)
    else:
        since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())  # first run: the backfill covers everything up to now
    newest = max([since] + [d.get("_updatedAt") or "" for d in docs])
    print(f"incremental: {len(docs)} docs since {since}")
    # 2. the backfill: Bangla first, then everything, newest first; the cursor survives across runs
    try:
        cur = json.loads(meta.get("rtally_backfill") or "{}")
    except ValueError:
        cur = {}
    phase, offset = cur.get("phase", PHASES[0][0]), int(cur.get("offset", 0))
    done = bool(cur.get("done"))
    while not done and s.new < args.max_new:
        expr = dict(PHASES).get(phase)
        if expr is None:
            done = True
            break
        batch = fetch_docs(expr, "_createdAt desc", offset, args.page)
        if not batch:
            idx = [p for p, _ in PHASES].index(phase)
            if idx + 1 < len(PHASES):
                phase, offset = PHASES[idx + 1][0], 0
                continue
            done = True
            break
        stopped = False
        for i, d in enumerate(batch):
            if not s.take(d, backfill=True):
                offset += i
                stopped = True
                break
        if stopped:
            break
        offset += len(batch)
    print(f"backfill: phase={phase} offset={offset} done={done}")
    s.shelves(state, keys)
    out.add("INSERT OR REPLACE INTO meta (key,value) VALUES ('rtally_since', ?)", newest)
    out.add("INSERT OR REPLACE INTO meta (key,value) VALUES ('rtally_backfill', ?)", json.dumps({"phase": phase, "offset": offset, "done": done}))
    out.add("INSERT OR REPLACE INTO meta (key,value) VALUES ('rtally_last', ?)", str(now()))
    if db is not None:
        apply_sqlite(db, out)
    if args.push:
        push_worker(args.push, args.token, out)
    print(f"stmts={len(out.stmts)} {s.n} took={time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
