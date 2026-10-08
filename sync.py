"""MovieHouse catalog sync — reads MovieBox the way the app does and writes the catalog index.

Runs where MovieBox is reachable (GitHub Actions, a dev box), never on Cloudflare.
Output: SQL statements applied to a local SQLite file (--db) and/or pushed to the web
Worker's /ingest (--push URL, bearer from --token or $INGEST_TOKEN / GitHub OIDC).

  uv run python sync.py --db catalog.sqlite --budget 1200
  uv run python sync.py --db catalog.sqlite --only tabs      # tabs + banners + their shelves only
  uv run python sync.py --db catalog.sqlite --only details   # subject details backlog only

Rules ported from the app (MovieBoxCards / HomeShelves / MovieBoxMobile):
  * adult = published index (/v1/mb-adult) OR adult genre OR fetched from an adult shelf/rail; never title text
  * mature = adult OR restrictKid OR adult certificate (R, TV-MA, A, ...)
  * ranking pages end only on an EMPTY page; list pages follow pager.hasMore; perPage max 20
  * blank genre/country/classify are omitted from list bodies, never sent empty
"""
from __future__ import annotations

import argparse, hashlib, json, os, re, sqlite3, sys, time, unicodedata, uuid
from dataclasses import dataclass, field

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "scripts"))  # dev checkout: shared client lives in scripts/
from mb_client import MB  # noqa: E402

GATEWAY = "https://api.sourovstore.dev"
APP_UA = "MovieHouse-Android/1.2.41"
ADULT_GENRES = {"erotic", "hot", "adult", "porn", "softcore", "xxx"}
ADULT_CERTS = {"R", "NC-17", "X", "AO", "TV-MA", "18", "18+", "A", "R18", "R18+", "M18", "MA15+", "21"}
SHORT_DRAMA = 7
HOME_KEEP = {re.sub(r"[^a-z0-9]", "", s) for s in [
    "hot short tv", "movies in minutes", "k drama shorts", "cricket viral shorts",
    "hot movie roundup", "shorts", "anime shorts", "late night shorts"]}

TAG_RE = re.compile(r"\s*\[[^\]]*\]")  # "[Hindi]", "[CAM]"


def now() -> int:
    return int(time.time())


def clean_title(t: str) -> str:
    return TAG_RE.sub("", t or "").strip() or (t or "").strip()


def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return s[:80] or "title"


def key(title: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (title or "").lower())


def country_of(v) -> str:
    """Their rows sometimes carry "Also Watched • 2.4M" where a country goes; that is a hint, not a place."""
    c = (v or "").strip()
    return "" if not c or re.search(r"•|watched|\d[KM]\b", c, re.I) else c


def year_of(d: str | None) -> int | None:
    m = re.match(r"(\d{4})", d or "")
    return int(m.group(1)) if m else None


def genres_of(g: str | None) -> list[str]:
    return [x.strip() for x in (g or "").split(",") if x.strip()]


@dataclass
class Out:
    """Collected SQL (sql, params) in order; applied to SQLite and/or pushed."""
    stmts: list[tuple[str, tuple]] = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    applied: int = 0

    def add(self, sql: str, *params):
        self.stmts.append((sql, params))

    def bump(self, k: str, n: int = 1):
        self.counts[k] = self.counts.get(k, 0) + n


SHELF_COLS = ("id", "tab_id", "title", "slug", "kind", "stype", "pos", "category_id", "rail_json", "adult", "shorts")
BANNER_COLS = ("tab_id", "pos", "image_url", "w", "h", "blur", "content", "subject_id", "interval_s")
GROUP_COLS = ("shelf_id", "gpos", "title", "category_id")
TAB_COLS = ("id", "title", "src_tab", "pos", "adult")


def _norm(v):
    return None if v is None else (int(v) if isinstance(v, bool) else v)


def _row(d: dict, cols: tuple) -> tuple:
    return tuple(_norm(d.get(c)) for c in cols)


@dataclass
class Layout:
    """The tabs/banners/shelves/groups/items this run wants, kept aside and written as a diff against what the
    index holds (Worker /ingest?plan=1&layout=…, or the local db). Before, every run deleted and re-inserted
    every shelf of every tab: ~200k D1 rows written a day against a 100k/day limit (measured 2026-10-06)."""
    tabs: list[tuple] | None = None                          # TAB_COLS rows, or None when the tabs phase did not run
    banners: dict[str, list[tuple]] = field(default_factory=dict)   # tab_id -> BANNER_COLS rows (tabs this run read)
    their: dict[str, dict[str, tuple]] = field(default_factory=dict)  # tab_id -> shelf_id -> SHELF_COLS row
    rails: dict[str, tuple] = field(default_factory=dict)    # rail id -> SHELF_COLS row
    groups: dict[str, list[tuple]] = field(default_factory=dict)  # shelf_id -> GROUP_COLS rows
    items: dict[tuple[str, int], list[str]] = field(default_factory=dict)  # (shelf_id, gpos) -> subject ids by pos

    def emit(self, out: Out, state: dict | None, t: int) -> dict:
        """Statements that turn `state` into this layout. No state = the old full rewrite (nothing is ever lost)."""
        n = {"tabs": 0, "banners": 0, "shelves": 0, "groups": 0, "items": 0, "deleted": 0}
        st = state or {}
        cur_tabs = [_row(r, TAB_COLS) for r in st.get("tabs") or []]
        cur_banners: dict[str, list[tuple]] = {}
        for r in st.get("banners") or []:
            cur_banners.setdefault(r["tab_id"], []).append(_row(r, BANNER_COLS))
        cur_shelves: dict[str, tuple] = {r["id"]: _row(r, SHELF_COLS) for r in st.get("shelves") or []}
        cur_groups: dict[str, list[tuple]] = {}
        for r in st.get("groups") or []:
            cur_groups.setdefault(r["shelf_id"], []).append(_row(r, GROUP_COLS))
        cur_items: dict[tuple[str, int], list[str]] = {}
        for r in st.get("items") or []:
            cur_items.setdefault((r["shelf_id"], int(r["gpos"])), []).append(str(r["subject_id"]))

        if self.tabs is not None and (state is None or cur_tabs != self.tabs):
            out.add("DELETE FROM tabs")
            for row in self.tabs:
                out.add("INSERT INTO tabs (id,title,src_tab,pos,adult) VALUES (?,?,?,?,?)", *row)
            n["tabs"] = len(self.tabs)

        for tab_id, rows in self.banners.items():
            if state is None or cur_banners.get(tab_id, []) != rows:
                out.add("DELETE FROM banners WHERE tab_id=?", tab_id)
                for row in rows:
                    out.add("INSERT INTO banners (tab_id,pos,image_url,w,h,blur,content,subject_id,interval_s) VALUES (?,?,?,?,?,?,?,?,?)", *row)
                n["banners"] += len(rows)

        def items_diff(key: tuple[str, int], want: list[str]):
            have = cur_items.get(key, []) if state is not None else []
            sid, gpos = key
            for pos, s in enumerate(want):
                if pos >= len(have) or have[pos] != s:
                    out.add("INSERT OR REPLACE INTO shelf_items (shelf_id,gpos,pos,subject_id) VALUES (?,?,?,?)", sid, gpos, pos, s)
                    n["items"] += 1
            if len(have) > len(want):
                out.add("DELETE FROM shelf_items WHERE shelf_id=? AND gpos=? AND pos>=?", sid, gpos, len(want))
                n["deleted"] += len(have) - len(want)

        def shelf_write(row: tuple):
            if state is None or cur_shelves.get(row[0]) != row:
                out.add("INSERT INTO shelves (id,tab_id,title,slug,kind,stype,pos,category_id,rail_json,adult,shorts,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(id) DO UPDATE SET tab_id=excluded.tab_id,title=excluded.title,slug=excluded.slug,kind=excluded.kind,stype=excluded.stype,pos=excluded.pos,"
                        "category_id=excluded.category_id,rail_json=excluded.rail_json,adult=excluded.adult,shorts=excluded.shorts,updated_at=excluded.updated_at", *row, t)
                n["shelves"] += 1
            want_groups = self.groups.get(row[0], [])
            if state is None or cur_groups.get(row[0], []) != want_groups:
                if state is not None and cur_groups.get(row[0]):
                    out.add("DELETE FROM shelf_groups WHERE shelf_id=?", row[0])
                for g in want_groups:
                    out.add("INSERT OR REPLACE INTO shelf_groups (shelf_id,gpos,title,category_id) VALUES (?,?,?,?)", *g)
                n["groups"] += len(want_groups)
            want_g = {k[1] for k in self.items if k[0] == row[0]}
            for key in list(self.items):
                if key[0] == row[0]:
                    items_diff(key, self.items[key])
            if state is not None:
                for key in cur_items:
                    if key[0] == row[0] and key[1] not in want_g:
                        out.add("DELETE FROM shelf_items WHERE shelf_id=? AND gpos=?", key[0], key[1])
                        n["deleted"] += len(cur_items[key])

        for tab_id, shelves in self.their.items():
            if state is not None:
                for sid, row in cur_shelves.items():
                    if row[1] == tab_id and row[4] == "their" and sid not in shelves and not sid.startswith("src-"):  # src-*: a second source's shelf (rtally_sync.py)
                        out.add("DELETE FROM shelf_items WHERE shelf_id=?", sid)
                        out.add("DELETE FROM shelf_groups WHERE shelf_id=?", sid)
                        out.add("DELETE FROM shelves WHERE id=?", sid)
                        n["deleted"] += 1
            else:
                out.add("DELETE FROM shelf_items WHERE shelf_id IN (SELECT id FROM shelves WHERE tab_id=? AND kind='their' AND id NOT LIKE 'src-%')", tab_id)
                out.add("DELETE FROM shelf_groups WHERE shelf_id IN (SELECT id FROM shelves WHERE tab_id=? AND kind='their' AND id NOT LIKE 'src-%')", tab_id)
                out.add("DELETE FROM shelves WHERE tab_id=? AND kind='their' AND id NOT LIKE 'src-%'", tab_id)
            for row in shelves.values():
                shelf_write(row)
        for row in self.rails.values():
            if state is None:
                out.add("DELETE FROM shelf_items WHERE shelf_id=?", row[0])
            shelf_write(row)
        return n


def layout_state_from_db(db: sqlite3.Connection, all_kinds: bool) -> dict:
    """The same picture /ingest?plan=1&layout=… gives, from a local SQLite copy."""
    def q(sql, cols):
        return [dict(zip(cols, r)) for r in db.execute(sql).fetchall()]
    kind = "" if all_kinds else " WHERE kind='their'"
    skind = "" if all_kinds else " WHERE s.kind='their'"
    return {
        "tabs": q("SELECT id,title,src_tab,pos,adult FROM tabs ORDER BY pos", TAB_COLS),
        "banners": q("SELECT tab_id,pos,image_url,w,h,blur,content,subject_id,interval_s FROM banners ORDER BY tab_id,pos", BANNER_COLS),
        "shelves": q(f"SELECT id,tab_id,title,slug,kind,stype,pos,category_id,rail_json,adult,shorts FROM shelves{kind} ORDER BY tab_id,pos", SHELF_COLS),
        "groups": q(f"SELECT g.shelf_id,g.gpos,g.title,g.category_id FROM shelf_groups g JOIN shelves s ON s.id=g.shelf_id{skind} ORDER BY g.shelf_id,g.gpos", GROUP_COLS),
        "items": q(f"SELECT i.shelf_id,i.gpos,i.pos,i.subject_id FROM shelf_items i JOIN shelves s ON s.id=i.shelf_id{skind} ORDER BY i.shelf_id,i.gpos,i.pos", ("shelf_id", "gpos", "pos", "subject_id")),
    }


def merge_subject(old: tuple, new: tuple) -> tuple:
    """Two sightings of one title in a run, folded the way the SQL upsert folds a new sighting into the stored row."""
    o, n = list(old), list(new)
    last = {0, 1, 2, 3, 13, 14, 17, 18, 20, 23, 24}                       # id,type,title,slug,cover_w/h,still_w/h,restrict_kid,has_resource,is_cam
    nonempty = {4, 8, 9, 10, 12, 15, 16, 21, 22, 26, 27}                  # description,genre,country,language,cover_url,cover_blur,still_url,corner,detail_url,subtitles,aka
    coalesce = {5, 6, 11, 19, 31, 32}                                     # release_date,year,imdb,content_rating,resolutions,codecs
    mx = {25, 28, 29, 30}                                                 # se_num,viewers,adult,mature
    out = []
    for i in range(len(n)):
        if i in last:
            out.append(n[i])
        elif i in nonempty:
            out.append(n[i] if (n[i] or "") != "" else o[i])
        elif i in coalesce:
            out.append(n[i] if n[i] is not None else o[i])
        elif i in mx:
            out.append(max(n[i] or 0, o[i] or 0))
        elif i == 7:                                                      # duration_s: a known runtime wins
            out.append(n[i] if (n[i] or 0) > 0 else o[i])
        else:
            out.append(n[i])
    return tuple(out)


def quantize(v: int) -> int:
    """Viewer counts drift every run; two significant digits keep the row's hash still until the count really moves."""
    v = int(v or 0)
    if v < 100:
        return v
    return int(float(f"{v:.2g}"))


class Sync:
    def __init__(self, mb: MB, out: Out, adult_ids: set[str], safe_ids: set[str], budget: int, db: sqlite3.Connection | None, plan: dict | None = None):
        # what the Worker says needs reading when there is no local db (GitHub Actions runs): {wanted, details, seasons}
        self.plan = plan or {}
        self.mb, self.out, self.adult_ids, self.safe_ids = mb, out, adult_ids, safe_ids
        self.budget = budget
        self.calls = 0
        self.db = db
        self.seen_subjects: set[str] = set()
        self.fresh_series: list[str] = []   # series upserted with detail this run (seasons follow)
        self.adult_subjects: set[str] = set()
        self.layout = Layout()
        self.pending: dict[str, dict] = {}   # sid -> merged values waiting for flush_subjects()

    # ---- budget
    def spend(self, n=1) -> bool:
        if self.calls + n > self.budget:
            return False
        self.calls += n
        return True

    # ---- subjects
    def is_adult(self, s: dict, from_adult_rail: bool) -> bool:
        sid = str(s.get("subjectId") or "")
        if sid in self.adult_ids:
            return True
        if sid in self.safe_ids:
            return False
        if from_adult_rail:
            return True
        return any(g.lower() in ADULT_GENRES for g in genres_of(s.get("genre")))

    def upsert_subject(self, s: dict, from_adult_rail=False, detail=False):
        sid = str(s.get("subjectId") or "")
        title = (s.get("title") or "").strip()
        if not sid or not title:
            return
        cover = s.get("cover") or {}
        still = s.get("stills") or {}
        adult = self.is_adult(s, from_adult_rail)
        if adult:
            self.adult_subjects.add(sid)
        cert = (s.get("contentRating") or "").strip() or None
        restrict = 1 if (s.get("restrictKid") in (1, True, "1")) else 0
        mature = 1 if (adult or restrict or (cert and cert.upper() in ADULT_CERTS)) else 0
        rd = (s.get("releaseDate") or "").strip() or None
        dur = s.get("durationSeconds") or 0
        if not dur:
            d = s.get("duration")
            if isinstance(d, (int, float)):
                dur = int(d)
            elif isinstance(d, str) and d.isdigit():
                dur = int(d)
        imdb = None
        try:
            imdb = float(s.get("imdbRatingValue") or s.get("imdbRate") or 0) or None
        except (TypeError, ValueError):
            pass
        t = now()
        ct = clean_title(title)
        # resolutions/codecs only come with the detail answer (resourceDetectors)
        res, codecs = None, None
        if detail:
            heights, cods = set(), set()
            for det in s.get("resourceDetectors") or []:
                for r in det.get("resolutionList") or []:
                    if r.get("resolution"):
                        heights.add(int(r["resolution"]))
                    if r.get("codecName"):
                        cods.add(str(r["codecName"]).lower())
            res = ",".join(str(h) for h in sorted(heights)) if heights else ""
            codecs = ",".join(sorted(cods))
        values = (
            sid, int(s.get("subjectType") or 0), title, slugify(ct), (s.get("description") or "").strip(), rd, year_of(rd), int(dur or 0),
            (s.get("genre") or "").strip(), country_of(s.get("countryName")), (s.get("language") or "").strip(), imdb,
            cover.get("url") or "", cover.get("width") or 0, cover.get("height") or 0, cover.get("thumbnail") or "",
            still.get("url") or "", still.get("width") or 0, still.get("height") or 0, cert, restrict, (s.get("corner") or "").strip(),
            (s.get("detailUrl") or "").strip(), 0 if s.get("hasResource") is False else 1, 1 if s.get("isCam") else 0,
            int(s.get("seNum") or s.get("season") or 0), (s.get("subtitles") or "").strip(), (s.get("aka") or "").strip(),
            int(s.get("viewers") or 0), 1 if adult else 0, mature, res, codecs,
        )
        # A title met on several shelves in one run is merged here (the same rules as the SQL below) and written once,
        # so its hash is the same from run to run; written per sighting, two sightings with different card data
        # would rewrite the row twice every run (measured locally: 10% of the titles).
        prev = self.pending.get(sid)
        if prev:
            values = merge_subject(prev["values"], values)
            detail = detail or prev["detail"]
        self.pending[sid] = {"values": values, "detail": detail, "t": t}
        for g in genres_of(s.get("genre")):
            self.out.add("INSERT OR IGNORE INTO subject_genres (subject_id, genre) VALUES (?,?)", sid, g)
        if detail:
            if int(s.get("subjectType") or 0) == 2:
                self.fresh_series.append(sid)
            self.out.add("DELETE FROM subject_staff WHERE subject_id=?", sid)
            for i, st in enumerate(s.get("staffList") or []):
                stid, name = str(st.get("staffId") or ""), (st.get("name") or "").strip()
                if not stid or not name:
                    continue
                self.out.add(
                    "INSERT INTO staff (id,name,slug,avatar,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name, slug=excluded.slug, avatar=COALESCE(NULLIF(excluded.avatar,''), staff.avatar), updated_at=excluded.updated_at",
                    stid, name, slugify(name), st.get("avatarUrl") or "", t)
                self.out.add("INSERT OR REPLACE INTO subject_staff (subject_id,staff_id,staff_type,character,pos) VALUES (?,?,?,?,?)",
                             sid, stid, int(st.get("staffType") or 0), (st.get("character") or "").strip() or None, i)

    def flush_subjects(self):
        """One upsert per title seen since the last flush. The hash of what is sent (viewers quantised) goes with it;
        the upsert's WHERE skips the write when the row was last written from the same data (measured 2026-10-07)."""
        for sid, p in self.pending.items():
            values, detail, t = p["values"], p["detail"], p["t"]
            sync_hash = hashlib.md5(json.dumps(values[:28] + (quantize(values[28]),) + values[29:], ensure_ascii=False).encode()).hexdigest()[:16]
            self._emit_subject(values, detail, t, sync_hash)
        self.out.bump("subjects", len(self.pending))
        self.pending = {}

    def _emit_subject(self, values: tuple, detail: bool, t: int, sync_hash: str):
        self.out.add(
            """INSERT INTO subjects (id,type,title,slug,description,release_date,year,duration_s,genre,country,language,imdb,
                 cover_url,cover_w,cover_h,cover_blur,still_url,still_w,still_h,content_rating,restrict_kid,corner,detail_url,
                 has_resource,is_cam,se_num,subtitles,aka,viewers,adult,mature,resolutions,codecs,detail_at,first_seen,updated_at,sync_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 type=excluded.type, title=excluded.title, slug=excluded.slug,
                 description=CASE WHEN length(excluded.description)>0 THEN excluded.description ELSE subjects.description END,
                 release_date=COALESCE(excluded.release_date, subjects.release_date), year=COALESCE(excluded.year, subjects.year),
                 duration_s=CASE WHEN excluded.duration_s>0 THEN excluded.duration_s ELSE subjects.duration_s END,
                 genre=CASE WHEN length(excluded.genre)>0 THEN excluded.genre ELSE subjects.genre END,
                 country=COALESCE(NULLIF(excluded.country,''), subjects.country), language=COALESCE(NULLIF(excluded.language,''), subjects.language),
                 imdb=COALESCE(excluded.imdb, subjects.imdb),
                 cover_url=COALESCE(NULLIF(excluded.cover_url,''), subjects.cover_url), cover_w=excluded.cover_w, cover_h=excluded.cover_h,
                 cover_blur=COALESCE(NULLIF(excluded.cover_blur,''), subjects.cover_blur),
                 still_url=COALESCE(NULLIF(excluded.still_url,''), subjects.still_url), still_w=excluded.still_w, still_h=excluded.still_h,
                 content_rating=COALESCE(excluded.content_rating, subjects.content_rating), restrict_kid=excluded.restrict_kid,
                 corner=COALESCE(NULLIF(excluded.corner,''), subjects.corner), detail_url=COALESCE(NULLIF(excluded.detail_url,''), subjects.detail_url),
                 has_resource=excluded.has_resource, is_cam=excluded.is_cam, se_num=MAX(excluded.se_num, subjects.se_num),
                 subtitles=COALESCE(NULLIF(excluded.subtitles,''), subjects.subtitles), aka=COALESCE(NULLIF(excluded.aka,''), subjects.aka),
                 viewers=MAX(excluded.viewers, subjects.viewers),
                 adult=MAX(excluded.adult, subjects.adult), mature=MAX(excluded.mature, subjects.mature),
                 resolutions=COALESCE(excluded.resolutions, subjects.resolutions), codecs=COALESCE(excluded.codecs, subjects.codecs),
                 detail_at=MAX(excluded.detail_at, subjects.detail_at), updated_at=excluded.updated_at, sync_hash=excluded.sync_hash
               WHERE subjects.sync_hash IS NOT excluded.sync_hash""",
            *values, t if detail else 0, t, t, sync_hash,
        )

    def sync_tabs(self, recipe: dict):
        t = now()
        tabs = list(recipe.get("tabs") or [])
        rows = [(tab["id"], tab.get("title") or tab["id"], int(tab.get("srcTab") or 0), i, 1 if tab.get("adult") else 0) for i, tab in enumerate(tabs)]
        # Music is the app's own section (bottom bar), fed by their music channel's tab: pos 90+ keeps it out of the strip
        music_src = int((recipe.get("music") or {}).get("srcTab") or 0)
        if music_src > 0:
            rows.append(("music", "Music", music_src, 90, 0))
            tabs.append({"id": "music", "title": "Music", "srcTab": music_src})
        self.layout.tabs = rows
        for tab in tabs:
            src = int(tab.get("srcTab") or 0)
            if src <= 0 or not self.spend():
                continue
            j = self.mb.tab_operating(src)
            if not j:
                print(f"  tab {tab['id']} ({src}): no answer {self.mb.last[0]}", file=sys.stderr)
                continue
            self.apply_tab(tab, j, t)

    def apply_tab(self, tab: dict, j: dict, t: int):
        tab_id = tab["id"]
        adult_tab = bool(tab.get("adult"))
        items = (j.get("data") or {}).get("items") or []
        # banners
        banner_rows: list[tuple] = []
        pos = 0
        for o in items:
            b = o.get("banner") or {}
            for it in b.get("banners") or []:
                img = it.get("image") or {}
                if not img.get("url"):
                    continue
                sub = it.get("subject")
                if sub:
                    self.upsert_subject(sub, adult_tab)
                banner_rows.append((tab_id, pos, img["url"], img.get("width") or 0, img.get("height") or 0, img.get("thumbnail") or "",
                                    (it.get("content") or (sub or {}).get("title") or "").strip(), str((sub or {}).get("subjectId") or it.get("subjectId") or "") or None,
                                    int(str(b.get("interval") or "4") or 4)))
                pos += 1
        self.layout.banners[tab_id] = banner_rows
        self.out.bump("banners", pos)
        # their shelves, in their order (written as a diff against the index, see Layout)
        shelves = self.layout.their.setdefault(tab_id, {})
        spos = 0
        for o in items:
            title = (o.get("title") or "").strip()
            if not title:
                continue
            stype = o.get("type") or ""
            op = str(o.get("opId") or spos)
            shelf_id = f"t{tab.get('srcTab')}-{op}"
            direct = o.get("subjects") or []
            groups: list[tuple[str, str, list]] = []
            subjects: list = []
            cat = shelf_cat(o, direct)
            if direct:
                subjects = direct
            elif (o.get("rankingListData") or {}).get("items"):
                for g in o["rankingListData"]["items"]:
                    rows = g.get("subjects") or []
                    name = (g.get("title") or "").strip()
                    if rows and name:
                        groups.append((name, str(g.get("categoryId") or "").strip(), rows))
                if not groups:
                    continue
                subjects = groups[0][2]
                cat = groups[0][1]
            elif (o.get("customData") or {}).get("items"):
                link = ""
                for e in o["customData"]["items"]:
                    if e.get("subject"):
                        subjects.append(e["subject"])
                    if not link:
                        link = first_link(e)
                if not subjects:
                    continue
                cat = shelf_cat_of(first_link(o) or link)
            else:
                continue
            types = []
            for s in subjects:
                st = int(s.get("subjectType") or 0)
                if st and st not in types:
                    types.append(st)
            shorts = 1 if (types and types[0] == SHORT_DRAMA) else 0
            # Front page trims unnamed short-drama shelves (HomeShelves.forTab) — keep the row but mark it
            drop = tab_id == "home" and shorts and key(title) not in HOME_KEEP
            if drop:
                continue
            adult_shelf = adult_tab or any(g.lower() in ADULT_GENRES for g in [title])
            shelves[shelf_id] = (shelf_id, tab_id, strip_glyphs(title), slugify(strip_glyphs(title)) + ("" if tab_id == "home" else f"-{tab_id}"), "their", stype, spos, cat or None, None,
                                 1 if adult_shelf else 0, shorts)
            if groups:
                self.layout.groups[shelf_id] = [(shelf_id, gi, name, gcat or None) for gi, (name, gcat, rows) in enumerate(groups)]
                for gi, (name, gcat, rows) in enumerate(groups):
                    self.add_items(shelf_id, gi, rows, adult_shelf)
            else:
                self.add_items(shelf_id, 0, subjects, adult_shelf)
            spos += 1
        self.out.bump("shelves", spos)

    def add_items(self, shelf_id: str, gpos: int, rows: list, adult: bool, start=0):
        seen = set()
        pos = start
        for s in rows:
            sid = str(s.get("subjectId") or "")
            if not sid or sid in seen or not (s.get("title") or "").strip():
                continue
            seen.add(sid)
            self.upsert_subject(s, adult)
            lst = self.layout.items.setdefault((shelf_id, gpos), [])
            if start == 0 and pos == 0:
                lst.clear()
            lst.append(sid)
            pos += 1
        return pos

    def sync_rails(self, recipe: dict, pages: int):
        """Recipe rails → shelves(kind=rail) with the first `pages` list pages each."""
        t = now()
        rails = [r for r in recipe.get("rails") or [] if r.get("tab") and r.get("subjectType")]
        for i, r in enumerate(rails):
            rid = r["id"]
            adult = bool(r.get("adult"))
            if self.calls >= self.budget:
                return
            row = (rid, r["tab"], r.get("title") or rid, slugify(f"{r.get('title') or rid}") + ("" if r["tab"] == "home" else f"-{r['tab']}"), "rail", "rail", 1000 + i, None,
                   json.dumps({k: r[k] for k in ("subjectType", "genre", "country", "classify", "sort", "year") if r.get(k)}), 1 if adult else 0,
                   1 if r.get("subjectType") == SHORT_DRAMA else 0)
            pos = 0
            got = False
            for page in range(1, pages + 1):
                if not self.spend():
                    break
                j = self.mb.list(int(r["subjectType"]), page=page, genre=r.get("genre", ""), country=r.get("country", ""),
                                 classify=r.get("classify", ""), sort=r.get("sort", ""), year=str(r.get("year", "") or ""))
                data = (j or {}).get("data") or {}
                items = data.get("items") or []
                if page == 1:
                    if not j:
                        break   # no answer for this rail: keep what the index has
                    self.layout.items[(rid, 0)] = []
                    got = True
                pos = self.add_items(rid, 0, items, adult, start=pos)
                if not (data.get("pager") or {}).get("hasMore"):
                    break
            if got:
                self.layout.rails[rid] = row
                self.out.bump("rails")

    def sync_clips(self, recipe: dict, pages: int):
        """The Clips section: MovieBox's short dramas (recipe clips.subjectType), the viewer's countries first like the app."""
        st = int((recipe.get("clips") or {}).get("subjectType") or SHORT_DRAMA)
        for country in ("Bangladesh", "India", ""):
            for page in range(1, pages + 1):
                if not self.spend():
                    return
                j = self.mb.list(st, page=page, country=country)
                data = (j or {}).get("data") or {}
                items = data.get("items") or []
                for it in items:
                    sid = str(it.get("subjectId") or "")
                    if sid and (it.get("title") or "").strip():
                        self.upsert_subject(it)
                self.out.bump("clips", len(items))
                if not (data.get("pager") or {}).get("hasMore"):
                    break

    def sync_rankings(self, max_shelves: int, max_pages: int = 10):
        """Their ranking endpoint pages a shelf's category for "More"; a page ends the list only by coming back EMPTY."""
        if self.db is not None:
            rows = self.db.execute(
                """SELECT s.id, s.adult, COALESCE(g.gpos, 0), COALESCE(g.category_id, s.category_id)
                   FROM shelves s LEFT JOIN shelf_groups g ON g.shelf_id = s.id
                   WHERE s.kind='their' AND COALESCE(g.category_id, s.category_id) IS NOT NULL
                   ORDER BY s.updated_at DESC, s.tab_id, s.pos LIMIT ?""", (max_shelves,)).fetchall()
            plan_rows = [(sid, adult, gpos, cat, None) for sid, adult, gpos, cat in rows]
        else:
            plan_rows = [(r["shelf_id"], r.get("adult"), int(r.get("gpos") or 0), r.get("cat"), [str(x) for x in r.get("have") or []])
                         for r in (self.plan.get("rankings") or [])][:max_shelves]
        for shelf_id, adult, gpos, cat, have in plan_rows:
            if not cat:
                continue
            if have is None:
                have = [r[0] for r in self.db.execute("SELECT subject_id FROM shelf_items WHERE shelf_id=? AND gpos=? ORDER BY pos", (shelf_id, gpos))]
            seen = set(have)
            pos = len(have)
            for page in range(1, max_pages + 1):
                if not self.spend():
                    return
                j = self.mb.ranking(cat, page=page)
                lst = ((j or {}).get("data") or {}).get("subjectList") or []
                if not lst:
                    break
                for sub in lst:
                    sid = str(sub.get("subjectId") or "")
                    if not sid or sid in seen or not (sub.get("title") or "").strip():
                        continue
                    seen.add(sid)
                    self.upsert_subject(sub, bool(adult))
                    self.out.add("INSERT OR REPLACE INTO shelf_items (shelf_id,gpos,pos,subject_id) VALUES (?,?,?,?)", shelf_id, gpos, pos, sid)
                    pos += 1
            self.out.bump("rankings")

    def sync_latest(self):
        """Newest films and series by their own Latest sort — what the Telegram bot watches too."""
        for st in (1, 2):
            for page in (1, 2, 3):
                if not self.spend():
                    return
                j = self.mb.list(st, page=page, sort="Latest")
                for s in ((j or {}).get("data") or {}).get("items") or []:
                    self.upsert_subject(s)

    def sync_details(self, limit: int):
        """subject-api/get for subjects without detail yet (staff, resolutions, subtitles). Visitors' wanted ids go first."""
        ids = [str(x) for x in self.plan.get("wanted") or []]
        if self.db is not None:
            rows = self.db.execute(
                """SELECT s.id FROM subjects s
                   WHERE s.detail_at=0 AND s.type IN (1,2,4,5,7)
                   ORDER BY (SELECT COUNT(*) FROM shelf_items i WHERE i.subject_id=s.id) DESC, s.updated_at DESC LIMIT ?""",
                (limit,)).fetchall()
            ids += [r[0] for r in rows if r[0] not in ids]
            wanted_local = self.db.execute("SELECT id FROM wanted ORDER BY hits DESC LIMIT 300").fetchall()
            ids = [r[0] for r in wanted_local if r[0] not in ids] + ids
        else:
            ids += [str(x) for x in self.plan.get("details") or [] if str(x) not in ids]
        # subjects seen this run but not yet in the db
        for sid in list(self.seen_subjects)[: max(0, limit - len(ids))]:
            if sid not in ids:
                ids.append(sid)
        for sid in ids[:limit]:
            if not self.spend():
                return
            j = self.mb.subject(sid)
            d = (j or {}).get("data")
            if not d:
                # not a subject MovieBox will describe — stop asking for it
                self.out.add("DELETE FROM wanted WHERE id=?", sid)
                continue
            self.upsert_subject(d, sid in self.adult_subjects, detail=True)
            self.out.add("DELETE FROM wanted WHERE id=?", sid)
            self.out.bump("details")

    def sync_seasons(self, limit: int):
        if self.db is not None:
            rows = self.db.execute(
                """SELECT s.id FROM subjects s WHERE s.type=2 AND NOT EXISTS (SELECT 1 FROM seasons z WHERE z.subject_id=s.id)
                   ORDER BY (SELECT COUNT(*) FROM shelf_items i WHERE i.subject_id=s.id) DESC, s.updated_at DESC LIMIT ?""", (limit,)).fetchall()
            ids = [r[0] for r in rows]
        else:
            ids = [str(x) for x in self.plan.get("seasons") or []][:limit]
        # a series that was just read in full (wanted/detail) needs its seasons in the same run
        for sid in self.fresh_series:
            if sid not in ids:
                ids.append(sid)
        t = now()
        for sid in ids:
            if not self.spend():
                return
            j = self.mb.season_info(sid)
            seasons = ((j or {}).get("data") or {}).get("seasons")
            if seasons is None:
                continue
            self.out.add("DELETE FROM seasons WHERE subject_id=?", sid)
            for se in seasons:
                n = int(se.get("se") or 0)
                if n <= 0:
                    continue
                res = ",".join(str(r.get("resolution")) for r in se.get("resolutions") or [] if r.get("resolution"))
                self.out.add("INSERT INTO seasons (subject_id,se,max_ep,resolutions,updated_at) VALUES (?,?,?,?,?)", sid, n, int(se.get("maxEp") or 0), res, t)
            self.out.bump("seasons")


# ---- helpers (MovieBoxMobile.kt ports)
def first_link(o: dict) -> str:
    for k in ("deepLink", "deeplink", "jumpUrl", "moreUrl", "url"):
        v = o.get(k)
        if v:
            return str(v)
    return ""


def shelf_cat_of(dl: str) -> str:
    for k in ("category=", "categoryType="):
        at = dl.find(k)
        if at < 0:
            continue
        rest = dl[at + len(k):]
        m = re.match(r"\d+", rest)
        if m and len(m.group(0)) >= 6:
            return m.group(0)
    return ""


def shelf_cat(section: dict, subjects: list) -> str:
    own = shelf_cat_of(first_link(section))
    if own:
        return own
    for s in subjects or []:
        hit = shelf_cat_of(first_link(s))
        if hit:
            return hit
    return ""


def strip_glyphs(title: str) -> str:
    """Their names carry pictographs (🔥, 🆓, 👁️); the app draws them without."""
    out = "".join(ch for ch in title if not (unicodedata.category(ch) in ("So", "Cs", "Mn") or ord(ch) in (0xFE0F, 0x200D)))
    return re.sub(r"\s+", " ", out).strip() or title


# ---- io
def fetch_json(url: str, headers: dict | None = None):
    r = httpx.get(url, headers=headers or {}, timeout=30)
    r.raise_for_status()
    return r.json()


def apply_sqlite(db: sqlite3.Connection, out: Out):
    cur = db.cursor()
    cur.execute("BEGIN")
    for sql, params in out.stmts[out.applied:]:
        cur.execute(sql, params)
    db.commit()
    out.applied = len(out.stmts)


def ingest_token(fallback: str) -> str:
    """The bearer for /ingest. On GitHub Actions a fresh OIDC token is minted for every call: the job's tokens live
    five minutes (exp - iat = 300 s), and a run that spends its budget on MovieBox takes longer than that — run 10 on
    2026-10-08 read its plan fine, walked for five minutes, then pushed with the expired token and got 403."""
    url, req = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"), os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if url and req:
        try:
            r = httpx.get(url, params={"audience": "moviehouse-web-ingest"}, headers={"Authorization": f"bearer {req}"}, timeout=30)
            r.raise_for_status()
            tok = r.json().get("value") or ""
            if tok:
                return tok
        except Exception as e:  # fall back to the token minted by the workflow step
            print(f"oidc: mint failed ({str(e)[:120]}), using the step's token", file=sys.stderr)
    return fallback


def push_worker(url: str, token: str, out: Out, chunk=400):
    """POST {stmts:[{sql,params}]} chunks to the Worker's /ingest, which runs D1 batch(). Already-pushed statements
    (out.applied) are skipped, so the sync can push after each phase and a failure late in the run loses little."""
    todo = out.stmts[out.applied:]
    if not todo:
        return
    with httpx.Client(timeout=120) as c:
        for i in range(0, len(todo), chunk):
            body = {"stmts": [{"sql": s, "params": list(p)} for s, p in todo[i:i + chunk]]}
            r = c.post(url, json=body, headers={"authorization": f"Bearer {ingest_token(token)}"})
            if r.status_code != 200:
                raise SystemExit(f"ingest {r.status_code}: {r.text[:300]}")
            out.applied += len(body["stmts"])
    print(f"pushed {len(todo)} statements", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", help="local sqlite file to apply to")
    ap.add_argument("--push", help="Worker /ingest URL")
    ap.add_argument("--token", default=os.environ.get("INGEST_TOKEN", ""))
    ap.add_argument("--budget", type=int, default=1200, help="max MovieBox calls this run")
    ap.add_argument("--rail-pages", type=int, default=3)
    ap.add_argument("--details", type=int, default=400)
    ap.add_argument("--seasons", type=int, default=120)
    ap.add_argument("--rankings", type=int, default=60, help="their shelves to page out via ranking-list this run")
    ap.add_argument("--clip-pages", type=int, default=5)
    ap.add_argument("--only", choices=["tabs", "rails", "details", "seasons", "latest", "rankings", "clips"], action="append")
    ap.add_argument("--proxy", action="store_true")
    ap.add_argument("--auto", action="store_true", help="pick the phases by the clock: the wide walk (rails clips rankings) at 02/08/14/20 UTC, tabs latest details seasons otherwise")
    args = ap.parse_args()
    if args.auto and not args.only:
        # the same split the Worker's cron uses (sync-trigger.ts); GitHub's schedule cannot pass inputs
        now_utc = time.gmtime()
        if now_utc.tm_hour in (2, 8, 14, 20) and now_utc.tm_min < 30:
            args.only = ["rails", "clips", "rankings"]
            args.budget = min(args.budget, 1500)
        else:
            args.only = ["tabs", "latest", "details", "seasons"]
            args.budget, args.details, args.seasons = min(args.budget, 400), min(args.details, 150), min(args.seasons, 60)
        print(f"auto: {' '.join(args.only)} budget={args.budget}")

    recipe = fetch_json(GATEWAY + "/v1/mb-recipe", {"User-Agent": APP_UA})
    adult_doc = fetch_json(GATEWAY + "/v1/mb-adult", {"User-Agent": APP_UA, "X-App-Version": "63"})
    adult_ids, safe_ids = set(adult_doc.get("adult") or []), set(adult_doc.get("safe") or [])
    print(f"recipe v{recipe.get('version')} tabs={len(recipe.get('tabs', []))} rails={len(recipe.get('rails', []))} adult-index={len(adult_ids)}")

    db = None
    if args.db:
        db = sqlite3.connect(args.db)
        schema = next(p for p in (os.path.join(HERE, "schema.sql"), os.path.join(HERE, "..", "schema.sql")) if os.path.exists(p))
        db.executescript(open(schema).read())

    from mb_client import PROXY
    mb = MB(recipe["mobile"], proxy=PROXY if args.proxy else None)
    if not mb.mint():
        raise SystemExit(f"visitor-login failed: {mb.last}")
    out = Out()
    plan = None
    only = set(args.only or [])
    wide = not only or "rails" in only
    if args.push and db is None:
        # the Worker knows what the index lacks; this runner has no copy of the database
        try:
            plan = fetch_json(args.push + f"?plan=1&details={args.details}&seasons={args.seasons}&rankings={args.rankings}&layout={'all' if wide else 'their'}", {"Authorization": f"Bearer {ingest_token(args.token)}"})
            st = plan.get("state") or {}
            print(f"plan: wanted={len(plan.get('wanted', []))} details={len(plan.get('details', []))} seasons={len(plan.get('seasons', []))} rankings={len(plan.get('rankings', []))} "
                  f"layout: shelves={len(st.get('shelves', []))} items={len(st.get('items', []))}")
        except Exception as e:  # the Worker may be over its D1 quota; the tab/rail/clip phases need no plan
            print(f"plan: unavailable ({str(e)[:120]}) — running without it", flush=True)
            plan = {}
    s = Sync(mb, out, adult_ids, safe_ids, args.budget, db, plan)
    t0 = time.time()
    if not only or "tabs" in only:
        s.sync_tabs(recipe)
    if not only or "latest" in only:
        s.sync_latest()
    if not only or "rails" in only:
        s.sync_rails(recipe, args.rail_pages)
    if not only or "clips" in only:
        s.sync_clips(recipe, args.clip_pages)
    s.flush_subjects()
    # the layout (tabs, banners, shelves, items) goes out as a diff against what the index holds
    state = plan.get("state") if plan else (layout_state_from_db(db, wide) if db is not None else None)
    print(f"layout diff: {s.layout.emit(out, state, now())}" + ("" if state is not None else " (no state: full rewrite)"))
    # subjects seen above must exist before detail/season queries read the db; a push after each phase keeps a late failure small
    if db is not None:
        apply_sqlite(db, out)
    if args.push and db is None:
        push_worker(args.push, args.token, out)
    if not only or "rankings" in only:
        s.sync_rankings(args.rankings)
    s.flush_subjects()
    if db is not None:
        apply_sqlite(db, out)
    if args.push and db is None:
        push_worker(args.push, args.token, out)
    if not only or "details" in only:
        s.sync_details(args.details)
    if not only or "seasons" in only:
        s.sync_seasons(args.seasons)
    s.flush_subjects()
    out.add("INSERT OR REPLACE INTO meta (key,value) VALUES ('last_sync', ?)", str(now()))
    out.add("INSERT OR REPLACE INTO meta (key,value) VALUES ('recipe_version', ?)", str(recipe.get("version")))
    if db is not None:
        apply_sqlite(db, out)
    if args.push:
        push_worker(args.push, args.token, out)
    print(f"calls={s.calls} stmts={len(out.stmts)} counts={out.counts} took={time.time()-t0:.0f}s host={mb.good_host}")


if __name__ == "__main__":
    main()

