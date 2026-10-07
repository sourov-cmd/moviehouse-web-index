-- MovieHouse catalog index (D1 / SQLite). Written by sync.py, read by the web Worker.
-- Every row is MovieBox's own record, normalised; nothing here is invented.

CREATE TABLE IF NOT EXISTS subjects (
  id TEXT PRIMARY KEY,                 -- MovieBox subjectId (19-digit string)
  type INTEGER NOT NULL,               -- 1 movie, 2 tv, 4 anime, 5 kids, 6 music, 7 short drama, 8 game, 9 sports, 10 food
  title TEXT NOT NULL,
  slug TEXT NOT NULL,                  -- url slug of the clean title
  description TEXT,
  release_date TEXT,                   -- YYYY-MM-DD as published
  year INTEGER,
  duration_s INTEGER,                  -- film runtime in seconds (0 unknown)
  genre TEXT,                          -- "Action, Adventure, Comedy" as published
  country TEXT,
  language TEXT,                       -- original language as published
  imdb REAL,
  cover_url TEXT, cover_w INTEGER, cover_h INTEGER, cover_blur TEXT,
  still_url TEXT, still_w INTEGER, still_h INTEGER,
  content_rating TEXT,                 -- "TV-PG", "R", "A" ... or NULL
  restrict_kid INTEGER DEFAULT 0,
  corner TEXT,                         -- their dub badge text ("Hindi", "Bengali")
  detail_url TEXT,                     -- their web detail URL (carries their slug)
  has_resource INTEGER DEFAULT 1,      -- 0 = coming soon
  is_cam INTEGER DEFAULT 0,
  se_num INTEGER DEFAULT 0,            -- seasons count they publish on the card
  subtitles TEXT,                      -- "English,Hindi,..." as published
  aka TEXT,
  viewers INTEGER DEFAULT 0,
  adult INTEGER DEFAULT 0,             -- app rule: adult genre, adult shelf, or published index
  mature INTEGER DEFAULT 0,            -- app rule: adult OR restrictKid OR adult certificate
  resolutions TEXT,                    -- "480,720,1080" available per resourceDetectors (detail)
  codecs TEXT,                         -- "h264,hevc"
  detail_at INTEGER DEFAULT 0,         -- unix s when subject-api/get was last read (0 = card data only)
  first_seen INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  sync_hash TEXT                       -- hash of what the sync last sent; the upsert skips unchanged rows
);
CREATE INDEX IF NOT EXISTS subjects_type_updated ON subjects(type, updated_at DESC);
CREATE INDEX IF NOT EXISTS subjects_slug ON subjects(slug);
CREATE INDEX IF NOT EXISTS subjects_detail ON subjects(detail_at);
CREATE INDEX IF NOT EXISTS subjects_year ON subjects(type, year DESC);

CREATE TABLE IF NOT EXISTS subject_genres (
  subject_id TEXT NOT NULL, genre TEXT NOT NULL,
  type INTEGER NOT NULL DEFAULT 0, ok INTEGER NOT NULL DEFAULT 0, viewers INTEGER NOT NULL DEFAULT 0,  -- copied from subjects by trigger (rank index)
  PRIMARY KEY (subject_id, genre)
);
CREATE INDEX IF NOT EXISTS subject_genres_genre ON subject_genres(genre, subject_id);

CREATE TABLE IF NOT EXISTS staff (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, slug TEXT NOT NULL, avatar TEXT, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS subject_staff (
  subject_id TEXT NOT NULL, staff_id TEXT NOT NULL, staff_type INTEGER, character TEXT, pos INTEGER,
  PRIMARY KEY (subject_id, staff_id)
);
CREATE INDEX IF NOT EXISTS subject_staff_staff ON subject_staff(staff_id);

CREATE TABLE IF NOT EXISTS seasons (
  subject_id TEXT NOT NULL, se INTEGER NOT NULL, max_ep INTEGER NOT NULL, resolutions TEXT, updated_at INTEGER NOT NULL,
  PRIMARY KEY (subject_id, se)
);

-- Layout: tabs from the recipe; shelves either theirs (tab-operating sections) or ours (recipe rails).
CREATE TABLE IF NOT EXISTS tabs (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, src_tab INTEGER DEFAULT 0, pos INTEGER NOT NULL, adult INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS shelves (
  id TEXT PRIMARY KEY,                 -- "t<srcTab>-<opId>" for theirs, rail id for ours
  tab_id TEXT NOT NULL,
  title TEXT NOT NULL,
  slug TEXT NOT NULL,                  -- for /c/<slug>
  kind TEXT NOT NULL,                  -- 'their' | 'rail'
  stype TEXT,                          -- SUBJECTS_MOVIE / RANKING_LIST_MULTI_TAB / CUSTOM / APPOINTMENT_LIST / rail
  pos INTEGER NOT NULL,
  category_id TEXT,                    -- their ranking category for "More"
  rail_json TEXT,                      -- list filter params for a rail (subjectType, genre, country, classify, sort)
  adult INTEGER DEFAULT 0,
  shorts INTEGER DEFAULT 0,            -- 1 when the shelf is short drama (never on the front page)
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS shelves_tab ON shelves(tab_id, pos);
CREATE INDEX IF NOT EXISTS shelves_slug ON shelves(slug);
CREATE TABLE IF NOT EXISTS shelf_groups (
  shelf_id TEXT NOT NULL, gpos INTEGER NOT NULL, title TEXT NOT NULL, category_id TEXT,
  PRIMARY KEY (shelf_id, gpos)
);
CREATE TABLE IF NOT EXISTS shelf_items (
  shelf_id TEXT NOT NULL, gpos INTEGER NOT NULL DEFAULT 0, pos INTEGER NOT NULL, subject_id TEXT NOT NULL,
  PRIMARY KEY (shelf_id, gpos, pos)
);
CREATE INDEX IF NOT EXISTS shelf_items_subject ON shelf_items(subject_id);

CREATE TABLE IF NOT EXISTS banners (
  tab_id TEXT NOT NULL, pos INTEGER NOT NULL, image_url TEXT NOT NULL, w INTEGER, h INTEGER, blur TEXT,
  content TEXT, subject_id TEXT, interval_s INTEGER DEFAULT 4,
  PRIMARY KEY (tab_id, pos)
);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- Subjects a visitor opened that the index did not have yet (live-only pages). The next sync reads
-- them first, so a title that is live on MovieBox becomes a crawlable page within one sync.
CREATE TABLE IF NOT EXISTS wanted (
  id TEXT PRIMARY KEY,
  hits INTEGER NOT NULL DEFAULT 1,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL
);
-- Indexes for the reads that otherwise scan the whole subjects table (D1 bills every scanned row).
CREATE INDEX IF NOT EXISTS subjects_first_seen ON subjects(first_seen);
CREATE INDEX IF NOT EXISTS subjects_browse ON subjects(adult, type, release_date DESC);
CREATE INDEX IF NOT EXISTS subjects_country ON subjects(country);
CREATE INDEX IF NOT EXISTS subjects_corner ON subjects(corner);
CREATE INDEX IF NOT EXISTS subjects_resource_seen ON subjects(adult, has_resource, first_seen DESC);


-- read-path objects (web migration 0004)
CREATE INDEX IF NOT EXISTS subject_genres_rank ON subject_genres(genre, type, ok, viewers DESC);

CREATE TRIGGER IF NOT EXISTS subject_genres_ai AFTER INSERT ON subject_genres
BEGIN
  UPDATE subject_genres SET type=s.type, ok=(s.adult=0 AND s.has_resource=1), viewers=s.viewers
    FROM subjects s WHERE s.id=new.subject_id AND subject_genres.subject_id=new.subject_id AND subject_genres.genre=new.genre;
END;
CREATE TRIGGER IF NOT EXISTS subjects_rank_ai AFTER INSERT ON subjects
BEGIN
  UPDATE subject_genres SET type=new.type, ok=(new.adult=0 AND new.has_resource=1), viewers=new.viewers WHERE subject_id=new.id;
END;
CREATE TRIGGER IF NOT EXISTS subjects_rank_au AFTER UPDATE OF type,adult,has_resource,viewers ON subjects
WHEN old.type IS NOT new.type OR old.adult IS NOT new.adult OR old.has_resource IS NOT new.has_resource OR old.viewers IS NOT new.viewers
BEGIN
  UPDATE subject_genres SET type=new.type, ok=(new.adult=0 AND new.has_resource=1), viewers=new.viewers WHERE subject_id=new.id;
END;

CREATE INDEX IF NOT EXISTS subjects_pop ON subjects(adult, type, viewers DESC);
CREATE INDEX IF NOT EXISTS subjects_top ON subjects(adult, type, imdb DESC);
CREATE INDEX IF NOT EXISTS subjects_country_pop ON subjects(adult, country, viewers DESC);
CREATE INDEX IF NOT EXISTS subjects_corner_pop ON subjects(adult, corner, viewers DESC);
CREATE INDEX IF NOT EXISTS subjects_country_new ON subjects(adult, country, release_date DESC);
CREATE INDEX IF NOT EXISTS subjects_corner_new ON subjects(adult, corner, release_date DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS subjects_fts USING fts5(title, aka, content='subjects', content_rowid='rowid', tokenize='trigram');
CREATE TRIGGER IF NOT EXISTS subjects_fts_ai AFTER INSERT ON subjects
BEGIN INSERT INTO subjects_fts(rowid,title,aka) VALUES (new.rowid,new.title,new.aka); END;
CREATE TRIGGER IF NOT EXISTS subjects_fts_ad AFTER DELETE ON subjects
BEGIN INSERT INTO subjects_fts(subjects_fts,rowid,title,aka) VALUES ('delete',old.rowid,old.title,old.aka); END;
CREATE TRIGGER IF NOT EXISTS subjects_fts_au AFTER UPDATE OF title,aka ON subjects
WHEN old.title IS NOT new.title OR old.aka IS NOT new.aka
BEGIN
  INSERT INTO subjects_fts(subjects_fts,rowid,title,aka) VALUES ('delete',old.rowid,old.title,old.aka);
  INSERT INTO subjects_fts(rowid,title,aka) VALUES (new.rowid,new.title,new.aka);
END;
INSERT INTO subjects_fts(subjects_fts) VALUES ('rebuild');
