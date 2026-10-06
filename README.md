# moviehouse-web-index

The catalog sync behind [moviehouse.cyou](https://moviehouse.cyou). A scheduled GitHub Actions job reads the
library the same way the MovieHouse Android app does and writes an index (titles, shelves, people, seasons)
into the website's database, so every title has a server-rendered, crawlable page.

- `sync.py` — the sync (tabs → latest → rails → clips → rankings → details → seasons); see its docstring.
- `mb_client.py` — the signed API client (a port of the app's `MovieBoxMobile.kt`).
- `schema.sql` — the index schema (mirrors the website's D1 migrations).
- `.github/workflows/sync.yml` — the schedule; auth is GitHub OIDC (audience `moviehouse-web-ingest`), no stored secret.

Run locally: `pip install -r requirements.txt && python sync.py --db catalog.sqlite --budget 300`.
