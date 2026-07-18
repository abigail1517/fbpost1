"""
post_facebook_multi.py — MULTI-PAGE / MULTI-THREADED VERSION
──────────────────────────────────────────────────────────────
Runs several Facebook Pages in parallel (each with its own Mega folder,
caption, storage_state / login session, and posting interval), all driven
from one Google Sheet. Safe to run from more than one GitHub repo / runner
at the same time — pages are LOCKED in the sheet while a runner is working
them, so a second repo running concurrently will automatically pick only
the pages nobody else currently owns.

Run modes:
    python -u post_facebook_multi.py --setup-sheet     # create/repair tabs+headers, then exit
    python -u post_facebook_multi.py --once            # one post per claimed page, then exit
    python -u post_facebook_multi.py                   # long-running: loops every page on its
                                                         # own interval until MaxRuntimeMinutes

Identify this runner (used for the lock column) with the REPO_ID env var,
e.g. REPO_ID=repo-a in one GitHub repo and REPO_ID=repo-b in another. If
not set, a random id is generated per run.


═══════════════════════════════════════════════════════════════════════
GOOGLE SHEET LAYOUT  (spreadsheet id: CAPTIONS_SHEET_ID env var, falls
back to DEFAULT_CAPTIONS_SHEET_ID below). Run --setup-sheet once and all
of this is created for you (existing data / other tabs are left alone).
═══════════════════════════════════════════════════════════════════════

Tab "Settings" — MASTER defaults, two columns, one row per setting:
    Key                     Value
    ThreadCount             3          # how many pages to post concurrently
    LoopIntervalMinutes     60         # default minutes between posts, per page
    MaxRuntimeMinutes       300        # default total runtime before exiting
    MegaFolder              fbreels
    MegaMoveFolder          fbreels_uploaded
    Link_Percentage         100
    UrlReplaceCount         1
    UrlReplaceMode          unique
    UrlReplaceEnabled       TRUE
    PostMode                rotation   # "rotation" or "queue" — see Pages.PostMode below
    LockTtlMinutes          90         # a lock older than this is considered abandoned
    HeartbeatMinutes        15         # how often to refresh the sheet lock + storage_state
                                       # while a page is actively running

Tab "Pages" — one row PER FACEBOOK PAGE. Every column except PageId and
FbStorageState is an OVERRIDE: leave it blank to fall back to Settings.
    PageId               REQUIRED, short unique id, e.g. "page1" (used in logs/locks)
    PageName             optional, for logs only
    Status               Active | Paused  (Paused rows are never picked)
    MegaFolder           override
    MegaMoveFolder       override
    Caption              override — the WITH-link caption for "rotation" mode
    WithoutLinkCap       override — the WITHOUT-link caption for "rotation" mode
    Link_Percentage      override
    LoopIntervalMinutes  override
    UrlReplaceCount      override
    UrlReplaceMode       override
    UrlReplaceEnabled    override
    PostMode             override: "rotation" (old behavior) or "queue" (new — see PostQueue tab)
    FbStorageState       Playwright storage_state JSON for THIS page's login session.
                          Seed it once (paste your session JSON); the script refreshes
                          it here after every post AND on a heartbeat, so it never goes
                          stale. Never shared between pages.
    LockedBy             written by the script — which runner currently owns this page
    LockedAt             written by the script — ISO timestamp of the last lock/heartbeat
    LastRunAt            written by the script — ISO timestamp of the last completed run
    LastPostedFile       written by the script — filename of the last successful post
    Notes                free text, ignored by the script

Tab "Urls" — shared (or per-page) pool of URLs to rotate into "rotation"
mode captions. Same as the single-page version, plus an optional PageId
column:
    Urls        one URL per row
    Status      blank = unused; script writes "Posted" after use (never deleted)
    PageId      OPTIONAL. Blank = usable by any page. If filled, that URL
                is only ever selected for that specific PageId.

Tab "PostQueue" — ONLY needed for pages with PostMode = "queue". This is
the "post based on the sheet data itself" mode: instead of a rotating
generic caption, each Mega video is matched by filename to an explicit
caption + hashtags row, and that row is marked Posted once used.
    FileName    exact Mega filename this row is for, e.g. "clip_014.mp4"
    Caption     the caption to use for this exact file
    Hashtags    optional, appended to the caption on its own line
    PageId      OPTIONAL — restrict this row to one page; blank = any page
    Status      blank = pending; script writes "Posted" after a successful post
"""

import asyncio, json, os, random, re, socket, subprocess, sys, tempfile, time, uuid
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone
import functools

print = functools.partial(print, flush=True)

try:
    import schedule  # noqa: F401  (kept optional / unused in this version — real
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False
    print("⚠️  Google API libraries not installed")

from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
FB_STORAGE_STATE_ENV      = "FB_STORAGE_STATE"      # one-time seed fallback only
GOOGLE_CREDS_ENV          = "GOOGLE_CREDENTIALS_JSON"
CAPTIONS_SHEET_ID_ENV     = "CAPTIONS_SHEET_ID"
DEFAULT_CAPTIONS_SHEET_ID = "1ICgS97JJ-Hrs9qsI1UV-xJvPg7ovmSHStGQPoOYr0Dk"

REPO_ID = os.environ.get("REPO_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"

VIDEO_EXTENSIONS  = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
MEGA_REMOTE_NAME  = "mega"
URL_REGEX         = re.compile(r'https?://\S+')
SHEETS_CELL_CHAR_LIMIT = 50000

SETTINGS_TAB   = "Settings"
PAGES_TAB      = "Pages"
URLS_TAB       = "Urls"
POSTQUEUE_TAB  = "PostQueue"

SETTINGS_DEFAULTS = {
    "threadcount":            "3",
    "loopintervalminutes":    "60",
    "maxruntimeminutes":      "300",
    "megafolder":             "fbreels",
    "megamovefolder":         "fbreels_uploaded",
    "link_percentage":        "100",
    "urlreplacecount":        "1",
    "urlreplacemode":         "unique",
    "urlreplaceenabled":      "TRUE",
    "postmode":               "rotation",
    "lockttlminutes":         "90",
    "heartbeatminutes":       "15",
}

PAGES_HEADERS = [
    "PageId", "PageName", "Status", "MegaFolder", "MegaMoveFolder", "Caption",
    "WithoutLinkCap", "Link_Percentage", "LoopIntervalMinutes", "UrlReplaceCount",
    "UrlReplaceMode", "UrlReplaceEnabled", "PostMode", "FbStorageState",
    "LockedBy", "LockedAt", "LastRunAt", "LastPostedFile", "Notes",
]
SETTINGS_HEADERS  = ["Key", "Value"]
URLS_HEADERS      = ["Urls", "Status", "PageId"]
POSTQUEUE_HEADERS = ["FileName", "Caption", "Hashtags", "PageId", "Status"]

STORAGE_STATE_DIR = Path("storage_states")   # local fallback cache, one file per PageId
SCREENSHOTS_DIR    = Path("screenshots")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER — prefixed with PageId so parallel logs stay readable
# ─────────────────────────────────────────────────────────────────────────────
def log(page_id, msg, kind="info"):
    icon = {"info": "ℹ️ ", "ok": "✅", "warn": "⚠️ ", "fail": "❌", "step": "▶️ "}.get(kind, "ℹ️ ")
    tag = f"[{page_id}]" if page_id else "[main]"
    print(f"{icon} {tag} {msg}")

def step(page_id, msg):  log(page_id, msg, "step")
def info(page_id, msg):  log(page_id, msg, "info")
def ok(page_id, msg):    log(page_id, msg, "ok")
def warn(page_id, msg):  log(page_id, msg, "warn")
def fail(page_id, msg):  log(page_id, msg, "fail")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def minutes_since(iso_ts: str) -> float:
    try:
        then = datetime.fromisoformat(iso_ts)
        if then.tzinfo is None:
            then = then.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - then).total_seconds() / 60.0
    except Exception:
        return 10 ** 9   # unparsable / empty -> treat as "ancient", i.e. unlocked


def _to_int(val, default):
    try:
        return int(str(val).strip())
    except (TypeError, ValueError):
        return default


def _to_bool(val, default):
    if val is None or str(val).strip() == "":
        return default
    return str(val).strip().lower() in ("true", "yes", "y", "1", "on", "enable", "enabled")


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS — thin generic layer (tab-aware)
# ─────────────────────────────────────────────────────────────────────────────

def build_google_creds():
    if not HAS_GOOGLE:
        raise RuntimeError("google-auth libraries not installed")
    creds_json = os.environ.get(GOOGLE_CREDS_ENV)
    if not creds_json:
        raise RuntimeError(f"Missing {GOOGLE_CREDS_ENV}")
    data = json.loads(creds_json)
    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes", ["https://www.googleapis.com/auth/spreadsheets"]),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def build_sheets_service(creds):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


class Sheet:
    """Small wrapper around the Sheets v4 API for one spreadsheet."""

    def __init__(self, service, spreadsheet_id):
        self.svc = service
        self.id = spreadsheet_id
        self._tab_cache = None

    # ---- low level -----------------------------------------------------
    def existing_tabs(self, refresh=False):
        if self._tab_cache is None or refresh:
            meta = self.svc.spreadsheets().get(spreadsheetId=self.id).execute()
            self._tab_cache = {s["properties"]["title"] for s in meta.get("sheets", [])}
        return self._tab_cache

    def ensure_tab(self, title: str, headers: list[str]):
        """Creates the tab with a header row if it doesn't exist yet. Never
        touches an existing tab's data (only adds missing header cells if
        the header row is completely empty)."""
        if title not in self.existing_tabs():
            self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.id,
                body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
            ).execute()
            self._tab_cache = None
            ok(None, f"Created missing tab '{title}'")
        rows = self.read_rows(title, "A1:Z1")
        if not rows or not any(c.strip() for c in rows[0]):
            self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{title}'!A1",
                valueInputOption="RAW", body={"values": [headers]},
            ).execute()
            ok(None, f"Wrote header row for '{title}': {headers}")
        else:
            info(None, f"Tab '{title}' already has a header row — leaving it as-is")

    def read_rows(self, tab: str, a1_range: str = "A:Z") -> list[list[str]]:
        try:
            result = self.svc.spreadsheets().values().get(
                spreadsheetId=self.id, range=f"'{tab}'!{a1_range}"
            ).execute()
        except Exception as e:
            warn(None, f"Sheets read failed ('{tab}'!{a1_range}): {e}")
            return []
        return result.get("values", [])

    def write_cell(self, tab: str, row_num: int, col_idx: int, value: str):
        col_letter = self._col_letter(col_idx)
        try:
            self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}",
                valueInputOption="RAW", body={"values": [[value]]},
            ).execute()
            return True
        except Exception as e:
            warn(None, f"Write failed '{tab}'!{col_letter}{row_num}: {e}")
            return False

    def clear_cell(self, tab: str, row_num: int, col_idx: int):
        col_letter = self._col_letter(col_idx)
        try:
            self.svc.spreadsheets().values().clear(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}", body={}
            ).execute()
        except Exception as e:
            warn(None, f"Clear failed '{tab}'!{col_letter}{row_num}: {e}")

    @staticmethod
    def _col_letter(idx: int) -> str:
        letters = ""
        idx += 1
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    # ---- table helpers ---------------------------------------------------
    def as_dicts(self, tab: str):
        """Returns (list_of_row_dicts, header_index_map). Each row dict has
        every header as a key (blank string if the cell is short), plus
        '_row' = the 1-based sheet row number for writing back."""
        rows = self.read_rows(tab)
        if not rows:
            return [], {}
        header = [h.strip() for h in rows[0]]
        col_index = {h.lower(): i for i, h in enumerate(header) if h}
        out = []
        for row_num, row in enumerate(rows[1:], start=2):
            d = {h: (row[i] if i < len(row) else "").strip() for h, i in col_index.items()}
            d["_row"] = row_num
            out.append(d)
        return out, col_index


def setup_sheet(sheets_service, spreadsheet_id):
    step(None, f"Setting up sheet structure on spreadsheet {spreadsheet_id}")
    sh = Sheet(sheets_service, spreadsheet_id)
    sh.ensure_tab(SETTINGS_TAB, SETTINGS_HEADERS)
    sh.ensure_tab(PAGES_TAB, PAGES_HEADERS)
    sh.ensure_tab(URLS_TAB, URLS_HEADERS)
    sh.ensure_tab(POSTQUEUE_TAB, POSTQUEUE_HEADERS)

    # Seed Settings with defaults for any key not already present
    settings_rows, _ = sh.as_dicts(SETTINGS_TAB)
    present_keys = {r.get("Key", "").strip().lower() for r in settings_rows}
    existing = sh.read_rows(SETTINGS_TAB)
    next_row = len(existing) + 1 if existing else 2
    appended = []
    for key, val in SETTINGS_DEFAULTS.items():
        if key not in present_keys:
            appended.append([key, val])
    if appended:
        sh.svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{SETTINGS_TAB}'!A{next_row}",
            valueInputOption="RAW",
            body={"values": appended},
        ).execute()
        ok(None, f"Seeded {len(appended)} default setting(s) into '{SETTINGS_TAB}'")
    else:
        info(None, f"'{SETTINGS_TAB}' already has all default keys")

    pages_rows, _ = sh.as_dicts(PAGES_TAB)
    if not pages_rows:
        warn(None, f"'{PAGES_TAB}' has no page rows yet — add one row per Facebook "
                    f"Page (PageId + FbStorageState are the minimum required fields)")
    ok(None, "Sheet setup complete")


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — master defaults + per-page overrides
# ─────────────────────────────────────────────────────────────────────────────

def load_master_settings(sh: Sheet) -> dict:
    rows, _ = sh.as_dicts(SETTINGS_TAB)
    settings = dict(SETTINGS_DEFAULTS)
    for r in rows:
        k = r.get("Key", "").strip().lower()
        v = r.get("Value", "").strip()
        if k and v:
            settings[k] = v
    return settings


def effective(page_row: dict, master: dict, key: str, column: str):
    """override (Pages tab column) -> master Settings default."""
    val = page_row.get(column, "").strip()
    return val if val else master.get(key.lower(), "")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PageConfig:
    page_id: str
    page_name: str
    row_num: int
    mega_folder: str
    mega_move_folder: str
    caption: str
    without_link_caption: str
    link_percentage: int
    loop_interval_minutes: int
    url_replace_count: int
    url_replace_mode: str
    url_replace_enabled: bool
    post_mode: str                 # "rotation" | "queue"
    storage_state_json: str | None
    max_runtime_minutes: int
    heartbeat_minutes: int
    lock_ttl_minutes: int


def build_page_config(row: dict, master: dict, max_runtime_minutes: int) -> PageConfig:
    return PageConfig(
        page_id=row["PageId"],
        page_name=row.get("PageName") or row["PageId"],
        row_num=row["_row"],
        mega_folder=effective(row, master, "megafolder", "MegaFolder"),
        mega_move_folder=effective(row, master, "megamovefolder", "MegaMoveFolder"),
        caption=row.get("Caption", ""),
        without_link_caption=row.get("WithoutLinkCap", ""),
        link_percentage=max(0, min(100, _to_int(effective(row, master, "link_percentage", "Link_Percentage"), 100))),
        loop_interval_minutes=_to_int(effective(row, master, "loopintervalminutes", "LoopIntervalMinutes"), 60),
        url_replace_count=_to_int(effective(row, master, "urlreplacecount", "UrlReplaceCount"), 1),
        url_replace_mode=(effective(row, master, "urlreplacemode", "UrlReplaceMode") or "unique").lower(),
        url_replace_enabled=_to_bool(effective(row, master, "urlreplaceenabled", "UrlReplaceEnabled"), True),
        post_mode=(effective(row, master, "postmode", "PostMode") or "rotation").lower(),
        storage_state_json=row.get("FbStorageState") or None,
        max_runtime_minutes=max_runtime_minutes,
        heartbeat_minutes=_to_int(master.get("heartbeatminutes"), 15),
        lock_ttl_minutes=_to_int(master.get("lockttlminutes"), 90),
    )


# ─────────────────────────────────────────────────────────────────────────────
# LOCKING — claim / heartbeat / release, safe across concurrent repos
# ─────────────────────────────────────────────────────────────────────────────

def claim_pages(sh: Sheet, master: dict) -> list[dict]:
    """Reads the Pages tab and claims every Active, unlocked-or-stale-locked
    row for THIS runner (REPO_ID) by writing LockedBy/LockedAt. Returns the
    row dicts (post-claim, with LockedBy already set to us) that we now own.
    Optimistic locking: we re-read immediately after writing to confirm no
    one else's id landed there in the meantime (best-effort — Sheets has no
    real compare-and-swap, but the re-read closes almost all of the race)."""
    step(None, f"Claiming pages for runner '{REPO_ID}'")
    header = sh.read_rows(PAGES_TAB, "A1:Z1")
    if not header:
        fail(None, f"'{PAGES_TAB}' tab is empty or missing — run --setup-sheet first")
        return []
    col_index = {h.strip().lower(): i for i, h in enumerate(header[0]) if h.strip()}
    if "pageid" not in col_index or "fbstoragestate" not in col_index:
        fail(None, f"'{PAGES_TAB}' is missing required columns — run --setup-sheet")
        return []

    rows, _ = sh.as_dicts(PAGES_TAB)
    ttl = _to_int(master.get("lockttlminutes"), 90)
    print(f"🔍 DEBUG: as_dicts(Pages) returned {len(rows)} row(s): {rows}")
    claimed = []

    for row in rows:
        page_id = row.get("PageId", "").strip()
        if not page_id:
            continue
        status = (row.get("Status", "") or "Active").strip().lower()
        if status == "paused":
            info(None, f"[{page_id}] Status=Paused — skipping")
            continue

        locked_by = row.get("LockedBy", "").strip()
        locked_at = row.get("LockedAt", "").strip()
        is_free = (not locked_by) or (locked_by == REPO_ID) or (minutes_since(locked_at) > ttl)

        if not is_free:
            info(None, f"[{page_id}] locked by '{locked_by}' "
                       f"{minutes_since(locked_at):.0f}m ago (< {ttl}m TTL) — skipping")
            continue

        lb_col = col_index["lockedby"]
        la_col = col_index.get("lockedat")
        sh.write_cell(PAGES_TAB, row["_row"], lb_col, REPO_ID)
        if la_col is not None:
            sh.write_cell(PAGES_TAB, row["_row"], la_col, now_iso())

        # Re-read this single row to confirm we actually own the lock now
        fresh, _ = sh.as_dicts(PAGES_TAB)
        fresh_row = next((r for r in fresh if r.get("PageId", "").strip() == page_id), None)
        if fresh_row and fresh_row.get("LockedBy", "").strip() == REPO_ID:
            ok(None, f"[{page_id}] claimed by '{REPO_ID}'")
            claimed.append(fresh_row)
        else:
            warn(None, f"[{page_id}] lost the claim race to another runner — skipping")

    return claimed


def heartbeat_lock(sh: Sheet, page_row_num: int, lb_col: int, la_col: int):
    sh.write_cell(PAGES_TAB, page_row_num, lb_col, REPO_ID)
    sh.write_cell(PAGES_TAB, page_row_num, la_col, now_iso())


def release_lock(sh: Sheet, page_row_num: int, lb_col: int, la_col: int):
    sh.clear_cell(PAGES_TAB, page_row_num, lb_col)
    sh.clear_cell(PAGES_TAB, page_row_num, la_col)


def get_pages_col_index(sh: Sheet) -> dict:
    header = sh.read_rows(PAGES_TAB, "A1:Z1")
    if not header:
        return {}
    return {h.strip().lower(): i for i, h in enumerate(header[0]) if h.strip()}


# ─────────────────────────────────────────────────────────────────────────────
# MEGA.NZ (via rclone)
# ─────────────────────────────────────────────────────────────────────────────

def _run_rclone(page_id, args: list[str], timeout: int = 300):
    cmd = ["rclone"] + args
    info(page_id, f"rclone {' '.join(args)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        fail(page_id, "rclone not installed / not on PATH")
        raise
    except subprocess.TimeoutExpired:
        fail(page_id, f"rclone timed out after {timeout}s")
        raise
    if result.returncode != 0:
        warn(page_id, f"rclone exit {result.returncode}: {result.stderr.strip()[:400]}")
    return result.returncode, result.stdout, result.stderr


def mega_list_videos(page_id, folder: str) -> list[dict]:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}"
    rc, out, err = _run_rclone(page_id, ["lsjson", remote_path, "--files-only"])
    if rc != 0:
        raise RuntimeError(f"rclone lsjson failed: {err.strip()[:300]}")
    entries = json.loads(out) if out.strip() else []
    videos = [e for e in entries if Path(e.get("Name", "")).suffix.lower() in VIDEO_EXTENSIONS]
    videos.sort(key=lambda e: e.get("ModTime", ""))
    info(page_id, f"Found {len(videos)} video(s) in {folder}")
    return videos


def mega_download_video(page_id, folder: str, file_name: str, dest_dir: str) -> str:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}/{file_name}"
    dest_path = os.path.join(dest_dir, file_name)
    rc, out, err = _run_rclone(page_id, ["copyto", remote_path, dest_path, "--progress"], timeout=1800)
    if rc != 0 or not os.path.exists(dest_path):
        raise RuntimeError(f"rclone download failed: {err.strip()[:300]}")
    ok(page_id, f"Downloaded {file_name} ({os.path.getsize(dest_path)//(1024*1024)} MB)")
    return dest_path


def mega_move_to_uploaded(page_id, src_folder: str, dst_folder: str, file_name: str):
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{dst_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    if rc != 0:
        raise RuntimeError(f"rclone move failed: {err.strip()[:300]}")
    ok(page_id, "Moved to uploaded folder")


# ─────────────────────────────────────────────────────────────────────────────
# URLS TAB — rotation-mode link swapping, optionally scoped per PageId
# ─────────────────────────────────────────────────────────────────────────────

URLS_POSTED_VALUES = {"posted", "replaced"}

def urls_get_next(sh: Sheet, page_id: str, count: int):
    rows, col_index = sh.as_dicts(URLS_TAB)
    if "urls" not in col_index:
        fail(page_id, f"No 'Urls' column in '{URLS_TAB}' tab")
        return [], [], None
    status_col = col_index.get("status")
    found_urls, found_rows = [], []
    for r in rows:
        if len(found_urls) >= count:
            break
        url = r.get("Urls", "").strip()
        if not url:
            continue
        row_page = r.get("PageId", "").strip()
        if row_page and row_page != page_id:
            continue   # this URL is reserved for a different page
        if r.get("Status", "").strip().lower() in URLS_POSTED_VALUES:
            continue
        found_urls.append(url)
        found_rows.append(r["_row"])
    return found_urls, found_rows, status_col


def urls_mark_posted(sh: Sheet, rows: list[int], status_col_idx, status_value="Posted"):
    if not rows or status_col_idx is None:
        return
    for row_num in rows:
        sh.write_cell(URLS_TAB, row_num, status_col_idx, status_value)


def replace_urls_in_caption(caption: str, new_urls: list[str], mode: str, replace_count: int) -> str:
    if not new_urls:
        return caption
    matches = list(URL_REGEX.finditer(caption))
    if not matches:
        if mode == "same":
            return f"{caption}\n{new_urls[0]}"
        return caption + "\n" + "\n".join(new_urls[:replace_count])
    n = min(replace_count, len(matches))
    pieces, last_end = [], 0
    for i, m in enumerate(matches):
        pieces.append(caption[last_end:m.start()])
        if i < n:
            pieces.append(new_urls[0] if mode == "same" else (new_urls[i] if i < len(new_urls) else m.group(0)))
        else:
            pieces.append(m.group(0))
        last_end = m.end()
    pieces.append(caption[last_end:])
    return "".join(pieces)


# ─────────────────────────────────────────────────────────────────────────────
# POSTQUEUE TAB — "queue" mode: explicit filename -> caption/hashtags mapping
# ─────────────────────────────────────────────────────────────────────────────

def postqueue_find_for_file(sh: Sheet, page_id: str, file_name: str):
    """Returns (caption, row_num) for the given filename, preferring a row
    scoped to this PageId over a blank-PageId (any-page) row. None if no
    pending entry exists for this file."""
    rows, _ = sh.as_dicts(POSTQUEUE_TAB)
    scoped, general = None, None
    for r in rows:
        if r.get("FileName", "").strip() != file_name:
            continue
        if r.get("Status", "").strip().lower() == "posted":
            continue
        row_page = r.get("PageId", "").strip()
        caption = r.get("Caption", "").strip()
        hashtags = r.get("Hashtags", "").strip()
        full_caption = f"{caption}\n{hashtags}" if hashtags else caption
        if row_page == page_id:
            scoped = (full_caption, r["_row"])
        elif not row_page and general is None:
            general = (full_caption, r["_row"])
    return scoped or general


def postqueue_mark_posted(sh: Sheet, row_num: int):
    rows, col_index = sh.as_dicts(POSTQUEUE_TAB)
    status_idx = col_index.get("status")
    if status_idx is not None:
        sh.write_cell(POSTQUEUE_TAB, row_num, status_idx, "Posted")


# ─────────────────────────────────────────────────────────────────────────────
# FACEBOOK STORAGE STATE resolution (per page)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_storage_state(page_id: str, sheet_json: str | None) -> str | None:
    if sheet_json:
        try:
            json.loads(sheet_json)
            return sheet_json
        except json.JSONDecodeError:
            fail(page_id, "FbStorageState in sheet is not valid JSON")

    env_val = os.environ.get(FB_STORAGE_STATE_ENV)
    if env_val:
        warn(page_id, "Falling back to shared FB_STORAGE_STATE env var (seed only — "
                       "add this page's own session to its FbStorageState cell)")
        return env_val

    local = STORAGE_STATE_DIR / f"{page_id}.json"
    if local.exists():
        info(page_id, f"Using local cached session: {local}")
        return local.read_text(encoding="utf-8")

    fail(page_id, "No Facebook session available for this page")
    return None


def save_storage_state_everywhere(sh: Sheet, page_cfg: PageConfig, fresh_json: str):
    STORAGE_STATE_DIR.mkdir(exist_ok=True)
    (STORAGE_STATE_DIR / f"{page_cfg.page_id}.json").write_text(fresh_json, encoding="utf-8")

    col_index = get_pages_col_index(sh)
    col = col_index.get("fbstoragestate")
    if col is None:
        warn(page_cfg.page_id, "No FbStorageState column found — cannot persist session to sheet")
        return
    if len(fresh_json) > SHEETS_CELL_CHAR_LIMIT:
        warn(page_cfg.page_id, f"Session JSON is {len(fresh_json)} chars — over the "
                                f"{SHEETS_CELL_CHAR_LIMIT} cell limit, not saved to sheet this cycle")
        return
    sh.clear_cell(PAGES_TAB, page_cfg.row_num, col)
    sh.write_cell(PAGES_TAB, page_cfg.row_num, col, fresh_json)
    ok(page_cfg.page_id, "Refreshed FbStorageState in sheet")


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT HELPERS (per page, prefixed logging)
# ─────────────────────────────────────────────────────────────────────────────

def is_picker_url(url): return any(x in url for x in ["device-based", "/caa/", "login/caa", "login/identifier"])
def is_hard_login_url(url): return "/login" in url and not is_picker_url(url)

def classify_url(url: str) -> str:
    if "checkpoint" in url: return "CHECKPOINT"
    if is_hard_login_url(url): return "LOGIN_WALL"
    if is_picker_url(url): return "DEVICE_PICKER"
    if "reels/create" in url: return "REELS_CREATE"
    if "facebook.com" in url: return "FACEBOOK_PAGE"
    return "OTHER"


FEED_SELECTORS = [
    '[aria-label="Home"]', '[data-pagelet="LeftRail"]', 'div[role="feed"]',
    '[aria-label="Create"]', 'span:has-text("What\'s on your mind?")',
    'div[aria-label="Stories"]', 'div[aria-label="Reels"]',
    'div[data-pagelet="FeedUnit_0"]', 'div[role="main"]',
]


async def save_screenshot(page_id, page, name: str):
    SCREENSHOTS_DIR.mkdir(exist_ok=True)
    try:
        await page.screenshot(path=str(SCREENSHOTS_DIR / f"{page_id}_{name}.png"), full_page=False)
    except Exception as e:
        warn(page_id, f"Screenshot failed: {e}")


async def dump_html(page_id, page, filename: str):
    try:
        content = await page.content()
        Path(f"{page_id}_{filename}").write_text(content, encoding="utf-8")
    except Exception as e:
        warn(page_id, f"HTML dump failed: {e}")


async def nuke_continue_button(page_id, page) -> bool:
    SELECTORS = [
        '[aria-label^="Continue"]', '[aria-label*="Continue"]',
        'div[role="button"][aria-label^="Continue"]',
        'div[role="button"]:has-text("Continue")',
        'span:text-is("Continue")', 'span:has-text("Continue")', 'button:has-text("Continue")',
    ]
    url_before = page.url
    found_sel = None
    for _ in range(10):
        for sel in SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    found_sel = sel; break
            except Exception:
                pass
        if found_sel: break
        await asyncio.sleep(1)

    if not found_sel:
        try:
            hit = await page.evaluate("""() => {
                const c = Array.from(document.querySelectorAll('div[role="button"],a[role="button"],button,a,span[tabindex]'));
                const b = c.find(el => /^continue/i.test((el.textContent||el.innerText||el.getAttribute('aria-label')||'').trim()));
                if (!b) return null; b.click(); return true; }""")
            if hit:
                await asyncio.sleep(5)
                return page.url != url_before
        except Exception:
            pass
        try:
            await page.goto("https://www.facebook.com/?sk=h_chr", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(5)
            return not is_picker_url(page.url) and not is_hard_login_url(page.url)
        except Exception:
            return False

    loc = page.locator(found_sel).first
    for method in [lambda: loc.click(timeout=5_000), lambda: loc.click(force=True, timeout=5_000),
                   lambda: loc.evaluate("el => el.click()")]:
        try:
            await method()
            await asyncio.sleep(5)
            if page.url != url_before:
                return True
        except Exception:
            pass
    return False


async def ensure_logged_in(page_id, page) -> bool:
    for attempt in range(6):
        url_type = classify_url(page.url)
        if url_type == "CHECKPOINT":
            fail(page_id, "Account checkpoint/restriction — manual action required")
            await save_screenshot(page_id, page, f"checkpoint_{attempt+1}")
            return False
        if url_type == "LOGIN_WALL":
            fail(page_id, "Hard login wall — session cookies EXPIRED")
            await save_screenshot(page_id, page, f"login_wall_{attempt+1}")
            return False
        if url_type == "DEVICE_PICKER":
            await nuke_continue_button(page_id, page)
            continue
        for sel in FEED_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        await asyncio.sleep(4)
    fail(page_id, "Login check exhausted all attempts")
    await save_screenshot(page_id, page, "login_failed_final")
    return False


def _nonempty_lines(text: str) -> list[str]:
    return [l for l in text.split("\n") if l.strip()]


async def _clear_field(page, field):
    await field.click(timeout=5_000)
    await asyncio.sleep(0.3)
    await page.keyboard.press("Control+a")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.2)


async def enter_caption_lexical(page_id, page, caption: str) -> bool:
    LEXICAL_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]
    expected_lines = len(_nonempty_lines(caption))

    async def strat_keyboard(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.keyboard.type(line, delay=12)
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strat_clipboard(field):
        await _clear_field(page, field)
        await page.evaluate("(t) => navigator.clipboard.writeText(t).catch(()=>{})", caption)
        await asyncio.sleep(0.3)
        await page.keyboard.press("Control+v")
        await asyncio.sleep(0.8)

    async def strat_exec_command(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, t) => { el.focus(); document.execCommand('insertText', false, t); }""",
                    [field, line])
            if i < len(lines) - 1:
                await page.evaluate(
                    """(el) => { el.focus(); document.execCommand('insertParagraph', false, null); }""", field)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strat_input_event(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, t) => {
                        el.focus();
                        const sel=window.getSelection(); const r=document.createRange();
                        r.selectNodeContents(el); r.collapse(false); sel.removeAllRanges(); sel.addRange(r);
                        el.dispatchEvent(new InputEvent('beforeinput',{inputType:'insertText',data:t,bubbles:true,cancelable:true}));
                        el.dispatchEvent(new InputEvent('input',{inputType:'insertText',data:t,bubbles:true}));
                    }""", [field, line])
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    for i, strategy in enumerate([strat_keyboard, strat_clipboard, strat_exec_command, strat_input_event], 1):
        for sel in LEXICAL_SELECTORS:
            try:
                field = page.locator(sel).first
                if await field.count() == 0:
                    continue
                await strategy(field)
                txt = await field.evaluate("el => (el.innerText || el.textContent || '').trim()")
                actual_lines = len(_nonempty_lines(txt))
                if txt and len(txt) > 2 and abs(actual_lines - expected_lines) <= 1:
                    ok(page_id, f"Caption entered via strategy {i} ({actual_lines}/{expected_lines} lines)")
                    return True
            except Exception as e:
                warn(page_id, f"Caption strategy {i}/{sel} raised: {e}")
    return False


async def run_upload_flow(page_id, page, caption: str, video_path: str) -> bool:
    published = False

    step(page_id, "Loading Facebook homepage")
    try:
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        fail(page_id, f"Page load failed: {e}")
        return False
    await asyncio.sleep(8)

    if not await ensure_logged_in(page_id, page):
        return False
    ok(page_id, "Login confirmed")

    step(page_id, "Navigating to Reels create")
    try:
        await page.goto("https://www.facebook.com/reels/create/", wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        fail(page_id, f"Nav to reels/create failed: {e}")
        return False
    await asyncio.sleep(8)

    step(page_id, "Attaching video")
    uploaded = False
    for sel in ['input[type="file"][accept*="video"]', 'input[type="file"]']:
        try:
            inp = page.locator(sel)
            if await inp.count() > 0:
                await inp.first.set_input_files(video_path)
                uploaded = True
                break
        except Exception as e:
            warn(page_id, f"Direct input {sel} failed: {e}")

    if not uploaded:
        for btn_name, sel in [
            ('Select video', 'div[role="button"]:has-text("Select video")'),
            ('Upload', 'div[role="button"]:has-text("Upload")'),
            ('Add video', 'div[role="button"]:has-text("Add video")'),
            ('aria-label', '[aria-label="Select video"]'),
        ]:
            el = page.locator(sel).first
            try:
                if await el.count() == 0:
                    continue
                async with page.expect_file_chooser(timeout=10_000) as fc_info:
                    await el.click(force=True)
                fc = await fc_info.value
                await fc.set_files(video_path)
                uploaded = True
                break
            except Exception as e:
                warn(page_id, f"Upload button '{btn_name}' failed: {e}")

    if not uploaded:
        fail(page_id, "Could not attach video")
        await save_screenshot(page_id, page, "no_upload")
        return False
    ok(page_id, "Video attached")

    step(page_id, "Waiting for Next to become active")
    next_selectors = ['div[aria-label="Next"][role="button"]', 'div[role="button"]:has-text("Next")',
                       'span:has-text("Next")', 'button:has-text("Next")']
    next_ready = False
    for elapsed in range(0, 180, 5):
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.get_attribute("aria-disabled") != "true":
                    next_ready = True; break
            except Exception:
                pass
        if next_ready: break
        await asyncio.sleep(5)
    if not next_ready:
        warn(page_id, "Next never became active after 3 min")

    CAPTION_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]', 'div[contenteditable="true"]',
    ]

    async def caption_visible():
        for sel in CAPTION_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    async def click_next():
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                    continue
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(timeout=10_000)
                return True
            except Exception:
                pass
        return False

    caption_found = await caption_visible()
    for attempt in range(1, 4):
        if caption_found:
            break
        if not await click_next():
            break
        for _ in range(8):
            if await caption_visible():
                caption_found = True; break
            await asyncio.sleep(2)

    step(page_id, "Entering caption")
    caption_ok = await enter_caption_lexical(page_id, page, caption)
    if not caption_ok:
        warn(page_id, "Caption entry unverified — continuing anyway")
    await save_screenshot(page_id, page, "after_caption")

    step(page_id, "Advancing to Post panel")

    async def post_visible():
        for sel in ['div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")', 'span:text-is("Post")']:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    if not await post_visible():
        for sel in ['div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")',
                    'div[aria-label="Next"][role="button"]', 'div[role="button"]:has-text("Next")',
                    'span:text-is("Post")', 'span:has-text("Next")']:
            try:
                btn = page.locator(sel).last
                if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                    continue
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(force=True)
                await asyncio.sleep(4)
                break
            except Exception:
                pass

    step(page_id, "Clicking Post / Publish")
    post_selectors = [
        'div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")',
        'span:text-is("Post")', 'div[aria-label="Publish"][role="button"]',
        'div[aria-label="Share now"][role="button"]', 'div[role="button"]:has-text("Post")',
        'div[role="button"]:has-text("Publish")', 'button[type="submit"]',
    ]
    post_clicked = False
    for sel in post_selectors:
        try:
            btn = page.locator(sel).last
            if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                continue
            await btn.scroll_into_view_if_needed(timeout=5_000)
            await btn.click(force=True)
            post_clicked = True
            await asyncio.sleep(5)
            break
        except Exception as e:
            warn(page_id, f"Post click '{sel}' failed: {e}")

    if not post_clicked:
        fail(page_id, "Could not click Post/Publish")
        await save_screenshot(page_id, page, "no_post_button")
        return False

    step(page_id, "Waiting for publish confirmation")
    confirm_selectors = ['span:has-text("Your reel is now shared")', 'span:has-text("Reel posted")',
                          'span:has-text("Published")', 'span:has-text("Your reel")', 'span:has-text("shared")']
    for elapsed in range(0, 60, 5):
        for sel in confirm_selectors:
            try:
                if await page.locator(sel).count() > 0:
                    published = True; break
            except Exception:
                pass
        if published: break
        await asyncio.sleep(5)

    if not published:
        try:
            gone = await page.locator('div[aria-label="Post"][role="button"]').count() == 0
            if gone and post_clicked:
                published = True
        except Exception:
            pass

    await save_screenshot(page_id, page, "final_result")
    if published:
        ok(page_id, "🎉 Published")
    else:
        warn(page_id, "Could not confirm publish — check screenshot")
    return published


# ─────────────────────────────────────────────────────────────────────────────
# PAGE WORKER — owns one persistent browser context for its whole run
# ─────────────────────────────────────────────────────────────────────────────

class PageWorker:
    """One Facebook Page. Keeps a single browser context OPEN for the whole
    run (instead of relaunching per post) so the session stays warm, and
    heartbeats the lock + storage_state back to the sheet on a timer even
    between posts — this is the "keep it alive / never log out" behavior."""

    def __init__(self, sh: Sheet, cfg: PageConfig, col_index: dict, semaphore: asyncio.Semaphore, once: bool):
        self.sh = sh
        self.cfg = cfg
        self.col_index = col_index
        self.semaphore = semaphore
        self.once = once
        self.browser = None
        self.context = None

    async def run(self):
        async with self.semaphore:
            await self._run_locked()

    async def _run_locked(self):
        cfg = self.cfg
        start = time.monotonic()
        async with async_playwright() as p:
            try:
                self.browser = await p.chromium.launch(
                    headless=True, timeout=30_000,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-blink-features=AutomationControlled",
                          "--disable-infobars", "--disable-dev-shm-usage",
                          "--single-process", "--no-zygote"],
                )
            except Exception as e:
                fail(cfg.page_id, f"Browser launch failed: {e}")
                return

            storage_state_json = resolve_storage_state(cfg.page_id, cfg.storage_state_json)
            if not storage_state_json:
                await self.browser.close()
                return

            try:
                state = json.loads(storage_state_json)
                self.context = await self.browser.new_context(
                    storage_state=state,
                    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
                    viewport={"width": 1280, "height": 900}, locale="en-US",
                    timezone_id="Asia/Karachi", accept_downloads=True,
                )
                await self.context.grant_permissions(
                    ["clipboard-read", "clipboard-write"], origin="https://www.facebook.com")
            except Exception as e:
                fail(cfg.page_id, f"Context creation failed: {e}")
                await self.browser.close()
                return

            last_heartbeat = time.monotonic()
            try:
                while True:
                    await self._post_once()
                    await self._heartbeat()
                    last_heartbeat = time.monotonic()

                    if self.once:
                        break
                    elapsed_min = (time.monotonic() - start) / 60
                    if elapsed_min >= cfg.max_runtime_minutes:
                        info(cfg.page_id, f"Runtime window ({cfg.max_runtime_minutes}m) reached — stopping")
                        break

                    # Sleep until the next post, heartbeating (and staying
                    # "live") every heartbeat_minutes in between.
                    remaining = cfg.loop_interval_minutes * 60
                    while remaining > 0:
                        chunk = min(remaining, cfg.heartbeat_minutes * 60)
                        await asyncio.sleep(chunk)
                        remaining -= chunk
                        if (time.monotonic() - last_heartbeat) / 60 >= cfg.heartbeat_minutes:
                            await self._heartbeat()
                            last_heartbeat = time.monotonic()
                        if (time.monotonic() - start) / 60 >= cfg.max_runtime_minutes:
                            remaining = 0
            finally:
                await self._release()
                await self.browser.close()
                ok(cfg.page_id, "Browser closed, lock released")

    async def _heartbeat(self):
        cfg = self.cfg
        try:
            fresh = await self.context.storage_state()
            fresh_json = json.dumps(fresh)
            save_storage_state_everywhere(self.sh, cfg, fresh_json)
        except Exception as e:
            warn(cfg.page_id, f"Heartbeat storage_state save failed: {e}")
        lb_col, la_col = self.col_index.get("lockedby"), self.col_index.get("lockedat")
        if lb_col is not None and la_col is not None:
            heartbeat_lock(self.sh, cfg.row_num, lb_col, la_col)

    async def _release(self):
        lb_col, la_col = self.col_index.get("lockedby"), self.col_index.get("lockedat")
        if lb_col is not None and la_col is not None:
            release_lock(self.sh, self.cfg.row_num, lb_col, la_col)

    async def _post_once(self):
        cfg = self.cfg
        pid = cfg.page_id
        step(pid, f"Post cycle starting (mode={cfg.post_mode})")

        try:
            videos = mega_list_videos(pid, cfg.mega_folder)
        except Exception as e:
            fail(pid, f"Could not list Mega folder: {e}")
            return
        if not videos:
            info(pid, "No videos pending — nothing to do this cycle")
            return

        video_meta = videos[0]
        file_name = video_meta["Name"]

        # ── Build the caption for this run, depending on PostMode ─────────
        used_url_rows, url_status_col = [], None
        pq_row_num = None

        if cfg.post_mode == "queue":
            match = postqueue_find_for_file(self.sh, pid, file_name)
            if not match:
                warn(pid, f"PostMode=queue but no pending PostQueue row for '{file_name}' — skipping this cycle")
                return
            caption, pq_row_num = match
            if not caption.strip():
                warn(pid, f"PostQueue row for '{file_name}' has an empty caption — skipping")
                return
        else:
            roll = random.randint(1, 100)
            use_link = roll <= cfg.link_percentage
            caption = cfg.caption if use_link else (cfg.without_link_caption or cfg.caption)
            if not caption.strip():
                fail(pid, "No caption configured (Caption / WithoutLinkCap both empty) — skipping")
                return
            if use_link and cfg.url_replace_enabled:
                n_matches = len(URL_REGEX.findall(caption))
                n_to_replace = min(cfg.url_replace_count, n_matches) if n_matches else cfg.url_replace_count
                fetch_count = 1 if cfg.url_replace_mode == "same" else max(n_to_replace, 1)
                new_urls, used_url_rows, url_status_col = urls_get_next(self.sh, pid, fetch_count)
                if new_urls:
                    if cfg.url_replace_mode == "unique" and len(new_urls) < n_to_replace:
                        n_to_replace = len(new_urls)
                    caption = replace_urls_in_caption(caption, new_urls, cfg.url_replace_mode, n_to_replace)
                else:
                    warn(pid, "No unused URLs available — posting caption without swap")

        with tempfile.TemporaryDirectory() as tmp:
            try:
                local_path = mega_download_video(pid, cfg.mega_folder, file_name, tmp)
            except Exception as e:
                fail(pid, f"Download failed: {e}")
                return

            try:
                page = await self.context.new_page()
                published = await run_upload_flow(pid, page, caption, local_path)
                await page.close()
            except Exception as e:
                fail(pid, f"Upload flow crashed: {e}")
                import traceback; print(traceback.format_exc())
                published = False

        if published:
            try:
                mega_move_to_uploaded(pid, cfg.mega_folder, cfg.mega_move_folder, file_name)
            except Exception as e:
                warn(pid, f"Move to uploaded failed: {e}")

            if cfg.post_mode == "queue" and pq_row_num:
                postqueue_mark_posted(self.sh, pq_row_num)
            if used_url_rows:
                urls_mark_posted(self.sh, used_url_rows, url_status_col)

            lastfile_col = self.col_index.get("lastpostedfile")
            if lastfile_col is not None:
                self.sh.write_cell(PAGES_TAB, cfg.row_num, lastfile_col, file_name)
        else:
            warn(pid, "Upload not confirmed — video left in source folder for retry")

        lastrun_col = self.col_index.get("lastrunat")
        if lastrun_col is not None:
            self.sh.write_cell(PAGES_TAB, cfg.row_num, lastrun_col, now_iso())


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

async def main_async(once: bool):
    creds = build_google_creds()
    service = build_sheets_service(creds)
    spreadsheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV) or DEFAULT_CAPTIONS_SHEET_ID
    sh = Sheet(service, spreadsheet_id)

    master = load_master_settings(sh)
    thread_count = max(1, _to_int(master.get("threadcount"), 3))
    max_runtime = _to_int(master.get("maxruntimeminutes"), 300)

    claimed_rows = claim_pages(sh, master)
    if not claimed_rows:
        info(None, "No pages available to claim this run (none Active/unlocked)")
        return

    col_index = get_pages_col_index(sh)
    configs = [build_page_config(r, master, max_runtime) for r in claimed_rows]

    info(None, f"Runner '{REPO_ID}' — {len(configs)} page(s) claimed, "
               f"running {thread_count} concurrently")

    semaphore = asyncio.Semaphore(thread_count)
    workers = [PageWorker(sh, cfg, col_index, semaphore, once) for cfg in configs]
    await asyncio.gather(*(w.run() for w in workers), return_exceptions=False)


def main():
    if "--setup-sheet" in sys.argv:
        creds = build_google_creds()
        service = build_sheets_service(creds)
        spreadsheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV) or DEFAULT_CAPTIONS_SHEET_ID
        setup_sheet(service, spreadsheet_id)
        return

    once = "--once" in sys.argv or bool(os.environ.get("RUN_ONCE"))
    print(f"🚀 Runner '{REPO_ID}' starting — {'single cycle (--once)' if once else 'continuous loop'}")
    asyncio.run(main_async(once))
    print("✅ Run complete")


if __name__ == "__main__":
    main()
