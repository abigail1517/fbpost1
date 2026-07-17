"""
post_facebook.py  — VERBOSE DEBUG VERSION
─────────────────────────────────────────
Every step prints exactly what it's doing and why it failed.
Run with:  python -u post_facebook.py --once

ALL runtime settings (Mega.nz folders, loop interval, max runtime, captions)
are read from the Google Sheet at CAPTIONS_SHEET_ID / DEFAULT_CAPTIONS_SHEET_ID.
Videos live on Mega.nz and are accessed via rclone (remote name "mega",
configured by the GitHub Actions workflow from the RCLONE_CONF_MEGA secret).
Nothing except the Facebook session, Google credentials (for Sheets only),
and the rclone config come from the GitHub Actions workflow. See the SHEET
HEADER COLUMNS section below.

SHEET HEADER COLUMNS (row 1 = headers, exact names, case-insensitive):
  Caption               — the caption to post WHEN this run includes a
                           link (see Link_Percentage below). It is NEVER
                           cleared — the same caption row (row 2, first
                           non-empty row) is reused every run. Each time
                           it's used, one or more URLs already inside it
                           are swapped for fresh ones pulled from the
                           'Urls' tab (see below).
  WithoutLinkCap         — a separate caption used WHEN this run is
                           chosen (by Link_Percentage) to post WITHOUT a
                           link. Also never cleared / reused every time.
                           No URL swapping ever happens on this caption.
                           Optional — if left blank, runs fall back to
                           the regular 'Caption' (with link) instead.
  Link_Percentage        — 0–100. Read from row 2 only. Defaults to 100
                           (always post WITH a link). Each run rolls a
                           random 1–100 number; if it's ≤ Link_Percentage,
                           the 'Caption' (with link) is used, otherwise
                           'WithoutLinkCap' is used. E.g. 30 → roughly
                           30% of runs post with a link, 70% without.
  MegaFolder             — Mega.nz folder (path, relative to the mega
                           remote root) holding pending videos.
                           Read from row 2 only. Defaults to "fbreels"
                           if blank/missing.
  MegaMoveFolder         — Mega.nz folder videos get moved to after a
                           successful post. Read from row 2 only.
                           Defaults to "fbreels_uploaded" if blank/missing.
  LoopIntervalMinutes    — minutes between posts. Read from row 2 only.
                           Defaults to 30 if blank/missing.
  MaxRuntimeMinutes      — total minutes this job runs before exiting
                           cleanly so the workflow can self-requeue.
                           Read from row 2 only. Defaults to 300 if blank.
  UrlReplaceCount        — how many URLs *inside the caption* to replace
                           each run. Read from row 2 only. Defaults to 1.
                           Only applies on runs using 'Caption' (with a
                           link). Ignored when UrlReplaceEnabled is FALSE.
  UrlReplaceMode         — "unique" (default) or "same". Read from row 2
                           only. Ignored when UrlReplaceEnabled is FALSE.
                             unique — each URL occurrence in the caption
                                      gets its OWN distinct URL from the
                                      'Urls' tab (e.g. 2 occurrences → 2
                                      different URLs pulled).
                             same   — all targeted occurrences in the
                                      caption are replaced with the SAME
                                      single URL pulled from the 'Urls'
                                      tab (e.g. 2 occurrences → 1 URL
                                      pulled, used twice).
  UrlReplaceEnabled      — TRUE (default) or FALSE. Read from row 2 only.
                           Set to FALSE to skip URL swapping entirely and
                           post the 'Caption' exactly as written — the
                           'Urls' tab is not touched or read in that case.

'Urls' TAB (same spreadsheet, separate tab named exactly "Urls"):
  Urls                   — one URL per row.
  Status                 — left blank for unused URLs. After a URL is used
                           in a successful post, this script writes
                           "Posted" into this column for that row so the
                           URL is skipped on future runs (it is never
                           deleted). Add this header yourself if it isn't
                           already there.

  FbStorageState         — (on the main tab, row 2, alongside Caption
                           etc.) holds the Facebook session (Playwright
                           storage_state JSON) as plain text. This is now
                           the PRIMARY source for the session — no more
                           relying on a GitHub secret that has to be
                           refreshed via the GitHub CLI. On every run,
                           after the browser closes, the script deletes
                           whatever was in this cell and writes the fresh
                           session back into it, so the sheet always holds
                           the newest cookies and the login never goes
                           stale between runs. FB_STORAGE_STATE (the old
                           GitHub secret) is now only used as a one-time
                           seed: if the sheet cell is empty (e.g. the very
                           first run), the script falls back to it, then
                           to a local storage_state.json file. Add this
                           header yourself if it isn't already there —
                           just paste your current session JSON into row 2
                           once, and the script takes over refreshing it
                           from there.

─────────────────────────────────────────
CHANGELOG (this version)
─────────────────────────────────────────
- Caption entry now types line-by-line with an explicit Enter keypress
  between lines instead of relying on embedded "\n" characters inside a
  single insertText/InputEvent/keyboard.type call. Facebook's Lexical
  editor does not reliably turn a literal "\n" inside a pasted/inserted
  string into a new paragraph node — it wants an actual Enter keypress
  (or a real clipboard paste with proper HTML/line data) to create a new
  line. This was the root cause of captions arriving "squished" (all
  lines and hashtags running together) even though the script logged a
  successful caption entry.
- The browser context now explicitly grants clipboard-read/clipboard-write
  permissions so the clipboard-paste strategy actually has a chance to
  work in headless Chromium (previously navigator.clipboard.writeText()
  could fail silently there, silently degrading to a worse strategy).
- Success verification for caption entry no longer just checks "some text
  landed" — it now compares the number of non-empty lines typed into the
  field against the number of non-empty lines in the intended caption.
  A strategy that squishes everything onto one line now correctly fails
  verification and the script falls through to try the next strategy,
  instead of silently reporting success on mangled output.
"""

import asyncio, json, os, random, re, subprocess, sys, tempfile, time
from pathlib import Path
from datetime import datetime
import functools

# Force unbuffered output — every print shows immediately in GitHub Actions
print = functools.partial(print, flush=True)

# ── optional scheduler ────────────────────────────────────────────────────────
try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

# ── Google Sheets (captions / settings / Urls tab only — no Drive) ────────────
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
STORAGE_STATE         = "storage_state.json"
SCREENSHOTS_DIR       = Path("screenshots")

FB_STORAGE_STATE_ENV      = "FB_STORAGE_STATE"
GOOGLE_CREDS_ENV          = "GOOGLE_CREDENTIALS_JSON"
CAPTIONS_SHEET_ID_ENV     = "CAPTIONS_SHEET_ID"
# Falls back to the sheet you shared if the env var / workflow input is blank
DEFAULT_CAPTIONS_SHEET_ID = "1ICgS97JJ-Hrs9qsI1UV-xJvPg7ovmSHStGQPoOYr0Dk"

DEFAULT_LOOP_INTERVAL_MINUTES = 30
DEFAULT_MAX_RUNTIME_MINUTES   = 300

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}

# rclone remote name — must match the "[mega]" section name in rclone.conf
MEGA_REMOTE_NAME          = "mega"
DEFAULT_MEGA_FOLDER       = "fbreels"
DEFAULT_MEGA_MOVE_FOLDER  = "fbreels_uploaded"

# Header names expected in row 1 of the sheet (case-insensitive match)
SETTINGS_COLUMNS = {
    "mega_folder":           "MegaFolder",
    "mega_move_folder":      "MegaMoveFolder",
    "loop_interval_minutes": "LoopIntervalMinutes",
    "max_runtime_minutes":   "MaxRuntimeMinutes",
    "url_replace_count":     "UrlReplaceCount",
    "url_replace_mode":      "UrlReplaceMode",
    "url_replace_enabled":   "UrlReplaceEnabled",
    "link_percentage":       "Link_Percentage",
}

DEFAULT_URL_REPLACE_COUNT   = 1
DEFAULT_URL_REPLACE_MODE    = "unique"   # "unique" or "same"
DEFAULT_URL_REPLACE_ENABLED = True
DEFAULT_LINK_PERCENTAGE     = 100        # 100 = always post WITH a link (old behavior)

# 'Urls' tab config
URLS_SHEET_NAME         = "Urls"
URLS_COLUMN_NAME        = "Urls"
URLS_STATUS_COLUMN_NAME = "Status"
URLS_POSTED_VALUES      = {"posted", "replaced"}  # values treated as "already used"
URL_REGEX                = re.compile(r'https?://\S+')

# Main-tab column that stores the Facebook session (Playwright storage_state
# JSON) so it can be refreshed every run without a GitHub secret.
FB_STORAGE_STATE_COLUMN_NAME = "FbStorageState"
SHEETS_CELL_CHAR_LIMIT       = 50000  # Google Sheets per-cell text limit

# ─────────────────────────────────────────────────────────────────────────────
# STEP LOGGER
# ─────────────────────────────────────────────────────────────────────────────
_step = 0
def step(msg):
    global _step
    _step += 1
    print(f"\n{'='*60}")
    print(f"  STEP {_step}: {msg}")
    print(f"{'='*60}")

def info(msg):   print(f"   ℹ️  {msg}")
def ok(msg):     print(f"   ✅ {msg}")
def warn(msg):   print(f"   ⚠️  {msg}")
def fail(msg):   print(f"   ❌ {msg}")
def debug(msg):  print(f"   🔍 {msg}")

# ─────────────────────────────────────────────────────────────────────────────
# Google credentials / Sheets service (Sheets only — no Drive)
# ─────────────────────────────────────────────────────────────────────────────

def build_google_creds():
    step("Building Google credentials (Sheets)")
    if not HAS_GOOGLE:
        fail("google-auth not installed")
        raise RuntimeError("Missing google-auth libraries")

    creds_json = os.environ.get(GOOGLE_CREDS_ENV)
    if not creds_json:
        fail(f"Env var {GOOGLE_CREDS_ENV} is not set")
        raise RuntimeError(f"Missing {GOOGLE_CREDS_ENV}")

    info(f"GOOGLE_CREDENTIALS_JSON length: {len(creds_json)} chars")

    try:
        creds_data = json.loads(creds_json)
    except json.JSONDecodeError as e:
        fail(f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")
        raise

    info(f"Credential keys present: {list(creds_data.keys())}")

    for field in ["token", "refresh_token", "client_id", "client_secret"]:
        if creds_data.get(field):
            ok(f"  {field}: present")
        else:
            warn(f"  {field}: MISSING or empty")

    creds = Credentials(
        token         = creds_data.get("token"),
        refresh_token = creds_data.get("refresh_token"),
        token_uri     = creds_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id     = creds_data.get("client_id"),
        client_secret = creds_data.get("client_secret"),
        # Videos are no longer on Drive, so only the Sheets scope is needed.
        scopes        = creds_data.get("scopes", [
            "https://www.googleapis.com/auth/spreadsheets",
        ]),
    )
    info(f"Token expired: {creds.expired}")
    info(f"Has refresh_token: {bool(creds.refresh_token)}")

    if creds.expired and creds.refresh_token:
        info("Refreshing expired Google token...")
        try:
            creds.refresh(Request())
            ok("Google token refreshed successfully")
        except Exception as e:
            fail(f"Token refresh failed: {e}")
            raise

    return creds


def build_sheets_service(creds):
    step("Building Google Sheets service")
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    ok("Google Sheets service built")
    return service


# ─────────────────────────────────────────────────────────────────────────────
# Mega.nz (via rclone) — video storage
# ─────────────────────────────────────────────────────────────────────────────

def _run_rclone(args: list[str], timeout: int = 300):
    """Runs an rclone command, returns (returncode, stdout, stderr)."""
    cmd = ["rclone"] + args
    info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        fail("rclone is not installed / not on PATH — check the workflow's rclone install step")
        raise
    except subprocess.TimeoutExpired:
        fail(f"rclone command timed out after {timeout}s")
        raise

    if result.stdout.strip():
        debug(f"stdout: {result.stdout.strip()[:500]}")
    if result.returncode != 0:
        warn(f"rclone exited with code {result.returncode}")
        if result.stderr.strip():
            warn(f"stderr: {result.stderr.strip()[:1000]}")
    return result.returncode, result.stdout, result.stderr


def mega_list_videos(folder: str) -> list[dict]:
    step(f"Listing videos in Mega.nz folder: {folder}")
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}"
    returncode, stdout, stderr = _run_rclone(["lsjson", remote_path, "--files-only"])
    if returncode != 0:
        fail(f"rclone lsjson failed for {remote_path}: {stderr.strip()[:300]}")
        raise RuntimeError(f"rclone lsjson failed: {stderr.strip()[:300]}")

    try:
        entries = json.loads(stdout) if stdout.strip() else []
    except json.JSONDecodeError as e:
        fail(f"Could not parse rclone lsjson output: {e}")
        raise

    videos = [
        e for e in entries
        if Path(e.get("Name", "")).suffix.lower() in VIDEO_EXTENSIONS
    ]
    videos.sort(key=lambda e: e.get("ModTime", ""))  # oldest first, like before

    info(f"Found {len(videos)} video(s)")
    for v in videos:
        size_mb = int(v.get("Size", 0) or 0) // (1024 * 1024)
        info(f"  • {v.get('Name')}  ({size_mb} MB)")
    return videos


def mega_download_video(folder: str, file_name: str, dest_dir: str) -> str:
    step(f"Downloading video from Mega.nz: {file_name}")
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}/{file_name}"
    dest_path = os.path.join(dest_dir, file_name)

    returncode, stdout, stderr = _run_rclone(
        ["copyto", remote_path, dest_path, "--progress"], timeout=1800,
    )
    if returncode != 0:
        fail(f"rclone copyto failed: {stderr.strip()[:300]}")
        raise RuntimeError(f"rclone download failed: {stderr.strip()[:300]}")

    if not os.path.exists(dest_path):
        fail(f"Download reported success but file not found: {dest_path}")
        raise RuntimeError("Downloaded file missing after rclone copyto")

    size_mb = os.path.getsize(dest_path) // (1024 * 1024)
    ok(f"Downloaded to: {dest_path}  ({size_mb} MB)")
    return dest_path


def mega_move_to_uploaded(src_folder: str, dst_folder: str, file_name: str):
    step(f"Moving '{file_name}' to Mega.nz uploaded folder")
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{dst_folder}/{file_name}"
    returncode, stdout, stderr = _run_rclone(["moveto", src, dst])
    if returncode != 0:
        fail(f"rclone moveto failed: {stderr.strip()[:300]}")
        raise RuntimeError(f"rclone move failed: {stderr.strip()[:300]}")
    ok("Moved successfully")


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheet settings, caption & URL helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read_sheet_rows(sheets_service, spreadsheet_id, range_str="A:Z"):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_str
        ).execute()
    except Exception as e:
        fail(f"Sheets API read failed ({range_str}): {e}")
        return []
    return result.get("values", [])


def sheet_get_settings(sheets_service, spreadsheet_id: str) -> dict:
    """
    Reads MegaFolder / MegaMoveFolder / LoopIntervalMinutes /
    MaxRuntimeMinutes from row 2 of the sheet (matched by header name in
    row 1). Returns a dict with whichever keys were found — missing or
    blank values are simply absent, callers apply their own defaults.
    """
    step(f"Fetching settings from Google Sheet: {spreadsheet_id}")
    rows = _read_sheet_rows(sheets_service, spreadsheet_id)
    if len(rows) < 2:
        warn("Sheet has no data row (row 2) — cannot read settings")
        return {}

    header = rows[0]
    data_row = rows[1]
    info(f"Header row: {header}")

    col_index = {h.strip().lower(): i for i, h in enumerate(header) if h.strip()}

    settings = {}
    for key, col_name in SETTINGS_COLUMNS.items():
        idx = col_index.get(col_name.lower())
        if idx is None:
            warn(f"Column '{col_name}' not found in header row")
            continue
        if idx < len(data_row) and data_row[idx].strip():
            settings[key] = data_row[idx].strip()
            info(f"  {col_name} = {settings[key]}")
        else:
            warn(f"  {col_name} is empty in row 2")

    return settings


def _sheet_get_named_column(sheets_service, spreadsheet_id: str, column_name: str, required: bool = True):
    """
    Generic helper: finds `column_name` (case-insensitive) in row 1 and
    returns the first non-empty value found below it, along with its
    row/column index. The cell is NEVER cleared here — callers reuse the
    same value every run. Returns (value, row_num, col_idx) or
    (None, None, None).
    """
    step(f"Fetching '{column_name}' from Google Sheet: {spreadsheet_id}")
    rows = _read_sheet_rows(sheets_service, spreadsheet_id)
    if not rows:
        warn("Sheet appears empty")
        return None, None, None

    header = rows[0]
    info(f"Header row: {header}")

    col_idx = None
    for i, h in enumerate(header):
        if h.strip().lower() == column_name.lower():
            col_idx = i
            break

    if col_idx is None:
        msg = f"No '{column_name}' column found in header row"
        if required:
            fail(msg)
        else:
            warn(msg)
        return None, None, None

    info(f"'{column_name}' column found at index {col_idx} (column {chr(ord('A') + col_idx)})")

    for row_num, row in enumerate(rows[1:], start=2):
        if len(row) > col_idx and row[col_idx].strip():
            value = row[col_idx].strip()
            ok(f"'{column_name}' found at row {row_num}: {value[:80]}")
            return value, row_num, col_idx

    warn(f"No value found in '{column_name}' column (all rows empty)")
    return None, None, None


def sheet_get_caption(sheets_service, spreadsheet_id: str):
    """
    Finds the 'Caption' column and returns the first non-empty caption
    found below the header, along with its row/column index. This is the
    caption used on runs that post WITH a link.

    NOTE: the caption is intentionally NEVER cleared by this script
    anymore — the same caption is reused on every run. Only the URL(s)
    inside it change (see sheet_get_next_urls / replace_urls_in_caption).
    Returns (caption, row_num, col_idx) or (None, None, None).
    """
    return _sheet_get_named_column(sheets_service, spreadsheet_id, "Caption", required=True)


def sheet_get_withoutlink_caption(sheets_service, spreadsheet_id: str):
    """
    Finds the 'WithoutLinkCap' column and returns the first non-empty
    value found below it — the caption used on runs that post WITHOUT a
    link (chosen via Link_Percentage). Optional column: if it's missing
    or empty, callers should fall back to the regular 'Caption'. Never
    cleared, same value reused every time it's picked.
    Returns (caption, row_num, col_idx) or (None, None, None).
    """
    return _sheet_get_named_column(sheets_service, spreadsheet_id, "WithoutLinkCap", required=False)


def _sheet_find_column_index(sheets_service, spreadsheet_id: str, column_name: str):
    """Returns the 0-based index of `column_name` in row 1, or None if not found."""
    rows = _read_sheet_rows(sheets_service, spreadsheet_id)
    if not rows:
        return None
    header = rows[0]
    for i, h in enumerate(header):
        if h.strip().lower() == column_name.lower():
            return i
    return None


def sheet_get_fb_storage_state(sheets_service, spreadsheet_id: str):
    """
    Reads the current Facebook session (storage_state JSON) from the
    'FbStorageState' column, row 2. This is the primary source for the
    session now — refreshed by sheet_set_fb_storage_state() after every
    run so it never goes stale. Optional column: if missing/empty,
    callers should fall back to the FB_STORAGE_STATE env var / local file.
    Returns (json_str, row_num, col_idx) or (None, None, None).
    """
    return _sheet_get_named_column(
        sheets_service, spreadsheet_id, FB_STORAGE_STATE_COLUMN_NAME, required=False
    )


def sheet_set_fb_storage_state(sheets_service, spreadsheet_id: str, storage_state_json: str):
    """
    Refreshes the Facebook session stored in the 'FbStorageState' column
    (row 2): the OLD value is deleted first, then the fresh session JSON
    is written in its place, so the sheet always holds exactly one
    up-to-date session and the login never expires between runs. If the
    'FbStorageState' header doesn't exist yet, this just warns and does
    nothing (add the header once to enable sheet-based session storage).
    """
    step(f"Refreshing '{FB_STORAGE_STATE_COLUMN_NAME}' in Google Sheet")

    if len(storage_state_json) > SHEETS_CELL_CHAR_LIMIT:
        warn(f"Session JSON is {len(storage_state_json)} chars, over the "
             f"{SHEETS_CELL_CHAR_LIMIT}-char Google Sheets cell limit — "
             f"cannot store it in the sheet this run")
        return

    col_idx = _sheet_find_column_index(sheets_service, spreadsheet_id, FB_STORAGE_STATE_COLUMN_NAME)
    if col_idx is None:
        warn(f"No '{FB_STORAGE_STATE_COLUMN_NAME}' column found in header row — "
             f"add this header (row 2 will hold the session JSON) to enable "
             f"sheet-based session refresh. Skipping for now.")
        return

    col_letter = chr(ord('A') + col_idx)
    cell_range = f"{col_letter}2"

    try:
        # Delete the old session first...
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=cell_range, body={}
        ).execute()
        # ...then write the fresh one in its place.
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=cell_range,
            valueInputOption="RAW",
            body={"values": [[storage_state_json]]},
        ).execute()
        ok(f"'{FB_STORAGE_STATE_COLUMN_NAME}' cell {cell_range} refreshed "
           f"with the latest session ({len(storage_state_json)} chars)")
    except Exception as e:
        warn(f"Could not refresh '{FB_STORAGE_STATE_COLUMN_NAME}' in sheet: {e}")


def sheet_get_next_urls(sheets_service, spreadsheet_id: str, count: int):
    """
    Reads the 'Urls' tab (same spreadsheet) and returns up to `count`
    unused URLs — rows whose 'Status' column is NOT "Posted"/"Replaced" —
    in sheet order, along with their row numbers and the detected column
    indices, so callers can mark them as used afterwards. URL rows are
    never deleted or cleared; only their Status cell gets written to.

    Returns (urls, rows, url_col_idx, status_col_idx):
      urls           — list[str] of up to `count` unused URLs
      rows           — list[int] matching row numbers for those URLs
      url_col_idx    — column index of the 'Urls' column (or None if missing)
      status_col_idx — column index of the 'Status' column (or None if missing)
    """
    step(f"Fetching up to {count} unused URL(s) from '{URLS_SHEET_NAME}' tab")
    range_str = f"'{URLS_SHEET_NAME}'!A:Z"
    rows_data = _read_sheet_rows(sheets_service, spreadsheet_id, range_str)
    if not rows_data:
        warn(f"'{URLS_SHEET_NAME}' tab appears empty or missing")
        return [], [], None, None

    header = rows_data[0]
    info(f"Header row ({URLS_SHEET_NAME}): {header}")

    url_col_idx = None
    status_col_idx = None
    for i, h in enumerate(header):
        hl = h.strip().lower()
        if hl == URLS_COLUMN_NAME.lower():
            url_col_idx = i
        elif hl == URLS_STATUS_COLUMN_NAME.lower():
            status_col_idx = i

    if url_col_idx is None:
        fail(f"No '{URLS_COLUMN_NAME}' column found in '{URLS_SHEET_NAME}' tab header row")
        return [], [], None, None

    info(f"'{URLS_COLUMN_NAME}' column at index {url_col_idx} "
         f"(column {chr(ord('A') + url_col_idx)})")

    if status_col_idx is None:
        warn(f"No '{URLS_STATUS_COLUMN_NAME}' column found in '{URLS_SHEET_NAME}' tab — "
             f"add a '{URLS_STATUS_COLUMN_NAME}' header so used URLs can be skipped "
             f"next time. Continuing without status tracking for now.")
    else:
        info(f"'{URLS_STATUS_COLUMN_NAME}' column at index {status_col_idx} "
             f"(column {chr(ord('A') + status_col_idx)})")

    found_urls, found_rows = [], []
    for row_num, row in enumerate(rows_data[1:], start=2):
        if len(found_urls) >= count:
            break
        if len(row) <= url_col_idx or not row[url_col_idx].strip():
            continue
        status_val = ""
        if status_col_idx is not None and len(row) > status_col_idx:
            status_val = row[status_col_idx].strip().lower()
        if status_val in URLS_POSTED_VALUES:
            continue
        url = row[url_col_idx].strip()
        found_urls.append(url)
        found_rows.append(row_num)
        info(f"  Selected URL row {row_num}: {url}")

    if not found_urls:
        warn(f"No unused URL found in '{URLS_SHEET_NAME}' tab (all marked "
             f"'{URLS_STATUS_COLUMN_NAME}' or empty)")
    else:
        ok(f"Selected {len(found_urls)} unused URL(s) (requested {count})")

    return found_urls, found_rows, url_col_idx, status_col_idx


def sheet_mark_urls_status(sheets_service, spreadsheet_id: str, rows: list[int],
                            status_col_idx, status_value: str = "Posted"):
    """
    Writes status_value into the 'Status' cell for each given row number in
    the 'Urls' tab, so those URLs are skipped on future runs. The URL cell
    itself is left untouched.
    """
    if not rows:
        return
    if status_col_idx is None:
        warn(f"Cannot mark URL(s) as '{status_value}' — no '{URLS_STATUS_COLUMN_NAME}' "
             f"column in '{URLS_SHEET_NAME}' tab (add one to enable status tracking)")
        return

    step(f"Marking {len(rows)} URL row(s) as '{status_value}' in '{URLS_SHEET_NAME}' tab")
    col_letter = chr(ord('A') + status_col_idx)
    for row_num in rows:
        try:
            cell_range = f"'{URLS_SHEET_NAME}'!{col_letter}{row_num}"
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=cell_range,
                valueInputOption="RAW",
                body={"values": [[status_value]]},
            ).execute()
            ok(f"Marked {cell_range} = {status_value}")
        except Exception as e:
            warn(f"Could not mark row {row_num} as '{status_value}': {e}")


def replace_urls_in_caption(caption: str, new_urls: list[str], mode: str, replace_count: int) -> str:
    """
    Replaces up to `replace_count` URL occurrences already inside the
    caption (in the order they appear) with URLs from new_urls.

      mode == "unique" — occurrence i is replaced with new_urls[i]
                          (each occurrence gets its own distinct URL).
      mode == "same"   — every targeted occurrence is replaced with
                          new_urls[0] (a single URL reused everywhere).

    If the caption has no URL at all, the new URL(s) are appended on
    their own lines instead so they still end up in the post:
      mode == "same"   → appends new_urls[0] once
      mode == "unique" → appends up to replace_count of new_urls
    """
    if not new_urls:
        warn("No new URLs supplied — caption left unchanged")
        return caption

    matches = list(URL_REGEX.finditer(caption))

    if not matches:
        warn("Caption had no existing URL(s) to replace — appending new URL(s) instead")
        if mode == "same":
            return f"{caption}\n{new_urls[0]}"
        return caption + "\n" + "\n".join(new_urls[:replace_count])

    n_to_replace = min(replace_count, len(matches))
    if n_to_replace < replace_count:
        warn(f"Caption only contains {len(matches)} URL(s) — replacing {n_to_replace} "
             f"instead of the requested {replace_count}")

    pieces = []
    last_end = 0
    for i, m in enumerate(matches):
        pieces.append(caption[last_end:m.start()])
        if i < n_to_replace:
            if mode == "same":
                pieces.append(new_urls[0])
            else:
                pieces.append(new_urls[i] if i < len(new_urls) else m.group(0))
        else:
            pieces.append(m.group(0))
        last_end = m.end()
    pieces.append(caption[last_end:])

    updated = "".join(pieces)
    ok(f"Replaced {n_to_replace} URL occurrence(s) in caption (mode={mode})")
    return updated


def sheet_clear_caption(sheets_service, spreadsheet_id: str, row_num: int, col_idx: int):
    """
    (Kept for reference / manual use — no longer called automatically.)
    Clears the caption cell. The main loop intentionally does NOT call
    this anymore, since the caption is meant to be reused every run.
    """
    step(f"Clearing used caption at row {row_num}")
    try:
        col_letter = chr(ord('A') + col_idx)
        cell_range = f"{col_letter}{row_num}"
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=cell_range, body={}
        ).execute()
        ok(f"Caption cell {cell_range} cleared")
    except Exception as e:
        warn(f"Could not clear caption cell: {e}")


def _to_int(val, default):
    try:
        return int(str(val).strip())
    except (TypeError, ValueError):
        return default


def _to_bool(val, default):
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in ("true", "yes", "y", "1", "on", "enable", "enabled"):
        return True
    if s in ("false", "no", "n", "0", "off", "disable", "disabled"):
        return False
    return default


def fetch_loop_timing():
    """
    Builds its own creds/Sheets service and reads LoopIntervalMinutes /
    MaxRuntimeMinutes from the sheet, with sane defaults if anything is
    missing. Used once at scheduler startup.
    """
    settings = {}
    try:
        creds = build_google_creds()
        sheets_service = build_sheets_service(creds)
        captions_sheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV) or DEFAULT_CAPTIONS_SHEET_ID
        settings = sheet_get_settings(sheets_service, captions_sheet_id)
    except Exception as e:
        warn(f"Could not read loop timing from sheet, using defaults: {e}")

    loop_interval = _to_int(settings.get("loop_interval_minutes"), DEFAULT_LOOP_INTERVAL_MINUTES)
    max_runtime   = _to_int(settings.get("max_runtime_minutes"), DEFAULT_MAX_RUNTIME_MINUTES)
    return loop_interval, max_runtime


# ─────────────────────────────────────────────────────────────────────────────
# Facebook / Playwright helpers
# ─────────────────────────────────────────────────────────────────────────────

def resolve_fb_storage_state(sheets_service=None, spreadsheet_id=None) -> str | None:
    step("Resolving Facebook storage state")

    # 1) PRIMARY: the 'FbStorageState' cell in the Google Sheet — this is
    #    kept fresh every run by sheet_set_fb_storage_state(), so it's the
    #    most reliable source and avoids GitHub-secret refresh entirely.
    if sheets_service is not None and spreadsheet_id:
        sheet_val, _row, _col = sheet_get_fb_storage_state(sheets_service, spreadsheet_id)
        if sheet_val:
            info(f"'{FB_STORAGE_STATE_COLUMN_NAME}' found in sheet, length={len(sheet_val)}")
            try:
                parsed = json.loads(sheet_val)
                cookies = parsed.get("cookies", [])
                ok(f"Valid JSON from sheet — {len(cookies)} cookies found")
                for c in cookies:
                    info(f"  Cookie: name={c.get('name')} expires={c.get('expires')} domain={c.get('domain')}")
                return sheet_val
            except json.JSONDecodeError as e:
                fail(f"'{FB_STORAGE_STATE_COLUMN_NAME}' in sheet is not valid JSON: {e}")
        else:
            warn(f"'{FB_STORAGE_STATE_COLUMN_NAME}' column empty/missing in sheet — "
                 f"falling back to FB_STORAGE_STATE env var / local file")

    # 2) FALLBACK: FB_STORAGE_STATE env var (secret) — useful as a one-time
    #    seed before the sheet has ever been populated.
    env_val = os.environ.get(FB_STORAGE_STATE_ENV)
    if env_val:
        info(f"FB_STORAGE_STATE env var found, length={len(env_val)}")
        try:
            parsed = json.loads(env_val)
            cookies = parsed.get("cookies", [])
            ok(f"Valid JSON — {len(cookies)} cookies found")
            for c in cookies:
                info(f"  Cookie: name={c.get('name')} expires={c.get('expires')} domain={c.get('domain')}")
            return env_val
        except json.JSONDecodeError as e:
            fail(f"FB_STORAGE_STATE is not valid JSON: {e}")
    else:
        warn("FB_STORAGE_STATE env var not set")

    # 3) LAST RESORT: a local storage_state.json left over from a previous run.
    if Path(STORAGE_STATE).exists():
        info(f"Found local {STORAGE_STATE} — using it")
        return Path(STORAGE_STATE).read_text(encoding="utf-8")

    fail("No valid Facebook session found!")
    return None


async def save_screenshot(page, name: str):
    SCREENSHOTS_DIR.mkdir(exist_ok=True)
    for p in [SCREENSHOTS_DIR / f"{name}.png", Path(f"{name}.png")]:
        try:
            await page.screenshot(path=str(p), full_page=False)
            info(f"Screenshot saved: {p}")
        except Exception as e:
            warn(f"Screenshot failed {p}: {e}")


async def dump_html(page, filename: str):
    try:
        content = await page.content()
        Path(filename).write_text(content, encoding="utf-8")
        info(f"HTML dumped: {filename} ({len(content)} chars)")
    except Exception as e:
        warn(f"HTML dump failed: {e}")


def is_picker_url(url: str) -> bool:
    return any(x in url for x in ["device-based", "/caa/", "login/caa", "login/identifier"])

def is_hard_login_url(url: str) -> bool:
    return "/login" in url and not is_picker_url(url)

def classify_url(url: str) -> str:
    if "checkpoint" in url:    return "CHECKPOINT"
    if is_hard_login_url(url): return "LOGIN_WALL"
    if is_picker_url(url):     return "DEVICE_PICKER"
    if "reels/create" in url:  return "REELS_CREATE"
    if "facebook.com" in url:  return "FACEBOOK_PAGE"
    return "OTHER"


async def force_tap(page, locator) -> bool:
    box = await locator.bounding_box()
    if box:
        cx, cy = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
        for method in [
            lambda: page.touchscreen.tap(cx, cy),
            lambda: page.mouse.click(cx, cy),
        ]:
            try:
                await method()
                return True
            except Exception:
                pass
    for method in [
        lambda: locator.click(force=True, timeout=5_000),
        lambda: locator.evaluate("el => el.click()"),
    ]:
        try:
            await method()
            return True
        except Exception:
            pass
    return False


FEED_SELECTORS = [
    '[aria-label="Home"]', '[data-pagelet="LeftRail"]', 'div[role="feed"]',
    '[aria-label="Create"]', 'span:has-text("What\'s on your mind?")',
    'div[aria-label="Stories"]', 'div[aria-label="Reels"]',
    'div[data-pagelet="FeedUnit_0"]', 'div[role="main"]',
]


async def nuke_continue_button(page, label: str) -> bool:
    info(f"Attempting to click Continue button [{label}]")
    SELECTORS = [
        '[aria-label^="Continue"]', '[aria-label*="Continue"]',
        'div[role="button"][aria-label^="Continue"]',
        'div[role="button"]:has-text("Continue")',
        'span:text-is("Continue")', 'span:has-text("Continue")',
        'button:has-text("Continue")',
    ]
    url_before = page.url

    found_sel = None
    for _ in range(10):
        for sel in SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    found_sel = sel
                    break
            except Exception:
                pass
        if found_sel:
            break
        await asyncio.sleep(1)

    if not found_sel:
        warn("No Continue button found in DOM after 10s")
        try:
            hit = await page.evaluate("""() => {
                const candidates = Array.from(document.querySelectorAll(
                    'div[role="button"],a[role="button"],button,a,span[tabindex]'
                ));
                const btn = candidates.find(el => {
                    const txt = (el.textContent||el.innerText||el.getAttribute('aria-label')||'').trim();
                    return /^continue/i.test(txt);
                });
                if (!btn) return null;
                btn.click();
                return btn.outerHTML.slice(0,200);
            }""")
            if hit:
                ok(f"JS found & clicked Continue: {hit[:80]}")
                await asyncio.sleep(5)
                return page.url != url_before
        except Exception as e:
            warn(f"JS search failed: {e}")

        info("Trying direct navigation bypass...")
        try:
            await page.goto("https://www.facebook.com/?sk=h_chr",
                            wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(5)
            if not is_picker_url(page.url) and not is_hard_login_url(page.url):
                ok(f"Direct nav bypassed picker → {page.url}")
                return True
        except Exception as e:
            warn(f"Direct nav failed: {e}")
        return False

    info(f"Found Continue button via: {found_sel}")
    loc = page.locator(found_sel).first
    for method_name, method in [
        ("standard click", lambda: loc.click(timeout=5_000)),
        ("force click",    lambda: loc.click(force=True, timeout=5_000)),
        ("JS click",       lambda: loc.evaluate("el => el.click()")),
    ]:
        try:
            await method()
            await asyncio.sleep(5)
            if page.url != url_before:
                ok(f"Continue clicked via {method_name} — URL changed")
                return True
            info(f"{method_name}: URL unchanged ({page.url})")
        except Exception as e:
            warn(f"{method_name} failed: {e}")

    return False


async def ensure_logged_in(page) -> bool:
    step("Checking Facebook login state")
    for attempt in range(6):
        url = page.url
        url_type = classify_url(url)
        info(f"Attempt {attempt+1}/6 — URL: {url}")
        info(f"URL type: {url_type}")

        if url_type == "CHECKPOINT":
            fail("Account checkpoint/restriction detected — manual action required")
            await save_screenshot(page, f"LOGIN_CHECKPOINT_{attempt+1}")
            await dump_html(page, f"checkpoint_{attempt+1}.html")
            return False

        if url_type == "LOGIN_WALL":
            fail("Hard login wall — session cookies are EXPIRED")
            await save_screenshot(page, f"LOGIN_WALL_{attempt+1}")
            return False

        if url_type == "DEVICE_PICKER":
            info("Device picker detected — trying to bypass")
            await dump_html(page, f"picker_{attempt+1}.html")
            ok_click = await nuke_continue_button(page, f"attempt={attempt+1}")
            await save_screenshot(page, f"after_continue_{attempt+1}")
            if not ok_click:
                warn(f"Could not click Continue on attempt {attempt+1}")
                await asyncio.sleep(3)
            continue

        for sel in FEED_SELECTORS:
            try:
                count = await page.locator(sel).count()
                if count > 0:
                    ok(f"Logged in confirmed via: {sel}")
                    return True
            except Exception:
                pass

        try:
            title = await page.title()
            info(f"Page title: {title}")
        except Exception:
            pass

        info(f"Feed not ready yet — waiting 4s (attempt {attempt+1}/6)")
        await asyncio.sleep(4)

    fail("Login check exhausted all 6 attempts")
    await dump_html(page, "login_failed_final.html")
    await save_screenshot(page, "LOGIN_FAILED_FINAL")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Caption entry — handles Lexical editor with line-aware fallback strategies
# ─────────────────────────────────────────────────────────────────────────────

def _nonempty_lines(text: str) -> list[str]:
    return [l for l in text.split("\n") if l.strip()]


async def _clear_field(page, field):
    await field.click(timeout=5_000)
    await asyncio.sleep(0.3)
    await page.keyboard.press("Control+a")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.2)


async def enter_caption_lexical(page, caption: str) -> bool:
    """
    Try several strategies to get `caption` into Facebook's Lexical
    editor, in order of fidelity. Each strategy is verified by comparing
    the number of non-empty lines that actually landed in the field
    against the number of non-empty lines in the intended caption — not
    just "some text is present" — since a squished, single-line result
    still contains all the characters but is not a correct caption.
    """
    LEXICAL_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]

    expected_lines = len(_nonempty_lines(caption))

    async def strategy_keyboard_type_lines(field):
        # PRIMARY strategy: type each line separately and press a real
        # Enter key between lines. This is what reliably creates new
        # paragraph nodes in Lexical — embedding "\n" inside a single
        # insertText/InputEvent/keyboard.type call does not.
        info("Strategy 1: keyboard.type line-by-line with explicit Enter")
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.keyboard.type(line, delay=12)
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strategy_clipboard(field):
        # Real clipboard paste — Lexical's paste handler understands
        # line breaks correctly when the clipboard actually contains
        # text with real newlines. Requires clipboard-write/read
        # permissions to be granted on the browser context.
        info("Strategy 2: clipboard paste via Ctrl+V")
        await _clear_field(page, field)
        await page.evaluate(
            "(text) => navigator.clipboard.writeText(text).catch(() => {})",
            caption,
        )
        await asyncio.sleep(0.3)
        await page.keyboard.press("Control+v")
        await asyncio.sleep(0.8)

    async def strategy_exec_command_per_line(field):
        # execCommand insertText per line + insertParagraph between
        # lines, rather than a single blob containing "\n".
        info("Strategy 3: execCommand insertText per line")
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, text) => {
                        el.focus();
                        document.execCommand('insertText', false, text);
                    }""",
                    [field, line],
                )
            if i < len(lines) - 1:
                await page.evaluate(
                    """(el) => {
                        el.focus();
                        document.execCommand('insertParagraph', false, null);
                    }""",
                    field,
                )
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strategy_input_event_per_line(field):
        # InputEvent dispatch per line, with a separate Enter keypress
        # (real KeyboardEvent, not just an inserted character) between
        # lines so Lexical's key handler creates a new block.
        info("Strategy 4: InputEvent dispatch per line + Enter keypress")
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, text) => {
                        el.focus();
                        const sel = window.getSelection();
                        const range = document.createRange();
                        range.selectNodeContents(el);
                        range.collapse(false);
                        sel.removeAllRanges();
                        sel.addRange(range);
                        const ev = new InputEvent('beforeinput', {
                            inputType: 'insertText', data: text,
                            bubbles: true, cancelable: true,
                        });
                        el.dispatchEvent(ev);
                        const ev2 = new InputEvent('input', {
                            inputType: 'insertText', data: text, bubbles: true,
                        });
                        el.dispatchEvent(ev2);
                    }""",
                    [field, line],
                )
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    strategies = [
        strategy_keyboard_type_lines,
        strategy_clipboard,
        strategy_exec_command_per_line,
        strategy_input_event_per_line,
    ]

    for i, strategy in enumerate(strategies, 1):
        for sel in LEXICAL_SELECTORS:
            try:
                field = page.locator(sel).first
                if await field.count() == 0:
                    continue
                await strategy(field)
                txt = await field.evaluate(
                    "el => (el.innerText || el.textContent || '').trim()"
                )
                actual_lines = len(_nonempty_lines(txt))
                if txt and len(txt) > 2 and abs(actual_lines - expected_lines) <= 1:
                    ok(f"Caption entered via strategy {i} / selector '{sel}' "
                       f"({len(txt)} chars, {actual_lines}/{expected_lines} lines matched)")
                    return True
                elif txt and len(txt) > 2:
                    warn(f"Strategy {i} / '{sel}': text landed but line count "
                         f"mismatch ({actual_lines} vs expected {expected_lines}) "
                         f"— likely squished, trying next strategy")
                else:
                    warn(f"Strategy {i} / '{sel}': field empty after attempt")
            except Exception as e:
                warn(f"Strategy {i} / '{sel}' raised: {e}")

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Upload flow
# ─────────────────────────────────────────────────────────────────────────────

async def upload_reel(caption: str, video_path: str, sheets_service=None, spreadsheet_id=None) -> bool:
    step("Starting Facebook Reel upload")
    if not Path(video_path).exists():
        fail(f"Video file not found: {video_path}")
        return False

    size_mb = Path(video_path).stat().st_size // (1024 * 1024)
    ok(f"Video: {video_path}  ({size_mb} MB)")
    info(f"Caption: {caption[:120]}")

    async with async_playwright() as p:
        step("Launching Chromium browser")
        try:
            browser = await p.chromium.launch(
                headless=True,
                timeout=30_000,
                args=[
                    "--no-sandbox", "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars", "--disable-dev-shm-usage",
                    "--single-process", "--no-zygote",
                ]
            )
            ok("Browser launched")
        except Exception as e:
            fail(f"Browser launch FAILED: {e}")
            return False

        storage_state_json = resolve_fb_storage_state(sheets_service, spreadsheet_id)
        if not storage_state_json:
            fail("No Facebook session available — aborting")
            await browser.close()
            return False

        context_kwargs = dict(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="Asia/Karachi",
            accept_downloads=True,
        )

        step("Creating browser context with session cookies")
        try:
            state = json.loads(storage_state_json)
            context_kwargs["storage_state"] = state
            context = await browser.new_context(**context_kwargs)
            ok(f"Context created with {len(state.get('cookies', []))} cookies")
        except Exception as e:
            fail(f"Context creation failed: {e}")
            await browser.close()
            return False

        # Grant clipboard permissions so the clipboard-paste caption
        # strategy actually has a chance to work in headless Chromium —
        # without this, navigator.clipboard.writeText() can fail silently.
        step("Granting clipboard permissions")
        try:
            await context.grant_permissions(
                ["clipboard-read", "clipboard-write"], origin="https://www.facebook.com"
            )
            ok("Clipboard permissions granted for facebook.com")
        except Exception as e:
            warn(f"Could not grant clipboard permissions: {e}")

        published = False
        try:
            published = await _run_upload_flow(context, caption, video_path)
        except Exception as e:
            fail(f"Upload flow crashed with exception: {e}")
            import traceback
            print(traceback.format_exc())
        finally:
            try:
                fresh = await context.storage_state()
                fresh_json = json.dumps(fresh)
                Path(STORAGE_STATE).write_text(fresh_json, encoding="utf-8")
                ok(f"Saved refreshed storage_state locally ({len(fresh.get('cookies', []))} cookies)")

                # Persist the refreshed session back to the Google Sheet so
                # the next run (even a fresh GitHub Actions job with no
                # local files) picks up the newest cookies instead of an
                # expiring GitHub secret.
                if sheets_service is not None and spreadsheet_id:
                    sheet_set_fb_storage_state(sheets_service, spreadsheet_id, fresh_json)
                else:
                    warn("No Sheets service/spreadsheet ID available — "
                         "could not refresh the session in the Google Sheet")
            except Exception as e:
                warn(f"Could not save storage_state: {e}")
            await browser.close()
            ok("Browser closed")

    return published


async def _run_upload_flow(context, caption: str, video_path: str) -> bool:
    page = await context.new_page()
    published = False

    # ── Step 1: Load Facebook ─────────────────────────────────────────────
    step("Loading Facebook homepage")
    try:
        response = await page.goto("https://www.facebook.com/",
                                   wait_until="domcontentloaded", timeout=60_000)
        info(f"HTTP status: {response.status if response else 'unknown'}")
    except Exception as e:
        fail(f"Page load failed: {e}")
        await save_screenshot(page, "FAIL_01_load")
        return False

    await asyncio.sleep(8)
    info(f"Current URL after load: {page.url}")
    info(f"URL type: {classify_url(page.url)}")
    await save_screenshot(page, "01_after_load")

    # ── Step 2: Login check ───────────────────────────────────────────────
    if not await ensure_logged_in(page):
        fail("ABORT: Could not confirm login")
        return False
    await save_screenshot(page, "02_logged_in")
    ok("Login confirmed — proceeding to upload")

    # ── Step 3: Navigate to Reels create ─────────────────────────────────
    step("Navigating to Reels create page")
    try:
        response = await page.goto("https://www.facebook.com/reels/create/",
                                   wait_until="domcontentloaded", timeout=60_000)
        info(f"HTTP status: {response.status if response else 'unknown'}")
    except Exception as e:
        fail(f"Navigation to reels/create failed: {e}")
        await save_screenshot(page, "FAIL_03_nav")
        return False

    await asyncio.sleep(8)
    info(f"Current URL: {page.url}")
    info(f"URL type: {classify_url(page.url)}")
    await save_screenshot(page, "03_reels_create")
    await dump_html(page, "03_reels_create.html")

    if "reels/create" not in page.url:
        warn(f"Got redirected away from reels/create to: {page.url}")

    # ── Step 4: Attach video ──────────────────────────────────────────────
    step("Attaching video file")
    uploaded = False

    for sel in ['input[type="file"][accept*="video"]', 'input[type="file"]']:
        try:
            inp = page.locator(sel)
            count = await inp.count()
            info(f"File input selector '{sel}': {count} found")
            if count > 0:
                await inp.first.set_input_files(video_path)
                ok(f"Video attached via direct input: {sel}")
                uploaded = True
                break
        except Exception as e:
            warn(f"Direct input {sel} failed: {e}")

    if not uploaded:
        info("Direct input failed — trying upload button click")
        button_selectors = [
            ('Select video',      'div[role="button"]:has-text("Select video")'),
            ('Upload',            'div[role="button"]:has-text("Upload")'),
            ('Add video',         'div[role="button"]:has-text("Add video")'),
            ('Select Video span', 'span:has-text("Select video")'),
            ('aria-label',        '[aria-label="Select video"]'),
            ('Add to reel',       'div[aria-label="Add to reel"]'),
            ('from computer',     'div:has-text("Select video from computer")'),
        ]
        for btn_name, sel in button_selectors:
            el = page.locator(sel).first
            try:
                count = await el.count()
                info(f"Upload button '{btn_name}': {count} found")
                if count == 0:
                    continue
                async with page.expect_file_chooser(timeout=10_000) as fc_info:
                    await el.click(force=True)
                fc = await fc_info.value
                await fc.set_files(video_path)
                ok(f"File chooser upload via: {btn_name}")
                uploaded = True
                break
            except Exception as e:
                warn(f"Button '{btn_name}' failed: {e}")

    await save_screenshot(page, "04_after_upload_attempt")
    await dump_html(page, "04_after_upload.html")

    if not uploaded:
        fail("ABORT: Could not attach video — no file input or upload button found")
        return False
    ok("Video attached successfully")

    # ── Step 5: Wait for Next button to become active ─────────────────────
    # Facebook flow: Upload → "Edit reel" screen (trim/CC) with Next button
    step("Waiting for Next button to become active (up to 3 min)")

    next_selectors = [
        'div[aria-label="Next"][role="button"]',
        'div[role="button"]:has-text("Next")',
        'span:has-text("Next")',
        'button:has-text("Next")',
    ]

    next_ready = False
    for elapsed in range(0, 180, 5):
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    disabled = await btn.get_attribute("aria-disabled")
                    info(f"[{elapsed}s] Next found via '{sel}', aria-disabled={disabled}")
                    if disabled != "true":
                        ok(f"Next button is active after {elapsed}s!")
                        next_ready = True
                        break
            except Exception:
                pass
        if next_ready:
            break
        if elapsed % 15 == 0:
            await save_screenshot(page, f"04_processing_{elapsed}s")
        await asyncio.sleep(5)

    if not next_ready:
        warn("Next button never became active after 3 minutes")
        await save_screenshot(page, "04_processing_timeout")
        await dump_html(page, "04_processing_timeout.html")

    # ── Step 5a/5b: Click Next until caption field appears ────────────────
    # Facebook shows 1–2 intermediate screens before "Reel settings" where
    # the caption lives.  We keep clicking Next (up to 3 times) until the
    # Lexical caption field is visible.
    step("Clicking Next until caption field appears (up to 3 clicks)")

    CAPTION_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]

    async def caption_field_visible() -> bool:
        for sel in CAPTION_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    async def click_next_btn() -> bool:
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() == 0:
                    continue
                disabled = await btn.get_attribute("aria-disabled")
                if disabled == "true":
                    continue
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(timeout=10_000)
                ok(f"Next clicked via: {sel}")
                return True
            except Exception as e:
                warn(f"Next click '{sel}' failed: {e}")
        return False

    caption_field_found = False
    for next_attempt in range(1, 4):          # try up to 3 Next clicks
        info(f"Next-click attempt {next_attempt}/3")

        # First check if caption field is already on screen
        if await caption_field_visible():
            ok(f"Caption field already visible before click {next_attempt}")
            caption_field_found = True
            break

        # Click Next
        clicked = await click_next_btn()
        if not clicked:
            warn(f"Could not find an active Next button on attempt {next_attempt}")
            await save_screenshot(page, f"05_no_next_{next_attempt}")
            break

        await save_screenshot(page, f"05_after_next{next_attempt}")
        info(f"URL after Next click {next_attempt}: {page.url}")

        # Wait up to 15s for caption field to appear
        for elapsed in range(0, 15, 2):
            if await caption_field_visible():
                ok(f"Caption field appeared {elapsed}s after Next click {next_attempt}")
                caption_field_found = True
                break
            await asyncio.sleep(2)

        if caption_field_found:
            break

        info(f"Caption field not visible after Next click {next_attempt} — trying another Next")

    if not caption_field_found:
        warn("Caption field never appeared after 3 Next clicks — dumping HTML for inspection")
        await dump_html(page, "05b_no_caption_field.html")
        await save_screenshot(page, "05b_no_caption_field")
    else:
        await save_screenshot(page, "05b_caption_ready")

    # ── Step 6: Enter caption ─────────────────────────────────────────────
    step("Entering caption text")
    info(f"Caption to type ({len(caption)} chars, "
         f"{len(_nonempty_lines(caption))} non-empty lines): {caption[:80]}")

    caption_ok = await enter_caption_lexical(page, caption)

    if not caption_ok:
        warn("Caption could not be entered with correct line breaks after all "
             "strategies — continuing anyway (post may have a squished or "
             "missing caption; check 06_after_caption.png)")
    await save_screenshot(page, "06_after_caption")

    # ── Step 7: Advance to Post panel if needed ───────────────────────────
    # If Post button is already visible we don't need another Next click.
    step("Advancing to Post panel (clicking Next if Post not yet visible)")

    async def post_button_visible() -> bool:
        for sel in [
            'div[aria-label="Post"][role="button"]',
            'div[role="button"]:text-is("Post")',
            'span:text-is("Post")',
        ]:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    if await post_button_visible():
        ok("Post button already visible — skipping extra Next click")
    else:
        # Click Next (or Post if labelled that way) to advance
        post_or_next_selectors = [
            'div[aria-label="Post"][role="button"]',
            'div[role="button"]:text-is("Post")',
            'div[aria-label="Next"][role="button"]',
            'div[role="button"]:has-text("Next")',
            'span:text-is("Post")',
            'span:has-text("Next")',
            'button:has-text("Post")',
            'button:has-text("Next")',
        ]
        clicked_next2 = False
        for sel in post_or_next_selectors:
            try:
                btn = page.locator(sel).last
                if await btn.count() == 0:
                    continue
                disabled = await btn.get_attribute("aria-disabled")
                if disabled == "true":
                    info(f"Skipping '{sel}' — disabled")
                    continue
                label_text = await btn.inner_text()
                info(f"Found button '{sel}' with text: {label_text!r}")
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(force=True)
                ok(f"Clicked '{label_text.strip()}' via: {sel}")
                clicked_next2 = True
                await asyncio.sleep(4)
                break
            except Exception as e:
                warn(f"Button '{sel}' failed: {e}")

        if not clicked_next2:
            warn("Could not click Next/Post — attempting Post step anyway")

    await save_screenshot(page, "07_before_post")
    await dump_html(page, "07_before_post.html")
    info(f"URL after second Next: {page.url}")

    # ── Step 8: Click Post/Publish ────────────────────────────────────────
    step("Clicking Post / Publish button")

    post_selectors = [
        ("aria-label Post",    'div[aria-label="Post"][role="button"]'),
        ("text Post exact",    'div[role="button"]:text-is("Post")'),
        ("span Post exact",    'span:text-is("Post")'),
        ("aria-label Publish", 'div[aria-label="Publish"][role="button"]'),
        ("aria-label Share",   'div[aria-label="Share now"][role="button"]'),
        ("text Post",          'div[role="button"]:has-text("Post")'),
        ("text Publish",       'div[role="button"]:has-text("Publish")'),
        ("text Share now",     'div[role="button"]:has-text("Share now")'),
        ("submit button",      'button[type="submit"]'),
    ]

    # Wait up to 10s for the Post button to appear
    post_btn_found = False
    for wait_elapsed in range(0, 10, 2):
        for sel_name, sel in post_selectors:
            try:
                count = await page.locator(sel).count()
                if count > 0:
                    info(f"Post button '{sel_name}' visible after {wait_elapsed}s")
                    post_btn_found = True
                    break
            except Exception:
                pass
        if post_btn_found:
            break
        info(f"Waiting for Post button... {wait_elapsed}s")
        await asyncio.sleep(2)

    if not post_btn_found:
        warn("Post button not yet visible — attempting click anyway")

    post_clicked = False
    for sel_name, sel in post_selectors:
        try:
            btn = page.locator(sel).last
            count = await btn.count()
            info(f"Post button '{sel_name}': {count} found")
            if count == 0:
                continue
            disabled = await btn.get_attribute("aria-disabled")
            if disabled == "true":
                warn(f"  '{sel_name}' is disabled — skipping")
                continue
            await btn.scroll_into_view_if_needed(timeout=5_000)
            await btn.click(force=True)
            ok(f"Post button clicked via: {sel_name}")
            post_clicked = True
            await asyncio.sleep(5)
            break
        except Exception as e:
            warn(f"Post '{sel_name}' failed: {e}")

    if not post_clicked:
        fail("Could not click any Post/Publish button")
        fail("Check 07_before_post.html to see available buttons")
        await save_screenshot(page, "FAIL_08_no_post_button")
        return False

    # ── Step 9: Wait for confirmation ─────────────────────────────────────
    step("Waiting for publish confirmation (up to 60s)")

    confirm_selectors = [
        'span:has-text("Your reel is now shared")',
        'span:has-text("Reel posted")',
        'span:has-text("Published")',
        'span:has-text("Your reel")',
        'div:has-text("Your reel was shared")',
        'span:has-text("shared")',
    ]

    for elapsed in range(0, 60, 5):
        for sel in confirm_selectors:
            try:
                if await page.locator(sel).count() > 0:
                    ok(f"🎉 PUBLISHED! Confirmed via: {sel} (after {elapsed}s)")
                    published = True
                    break
            except Exception:
                pass
        if published:
            break
        info(f"Waiting for confirmation... {elapsed}s")
        if elapsed % 15 == 0:
            await save_screenshot(page, f"09_waiting_confirm_{elapsed}s")
        await asyncio.sleep(5)

    if not published:
        info("No explicit confirmation — checking page state...")
        try:
            url_after = page.url
            title_after = await page.title()
            info(f"Final URL: {url_after}")
            info(f"Final title: {title_after}")
        except Exception:
            pass

        try:
            post_panel_gone = await page.locator('div[aria-label="Post"][role="button"]').count() == 0
            info(f"Post panel gone: {post_panel_gone}")
            if post_panel_gone and post_clicked:
                ok("🎉 PUBLISHED (inferred — Post panel gone, no errors detected)")
                published = True
        except Exception:
            pass

    await save_screenshot(page, "09_final_result")
    await dump_html(page, "09_final_result.html")

    if not published:
        warn("Could not confirm publish — check 09_final_result.png")
        warn("The reel may have posted anyway; check your Facebook profile")

    return published


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_once():
    global _step
    _step = 0
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"  🚀 Run started at {ts}")
    print(f"  Python: {sys.version}")
    print(f"  PID: {os.getpid()}")
    print(f"{'='*60}")

    step("Checking environment variables")
    info(f"{CAPTIONS_SHEET_ID_ENV}: {os.environ.get(CAPTIONS_SHEET_ID_ENV) or '(using default sheet)'}")
    for secret in [FB_STORAGE_STATE_ENV, GOOGLE_CREDS_ENV]:
        val = os.environ.get(secret, "")
        info(f"{secret}: {'SET (' + str(len(val)) + ' chars)' if val else 'NOT SET'}")
    info(f"Mega.nz remote (rclone): '{MEGA_REMOTE_NAME}' (configured via rclone.conf in the workflow)")

    try:
        creds          = build_google_creds()
        sheets_service = build_sheets_service(creds)
    except Exception as e:
        fail(f"Google service setup failed: {e}")
        return

    captions_sheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV) or DEFAULT_CAPTIONS_SHEET_ID

    settings = sheet_get_settings(sheets_service, captions_sheet_id)
    mega_folder      = settings.get("mega_folder") or DEFAULT_MEGA_FOLDER
    mega_move_folder = settings.get("mega_move_folder") or DEFAULT_MEGA_MOVE_FOLDER
    info(f"Mega.nz source folder: {mega_folder}")
    info(f"Mega.nz move-to-uploaded folder: {mega_move_folder}")

    # ── Decide WITH-link vs WITHOUT-link caption for this run ───────────
    link_percentage = _to_int(settings.get("link_percentage"), DEFAULT_LINK_PERCENTAGE)
    link_percentage = max(0, min(100, link_percentage))
    roll = random.randint(1, 100)
    use_link_caption = roll <= link_percentage
    info(f"Link_Percentage={link_percentage}% — roll={roll}/100 → "
         f"{'WITH link (Caption)' if use_link_caption else 'WITHOUT link (WithoutLinkCap)'}")

    if use_link_caption:
        caption, cap_row, cap_col = sheet_get_caption(sheets_service, captions_sheet_id)
        if not caption:
            fail("No caption available in the Google Sheet — aborting this run "
                 "(add a value to the 'Caption' column)")
            return
    else:
        caption, cap_row, cap_col = sheet_get_withoutlink_caption(sheets_service, captions_sheet_id)
        if not caption:
            warn("'WithoutLinkCap' column is empty or missing — falling back to the "
                 "regular 'Caption' (with link) for this run instead")
            use_link_caption = True
            caption, cap_row, cap_col = sheet_get_caption(sheets_service, captions_sheet_id)
            if not caption:
                fail("No caption available in the Google Sheet — aborting this run "
                     "(add a value to the 'Caption' or 'WithoutLinkCap' column)")
                return

    # ── URL replacement settings (only ever applies to the WITH-link caption) ─
    url_replace_enabled = use_link_caption and _to_bool(
        settings.get("url_replace_enabled"), DEFAULT_URL_REPLACE_ENABLED
    )
    new_urls, used_rows, url_col_idx, status_col_idx = [], [], None, None

    if not use_link_caption:
        info("Posting WITHOUT a link this run — 'Urls' tab not touched")
    elif not url_replace_enabled:
        info("UrlReplaceEnabled is FALSE — posting caption as-is, 'Urls' tab not touched")
    else:
        url_replace_count = _to_int(settings.get("url_replace_count"), DEFAULT_URL_REPLACE_COUNT)
        url_replace_mode = (settings.get("url_replace_mode") or DEFAULT_URL_REPLACE_MODE).strip().lower()
        if url_replace_mode not in ("unique", "same"):
            warn(f"Unknown UrlReplaceMode '{url_replace_mode}' — defaulting to 'unique'")
            url_replace_mode = "unique"
        info(f"URL replace settings: UrlReplaceCount={url_replace_count}, UrlReplaceMode={url_replace_mode}")

        # How many URL occurrences actually exist in the caption right now
        n_matches = len(URL_REGEX.findall(caption))
        n_to_replace = min(url_replace_count, n_matches) if n_matches else url_replace_count

        # "same" mode only ever needs ONE URL pulled from the sheet, no matter
        # how many occurrences it gets stamped into. "unique" mode needs one
        # distinct URL per occurrence being replaced.
        fetch_count = 1 if url_replace_mode == "same" else max(n_to_replace, 1)

        new_urls, used_rows, url_col_idx, status_col_idx = sheet_get_next_urls(
            sheets_service, captions_sheet_id, fetch_count
        )

        if new_urls:
            if url_replace_mode == "unique" and len(new_urls) < n_to_replace:
                warn(f"Only {len(new_urls)} unused URL(s) available — replacing "
                     f"{len(new_urls)} instead of {n_to_replace}")
                n_to_replace = len(new_urls)
            caption = replace_urls_in_caption(caption, new_urls, url_replace_mode, n_to_replace)
        else:
            warn(f"No unused URLs available in the '{URLS_SHEET_NAME}' tab — "
                 f"posting caption without swapping any URL")

    try:
        videos = mega_list_videos(mega_folder)
    except Exception as e:
        fail(f"Could not list Mega.nz folder: {e}")
        return

    if not videos:
        info("No videos in Mega.nz folder — nothing to do this run")
        return

    video_meta = videos[0]
    file_name  = video_meta["Name"]
    ok(f"Selected video: {file_name}")

    with tempfile.TemporaryDirectory() as tmp:
        try:
            local_path = mega_download_video(mega_folder, file_name, tmp)
        except Exception as e:
            fail(f"Download failed: {e}")
            return

        try:
            published = asyncio.run(upload_reel(
                caption=caption,
                video_path=local_path,
                sheets_service=sheets_service,
                spreadsheet_id=captions_sheet_id,
            ))
        except Exception as e:
            fail(f"Upload exception: {e}")
            import traceback
            print(traceback.format_exc())
            published = False

    if published:
        try:
            mega_move_to_uploaded(mega_folder, mega_move_folder, file_name)
        except Exception as e:
            warn(f"Move to uploaded folder failed: {e}")

        # Used URLs are marked "Posted" in the Status column (never deleted),
        # so they're skipped next time. The caption itself is left in place
        # so the same caption (with fresh URL(s)) is used again next run.
        if new_urls and used_rows:
            sheet_mark_urls_status(sheets_service, captions_sheet_id, used_rows, status_col_idx, "Posted")
        info(f"Caption left in sheet (not cleared) — will be reused next run "
             f"({'with fresh URL(s)' if use_link_caption else 'no link'})")
    else:
        warn("Upload not confirmed — video stays in the Mega.nz source folder for retry, "
             "caption left untouched and no URLs marked as Posted")

    print(f"\n{'='*60}")
    print(f"  Run complete. Published={published}")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────────────────────

def run_scheduled():
    if not HAS_SCHEDULE:
        fail("'schedule' package not installed. Run: pip install schedule")
        sys.exit(1)

    loop_interval_minutes, max_runtime_minutes = fetch_loop_timing()
    print(f"⏰ Scheduler started — posting every {loop_interval_minutes} minute(s); "
          f"this window closes after {max_runtime_minutes} minute(s) "
          f"(both read from the Google Sheet)")

    run_once()
    schedule.every(loop_interval_minutes).minutes.do(run_once)

    start_time = time.monotonic()
    iteration = 0
    while True:
        elapsed_minutes = (time.monotonic() - start_time) / 60
        if elapsed_minutes >= max_runtime_minutes:
            print(f"⏹️  Runtime window of {max_runtime_minutes} minute(s) reached — "
                  f"exiting cleanly so the workflow can self-requeue")
            return
        schedule.run_pending()
        time.sleep(30)
        iteration += 1
        if iteration % 20 == 0:
            next_run = schedule.next_run()
            print(f"⏳ Alive — next run at {next_run.strftime('%H:%M:%S') if next_run else 'unknown'}")


if __name__ == "__main__":
    if "--once" in sys.argv or os.environ.get("RUN_ONCE"):
        run_once()
    else:
        run_scheduled()
