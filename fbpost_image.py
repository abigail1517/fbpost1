"""
fbpost_image.py — MULTI-PAGE / MULTI-THREADED IMAGE POST VERSION
──────────────────────────────────────────────────────────────
Identical architecture to fbpost.py (reels), except it posts IMAGES via
Facebook's normal post composer instead of videos via Reels/create.
Everything else — the Google Sheet layout, page assignment
(AssignedRepo/AssignedStatus/AssignedAt), URL rotation, PostQueue mode,
retries, and the early self-requeue — works exactly the same way. If
you've already set up the sheet for the reel version you can reuse the
same spreadsheet: just point this script's MegaFolder at your images
folder (default "fbimages" instead of "fbreels").

DEBUG BUILD — this version adds, compared to the original:
    - A screenshot after every meaningful step of the post flow
      (screenshots/<page_id>_<step>.png), not just on failure.
    - An HTML dump of the page after every meaningful step
      (debug_html/<page_id>_<step>.html) so you can inspect the actual
      DOM Facebook served you when something doesn't click/type.
    - A rewritten caption-entry routine that force-clicks the field
      when a transient overlay is intercepting pointer events (this
      was the actual cause of "Caption entry unverified" in your log
      — Playwright found the field, confirmed it was visible/stable,
      but a still-animating composer/attachment overlay sat on top of
      it and blocked the plain click on every single strategy).
    - A rewritten Post-button click routine that logs every candidate
      button's text/aria-label/disabled-state it finds on the page
      before giving up, and falls back to a forced click + JS click
      if the normal click is blocked the same way.

Run modes:
    python -u fbpost_image.py --setup-sheet
    python -u fbpost_image.py --once
    python -u fbpost_image.py
"""

import asyncio, json, os, random, re, socket, subprocess, sys, tempfile, time
import urllib.request, urllib.error
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
import functools

print = functools.partial(print, flush=True)

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
FB_STORAGE_STATE_ENV      = "FB_STORAGE_STATE"
GOOGLE_CREDS_ENV          = "GOOGLE_CREDENTIALS_JSON"
CAPTIONS_SHEET_ID_ENV     = "CAPTIONS_SHEET_ID"
DEFAULT_CAPTIONS_SHEET_ID = "1RoqxuNbj74jfHCftM0iOuRjHEAlyzJ-pEOR_rYPhaII"

SELF_REQUEUE_LEAD_SECONDS = 60
_REQUEUED = False


def _default_repo_id() -> str:
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    if gh_repo:
        return gh_repo.strip().replace("/", "-")
    return socket.gethostname()


REPO_ID = os.environ.get("REPO_ID") or _default_repo_id()

IMAGE_EXTENSIONS  = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
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
    "megafolder":             "fbimages",
    "megamovefolder":         "fbimages_uploaded",
    "link_percentage":        "100",
    "urlreplacecount":        "1",
    "urlreplacemode":         "unique",
    "urlreplaceenabled":      "TRUE",
    "postmode":               "rotation",
    "assignedttlminutes":     "1440",
    "heartbeatminutes":       "15",
    "maxpagesperrepo":        "5",
    "linkrejectmaxretries":   "2",
    "urlswaponrejectonly":    "FALSE",
}

PAGES_HEADERS = [
    "PageId", "PageName", "Status", "MegaFolder", "MegaMoveFolder", "Caption",
    "WithoutLinkCap", "Link_Percentage", "LoopIntervalMinutes", "UrlReplaceCount",
    "UrlReplaceMode", "UrlReplaceEnabled", "UrlSwapOnRejectOnly", "PostMode", "FbStorageState",
    "LastPostedFile", "Notes",
    "AssignedRepo", "AssignedStatus", "AssignedAt",
]
SETTINGS_HEADERS  = ["Key", "Value"]
URLS_HEADERS      = ["Urls", "Status", "PageId"]
POSTQUEUE_HEADERS = ["FileName", "Caption", "Hashtags", "PageId", "Status"]

STORAGE_STATE_DIR = Path("storage_states")
SCREENSHOTS_DIR    = Path("screenshots")
DEBUG_HTML_DIR     = Path("debug_html")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
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
        return 10 ** 9


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
# GOOGLE SHEETS
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
    def __init__(self, service, spreadsheet_id):
        self.svc = service
        self.id = spreadsheet_id
        self._tab_cache = None

    @staticmethod
    def _retry(fn, *, attempts=3, base_delay=1.5, page_id=None, what="Sheets call"):
        last_err = None
        for i in range(1, attempts + 1):
            try:
                return fn()
            except Exception as e:
                last_err = e
                if i < attempts:
                    warn(page_id, f"{what} failed (attempt {i}/{attempts}): {e} — retrying")
                    time.sleep(base_delay * i)
        warn(page_id, f"{what} failed after {attempts} attempts: {last_err}")
        raise last_err

    def existing_tabs(self, refresh=False):
        if self._tab_cache is None or refresh:
            meta = self._retry(
                lambda: self.svc.spreadsheets().get(spreadsheetId=self.id).execute(),
                what="Sheets metadata read")
            self._tab_cache = {s["properties"]["title"] for s in meta.get("sheets", [])}
        return self._tab_cache

    def ensure_tab(self, title: str, headers: list[str]):
        if title not in self.existing_tabs():
            self._retry(lambda: self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.id,
                body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
            ).execute(), what=f"create tab '{title}'")
            self._tab_cache = None
            ok(None, f"Created missing tab '{title}'")
        rows = self.read_rows(title, "A1:Z1")
        if not rows or not any(c.strip() for c in rows[0]):
            self._retry(lambda: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{title}'!A1",
                valueInputOption="RAW", body={"values": [headers]},
            ).execute(), what=f"write header row for '{title}'")
            ok(None, f"Wrote header row for '{title}': {headers}")
        else:
            info(None, f"Tab '{title}' already has a header row — leaving it as-is")

    def ensure_columns(self, title: str, required_headers: list[str]):
        if title not in self.existing_tabs():
            return
        rows = self.read_rows(title, "A1:Z1")
        header = rows[0] if rows else []
        present = {h.strip().lower() for h in header if h.strip()}
        missing = [h for h in required_headers if h.lower() not in present]
        if not missing:
            return
        next_col = len(header)
        for i, h in enumerate(missing):
            col_letter = self._col_letter(next_col + i)
            self._retry(lambda cl=col_letter, hh=h: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{title}'!{cl}1",
                valueInputOption="RAW", body={"values": [[hh]]},
            ).execute(), what=f"add column '{h}' to '{title}'")
        ok(None, f"Added missing column(s) to '{title}': {missing}")

    def read_rows(self, tab: str, a1_range: str = "A:Z") -> list[list[str]]:
        try:
            result = self._retry(lambda: self.svc.spreadsheets().values().get(
                spreadsheetId=self.id, range=f"'{tab}'!{a1_range}"
            ).execute(), what=f"read '{tab}'!{a1_range}")
        except Exception as e:
            warn(None, f"Sheets read failed ('{tab}'!{a1_range}) after retries: {e}")
            return []
        return result.get("values", [])

    def write_cell(self, tab: str, row_num: int, col_idx: int, value: str):
        col_letter = self._col_letter(col_idx)
        try:
            self._retry(lambda: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}",
                valueInputOption="RAW", body={"values": [[value]]},
            ).execute(), what=f"write '{tab}'!{col_letter}{row_num}")
            return True
        except Exception as e:
            warn(None, f"Write failed '{tab}'!{col_letter}{row_num}' after retries: {e}")
            return False

    def clear_cell(self, tab: str, row_num: int, col_idx: int):
        col_letter = self._col_letter(col_idx)
        try:
            self._retry(lambda: self.svc.spreadsheets().values().clear(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}", body={}
            ).execute(), what=f"clear '{tab}'!{col_letter}{row_num}")
        except Exception as e:
            warn(None, f"Clear failed '{tab}'!{col_letter}{row_num}' after retries: {e}")

    @staticmethod
    def _col_letter(idx: int) -> str:
        letters = ""
        idx += 1
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    def as_dicts(self, tab: str):
        rows = self.read_rows(tab)
        if not rows:
            return [], {}
        header = [h.strip() for h in rows[0]]
        col_index = {h.lower(): i for i, h in enumerate(header) if h}
        out = []
        for row_num, row in enumerate(rows[1:], start=2):
            d = {header[i]: (row[i] if i < len(row) else "").strip()
                 for i in range(len(header)) if header[i]}
            d["_row"] = row_num
            out.append(d)
        return out, col_index


def setup_sheet(sheets_service, spreadsheet_id):
    step(None, f"Verifying sheet structure on spreadsheet {spreadsheet_id}")
    sh = Sheet(sheets_service, spreadsheet_id)
    sh.ensure_tab(SETTINGS_TAB, SETTINGS_HEADERS)
    sh.ensure_tab(PAGES_TAB, PAGES_HEADERS)
    sh.ensure_tab(URLS_TAB, URLS_HEADERS)
    sh.ensure_tab(POSTQUEUE_TAB, POSTQUEUE_HEADERS)
    sh.ensure_columns(PAGES_TAB, ["AssignedRepo", "AssignedStatus", "AssignedAt"])

    settings_rows, _ = sh.as_dicts(SETTINGS_TAB)
    present_keys = {r.get("Key", "").strip().lower() for r in settings_rows}
    existing = sh.read_rows(SETTINGS_TAB)
    next_row = len(existing) + 1 if existing else 2
    appended = [[key, val] for key, val in SETTINGS_DEFAULTS.items() if key not in present_keys]
    if appended:
        sh.svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{SETTINGS_TAB}'!A{next_row}",
            valueInputOption="RAW",
            body={"values": appended},
        ).execute()
        ok(None, f"Seeded {len(appended)} default setting(s) into '{SETTINGS_TAB}'")

    pages_rows, _ = sh.as_dicts(PAGES_TAB)
    if not pages_rows:
        warn(None, f"'{PAGES_TAB}' has no page rows yet — add one row per Facebook "
                    f"Page (PageId + FbStorageState are the minimum required fields)")
    ok(None, "Sheet structure OK")
    return sh


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
    url_swap_on_reject_only: bool
    post_mode: str
    storage_state_json: str | None
    max_runtime_minutes: int
    heartbeat_minutes: int
    link_reject_max_retries: int


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
        url_swap_on_reject_only=_to_bool(effective(row, master, "urlswaponrejectonly", "UrlSwapOnRejectOnly"), False),
        post_mode=(effective(row, master, "postmode", "PostMode") or "rotation").lower(),
        storage_state_json=row.get("FbStorageState") or None,
        max_runtime_minutes=max_runtime_minutes,
        heartbeat_minutes=_to_int(master.get("heartbeatminutes"), 15),
        link_reject_max_retries=_to_int(master.get("linkrejectmaxretries"), 2),
    )


# ─────────────────────────────────────────────────────────────────────────────
# ASSIGNMENT (identical to fbpost.py)
# ─────────────────────────────────────────────────────────────────────────────

def claim_pages(sh: Sheet, master: dict) -> list[dict]:
    step(None, f"Claiming pages for runner '{REPO_ID}'")
    header = sh.read_rows(PAGES_TAB, "A1:Z1")
    if not header:
        fail(None, f"'{PAGES_TAB}' tab is empty or missing")
        return []
    col_index = {h.strip().lower(): i for i, h in enumerate(header[0]) if h.strip()}
    if "pageid" not in col_index or "fbstoragestate" not in col_index:
        fail(None, f"'{PAGES_TAB}' is missing required columns")
        return []
    if not all(c in col_index for c in ("assignedrepo", "assignedstatus", "assignedat")):
        fail(None, f"'{PAGES_TAB}' is missing AssignedRepo/AssignedStatus/AssignedAt columns")
        return []

    ar_col, as_col, aa_col = col_index["assignedrepo"], col_index["assignedstatus"], col_index["assignedat"]
    rows, _ = sh.as_dicts(PAGES_TAB)
    assigned_ttl = _to_int(master.get("assignedttlminutes"), 1440)
    max_per_repo = max(1, _to_int(master.get("maxpagesperrepo"), 5))
    claimed = []

    def active_rows():
        return [r for r in rows
                if r.get("PageId", "").strip()
                and (r.get("Status", "") or "Active").strip().lower() != "paused"]

    my_in_use_count = sum(
        1 for r in active_rows()
        if r.get("AssignedRepo", "").strip() == REPO_ID
        and r.get("AssignedStatus", "").strip().lower() == "inuse"
    )

    for row in active_rows():
        page_id = row.get("PageId", "").strip()
        assigned_repo = row.get("AssignedRepo", "").strip()
        assigned_status = row.get("AssignedStatus", "").strip().lower()
        assigned_at = row.get("AssignedAt", "").strip()

        if assigned_repo and assigned_repo != REPO_ID:
            if assigned_status == "inuse":
                info(None, f"[{page_id}] in use by '{assigned_repo}' — skipping")
                continue
            if minutes_since(assigned_at) <= assigned_ttl:
                info(None, f"[{page_id}] owned by '{assigned_repo}' (idle) — not ours, skipping")
                continue
            warn(None, f"[{page_id}] '{assigned_repo}' assignment stale "
                        f"({minutes_since(assigned_at):.0f}m > {assigned_ttl}m) — taking over")
        elif not assigned_repo:
            if my_in_use_count >= max_per_repo:
                info(None, f"[{page_id}] unassigned, but repo '{REPO_ID}' already has "
                            f"{my_in_use_count}/{max_per_repo} page(s) — leaving for another repo")
                continue

        sh.write_cell(PAGES_TAB, row["_row"], ar_col, REPO_ID)
        sh.write_cell(PAGES_TAB, row["_row"], as_col, "InUse")
        sh.write_cell(PAGES_TAB, row["_row"], aa_col, now_iso())
        my_in_use_count += 1
        ok(None, f"[{page_id}] claimed ('InUse') by '{REPO_ID}'")
        row["AssignedRepo"], row["AssignedStatus"], row["AssignedAt"] = REPO_ID, "InUse", now_iso()
        claimed.append(row)

    return claimed


def get_pages_col_index(sh: Sheet) -> dict:
    header = sh.read_rows(PAGES_TAB, "A1:Z1")
    if not header:
        return {}
    return {h.strip().lower(): i for i, h in enumerate(header[0]) if h.strip()}


# ─────────────────────────────────────────────────────────────────────────────
# SELF-REQUEUE (identical mechanism to fbpost.py)
# ─────────────────────────────────────────────────────────────────────────────

def trigger_self_requeue(spreadsheet_id: str):
    global _REQUEUED
    if _REQUEUED:
        return
    token       = os.environ.get("GH_PAT")
    repo        = os.environ.get("GITHUB_REPOSITORY")
    workflow    = os.environ.get("GITHUB_WORKFLOW_FILE")
    ref         = os.environ.get("GITHUB_REF_NAME")
    if not all([token, repo, workflow, ref]):
        warn(None, "Self-requeue from Python skipped (missing GH_PAT / GITHUB_WORKFLOW_FILE "
                    "/ GITHUB_REF_NAME) — the workflow's own fallback step will handle it")
        return

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    payload = json.dumps({"ref": ref, "inputs": {"captions_sheet_id": spreadsheet_id}}).encode()
    req = urllib.request.Request(url, data=payload, method="POST", headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "fb-image-self-requeue",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            _REQUEUED = True
            ok(None, f"Next run queued early (HTTP {resp.status}) — will start back-to-back with this one")
            try:
                Path(".requeued").write_text(now_iso(), encoding="utf-8")
            except Exception:
                pass
    except urllib.error.HTTPError as e:
        warn(None, f"Self-requeue API call failed: HTTP {e.code} {e.read()[:300]}")
    except Exception as e:
        warn(None, f"Self-requeue API call failed: {e}")


async def schedule_self_requeue(spreadsheet_id: str, max_runtime_minutes: int):
    wait = max(0, max_runtime_minutes * 60 - SELF_REQUEUE_LEAD_SECONDS)
    try:
        await asyncio.sleep(wait)
        info(None, f"~{SELF_REQUEUE_LEAD_SECONDS}s left in this job's window — requeuing next run now")
        trigger_self_requeue(spreadsheet_id)
    except asyncio.CancelledError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# MEGA.NZ (via rclone)
# ─────────────────────────────────────────────────────────────────────────────

def _run_rclone(page_id, args: list[str], timeout: int = 300, quiet_substrings: tuple[str, ...] = ()):
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
        stderr_lower = (result.stderr or "").lower()
        if quiet_substrings and any(s.lower() in stderr_lower for s in quiet_substrings):
            info(page_id, f"rclone exit {result.returncode} (expected): {result.stderr.strip()[:200]}")
        else:
            warn(page_id, f"rclone exit {result.returncode}: {result.stderr.strip()[:400]}")
    return result.returncode, result.stdout, result.stderr


def mega_list_images(page_id, folder: str, missing_dir_ok: bool = False) -> list[dict]:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}"
    quiet = ("directory not found",) if missing_dir_ok else ()
    rc, out, err = _run_rclone(page_id, ["lsjson", remote_path, "--files-only"], quiet_substrings=quiet)
    if rc != 0:
        if missing_dir_ok and "directory not found" in (err or "").lower():
            info(page_id, f"No claim folder yet at '{folder}' — nothing currently claimed there")
            return []
        raise RuntimeError(f"rclone lsjson failed: {err.strip()[:300]}")
    entries = json.loads(out) if out.strip() else []
    images = [e for e in entries if Path(e.get("Name", "")).suffix.lower() in IMAGE_EXTENSIONS]
    images.sort(key=lambda e: e.get("ModTime", ""))
    info(page_id, f"Found {len(images)} image(s) in {folder}")
    return images


def mega_download_image(page_id, folder: str, file_name: str, dest_dir: str) -> str:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}/{file_name}"
    dest_path = os.path.join(dest_dir, file_name)
    rc, out, err = _run_rclone(page_id, ["copyto", remote_path, dest_path, "--progress"], timeout=600)
    if rc != 0 or not os.path.exists(dest_path):
        raise RuntimeError(f"rclone download failed: {err.strip()[:300]}")
    ok(page_id, f"Downloaded {file_name} ({os.path.getsize(dest_path)//1024} KB)")
    return dest_path


def mega_move_to_uploaded(page_id, src_folder: str, dst_folder: str, file_name: str):
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{dst_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    if rc != 0:
        raise RuntimeError(f"rclone move failed: {err.strip()[:300]}")
    ok(page_id, "Moved to uploaded folder")


def mega_claim_folder(mega_folder: str, page_id: str) -> str:
    return f"{mega_folder}/_claimed_{page_id}"


def mega_try_claim(page_id, src_folder: str, file_name: str, claim_folder: str) -> bool:
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{claim_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    return rc == 0


def mega_return_to_pool(page_id, claim_folder: str, mega_folder: str, file_name: str) -> bool:
    src = f"{MEGA_REMOTE_NAME}:{claim_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{mega_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    if rc != 0:
        warn(page_id, f"Could not return '{file_name}' to the pool: {err.strip()[:200]}")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# URLS TAB (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

URLS_POSTED_VALUES = {"posted", "replaced", "rejected"}

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
            continue
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
# POSTQUEUE TAB (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def postqueue_find_for_file(sh: Sheet, page_id: str, file_name: str):
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
# FACEBOOK STORAGE STATE (unchanged)
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
# PLAYWRIGHT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_picker_url(url): return any(x in url for x in ["device-based", "/caa/", "login/caa", "login/identifier"])
def is_hard_login_url(url): return "/login" in url and not is_picker_url(url)

def classify_url(url: str) -> str:
    if "checkpoint" in url: return "CHECKPOINT"
    if is_hard_login_url(url): return "LOGIN_WALL"
    if is_picker_url(url): return "DEVICE_PICKER"
    if "facebook.com" in url: return "FACEBOOK_PAGE"
    return "OTHER"


FEED_SELECTORS = [
    '[aria-label="Home"]', '[data-pagelet="LeftRail"]', 'div[role="feed"]',
    '[aria-label="Create"]', 'span:has-text("What\'s on your mind?")',
    'div[aria-label="Stories"]', 'div[data-pagelet="FeedUnit_0"]', 'div[role="main"]',
]


async def save_screenshot(page_id, page, name: str):
    """Save a screenshot for this step. Called unconditionally at every
    meaningful step now, not just on failure, so you get a full visual
    trail of exactly what the composer looked like at each stage."""
    SCREENSHOTS_DIR.mkdir(exist_ok=True)
    try:
        await page.screenshot(path=str(SCREENSHOTS_DIR / f"{page_id}_{name}.png"), full_page=False)
        info(page_id, f"📸 Screenshot saved: {name}")
    except Exception as e:
        warn(page_id, f"Screenshot failed ({name}): {e}")


async def save_html_dump(page_id, page, name: str):
    """Dump the full page HTML for offline debugging. Lets you inspect
    exactly what DOM/selectors Facebook served at each step without
    needing to reproduce the run interactively."""
    DEBUG_HTML_DIR.mkdir(exist_ok=True)
    try:
        html = await page.content()
        path = DEBUG_HTML_DIR / f"{page_id}_{name}.html"
        path.write_text(html, encoding="utf-8")
        info(page_id, f"🧾 HTML dump saved: {name} ({len(html)} chars)")
    except Exception as e:
        warn(page_id, f"HTML dump failed ({name}): {e}")


async def debug_step(page_id, page, name: str):
    """Convenience: screenshot + HTML dump together for a given step."""
    await save_screenshot(page_id, page, name)
    await save_html_dump(page_id, page, name)


SHORTCUT_POPUP_SELECTORS = [
    'div[aria-label="Close"]:near(:text("Keep single-character shortcuts"))',
    'span:text-is("Keep single-character shortcuts turned on?")',
    'span:has-text("single-character shortcuts")',
]


async def dismiss_shortcut_popup(page_id, page) -> bool:
    """Facebook shows a 'Keep single-character shortcuts turned on?' modal
    whenever a stray keystroke (e.g. a bare '/' from inside a URL typed
    while the caption box wasn't actually focused) reaches the page body
    instead of the contenteditable field. Once it appears it sits on top
    of the composer and blocks every subsequent click/type — which is why
    caption entry AND the Post click were both failing. We look for it and
    close it (prefer the explicit 'Turn off' choice so it stops recurring;
    fall back to the X) before continuing."""
    try:
        heading = page.locator('span:has-text("single-character shortcuts")').first
        if await heading.count() == 0:
            return False
    except Exception:
        return False

    warn(page_id, "Detected 'Keep single-character shortcuts' popup — dismissing it")
    for sel in ['div[role="button"]:has-text("Turn off")',
                'span:text-is("Turn off")',
                '[aria-label="Close"]',
                'div[role="button"]:has-text("Keep Turned On")']:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click(timeout=4_000, force=True)
                await asyncio.sleep(1)
                return True
        except Exception:
            pass
    # last resort: Escape key
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(1)
    except Exception:
        pass
    return True


async def dump_candidate_buttons(page_id, page, name: str, limit: int = 40):
    """Log every button-like element currently on the page (text, aria-label,
    role, disabled state) so failures are diagnosable from the log alone,
    without needing to open the HTML dump."""
    try:
        buttons = await page.evaluate(
            """(limit) => {
                const els = Array.from(document.querySelectorAll(
                    'div[role="button"], span[role="button"], button, a[role="button"]'
                ));
                return els.slice(0, limit).map(el => ({
                    tag: el.tagName,
                    role: el.getAttribute('role'),
                    aria: el.getAttribute('aria-label'),
                    disabled: el.getAttribute('aria-disabled'),
                    text: (el.innerText || el.textContent || '').trim().slice(0, 40),
                }));
            }""", limit)
    except Exception as e:
        warn(page_id, f"Could not enumerate buttons for '{name}': {e}")
        return
    info(page_id, f"🔍 [{name}] {len(buttons)} button-like element(s) on page:")
    for b in buttons:
        if b["text"] or b["aria"]:
            print(f"      <{b['tag']} role={b['role']} aria-label={b['aria']!r} "
                  f"aria-disabled={b['disabled']!r}> text={b['text']!r}")


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
            await debug_step(page_id, page, f"checkpoint_{attempt+1}")
            return False
        if url_type == "LOGIN_WALL":
            fail(page_id, "Hard login wall — session cookies EXPIRED")
            await debug_step(page_id, page, f"login_wall_{attempt+1}")
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
    await debug_step(page_id, page, "login_failed_final")
    return False


def _nonempty_lines(text: str) -> list[str]:
    return [l for l in text.split("\n") if l.strip()]


async def _clear_field(page, field):
    """Click into the field and select-all + delete existing content.

    FIX: the original version used a plain `.click()`. The composer's
    caption box sits directly under an attachment/media preview overlay
    that is still finishing its mount/animation transition right after
    an image is attached — Playwright's actionability check confirms the
    element is visible/enabled/stable, but the *click itself* still lands
    on the overlay ("subtree intercepts pointer events"), so the click
    silently no-ops on every retry. We now fall back to a forced click
    (which skips the interception check) and finally a raw JS focus if
    even that fails.
    """
    await dismiss_shortcut_popup(None, page)
    try:
        await field.click(timeout=5_000)
    except Exception:
        try:
            await field.click(timeout=5_000, force=True)
        except Exception:
            await field.evaluate("el => el.focus()")
    await asyncio.sleep(0.3)

    # Verify the field actually has DOM focus before we trust the keyboard.
    # If it doesn't, a plain/force click landed but didn't focus (common
    # when an overlay is still covering it) — force focus via JS instead,
    # which is what actually prevents keystrokes leaking to the page and
    # triggering the shortcuts popup.
    try:
        is_focused = await field.evaluate("el => document.activeElement === el")
        if not is_focused:
            await field.evaluate("el => el.focus()")
            await asyncio.sleep(0.2)
    except Exception:
        pass

    await page.keyboard.press("Control+a")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.2)
    await dismiss_shortcut_popup(None, page)


async def enter_caption_lexical(page_id, page, caption: str) -> bool:
    LEXICAL_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder*="mind"]',
        'div[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]
    expected_lines = len(_nonempty_lines(caption))

    # Give any transient overlay (attachment preview mounting, composer
    # resize animation, etc.) a moment to settle before we even try —
    # this alone eliminates most "subtree intercepts pointer events" hits.
    try:
        await page.wait_for_load_state("networkidle", timeout=4_000)
    except Exception:
        pass
    await asyncio.sleep(0.5)

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
                await dismiss_shortcut_popup(page_id, page)
                txt = await field.evaluate("el => (el.innerText || el.textContent || '').trim()")
                actual_lines = len(_nonempty_lines(txt))
                if txt and len(txt) > 2 and abs(actual_lines - expected_lines) <= 1:
                    ok(page_id, f"Caption entered via strategy {i} ({actual_lines}/{expected_lines} lines)")
                    return True
                else:
                    info(page_id, f"Strategy {i}/{sel} produced unexpected text: {txt[:60]!r}")
            except Exception as e:
                warn(page_id, f"Caption strategy {i}/{sel} raised: {e}")

    fail(page_id, "All caption entry strategies failed — caption box was never verified non-empty")
    await debug_step(page_id, page, "caption_all_strategies_failed")
    await dump_candidate_buttons(page_id, page, "caption_all_strategies_failed")
    return False


LINK_REJECTION_SELECTORS = [
    'span:has-text("couldn\'t be shared")',
    'span:has-text("goes against our Community Standards")',
    'div:has-text("goes against our Community Standards")',
]


async def detect_link_rejection(page_id, page) -> bool:
    for sel in LINK_REJECTION_SELECTORS:
        try:
            if await page.locator(sel).count() > 0:
                return True
        except Exception:
            pass
    return False


async def click_next_button(page_id, page) -> bool:
    """Click the 'Next' button that appears after attaching media, before
    the caption box's real Post step. This mirrors the reel script's
    attach -> Next -> caption -> Post flow. Only used if a Next button is
    actually present and enabled — plain single-photo posts sometimes skip
    straight to a Post button with no Next step at all, so callers must
    treat a missing Next button as normal, not a failure."""
    await dismiss_shortcut_popup(page_id, page)

    next_selectors = [
        'div[aria-label="Next"][role="button"]',
        'div[role="button"]:text-is("Next")',
        'div[role="button"]:has-text("Next")',
        'span:text-is("Next")',
        'button:has-text("Next")',
    ]

    found_any = False
    for sel in next_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() == 0:
                continue
            found_any = True
            if await btn.get_attribute("aria-disabled") == "true":
                info(page_id, f"Next button ('{sel}') present but disabled — not ready yet")
                continue
            for method_name, method in [
                ("plain", lambda: btn.click(timeout=6_000)),
                ("force", lambda: btn.click(timeout=6_000, force=True)),
                ("js", lambda: btn.evaluate("el => el.click()")),
            ]:
                try:
                    await btn.scroll_into_view_if_needed(timeout=5_000)
                except Exception:
                    pass
                try:
                    await method()
                    info(page_id, f"Next button clicked via '{method_name}' method")
                    await asyncio.sleep(3)
                    await dismiss_shortcut_popup(page_id, page)
                    return True
                except Exception as e:
                    warn(page_id, f"Next click ({method_name}) failed: {e}")
        except Exception as e:
            warn(page_id, f"Next selector '{sel}' raised: {e}")

    if not found_any:
        info(page_id, "No Next button found — this composer may go straight to Post")
    else:
        warn(page_id, "Next button found but could not be clicked by any method")
    return False


async def click_post_button(page_id, page) -> bool:
    """Robust Post/Publish click.

    FIX: the original selector list relied on the button always being an
    exact-text `div[role="button"]` with aria-label "Post"/"Publish". In
    practice Facebook sometimes wraps the label in nested spans (so
    `:text-is("Post")` matches nothing), sometimes leaves the accessible
    name off the outer button and only sets it on the SVG/span inside,
    and the button can also be intercepted by the same kind of transient
    overlay that was blocking the caption box. This version:
      1. Tries a much wider set of selectors, checked in order.
      2. Falls back to scanning ALL button-like elements for one whose
         visible text is exactly/contains "Post" or "Publish".
      3. Uses `force=True` and finally a raw JS `.click()` if the normal
         click is blocked by an overlay.
      4. Logs every candidate it saw before giving up, so the log alone
         tells you why it failed next time.
    """
    await dismiss_shortcut_popup(page_id, page)
    await dump_candidate_buttons(page_id, page, "before_post_click")

    post_selectors = [
        'div[aria-label="Post"][role="button"]',
        'div[aria-label="Publish"][role="button"]',
        'div[role="button"]:text-is("Post")',
        'div[role="button"]:has-text("Post")',
        'span:text-is("Post")',
        'span:has-text("Post")',
        'button[type="submit"]',
        'button:has-text("Post")',
    ]

    async def try_click(loc):
        # verify not disabled
        try:
            if await loc.get_attribute("aria-disabled") == "true":
                return False
        except Exception:
            pass
        for method_name, method in [
            ("plain", lambda: loc.click(timeout=6_000)),
            ("force", lambda: loc.click(timeout=6_000, force=True)),
            ("js", lambda: loc.evaluate("el => el.click()")),
        ]:
            try:
                await loc.scroll_into_view_if_needed(timeout=5_000)
            except Exception:
                pass
            try:
                await method()
                info(page_id, f"Post button clicked via '{method_name}' method")
                return True
            except Exception as e:
                warn(page_id, f"Post click ({method_name}) failed: {e}")
        return False

    for sel in post_selectors:
        try:
            btn = page.locator(sel).last
            if await btn.count() == 0:
                continue
            if await try_click(btn):
                return True
        except Exception as e:
            warn(page_id, f"Post selector '{sel}' raised: {e}")

    # Fallback: scan every button-like element for exact/contains text match,
    # walking up to the clickable ancestor with role="button" if the text
    # node itself is a nested span.
    info(page_id, "No selector matched — falling back to full-page text scan for Post/Publish")
    try:
        clicked = await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll(
                'div[role="button"], span[role="button"], button, a[role="button"]'
            ));
            const isMatch = (el) => {
                const t = (el.innerText || el.textContent || '').trim();
                const aria = (el.getAttribute('aria-label') || '').trim();
                return /^(post|publish)$/i.test(t) || /^(post|publish)$/i.test(aria);
            };
            let target = candidates.find(isMatch);
            if (!target) return false;
            // walk up to nearest role=button ancestor if this is a text node wrapper
            let el = target;
            for (let i = 0; i < 4 && el; i++) {
                if (el.getAttribute && el.getAttribute('role') === 'button') { target = el; break; }
                el = el.parentElement;
            }
            if (target.getAttribute('aria-disabled') === 'true') return false;
            target.click();
            return true;
        }""")
        if clicked:
            info(page_id, "Post button clicked via full-page JS text scan")
            return True
    except Exception as e:
        warn(page_id, f"Full-page JS text scan failed: {e}")

    fail(page_id, "Could not click Post/Publish by any method")
    await debug_step(page_id, page, "post_button_not_found")
    return False


async def run_upload_flow(page_id, page, caption: str, image_path: str) -> dict:
    """Posts a single image to the Page's feed via the standard Facebook
    composer ("What's on your mind?" -> Photo/video -> attach -> caption
    -> Post). Returns {"published": bool, "link_rejected": bool}.

    DEBUG BUILD: a screenshot + HTML dump is captured after every step,
    success or failure, so you have a complete visual/DOM trail of the run.
    """
    published = False
    link_rejected = False

    step(page_id, "Loading Facebook homepage")
    try:
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        fail(page_id, f"Page load failed: {e}")
        await debug_step(page_id, page, "01_homepage_load_failed")
        return {"published": False, "link_rejected": False}
    await asyncio.sleep(8)
    await debug_step(page_id, page, "01_homepage_loaded")

    if not await ensure_logged_in(page_id, page):
        return {"published": False, "link_rejected": False}
    ok(page_id, "Login confirmed")
    await debug_step(page_id, page, "02_login_confirmed")

    step(page_id, "Opening post composer")
    opened = False
    for sel in ['span:has-text("What\'s on your mind?")', '[aria-label="Create a post"]',
                'div[aria-label="Create a post"]', 'div[role="button"]:has-text("What\'s on your mind?")']:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click(timeout=8_000)
                opened = True
                break
        except Exception as e:
            warn(page_id, f"Composer trigger '{sel}' failed: {e}")

    if not opened:
        fail(page_id, "Could not open the post composer")
        await debug_step(page_id, page, "03_no_composer")
        await dump_candidate_buttons(page_id, page, "no_composer")
        return {"published": False, "link_rejected": False}
    await asyncio.sleep(4)
    await debug_step(page_id, page, "03_composer_opened")

    step(page_id, "Clicking Photo/video")
    photo_clicked = False
    for sel in ['[aria-label="Photo/video"]', 'div[aria-label="Photo/video"]',
                'div[role="button"]:has-text("Photo/video")', 'span:has-text("Photo/video")']:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click(timeout=8_000)
                photo_clicked = True
                break
        except Exception as e:
            warn(page_id, f"Photo/video button '{sel}' failed: {e}")
    if not photo_clicked:
        info(page_id, "No explicit Photo/video button found — the file input may already be present")
    await asyncio.sleep(2)
    await debug_step(page_id, page, "04_photo_video_clicked")

    step(page_id, "Attaching image")
    uploaded = False
    for sel in ['input[type="file"][accept*="image"]', 'input[type="file"]']:
        try:
            inp = page.locator(sel)
            if await inp.count() > 0:
                await inp.first.set_input_files(image_path)
                uploaded = True
                break
        except Exception as e:
            warn(page_id, f"Direct input {sel} failed: {e}")

    if not uploaded:
        for btn_name, sel in [
            ('Add photos', 'div[role="button"]:has-text("Add Photos")'),
            ('Add photos/videos', 'div[role="button"]:has-text("Add Photos/Videos")'),
            ('aria-label', '[aria-label="Add Photos/Videos"]'),
        ]:
            el = page.locator(sel).first
            try:
                if await el.count() == 0:
                    continue
                async with page.expect_file_chooser(timeout=10_000) as fc_info:
                    await el.click(force=True)
                fc = await fc_info.value
                await fc.set_files(image_path)
                uploaded = True
                break
            except Exception as e:
                warn(page_id, f"Upload button '{btn_name}' failed: {e}")

    if not uploaded:
        fail(page_id, "Could not attach image")
        await debug_step(page_id, page, "05_no_upload")
        return {"published": False, "link_rejected": False}
    ok(page_id, "Image attached")
    await asyncio.sleep(4)
    await debug_step(page_id, page, "05_image_attached")

    # Let the attachment preview / composer resize animation fully settle
    # before touching the caption box — this is the overlay that was
    # intercepting pointer events in your log.
    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except Exception:
        pass
    await asyncio.sleep(1.5)
    await dismiss_shortcut_popup(page_id, page)

    step(page_id, "Entering caption")
    await debug_step(page_id, page, "06_before_caption")
    caption_ok = await enter_caption_lexical(page_id, page, caption)
    if not caption_ok:
        warn(page_id, "Caption entry unverified — continuing anyway")
    await debug_step(page_id, page, "07_after_caption")

    # This composer uses a two-step flow, same as the reels script: after
    # attaching media (and, per the screenshots, entering the caption in
    # the box already visible at this stage) there is a "Next" button —
    # NOT a Post button yet. Clicking Post here was failing because there
    # was no Post button on screen at all; it only appears on the screen
    # that follows Next. A plain single-photo post sometimes skips Next
    # entirely, so we only click it if it's actually present/enabled.
    step(page_id, "Checking for Next button")
    next_clicked = await click_next_button(page_id, page)
    await debug_step(page_id, page, "07b_after_next_click" if next_clicked else "07b_no_next_button")

    if next_clicked:
        # The Next screen occasionally re-renders the caption box empty —
        # verify and re-enter if so, rather than assuming it carried over.
        try:
            existing = await page.evaluate("""() => {
                const el = document.querySelector('div[data-lexical-editor="true"][contenteditable="true"]')
                         || document.querySelector('div[contenteditable="true"]');
                return el ? (el.innerText || el.textContent || '').trim() : '';
            }""")
        except Exception:
            existing = ""
        if len(existing) < 2:
            info(page_id, "Caption box empty after Next — re-entering caption")
            await dismiss_shortcut_popup(page_id, page)
            caption_ok = await enter_caption_lexical(page_id, page, caption)
            if not caption_ok:
                warn(page_id, "Caption entry unverified on post-Next screen — continuing anyway")
            await debug_step(page_id, page, "07c_caption_after_next")

    step(page_id, "Clicking Post")
    await dismiss_shortcut_popup(page_id, page)
    post_clicked = await click_post_button(page_id, page)
    await debug_step(page_id, page, "08_after_post_click_attempt")

    if not post_clicked:
        fail(page_id, "Could not click Post")
        return {"published": False, "link_rejected": False}

    await asyncio.sleep(3)
    if await detect_link_rejection(page_id, page):
        link_rejected = True
        fail(page_id, "Facebook rejected the post: link violates Community Standards")
        await debug_step(page_id, page, "09_link_rejected")
        return {"published": False, "link_rejected": True}

    step(page_id, "Waiting for publish confirmation")
    for elapsed in range(0, 60, 5):
        if await detect_link_rejection(page_id, page):
            link_rejected = True
            break
        try:
            # Composer dialog closing (no longer visible) is our success signal
            still_open = await page.locator('div[aria-label="Post"][role="button"]').count() > 0
            if not still_open:
                published = True
                break
        except Exception:
            pass
        await asyncio.sleep(5)

    if link_rejected:
        fail(page_id, "Facebook rejected the post: link violates Community Standards")
        await debug_step(page_id, page, "10_link_rejected_final")
        return {"published": False, "link_rejected": True}

    await debug_step(page_id, page, "11_final_result")
    if published:
        ok(page_id, "🎉 Published")
    else:
        warn(page_id, "Could not confirm publish — check screenshot/HTML dump")
    return {"published": published, "link_rejected": link_rejected}


# ─────────────────────────────────────────────────────────────────────────────
# PAGE WORKER
# ─────────────────────────────────────────────────────────────────────────────

class PageWorker:
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
                ok(cfg.page_id, "Browser closed, page released ('Idle')")

    async def _heartbeat(self):
        cfg = self.cfg
        try:
            fresh = await self.context.storage_state()
            fresh_json = json.dumps(fresh)
            save_storage_state_everywhere(self.sh, cfg, fresh_json)
        except Exception as e:
            warn(cfg.page_id, f"Heartbeat storage_state save failed: {e}")
        ar_col = self.col_index.get("assignedrepo")
        as_col = self.col_index.get("assignedstatus")
        aa_col = self.col_index.get("assignedat")
        if ar_col is not None and as_col is not None and aa_col is not None:
            self.sh.write_cell(PAGES_TAB, cfg.row_num, ar_col, REPO_ID)
            self.sh.write_cell(PAGES_TAB, cfg.row_num, as_col, "InUse")
            self.sh.write_cell(PAGES_TAB, cfg.row_num, aa_col, now_iso())

    async def _release(self):
        as_col = self.col_index.get("assignedstatus")
        aa_col = self.col_index.get("assignedat")
        if as_col is not None and aa_col is not None:
            self.sh.write_cell(PAGES_TAB, self.cfg.row_num, as_col, "Idle")
            self.sh.write_cell(PAGES_TAB, self.cfg.row_num, aa_col, now_iso())

    async def _post_once(self):
        cfg = self.cfg
        pid = cfg.page_id
        step(pid, f"Post cycle starting (mode={cfg.post_mode})")
        info(pid, f"Resolved settings: UrlReplaceEnabled={cfg.url_replace_enabled}, "
                  f"UrlSwapOnRejectOnly={cfg.url_swap_on_reject_only}, "
                  f"UrlReplaceMode={cfg.url_replace_mode}, UrlReplaceCount={cfg.url_replace_count}, "
                  f"Link_Percentage={cfg.link_percentage}, LinkRejectMaxRetries={cfg.link_reject_max_retries}")

        claim_folder = mega_claim_folder(cfg.mega_folder, pid)

        try:
            leftover = mega_list_images(pid, claim_folder, missing_dir_ok=True)
        except Exception as e:
            warn(pid, f"Could not check claim folder '{claim_folder}': {e}")
            leftover = []

        claimed_file, claimed_match = None, None

        if leftover:
            claimed_file = leftover[0]["Name"]
            info(pid, f"Resuming previously-claimed image: {claimed_file}")
            if cfg.post_mode == "queue":
                claimed_match = postqueue_find_for_file(self.sh, pid, claimed_file)
                if not claimed_match:
                    warn(pid, f"Resumed claim '{claimed_file}' has no PostQueue row anymore — returning to pool")
                    mega_return_to_pool(pid, claim_folder, cfg.mega_folder, claimed_file)
                    return
        else:
            try:
                images = mega_list_images(pid, cfg.mega_folder)
            except Exception as e:
                fail(pid, f"Could not list Mega folder: {e}")
                return
            if not images:
                info(pid, "No images pending — nothing to do this cycle")
                return

            for v in images:
                name = v["Name"]
                match = None
                if cfg.post_mode == "queue":
                    match = postqueue_find_for_file(self.sh, pid, name)
                    if not match:
                        continue
                if mega_try_claim(pid, cfg.mega_folder, name, claim_folder):
                    claimed_file, claimed_match = name, match
                    ok(pid, f"Claimed image: {name}")
                    break
                info(pid, f"Lost the claim race on '{name}' to another worker — trying next")

            if not claimed_file:
                if cfg.post_mode == "queue":
                    warn(pid, "No pending image currently has a matching PostQueue caption — skipping this cycle")
                else:
                    warn(pid, "Could not claim any image this cycle (all lost the race) — skipping")
                return

        file_name = claimed_file

        pq_row_num = None
        base_caption, use_link = None, False

        if cfg.post_mode == "queue":
            caption, pq_row_num = claimed_match
            if not caption.strip():
                warn(pid, f"PostQueue row for '{file_name}' has an empty caption — returning image to pool")
                mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)
                return
        else:
            roll = random.randint(1, 100)
            use_link = roll <= cfg.link_percentage
            base_caption = cfg.caption if use_link else (cfg.without_link_caption or cfg.caption)
            if not base_caption.strip():
                fail(pid, "No caption configured (Caption / WithoutLinkCap both empty) — skipping")
                return

        def build_rotation_caption(exclude_rows: set[int], force_swap: bool):
            cap = base_caption
            rows_used, status_col = [], None
            should_swap = use_link and cfg.url_replace_enabled and (force_swap or not cfg.url_swap_on_reject_only)
            if should_swap:
                n_matches = len(URL_REGEX.findall(cap))
                n_to_replace = min(cfg.url_replace_count, n_matches) if n_matches else cfg.url_replace_count
                fetch_count = 1 if cfg.url_replace_mode == "same" else max(n_to_replace, 1)
                fetched_urls, fetched_rows, status_col = urls_get_next(
                    self.sh, pid, fetch_count + len(exclude_rows))
                pairs = [(u, r) for u, r in zip(fetched_urls, fetched_rows) if r not in exclude_rows][:fetch_count]
                new_urls = [u for u, _ in pairs]
                rows_used = [r for _, r in pairs]
                if new_urls:
                    n_eff = n_to_replace
                    if cfg.url_replace_mode == "unique" and len(new_urls) < n_eff:
                        n_eff = len(new_urls)
                    cap = replace_urls_in_caption(cap, new_urls, cfg.url_replace_mode, n_eff)
                else:
                    warn(pid, "No unused URLs available — posting caption without swap")
            return cap, rows_used, status_col

        used_url_rows, url_status_col = [], None
        if cfg.post_mode != "queue":
            caption, used_url_rows, url_status_col = build_rotation_caption(exclude_rows=set(), force_swap=False)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                local_path = mega_download_image(pid, claim_folder, file_name, tmp)
            except Exception as e:
                fail(pid, f"Download failed: {e}")
                mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)
                return

            published = False
            tried_url_rows: set[int] = set()
            max_attempts = 1 + (cfg.link_reject_max_retries if cfg.post_mode != "queue" else 0)

            for attempt in range(1, max_attempts + 1):
                try:
                    page = await self.context.new_page()
                    result = await run_upload_flow(pid, page, caption, local_path)
                    await page.close()
                except Exception as e:
                    fail(pid, f"Upload flow crashed: {e}")
                    import traceback; print(traceback.format_exc())
                    result = {"published": False, "link_rejected": False}

                published = result.get("published", False)
                if published:
                    break

                if not result.get("link_rejected"):
                    break

                if used_url_rows:
                    urls_mark_posted(self.sh, used_url_rows, url_status_col, status_value="Rejected")
                    warn(pid, f"Blacklisted rejected URL row(s) {used_url_rows} as 'Rejected'")
                    tried_url_rows.update(used_url_rows)

                if attempt >= max_attempts:
                    warn(pid, "Link rejection retry limit reached — giving up this cycle")
                    break
                if not (use_link and cfg.url_replace_enabled):
                    warn(pid, "Link rejected but URL replacement isn't enabled for this page — cannot retry")
                    break

                info(pid, f"Retrying with a fresh URL (attempt {attempt + 1}/{max_attempts})")
                caption, used_url_rows, url_status_col = build_rotation_caption(
                    exclude_rows=tried_url_rows, force_swap=True)
                if not used_url_rows and use_link and cfg.url_replace_enabled:
                    warn(pid, "No more unused URLs left to retry with — giving up this cycle")
                    break

        if published:
            try:
                mega_move_to_uploaded(pid, claim_folder, cfg.mega_move_folder, file_name)
            except Exception as e:
                warn(pid, f"Move to uploaded failed (image stays claimed under {claim_folder} for next cycle): {e}")

            if cfg.post_mode == "queue" and pq_row_num:
                postqueue_mark_posted(self.sh, pq_row_num)
            if used_url_rows:
                urls_mark_posted(self.sh, used_url_rows, url_status_col, status_value="Posted")

            lastfile_col = self.col_index.get("lastpostedfile")
            if lastfile_col is not None:
                self.sh.write_cell(PAGES_TAB, cfg.row_num, lastfile_col, file_name)
        else:
            warn(pid, "Upload not confirmed — returning image to the shared pool for retry")
            mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

async def main_async(once: bool):
    creds = build_google_creds()
    service = build_sheets_service(creds)
    spreadsheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV) or DEFAULT_CAPTIONS_SHEET_ID

    sh = setup_sheet(service, spreadsheet_id)

    master = load_master_settings(sh)
    thread_count = max(1, _to_int(master.get("threadcount"), 3))
    max_runtime = _to_int(master.get("maxruntimeminutes"), 300)

    requeue_task = None
    if not once:
        requeue_task = asyncio.create_task(schedule_self_requeue(spreadsheet_id, max_runtime))

    try:
        claimed_rows = claim_pages(sh, master)
    except Exception as e:
        fail(None, "claim_pages() failed — see traceback below")
        import traceback; print(traceback.format_exc())
        if requeue_task:
            requeue_task.cancel()
        raise

    if not claimed_rows:
        info(None, "No pages available to claim this run (none Active/unassigned to us)")
        if not once:
            trigger_self_requeue(spreadsheet_id)
        if requeue_task:
            requeue_task.cancel()
        return

    col_index = get_pages_col_index(sh)
    configs = [build_page_config(r, master, max_runtime) for r in claimed_rows]

    info(None, f"Runner '{REPO_ID}' — {len(configs)} page(s) claimed, "
               f"running {thread_count} concurrently")

    semaphore = asyncio.Semaphore(thread_count)
    workers = [PageWorker(sh, cfg, col_index, semaphore, once) for cfg in configs]
    await asyncio.gather(*(w.run() for w in workers), return_exceptions=False)

    if not once:
        trigger_self_requeue(spreadsheet_id)
    if requeue_task:
        requeue_task.cancel()


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
