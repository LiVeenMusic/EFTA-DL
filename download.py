import os
import re
import csv
import json
import time
import hashlib
import difflib
import sqlite3
import threading
import signal
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# ================= CONFIG =================

DEFAULT_DATASET = 10  # Default dataset number
START_URL = f"https://www.justice.gov/epstein/doj-disclosures/data-set-{DEFAULT_DATASET}-files?page=0"
DOWNLOAD_DIR = "downloads"
STATE_FILE = "crawl_state.json"
CSV_LOG = "downloads.csv"

MAX_WORKERS = 10            # parallel downloads
REQUEST_DELAY = 0.4          # seconds between page fetches
DRY_RUN = False            # set True to disable downloads
MAX_PAGES_PER_PART = 1000    # max page folders per part folder

# Network timeouts (seconds)
DOWNLOAD_CONNECT_TIMEOUT = 15
DOWNLOAD_READ_TIMEOUT = 30
DOWNLOAD_TOTAL_TIMEOUT = 1200

# CLI-configurable defaults (can be overridden via command line)
DEFAULT_MAX_EMPTY_CONSECUTIVE = 50
DEFAULT_MAX_FORBIDDEN_CONSECUTIVE = 3
DEFAULT_SCRAPE_MAX_RETRIES = 3
DEFAULT_USE_PLAYWRIGHT = False   # when True, try Playwright if requests keep getting 403
DEBUG_HTTP = False               # when True, print extra HTTP diagnostics
USE_SYSTEM_PROXY = True         # when False, ignore system proxy settings
DEFAULT_USE_HTTPX = True         # httpx (HTTP/2) fallback enabled by default
ENABLE_CSV_LOG = False           # when False, disable downloads.csv logging
ENABLE_STATE_FILE = False        # when False, disable crawl_state.json usage
ENABLE_DB_STATE = True           # when True, persist crawl state in the DB
END_PAGE_NUM = None              # when set, stop crawling after reaching this page

# ==========================================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
CONFLICTS_SUBDIR = "conflicts"
# CONFLICT_DIR may be configured via CLI; do not create its
# directory at import-time. Part-level conflicts directories are
# created lazily only when saving files into them.
CONFLICT_DIR = os.path.join(DOWNLOAD_DIR, CONFLICTS_SUBDIR)
lock = threading.RLock()

# Part folder tracking (part_1, part_2, ...) - now tracks page folders not files
CURRENT_PART_INDEX = 0
CURRENT_PART_COUNT = 0  # counts page folders in current part

# Organize mode flag and current page tracking
ORGANIZE_MODE = False
CURRENT_PAGE_NUM = None  # set during scraping to track which page we're on
UNMATCHED_DIR_NAME = "unmatched"

# Persistent index DB to avoid scanning the filesystem on every lookup
INDEX_DB = "downloads.db"
# When True, do not fall back to scanning the filesystem if DB lookup misses
NO_FS_FALLBACK = False
# Per-thread DB connection storage to avoid contention and connection churn
DB_CONN_LOCAL = threading.local()
# Time threshold (seconds) for reporting slow DB operations
DB_SLOW_QUERY_THRESHOLD = 2.0


def db_connect():
    """Return a thread-local SQLite connection, creating it if needed.

    Ensures PRAGMA settings (WAL mode, busy timeout) are applied so readers are
    not blocked by short write transactions. If a previously stored connection
    was closed, reopen a fresh one.
    """
    conn = getattr(DB_CONN_LOCAL, "conn", None)
    db_path = getattr(DB_CONN_LOCAL, "db_path", None)
    if conn and db_path == INDEX_DB:
        # Test whether the stored connection is still usable
        try:
            conn.execute("SELECT 1")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.DatabaseError):
            # connection was closed or corrupted; fall through to recreate
            try:
                conn.close()
            except Exception:
                pass

    # Create new connection for this thread
    # Increase timeout to tolerate short locks and set check_same_thread False
    conn = sqlite3.connect(INDEX_DB, timeout=60, check_same_thread=False)
    # Enable WAL to allow concurrent readers/writers
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except Exception:
        # Ignore if PRAGMA not supported for some reason
        pass
    DB_CONN_LOCAL.conn = conn
    DB_CONN_LOCAL.db_path = INDEX_DB
    return conn


def init_db(index_db_path=None):
    global INDEX_DB
    if index_db_path:
        INDEX_DB = index_db_path
    # Ensure we create the DB and apply PRAGMAs on the connection we got
    conn = db_connect()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS files (
        filename TEXT PRIMARY KEY,
        path TEXT,
        size INTEGER,
        hash TEXT,
        page_num INTEGER,
        status TEXT,
        updated_ts INTEGER
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS crawl_state (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_ts INTEGER
    )
    """)
    conn.commit()
    # Keep the connection open (thread-local) for reuse; do not close here
    # but ensure the main thread has the PRAGMAs set. If the connection needs
    # to be refreshed later, db_connect() will recreate it.
    return


def db_get_file(filename):
    start = time.time()
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT filename, path, size, hash, page_num, status FROM files WHERE filename=?", (filename,))
        row = c.fetchone()
    except sqlite3.OperationalError as e:
        # transient DB lock; wait a tiny bit and retry once
        time.sleep(0.1)
        try:
            conn = db_connect()
            c = conn.cursor()
            c.execute("SELECT filename, path, size, hash, page_num, status FROM files WHERE filename=?", (filename,))
            row = c.fetchone()
        except Exception:
            row = None
    elapsed = time.time() - start
    if elapsed > DB_SLOW_QUERY_THRESHOLD:
        print(f"⚠️ Slow DB lookup for {filename}: {elapsed:.2f}s")
    if row:
        return {
            "filename": row[0],
            "path": row[1],
            "size": row[2],
            "hash": row[3],
            "page_num": row[4],
            "status": row[5]
        }
    return None


def db_upsert_file(filename, path, size, h, page_num=None, status="downloaded"):
    with lock:
        try:
            conn = db_connect()
            c = conn.cursor()
            c.execute("""
                INSERT INTO files (filename, path, size, hash, page_num, status, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(filename) DO UPDATE SET
                    path=excluded.path,
                    size=excluded.size,
                    hash=excluded.hash,
                    page_num=excluded.page_num,
                    status=excluded.status,
                    updated_ts=excluded.updated_ts
            """, (filename, path, size, h, page_num, status, int(time.time())))
            conn.commit()
        except sqlite3.ProgrammingError:
            # Possibly operating on a closed connection — recreate and retry once
            conn = db_connect()
            c = conn.cursor()
            c.execute("""
                INSERT INTO files (filename, path, size, hash, page_num, status, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(filename) DO UPDATE SET
                    path=excluded.path,
                    size=excluded.size,
                    hash=excluded.hash,
                    page_num=excluded.page_num,
                    status=excluded.status,
                    updated_ts=excluded.updated_ts
            """, (filename, path, size, h, page_num, status, int(time.time())))
            conn.commit()


def load_index_into_memory():
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT filename, hash FROM files WHERE status='downloaded' OR status='duplicate' OR status='conflict'")
    for fn, h in c.fetchall():
        seen_files.add(fn)
        if h:
            seen_hashes.add(h)
            file_hash_map[fn] = h
    # keep the thread-local connection open for reuse; do not close it here


def db_get_state(key):
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT value FROM crawl_state WHERE key=?", (key,))
        row = c.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def db_set_state(key, value):
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO crawl_state (key, value, updated_ts)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_ts=excluded.updated_ts
            """,
            (key, value, int(time.time()))
        )
        conn.commit()
        return True
    except Exception:
        return False


def rescan_and_rebuild_index(download_dir):
    print(f"🔁 Rescanning {download_dir} to rebuild index (this may take a while)...")
    # Report DB health and existing counts before rescan
    before_count = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT count(*) FROM files")
        before_count = c.fetchone()[0]
        print(f"🗄️  Opened index DB '{INDEX_DB}' with {before_count} existing entries.")
    except Exception as e:
        print(f"⚠️ Could not query index DB before rescan: {e}")
        print("   The table will be created if necessary.")

    processed = 0
    for root, dirs, files in os.walk(download_dir):
        for f in files:
            if f.endswith(".part"):
                continue
            path = os.path.join(root, f)
            try:
                size = os.path.getsize(path)
            except Exception:
                size = None
            try:
                h = file_hash(path)
            except Exception:
                h = None
            
            # Determine status and page number from path
            status = "conflict" if f"{os.sep}{CONFLICTS_SUBDIR}{os.sep}" in path else "downloaded"
            
            # Try to extract page number from path (e.g., .../page_123/...)
            page_num = None
            page_match = re.search(r"[/\\]page_(\\d+)[/\\]", path)
            if page_match:
                page_num = int(page_match.group(1))
            
            db_upsert_file(f, path, size, h, page_num=page_num, status=status)
            processed += 1

    # Summarize results
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT count(*) FROM files")
        after_count = c.fetchone()[0]
    except Exception as e:
        after_count = None
        print(f"⚠️ Could not query index DB after rescan: {e}")

    if after_count is not None and before_count is not None:
        added = after_count - before_count
        print(f"✅ Rescan complete: processed {processed} files; index has {after_count} entries ({added:+d} change).")
    else:
        print(f"✅ Rescan complete: processed {processed} files; index entries: {after_count if after_count is not None else 'unknown'}.")

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.justice.gov/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": "\"Chromium\";v=\"120\", \"Not?A_Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\""
})

# Hardcoded cookies are session-specific and expire quickly.
# These are optional and will be replaced by browser_cookie3 if USE_BROWSER_COOKIES=True.
# If you have fresh cookies from your browser, you can add them here temporarily.
session.cookies.update({
    # "ak_bmsc": "...",  # deprecated/expired
     "justiceGovAgeVerified": "true",
     "QueueITAccepted-SDFrts345E-V3_usdojfiles": "EventId%3Dusdojfiles%26RedirectType%3Dsafetynet%26IssueTime%3D1769822574%26Hash%3Db8afa9bbfd5e20d7d24cf18882bb1ba3dda2925742a3f26a6c135af4b7d94b74"
})


def log_http_response(resp, label="response", max_body=800):
    """Print a compact diagnostic summary for HTTP responses."""
    try:
        print(f"\n🔎 HTTP {label} diagnostics:")
        print(f"  url: {resp.url}")
        print(f"  status: {resp.status_code}")
        print(f"  server: {resp.headers.get('Server', 'unknown')}")
        print(f"  content-type: {resp.headers.get('Content-Type', 'unknown')}")
        set_cookie = resp.headers.get("Set-Cookie")
        if set_cookie:
            print(f"  set-cookie: present ({len(set_cookie)} chars)")
        else:
            print("  set-cookie: none")
        text = resp.text or ""
        snippet = text[:max_body].replace("\n", " ").replace("\r", " ")
        print(f"  body-snippet: {snippet}")
    except Exception as e:
        print(f"⚠️ Failed to log HTTP diagnostics: {e}")

# ---------------- BROWSER COOKIE LOADING ----------------
# Terminal-only Linux servers typically do NOT have browser profiles available.
# For that reason, browser cookie import is disabled by default. You can enable
# it explicitly if a browser profile exists on the server.
USE_BROWSER_COOKIES = False  # set True to import cookies from a local browser profile
BROWSER_NAME = "firefox"     # browser to read cookies from (firefox/chrome/brave)


def load_browser_cookies(browser_name=BROWSER_NAME):
    """Attempt to load cookies from the specified browser profile.

    Requires the `browser_cookie3` package. Returns a requests-compatible dict of cookies
    or raises an ImportError/RuntimeError with a helpful message.
    """
    try:
        import browser_cookie3
    except Exception as e:
        raise ImportError("browser_cookie3 is not installed. Install with `pip install browser_cookie3`") from e

    try:
        # browser_cookie3 has functions like chrome(), firefox(), brave(), etc.
        fn = getattr(browser_cookie3, browser_name, None)
        if not fn:
            # fallback to chrome() since Brave is Chromium-based
            fn = browser_cookie3.chrome
        jar = fn()

        # Convert cookiejar to dict
        cookies = {}
        for c in jar:
            cookies[c.name] = c.value
        return cookies
    except Exception as e:
        raise RuntimeError(f"Failed to load cookies from {browser_name}: {e}") from e

# --------------------------------------------------------------

# Optional Playwright-based fetch to bypass anti-bot/CDN blocks
def fetch_html_via_playwright(url, referer=None, timeout=30):
    """Return the page HTML using Playwright (synchronously).

    Requires `playwright` to be installed and a browser (chromium) to be installed via
    `playwright install`.
    Returns HTML text on success, or None on failure.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        print(f"⚠️ Playwright not installed: {e}")
        print("   Install with: pip install playwright && playwright install")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            # add a referer header if supplied
            if referer:
                context.set_extra_http_headers({"referer": referer})
            page = context.new_page()
            page.goto(url, timeout=timeout * 1000, wait_until="networkidle")
            html = page.content()
            browser.close()
            print(f"✅ Playwright successfully fetched page (bypassed anti-bot block)")
            return html
    except Exception as e:
        print(f"⚠️ Playwright fetch failed: {e}")
        return None


def fetch_html_via_httpx(url, referer=None, timeout=30):
    """Return the page HTML using httpx with HTTP/2 enabled.

    Requires `httpx` to be installed.
    Returns HTML text on success, or None on failure.
    """
    try:
        import httpx
    except ImportError as e:
        print(f"⚠️ httpx not installed: {e}")
        print("   Install with: pip install httpx")
        return None

    headers = dict(session.headers)
    if referer:
        headers["Referer"] = referer

    try:
        with httpx.Client(http2=True, headers=headers, timeout=timeout, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 403 and DEBUG_HTTP:
                # Mirror diagnostics style for requests responses
                print("\n🔎 HTTP 403 diagnostics (httpx):")
                print(f"  url: {resp.url}")
                print(f"  status: {resp.status_code}")
                print(f"  server: {resp.headers.get('Server', 'unknown')}")
                print(f"  content-type: {resp.headers.get('Content-Type', 'unknown')}")
                set_cookie = resp.headers.get("Set-Cookie")
                if set_cookie:
                    print(f"  set-cookie: present ({len(set_cookie)} chars)")
                else:
                    print("  set-cookie: none")
                snippet = (resp.text or "")[:800].replace("\n", " ").replace("\r", " ")
                print(f"  body-snippet: {snippet}")
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        print(f"⚠️ httpx fetch failed: {e}")
        return None

seen_files = set()
seen_hashes = set()
file_hash_map = {}  # filename -> hash
stop_flag = False
START_PAGE_OVERRIDE = False  # set True when --start-page is provided on CLI


def _iter_part_dirs(download_dir):
    parts = []
    try:
        for name in os.listdir(download_dir):
            m = re.match(r"part_(\d+)$", name)
            if m:
                parts.append((int(m.group(1)), os.path.join(download_dir, name)))
    except FileNotFoundError:
        return []
    return sorted(parts, key=lambda x: x[0])


def init_part_tracker(download_dir):
    """Initialize part index and count based on existing part_x folders.
    
    Now counts page folders instead of individual files.
    """
    global CURRENT_PART_INDEX, CURRENT_PART_COUNT
    parts = _iter_part_dirs(download_dir)
    if not parts:
        CURRENT_PART_INDEX = 1
        CURRENT_PART_COUNT = 0
        return

    last_index, last_dir = parts[-1]
    # Count page_* directories in the last part
    count = 0
    try:
        for entry in os.scandir(last_dir):
            if entry.is_dir() and entry.name.startswith("page_"):
                count += 1
    except FileNotFoundError:
        count = 0

    if count >= MAX_PAGES_PER_PART:
        CURRENT_PART_INDEX = last_index + 1
        CURRENT_PART_COUNT = 0
    else:
        CURRENT_PART_INDEX = last_index
        CURRENT_PART_COUNT = count


def allocate_part_for_page(page_num):
    """Allocate a part directory for a given page number.
    
    This is now deterministic based on page number, not sequential.
    Updates CURRENT_PART_INDEX/COUNT for tracking purposes.
    """
    global CURRENT_PART_INDEX, CURRENT_PART_COUNT
    part_idx = get_part_index_for_page(page_num)
    
    # Update tracking if we're moving to a new part
    if part_idx > CURRENT_PART_INDEX:
        CURRENT_PART_INDEX = part_idx
        CURRENT_PART_COUNT = 1
    elif part_idx == CURRENT_PART_INDEX:
        CURRENT_PART_COUNT += 1
    
    part_dir = os.path.join(DOWNLOAD_DIR, f"part_{part_idx}")
    return part_dir


def preview_part_dir_for_page(page_num):
    """Preview which part directory would contain a given page."""
    part_idx = get_part_index_for_page(page_num)
    return os.path.join(DOWNLOAD_DIR, f"part_{part_idx}")


def part_dir_from_path(path):
    if not path:
        return None
    dir_path = os.path.dirname(path)
    if os.path.basename(dir_path) == CONFLICTS_SUBDIR:
        dir_path = os.path.dirname(dir_path)
    if re.match(r"part_\d+$", os.path.basename(dir_path)):
        return dir_path
    return None


def page_from_url(url):
    """Return integer page number from a URL like ...?page=N, or None if not found."""
    m = re.search(r"page=(\d+)", url)
    return int(m.group(1)) if m else None


def get_part_index_for_page(page_num):
    """Return which part folder a given page should belong to.
    
    part_1: pages 0-99
    part_2: pages 100-199
    etc.
    """
    return (page_num // MAX_PAGES_PER_PART) + 1


def get_download_path_for_page(page_num, filename, is_conflict=False):
    """Generate the full path where a file should be saved based on its page number.
    
    Downloads: downloads/part_X/page_Y/filename
    Conflicts: downloads/conflicts/page_Y/filename (no part folders)
    """
    if is_conflict:
        page_dir = os.path.join(CONFLICT_DIR, f"page_{page_num}")
        return os.path.join(page_dir, filename)
    else:
        part_idx = get_part_index_for_page(page_num)
        part_dir = os.path.join(DOWNLOAD_DIR, f"part_{part_idx}")
        page_dir = os.path.join(part_dir, f"page_{page_num}")
        return os.path.join(page_dir, filename)


def load_state():
    if not ENABLE_STATE_FILE:
        if ENABLE_DB_STATE:
            try:
                raw = db_get_state("crawl_state")
                if raw:
                    data = json.loads(raw)
                    data.setdefault("current_url", START_URL)
                    data.setdefault("seen_files", [])
                    data.setdefault("seen_hashes", [])
                    data.setdefault("file_hashes", {})
                    return data
            except Exception:
                pass
        return {
            "current_url": START_URL,
            "seen_files": [],
            "seen_hashes": [],
            "file_hashes": {}
        }
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # ensure compatibility with older state files
            data.setdefault("seen_files", [])
            data.setdefault("seen_hashes", [])
            data.setdefault("file_hashes", {})
            return data
    return {
        "current_url": START_URL,
        "seen_files": [],
        "seen_hashes": [],
        "file_hashes": {}
    }


def save_state(url):
    """Write crawl state to STATE_FILE only when the current page number
    surpasses the saved page number. This prevents --start-page from being
    overwritten by older state while still allowing progress to be saved once
    the run moves past the saved page.
    """
    if not ENABLE_STATE_FILE:
        if ENABLE_DB_STATE:
            try:
                payload = {
                    "current_url": url,
                    "seen_files": list(seen_files),
                    "seen_hashes": list(seen_hashes),
                    "file_hashes": file_hash_map
                }
                db_set_state("crawl_state", json.dumps(payload))
            except Exception:
                pass
        return
    new_page = page_from_url(url)

    # If the state file exists, inspect the saved page number
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                saved_url = data.get("current_url")
                saved_page = page_from_url(saved_url) if saved_url else None
        except Exception:
            # If we can't read it for any reason, be conservative and write state
            saved_page = None
    else:
        saved_page = None

    # Only write the state file if no saved_page exists, or the new page is
    # strictly greater than the saved page (monotonic progress), or saved_page
    # is None (file unreadable/absent).
    if saved_page is not None and new_page is not None and new_page <= saved_page:
        # Skip writing to avoid regressing the saved page
        # (this keeps --start-page effective until we surpass saved page)
        print(f"(state unchanged) saved page={saved_page} >= current page={new_page}; not updating {STATE_FILE}")
        return

    # Otherwise persist state (new_page > saved_page or no saved page present)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "current_url": url,
            "seen_files": list(seen_files),
            "seen_hashes": list(seen_hashes),
            "file_hashes": file_hash_map
        }, f, indent=2)


def log_csv(row):
    if not ENABLE_CSV_LOG:
        return
    exists = os.path.exists(CSV_LOG)
    with open(CSV_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["filename", "url", "status", "dest"])
        # normalize rows to 4 columns
        if len(row) < 4:
            row = row + [""] * (4 - len(row))
        writer.writerow(row)


def file_hash(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()


def _pdf_escape(text):
    """Escape text for use inside a PDF literal string."""
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def write_text_pdf(lines, out_path):
    """Write a text PDF using only the Python standard library.

    Supports either plain strings or styled entries:
      {"text": "...", "color": (r, g, b)}
      {"page_break": True}
    where RGB values are floats in [0.0, 1.0].
    """
    page_width = 612
    page_height = 792
    left_margin = 40
    top_y = 760
    line_height = 12
    max_chars = 110
    lines_per_page = 58

    if not lines:
        lines = ["(no content)"]

    normalized = []
    for item in lines:
        if isinstance(item, dict):
            if item.get("page_break"):
                normalized.append({"page_break": True})
                continue
            text = str(item.get("text", ""))
            color = item.get("color", (0, 0, 0))
        else:
            text = str(item)
            color = (0, 0, 0)

        if len(text) <= max_chars:
            normalized.append({"text": text, "color": color})
            continue
        for i in range(0, len(text), max_chars):
            normalized.append({"text": text[i:i + max_chars], "color": color})

    pages = []
    current_page = []
    for entry in normalized:
        if entry.get("page_break"):
            if current_page:
                pages.append(current_page)
                current_page = []
            continue
        current_page.append(entry)
        if len(current_page) >= lines_per_page:
            pages.append(current_page)
            current_page = []
    if current_page:
        pages.append(current_page)

    if not pages:
        pages = [[{"text": "(no content)", "color": (0, 0, 0)}]]

    objects = {}
    page_obj_nums = []
    content_obj_nums = []

    # 1: catalog, 2: pages root, 3: font
    objects[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects[3] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>"

    next_obj = 4
    for page_lines in pages:
        page_obj = next_obj
        content_obj = next_obj + 1
        next_obj += 2

        page_obj_nums.append(page_obj)
        content_obj_nums.append(content_obj)

        stream_lines = [
            "BT",
            "/F1 9 Tf",
            f"{left_margin} {top_y} Td",
            f"{line_height} TL"
        ]

        for idx, entry in enumerate(page_lines):
            if idx != 0:
                stream_lines.append("T*")
            line = entry.get("text", "")
            color = entry.get("color", (0, 0, 0))
            try:
                r, g, b = color
            except Exception:
                r, g, b = 0, 0, 0
            escaped = _pdf_escape(line)
            stream_lines.append(f"{r:.3f} {g:.3f} {b:.3f} rg")
            stream_lines.append(f"({escaped}) Tj")
        stream_lines.append("ET")

        stream = "\n".join(stream_lines).encode("latin-1", errors="replace")
        objects[content_obj] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii") +
            stream +
            b"\nendstream"
        )

        objects[page_obj] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_obj} 0 R >>"
        ).encode("ascii")

    kids = " ".join(f"{n} 0 R" for n in page_obj_nums)
    objects[2] = f"<< /Type /Pages /Kids [ {kids} ] /Count {len(page_obj_nums)} >>".encode("ascii")

    max_obj = max(objects.keys())
    blob = bytearray()
    blob.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")

    offsets = [0] * (max_obj + 1)
    for obj_num in range(1, max_obj + 1):
        content = objects[obj_num]
        offsets[obj_num] = len(blob)
        blob.extend(f"{obj_num} 0 obj\n".encode("ascii"))
        blob.extend(content)
        blob.extend(b"\nendobj\n")

    xref_offset = len(blob)
    blob.extend(f"xref\n0 {max_obj + 1}\n".encode("ascii"))
    blob.extend(b"0000000000 65535 f \n")
    for obj_num in range(1, max_obj + 1):
        blob.extend(f"{offsets[obj_num]:010d} 00000 n \n".encode("ascii"))

    blob.extend(
        (
            f"trailer\n<< /Size {max_obj + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("ascii")
    )

    with open(out_path, "wb") as f:
        f.write(blob)


def _looks_binary(buf):
    if not buf:
        return False
    if b"\x00" in buf:
        return True
    printable = 0
    for b in buf[:4096]:
        if b in (9, 10, 13) or 32 <= b <= 126:
            printable += 1
    sample_len = min(len(buf), 4096)
    return sample_len > 0 and (printable / sample_len) < 0.70


def _hexdump_lines(path, limit_bytes=262144):
    lines = []
    with open(path, "rb") as f:
        data = f.read(limit_bytes)
    for offset in range(0, len(data), 16):
        chunk = data[offset:offset + 16]
        hex_chunk = " ".join(f"{b:02x}" for b in chunk)
        ascii_chunk = "".join(chr(b) if 32 <= b <= 126 else "." for b in chunk)
        lines.append(f"{offset:08x}: {hex_chunk:<47} |{ascii_chunk}|")
    if len(data) >= limit_bytes:
        lines.append(f"... hexdump truncated at {limit_bytes} bytes ...")
    return lines


def _extract_pdf_text_lines(path, max_pages=250):
    try:
        from pypdf import PdfReader
    except Exception:
        return None


def _extract_pdf_page_lines(path, max_pages=250):
    """Return PDF text as list-of-pages, where each page is a list of lines."""
    try:
        from pypdf import PdfReader
    except Exception:
        return None

    try:
        reader = PdfReader(path)
        pages = []
        for page in reader.pages[:max_pages]:
            text = page.extract_text() or ""
            if text.strip():
                pages.append(text.splitlines())
            else:
                pages.append(["<no extractable text>"])
        return pages
    except Exception:
        return None

    try:
        reader = PdfReader(path)
        lines = []
        for idx, page in enumerate(reader.pages[:max_pages]):
            text = page.extract_text() or ""
            lines.append(f"[PAGE {idx + 1}]")
            if text.strip():
                lines.extend(text.splitlines())
            else:
                lines.append("<no extractable text>")
        if len(reader.pages) > max_pages:
            lines.append(f"... text extraction truncated at {max_pages} pages ...")
        return lines
    except Exception:
        return None


def extract_comparison_lines(path):
    """Extract line-based content for diffing, with binary-safe fallback."""
    ext = os.path.splitext(path)[1].lower()

    if ext == ".pdf":
        pdf_lines = _extract_pdf_text_lines(path)
        if pdf_lines:
            return pdf_lines

    with open(path, "rb") as f:
        raw = f.read(524288)

    if not _looks_binary(raw):
        text = raw.decode("utf-8", errors="replace")
        lines = text.splitlines()
        if len(raw) >= 524288:
            lines.append("... text extraction truncated at 524288 bytes ...")
        return lines

    return _hexdump_lines(path)


def get_compare_pdf_path(conflict_path, filename, file_hash_value=None):
    """Return a compare-report path alongside the conflict file."""
    conflict_dir = os.path.dirname(conflict_path)
    stem, _ = os.path.splitext(filename)

    base_path = os.path.join(conflict_dir, f"{stem}_compare.pdf")
    if not os.path.exists(base_path):
        return base_path

    # If the canonical compare path already exists, create a unique sibling.
    suffix = None
    conflict_name = os.path.basename(conflict_path)
    if "__" in conflict_name:
        suffix = conflict_name.split("__", 1)[1]
    if not suffix and file_hash_value:
        suffix = file_hash_value[:8]
    if not suffix:
        suffix = str(int(time.time()))

    return os.path.join(conflict_dir, f"{stem}_compare__{suffix}.pdf")


def generate_conflict_compare_pdf(existing_path, conflict_path, filename, file_hash_value=None):
    """Create a comparison PDF report for two conflicting files.

    The generated report is line-based and marks additions/deletions from a
    unified diff so conflicts can be triaged quickly.
    """
    if not existing_path or not os.path.exists(existing_path):
        print(f"⚠️ Could not create compare PDF for {filename}: original file not found")
        return None
    if not conflict_path or not os.path.exists(conflict_path):
        print(f"⚠️ Could not create compare PDF for {filename}: conflict file not found")
        return None

    compare_pdf_path = get_compare_pdf_path(conflict_path, filename, file_hash_value=file_hash_value)

    try:
        ext = os.path.splitext(filename)[1].lower()

        # For PDF-to-PDF conflicts, build a page-aware diff report where each
        # changed source page starts on a new report page for fast navigation.
        if ext == ".pdf":
            old_pages = _extract_pdf_page_lines(existing_path)
            new_pages = _extract_pdf_page_lines(conflict_path)
            if old_pages is not None and new_pages is not None:
                report_lines = [
                    {"text": f"Conflict comparison for: {filename}", "color": (0.0, 0.0, 0.0)},
                    {"text": f"Original: {existing_path}", "color": (0.15, 0.15, 0.15)},
                    {"text": f"Conflict: {conflict_path}", "color": (0.15, 0.15, 0.15)},
                    {"text": "Legend: RED removed, GREEN added, BLUE hunk header", "color": (0.2, 0.2, 0.2)},
                    {"text": "", "color": (0, 0, 0)},
                ]

                changed_pages = 0
                max_pages = max(len(old_pages), len(new_pages))
                for idx in range(max_pages):
                    old_lines = old_pages[idx] if idx < len(old_pages) else []
                    new_lines = new_pages[idx] if idx < len(new_pages) else []

                    diff = list(
                        difflib.unified_diff(
                            old_lines,
                            new_lines,
                            fromfile=f"original_page_{idx + 1}",
                            tofile=f"conflict_page_{idx + 1}",
                            lineterm="",
                            n=2,
                        )
                    )

                    has_real_changes = any(
                        (ln.startswith("+") and not ln.startswith("+++")) or
                        (ln.startswith("-") and not ln.startswith("---"))
                        for ln in diff
                    )
                    if not has_real_changes:
                        continue

                    changed_pages += 1
                    if changed_pages > 1:
                        report_lines.append({"page_break": True})

                    report_lines.append({"text": f"Page {idx + 1} changes", "color": (0.0, 0.0, 0.0)})
                    report_lines.append({"text": "", "color": (0, 0, 0)})

                    max_diff_lines = 2200
                    for line in diff[:max_diff_lines]:
                        if line.startswith("@@"):
                            report_lines.append({"text": f"[HUNK] {line}", "color": (0.1, 0.2, 0.8)})
                        elif line.startswith("+") and not line.startswith("+++"):
                            report_lines.append({"text": f"[ADD] {line[1:]}", "color": (0.0, 0.55, 0.0)})
                        elif line.startswith("-") and not line.startswith("---"):
                            report_lines.append({"text": f"[DEL] {line[1:]}", "color": (0.78, 0.0, 0.0)})
                        elif line.startswith("---") or line.startswith("+++"):
                            continue
                        else:
                            report_lines.append({"text": line, "color": (0.25, 0.25, 0.25)})
                    if len(diff) > max_diff_lines:
                        report_lines.append({"text": f"... diff truncated at {max_diff_lines} lines ...", "color": (0.35, 0.35, 0.35)})

                if changed_pages == 0:
                    report_lines.append({"text": "No page-level text differences found.", "color": (0.1, 0.45, 0.1)})

                os.makedirs(os.path.dirname(compare_pdf_path), exist_ok=True)
                write_text_pdf(report_lines, compare_pdf_path)
                return compare_pdf_path

        old_lines = extract_comparison_lines(existing_path)
        new_lines = extract_comparison_lines(conflict_path)
        diff = list(
            difflib.unified_diff(
                old_lines,
                new_lines,
                fromfile=os.path.basename(existing_path),
                tofile=os.path.basename(conflict_path),
                lineterm="",
                n=2,
            )
        )

        report_lines = [
            {"text": f"Conflict comparison for: {filename}", "color": (0.0, 0.0, 0.0)},
            {"text": f"Original: {existing_path}", "color": (0.15, 0.15, 0.15)},
            {"text": f"Conflict: {conflict_path}", "color": (0.15, 0.15, 0.15)},
            {"text": "Legend: RED removed, GREEN added, BLUE hunk header", "color": (0.2, 0.2, 0.2)},
            {"text": "", "color": (0, 0, 0)},
        ]

        if not diff:
            report_lines.append({"text": "No diff lines produced by comparator.", "color": (0.1, 0.45, 0.1)})
        else:
            max_diff_lines = 1800
            trimmed = diff[:max_diff_lines]
            for line in trimmed:
                if line.startswith("@@"):
                    report_lines.append({"text": f"[HUNK] {line}", "color": (0.1, 0.2, 0.8)})
                elif line.startswith("+") and not line.startswith("+++"):
                    report_lines.append({"text": f"[ADD] {line[1:]}", "color": (0.0, 0.55, 0.0)})
                elif line.startswith("-") and not line.startswith("---"):
                    report_lines.append({"text": f"[DEL] {line[1:]}", "color": (0.78, 0.0, 0.0)})
                else:
                    report_lines.append({"text": line, "color": (0.25, 0.25, 0.25)})
            if len(diff) > max_diff_lines:
                report_lines.append({"text": f"... diff truncated at {max_diff_lines} lines ...", "color": (0.35, 0.35, 0.35)})

        os.makedirs(os.path.dirname(compare_pdf_path), exist_ok=True)
        write_text_pdf(report_lines, compare_pdf_path)
        return compare_pdf_path
    except Exception as e:
        print(f"⚠️ Failed to create compare PDF for {filename}: {e}")
        return None


def generate_visual_pdf_comparison(existing_path, conflict_path, filename, file_hash_value=None):
    """Create a visual side-by-side PDF comparison using PyMuPDF.

    This generates a PDF where each page shows the original and conflict versions
    side-by-side, making visual differences easy to spot. Only works for PDF files.
    """
    # Only works for PDF files
    ext = os.path.splitext(filename)[1].lower()
    if ext != ".pdf":
        return None

    try:
        import fitz  # PyMuPDF
    except ImportError:
        # Silently skip if PyMuPDF not installed (text diff is still useful)
        return None

    if not existing_path or not os.path.exists(existing_path):
        return None
    if not conflict_path or not os.path.exists(conflict_path):
        return None

    try:
        # Open both PDFs
        doc_old = fitz.open(existing_path)
        doc_new = fitz.open(conflict_path)

        # Create output path
        conflict_dir = os.path.dirname(conflict_path)
        stem, _ = os.path.splitext(filename)

        base_path = os.path.join(conflict_dir, f"{stem}_visual_compare.pdf")
        if os.path.exists(base_path):
            suffix = file_hash_value[:8] if file_hash_value else str(int(time.time()))
            base_path = os.path.join(conflict_dir, f"{stem}_visual_compare__{suffix}.pdf")

        # Create new PDF for side-by-side comparison
        output = fitz.open()

        max_pages = max(len(doc_old), len(doc_new))

        for page_num in range(max_pages):
            # Create a new page (landscape to fit both documents side by side)
            # Use A4 landscape dimensions: 842 x 595 points
            new_page = output.new_page(width=842, height=595)

            # Left side: original document
            if page_num < len(doc_old):
                old_page = doc_old[page_num]
                # Scale to fit left half with margin
                dst_rect = fitz.Rect(10, 30, 411, 585)
                new_page.show_pdf_page(dst_rect, doc_old, page_num)
                # Add label at top
                new_page.insert_text((10, 20), f"ORIGINAL (page {page_num + 1})",
                                    fontsize=9, color=(0, 0, 0.8))
            else:
                # If original has fewer pages, show placeholder
                new_page.insert_text((100, 300), f"(Original has only {len(doc_old)} pages)",
                                    fontsize=10, color=(0.5, 0.5, 0.5))
                new_page.insert_text((10, 20), "ORIGINAL",
                                    fontsize=9, color=(0, 0, 0.8))

            # Right side: conflict document
            if page_num < len(doc_new):
                new_page_obj = doc_new[page_num]
                # Scale to fit right half with margin
                dst_rect = fitz.Rect(431, 30, 832, 585)
                new_page.show_pdf_page(dst_rect, doc_new, page_num)
                # Add label at top
                new_page.insert_text((431, 20), f"CONFLICT (page {page_num + 1})",
                                    fontsize=9, color=(0.8, 0, 0))
            else:
                # If conflict has fewer pages, show placeholder
                new_page.insert_text((500, 300), f"(Conflict has only {len(doc_new)} pages)",
                                    fontsize=10, color=(0.5, 0.5, 0.5))
                new_page.insert_text((431, 20), "CONFLICT",
                                    fontsize=9, color=(0.8, 0, 0))

            # Draw vertical dividing line
            new_page.draw_line((421, 0), (421, 595), color=(0.3, 0.3, 0.3), width=2)

        # Add title page at the beginning
        title_page = output.new_page(0, width=842, height=595)
        title_page.insert_text((50, 100), "PDF CONFLICT — VISUAL COMPARISON",
                              fontsize=20, color=(0, 0, 0))
        title_page.insert_text((50, 150), f"Filename: {filename}",
                              fontsize=12, color=(0, 0, 0))
        title_page.insert_text((50, 180), f"Original file: {os.path.basename(existing_path)}",
                              fontsize=10, color=(0, 0, 0.8))
        title_page.insert_text((50, 200), f"Conflict file: {os.path.basename(conflict_path)}",
                              fontsize=10, color=(0.8, 0, 0))
        title_page.insert_text((50, 230), f"Page counts: Original={len(doc_old)} | Conflict={len(doc_new)}",
                              fontsize=10, color=(0, 0, 0))
        title_page.insert_text((50, 280), "Each page shows the two versions side-by-side for comparison.",
                              fontsize=9, color=(0.3, 0.3, 0.3))
        title_page.insert_text((50, 300), "Left (blue label) = Original | Right (red label) = Conflict",
                              fontsize=9, color=(0.3, 0.3, 0.3))

        # Save the output
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        output.save(base_path)
        output.close()
        doc_old.close()
        doc_new.close()

        return base_path

    except Exception as e:
        print(f"⚠️ Failed to create visual PDF comparison for {filename}: {e}")
        return None


def find_existing_file(filename):
    """Search for an existing file with the given filename using the index DB
    first (fast), and fall back to scanning filesystem (updates index lazily).
    Returns absolute path or None if not found.
    """
    # Consult the index database first
    try:
        info = db_get_file(filename)
        if info and info.get("path"):
            if os.path.exists(info["path"]):
                return info["path"]
            else:
                # Path in DB no longer exists on disk; treat as missing
                if NO_FS_FALLBACK:
                    return None
                # otherwise fall through to filesystem fallback
        else:
            # No DB record for this filename
            if NO_FS_FALLBACK:
                # configured to never scan filesystem; report not found
                return None
    except Exception:
        # DB error -> decide based on NO_FS_FALLBACK
        if NO_FS_FALLBACK:
            return None
        # otherwise continue to filesystem fallback
        pass

    # Fallback: perform original filesystem search (and update DB for future runs)
    # Check top-level (legacy location)
    candidate = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(candidate):
        try:
            size = os.path.getsize(candidate)
            h = file_hash(candidate)
        except Exception:
            size = None
            h = None
        db_upsert_file(filename, candidate, size, h, page_num=None, status="downloaded")
        return candidate

    # Search new structure: part_*/page_*/ folders
    for _, part_dir in _iter_part_dirs(DOWNLOAD_DIR):
        # Iterate through page folders in this part
        try:
            for entry in os.scandir(part_dir):
                if entry.is_dir() and entry.name.startswith("page_"):
                    page_dir = entry.path
                    cand = os.path.join(page_dir, filename)
                    if os.path.exists(cand):
                        try:
                            size = os.path.getsize(cand)
                            h = file_hash(cand)
                            # Extract page number from folder name
                            page_match = re.match(r"page_(\d+)", entry.name)
                            pg = int(page_match.group(1)) if page_match else None
                        except Exception:
                            size = None
                            h = None
                            pg = None
                        db_upsert_file(filename, cand, size, h, page_num=pg, status="downloaded")
                        return cand
        except FileNotFoundError:
            continue

    # Check conflicts structure: conflicts/page_*/
    if os.path.exists(CONFLICT_DIR):
        try:
            for entry in os.scandir(CONFLICT_DIR):
                if entry.is_dir() and entry.name.startswith("page_"):
                    page_dir = entry.path
                    conf_cand = os.path.join(page_dir, filename)
                    if os.path.exists(conf_cand):
                        try:
                            size = os.path.getsize(conf_cand)
                            h = file_hash(conf_cand)
                            # Extract page number from folder name
                            page_match = re.match(r"page_(\d+)", entry.name)
                            pg = int(page_match.group(1)) if page_match else None
                        except Exception:
                            size = None
                            h = None
                            pg = None
                        db_upsert_file(filename, conf_cand, size, h, page_num=pg, status="conflict")
                        return conf_cand
        except FileNotFoundError:
            pass

    return None


def download_file(url, page_num=None):
    """Download a file from the given URL.
    
    Args:
        url: The URL to download from
        page_num: The page number where this file was found (required for new structure)
    """
    filename = url.split("/")[-1].split("?")[0]
    tmp_path = os.path.join(DOWNLOAD_DIR, filename + ".part")
    
    # Use global CURRENT_PAGE_NUM if page_num not provided
    if page_num is None:
        page_num = CURRENT_PAGE_NUM
    
    if page_num is None:
        print(f"⚠️ Warning: page_num is None for {filename}, cannot determine correct folder")
        return False

    if ORGANIZE_MODE:
        # In organize mode, we don't download, just track what we would organize
        return True

    if DRY_RUN:
        with lock:
            if filename in seen_files:
                existing = find_existing_file(filename)
                planned_dest = existing if existing else get_download_path_for_page(page_num, filename)
                print(f"🧪 DRY-RUN SKIP: {filename} already seen -> {planned_dest}")
                log_csv([filename, url, "DRY_RUN_SKIPPED_ALREADY_DOWNLOADED", planned_dest])
                return True
            planned_dest = get_download_path_for_page(page_num, filename)
            print(f"🧪 DRY-RUN: {filename} -> {planned_dest}")
            log_csv([filename, url, "DRY_RUN", ""])
            seen_files.add(filename)
            return True

    try:
        # If we've already downloaded this filename, try to avoid re-downloading:
        with lock:
            already_seen = filename in seen_files
            existing_hash = file_hash_map.get(filename)
            existing_path = find_existing_file(filename)

        if already_seen and existing_path and os.path.exists(existing_path):
            # try a HEAD request to double-check content-length; if it matches, skip
            try:
                head = session.head(
                    url,
                    allow_redirects=True,
                    timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)
                )
                if head.ok:
                    cl = head.headers.get("Content-Length")
                    if cl is not None and int(cl) == os.path.getsize(existing_path):
                        print(f"⏭️ Skipping {filename}: already downloaded at {existing_path}")
                        log_csv([filename, url, "SKIPPED_ALREADY_DOWNLOADED", existing_path])
                        return True
            except Exception:
                pass

        print(f"⬇️  {filename}")
        r = session.get(
            url,
            stream=True,
            timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)
        )
        r.raise_for_status()

        start_ts = time.time()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
                if time.time() - start_ts > DOWNLOAD_TOTAL_TIMEOUT:
                    raise TimeoutError(
                        f"Download exceeded {DOWNLOAD_TOTAL_TIMEOUT}s for {filename}"
                    )

        h = file_hash(tmp_path)

        with lock:
            # If the filename was previously seen, check whether the file actually
            # exists on disk. If it has been deleted since it was seen, treat this
            # as a fresh download (not a filename conflict).
            if filename in seen_files:
                existing_path = find_existing_file(filename)
                if not existing_path or not os.path.exists(existing_path):
                    # File recorded as seen previously but missing now — treat as new
                    # and fall through to the duplicate-by-hash check or fresh save.
                    pass
                else:
                    # Ensure we have an existing hash to compare; compute if missing
                    existing_hash = file_hash_map.get(filename)
                    if existing_hash is None:
                        try:
                            existing_hash = file_hash(existing_path)
                        except Exception:
                            existing_hash = None

                    # If hashes differ (or we couldn't obtain existing hash), it's a conflict
                    if existing_hash is None or existing_hash != h:
                        # Save conflict to conflicts/page_X/ (no part folders for conflicts)
                        conflict_dest = get_download_path_for_page(page_num, filename, is_conflict=True)
                        conflict_dir = os.path.dirname(conflict_dest)
                        compare_dest = get_compare_pdf_path(conflict_dest, filename, file_hash_value=h)
                        
                        if args.dry_run:
                            if os.path.exists(conflict_dest):
                                conflict_dest = conflict_dest.replace(filename, f"{filename}__{h[:8]}")
                            compare_dest = get_compare_pdf_path(conflict_dest, filename, file_hash_value=h)
                            print(f"🧪 DRY-RUN: would save conflicting content for {filename} -> {conflict_dest}")
                            print(f"🧪 DRY-RUN: would generate compare PDF -> {compare_dest}")
                            log_csv([filename, url, "DRY_RUN_CONFLICT", conflict_dest])
                            seen_hashes.add(h)
                            return True
                        
                        os.makedirs(conflict_dir, exist_ok=True)
                        
                        # If a conflict already exists at this path, append hash to filename
                        if os.path.exists(conflict_dest):
                            conflict_dest = conflict_dest.replace(filename, f"{filename}__{h[:8]}")
                        
                        os.replace(tmp_path, conflict_dest)
                        try:
                            db_upsert_file(filename, conflict_dest, os.path.getsize(conflict_dest), h, page_num=page_num, status="conflict")
                        except Exception:
                            pass

                        # Generate text-based diff report
                        compare_pdf_path = generate_conflict_compare_pdf(existing_path, conflict_dest, filename, file_hash_value=h)
                        if compare_pdf_path:
                            print(f"📝 Text diff PDF: {compare_pdf_path}")

                        # Generate visual side-by-side comparison for PDFs
                        visual_pdf_path = generate_visual_pdf_comparison(existing_path, conflict_dest, filename, file_hash_value=h)
                        if visual_pdf_path:
                            print(f"🖼️  Visual comparison PDF: {visual_pdf_path}")

                        print(f"⚠️ Filename conflict for {filename}; different content saved to {conflict_dest}")
                        log_csv([filename, url, "FILENAME_CONFLICT", conflict_dest])
                        seen_hashes.add(h)
                        return True
                    else:
                        # Same filename and same hash — treat as duplicate-by-content.
                        # Instead of saving a duplicate file, skip saving and remove the
                        # temporary download. Keep tracking the hash so future duplicates
                        # are detected, but do not create duplicate files on disk.
                        if args.dry_run:
                            print(f"🧪 DRY-RUN SKIP: duplicate content for {filename}; existing at {existing_path}")
                            log_csv([filename, url, "DRY_RUN_SKIPPED_DUPLICATE", existing_path])
                            return True
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass
                        seen_hashes.add(h)
                        file_hash_map[filename] = h
                        print(f"⏭️ Skipping duplicate {filename}: identical content already exists at {existing_path}")
                        log_csv([filename, url, "SKIPPED_DUPLICATE_HASH", existing_path])
                        return True

            # Duplicate by content (hash seen elsewhere): skip saving duplicate file
            if h in seen_hashes:
                if args.dry_run:
                    print(f"🧪 DRY-RUN SKIP: duplicate content for {filename}; hash seen previously")
                    log_csv([filename, url, "DRY_RUN_SKIPPED_DUPLICATE", ""])
                    return True
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                # Do not create duplicate files — just record that we've seen the hash
                print(f"⏭️ Skipping duplicate {filename}; content already downloaded elsewhere")
                log_csv([filename, url, "SKIPPED_DUPLICATE_HASH", ""])
                return True

            # Fresh file: move into page folder and record
            final_path = get_download_path_for_page(page_num, filename)
            final_dir = os.path.dirname(final_path)
            
            if args.dry_run:
                print(f"🧪 DRY-RUN: would download {filename} -> {final_path}")
                log_csv([filename, url, "DRY_RUN", final_path])
                seen_files.add(filename)
                seen_hashes.add(h)
                file_hash_map[filename] = h
                return True
            
            # Ensure target page folder exists now that we're about to save
            os.makedirs(final_dir, exist_ok=True)
            os.replace(tmp_path, final_path)
            seen_files.add(filename)
            seen_hashes.add(h)
            file_hash_map[filename] = h
            try:
                db_upsert_file(filename, final_path, os.path.getsize(final_path), h, page_num=page_num, status="downloaded")
            except Exception:
                pass

        log_csv([filename, url, "DOWNLOADED", final_path])
        print(f"✅ Downloaded {filename} -> {final_path}")
        return True

    except Exception as e:
        try:
            try:
                r.close()
            except Exception:
                pass
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        log_csv([filename, url, f"ERROR: {e}", ""])
        return False


def scrape_links(url, referer=None, max_retries=None):
    """Fetch and parse the given page for file links.

    On 403 responses this will retry with exponential backoff a few times.
    Returns:
      - list of links when successful
      - empty list when page parsed successfully but no file links found
      - None when persistent 403 (after retries)
    """
    if max_retries is None:
        max_retries = DEFAULT_SCRAPE_MAX_RETRIES

    print(f"\n📄 Fetching page: {url}")
    headers = {}
    if referer:
        headers["Referer"] = referer

    def _parse_links(html_text):
        soup = BeautifulSoup(html_text, "html.parser")
        container = soup.find("div", class_="item-list")
        if not container:
            return []

        links = []
        for a in container.find_all("a", href=True):
            href = a["href"]
            # Download any file with an extension (anything with a dot before the last path segment)
            # This captures all file types, not just PDF/ZIP/CSV/TXT
            if re.search(r"\.[a-zA-Z0-9]+$", href):
                links.append(urljoin(url, href))
        return links

    # Prefer httpx (HTTP/2) first when enabled
    if DEFAULT_USE_HTTPX:
        print("🔁 Attempting httpx (HTTP/2) first...")
        html = fetch_html_via_httpx(url, referer=referer)
        if html:
            return _parse_links(html)
        else:
            print("⚠️ httpx did not return content; falling back to requests.")

    attempts = 0
    while attempts < max_retries:
        try:
            r = session.get(url, timeout=30, headers=headers)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error: {e}")
            attempts += 1
            if attempts < max_retries:
                wait = 1 * attempts
                print(f"   Retrying in {wait}s...")
                time.sleep(wait)
            continue

        if r.status_code == 403:
            if DEBUG_HTTP:
                log_http_response(r, label="403")
            attempts += 1
            wait = 1 * attempts
            print(f"⚠️ 403 Forbidden (attempt {attempts}/{max_retries}) — waiting {wait}s and retrying...")
            time.sleep(wait)
            continue

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ HTTP error {r.status_code}: {e}")
            if DEBUG_HTTP:
                log_http_response(r, label=f"http_{r.status_code}")
            return None

        return _parse_links(r.text)

    print("⚠️ Failed to fetch page after retries (403).")

    # If Playwright fallback is enabled, try to fetch the page with a real browser
    if DEFAULT_USE_PLAYWRIGHT:
        print("🔁 Attempting Playwright fallback to fetch page...")
        html = fetch_html_via_playwright(url, referer=referer)
        if html:
            return _parse_links(html)
        else:
            print("⚠️ Playwright fallback did not return content.")

    # Return None to signal persistent 403 to the caller
    return None


def increment_page(url):
    match = re.search(r"page=(\d+)", url)
    if not match:
        raise ValueError("No page parameter found")
    page = int(match.group(1))
    return re.sub(r"page=\d+", f"page={page + 1}", url)


def organize_files():
    """Organize existing files into page-based folder structure.
    
    Crawls the website to get current page numbers for each filename,
    then moves local files to their correct page folders. Files that
    can't be matched go to downloads/unmatched/.
    """
    global stop_flag, CURRENT_PAGE_NUM
    
    print("🗂️  Starting organize mode...")
    print("   Scanning website to map filenames to pages...")
    
    # Build a map of filename -> page_num by crawling the website
    filename_to_page = {}
    url = START_URL
    prev_url = None
    empty_count = 0
    forbidden_count = 0
    max_page_scanned = -1  # Track the highest page we actually scanned
    
    MAX_EMPTY_CONSECUTIVE = DEFAULT_MAX_EMPTY_CONSECUTIVE
    MAX_FORBIDDEN_CONSECUTIVE = DEFAULT_MAX_FORBIDDEN_CONSECUTIVE
    
    # Flag to gracefully finish current page on interrupt
    organize_interrupt_flag = False
    
    def _organize_sigint_handler(signum, frame):
        nonlocal organize_interrupt_flag
        print('\n⚠️ Organize interrupted by user — finishing current page before stopping...')
        organize_interrupt_flag = True
    
    try:
        signal.signal(signal.SIGINT, _organize_sigint_handler)
    except Exception:
        pass
    
    try:
        while not stop_flag:
            page_num = page_from_url(url)
            CURRENT_PAGE_NUM = page_num
            
            # Check if we've reached the end page limit
            if END_PAGE_NUM is not None and page_num > END_PAGE_NUM:
                print(f"🛑 Reached end page limit (page {END_PAGE_NUM}); stopping scan.")
                break
            
            print(f"📄 Scanning page {page_num}: {url}")
            result = scrape_links(url, referer=prev_url, max_retries=DEFAULT_SCRAPE_MAX_RETRIES)
            
            # Track the highest page we successfully scanned
            if result is not None:
                max_page_scanned = max(max_page_scanned, page_num)
            
            if result is None:
                forbidden_count += 1
                if forbidden_count >= MAX_FORBIDDEN_CONSECUTIVE:
                    print("🛑 Stopping scan due to repeated 403 Forbidden responses.")
                    break
                time.sleep(REQUEST_DELAY * 2)
            elif not result:
                empty_count += 1
                if empty_count >= MAX_EMPTY_CONSECUTIVE:
                    print("🛑 No more pages to scan.")
                    break
            else:
                empty_count = 0
                forbidden_count = 0
                
                # Extract filenames and map to page number
                for link in result:
                    filename = link.split("/")[-1].split("?")[0]
                    # Use the first occurrence of a filename (earliest page it appears on)
                    if filename not in filename_to_page:
                        filename_to_page[filename] = page_num
            
            # Check if interrupt was requested; if so, finish this page and stop
            if organize_interrupt_flag:
                print(f"✅ Completed page {page_num}; stopping organize scan.")
                break
            
            prev_url = url
            url = increment_page(url)
            time.sleep(REQUEST_DELAY)
            
    except KeyboardInterrupt:
        print('\n⚠️ Organize interrupted by user.')
        stop_flag = True
    
    print(f"✅ Scanned website and found {len(filename_to_page)} unique files.")
    
    # Now scan local files and move them to correct locations
    print("\n🗂️  Organizing local files...")
    
    # Gather all files from database
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT filename, path, hash, page_num, status FROM files")
    db_files = c.fetchall()
    
    moved = 0
    updated_db = 0
    moved_to_unmatched = 0
    stayed_in_unmatched = 0
    restored_from_unmatched = 0
    already_unmatched = 0
    already_correct = 0
    skipped_with_page = 0  # files already assigned page_num, not in current scan
    
    unmatched_dir = os.path.join(DOWNLOAD_DIR, UNMATCHED_DIR_NAME)
    
    for filename, old_path, file_hash, db_page_num, status in db_files:
        if not old_path or not os.path.exists(old_path):
            print(f"⚠️ File in DB but missing on disk: {filename}")
            continue
        
        # Get the lowest page number from current website scan
        target_page_num = filename_to_page.get(filename)
        
        if target_page_num is None:
            # File not found in current website scan
            if status == "unmatched":
                # Already in unmatched/, keep it there (no change needed)
                already_unmatched += 1
                continue
            elif db_page_num is not None:
                # File had a page assignment but not found in scan
                # Check if we actually scanned the page where this file should be
                if db_page_num > max_page_scanned:
                    # File is assigned to a page we haven't scanned yet (early stop)
                    # Don't move it - just skip it
                    skipped_with_page += 1
                    continue
                else:
                    # File was on a page we scanned, but it's missing now
                    # Move to unmatched/ (file disappeared from website)
                    new_path = os.path.join(unmatched_dir, filename)
                    os.makedirs(unmatched_dir, exist_ok=True)
                    
                    if old_path != new_path:
                        # Handle potential filename collisions in unmatched/
                        if os.path.exists(new_path):
                            base, ext = os.path.splitext(filename)
                            new_path = os.path.join(unmatched_dir, f"{base}__{file_hash[:8]}{ext}")
                        
                        try:
                            os.replace(old_path, new_path)
                            # Keep the original page_num in DB for tracking
                            db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=db_page_num, status="unmatched")
                            print(f"⚠️  Disappeared from page {db_page_num}: {filename} -> unmatched/")
                            moved_to_unmatched += 1
                        except Exception as e:
                            print(f"⚠️ Failed to move {filename} to unmatched: {e}")
            else:
                # File never had a page assignment and still not found
                # Skip it (was probably interrupted during previous organize)
                skipped_with_page += 1
            continue
        
        # File found in scan at target_page_num (lowest page it appears on)
        if status == "unmatched":
            # File is currently in unmatched/
            if db_page_num is None:
                # File never had a valid page before, assign it now
                # (This shouldn't normally happen, but handle gracefully)
                is_conflict = False  # Assume not a conflict
                new_path = get_download_path_for_page(target_page_num, filename, is_conflict=is_conflict)
                new_dir = os.path.dirname(new_path)
                os.makedirs(new_dir, exist_ok=True)
                
                try:
                    os.replace(old_path, new_path)
                    db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=target_page_num, status="downloaded")
                    print(f"✅ Restored from unmatched: {filename} -> page_{target_page_num}/")
                    restored_from_unmatched += 1
                except Exception as e:
                    print(f"⚠️ Failed to restore {filename} from unmatched: {e}")
            elif target_page_num < db_page_num:
                # File reappeared at LOWER page than it had before
                # Restore from unmatched/ to correct page folder
                is_conflict = False  # Restored files are not conflicts
                new_path = get_download_path_for_page(target_page_num, filename, is_conflict=is_conflict)
                new_dir = os.path.dirname(new_path)
                os.makedirs(new_dir, exist_ok=True)
                
                try:
                    os.replace(old_path, new_path)
                    db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=target_page_num, status="downloaded")
                    print(f"✅ Restored from unmatched (page {db_page_num} → {target_page_num}): {filename}")
                    restored_from_unmatched += 1
                except Exception as e:
                    print(f"⚠️ Failed to restore {filename} from unmatched: {e}")
            else:
                # File reappeared at same or HIGHER page (>= db_page_num)
                # Keep in unmatched/ (don't restore)
                stayed_in_unmatched += 1
                continue
        else:
            # File is in normal location (not unmatched)
            if db_page_num is None:
                # New file without page assignment
                is_conflict = (status == "conflict")
                new_path = get_download_path_for_page(target_page_num, filename, is_conflict=is_conflict)
                new_dir = os.path.dirname(new_path)
                os.makedirs(new_dir, exist_ok=True)
                
                try:
                    if old_path != new_path:
                        os.replace(old_path, new_path)
                        print(f"📦 Assigned to page_{target_page_num}: {filename}")
                    db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=target_page_num, status=status)
                    moved += 1
                except Exception as e:
                    print(f"⚠️ Failed to assign {filename}: {e}")
            elif target_page_num < db_page_num:
                # File now appears at LOWER page (page number can only decrease)
                is_conflict = (status == "conflict")
                new_path = get_download_path_for_page(target_page_num, filename, is_conflict=is_conflict)
                new_dir = os.path.dirname(new_path)
                os.makedirs(new_dir, exist_ok=True)
                
                try:
                    if old_path != new_path:
                        os.replace(old_path, new_path)
                        print(f"⬇️  Lowered page (page {db_page_num} → {target_page_num}): {filename}")
                    db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=target_page_num, status=status)
                    moved += 1
                except Exception as e:
                    print(f"⚠️ Failed to move {filename}: {e}")
            elif target_page_num > db_page_num:
                # File found ONLY at higher page (disappeared from lower page)
                # Move to unmatched/ to track the removal
                new_path = os.path.join(unmatched_dir, filename)
                os.makedirs(unmatched_dir, exist_ok=True)
                
                if old_path != new_path:
                    # Handle potential filename collisions in unmatched/
                    if os.path.exists(new_path):
                        base, ext = os.path.splitext(filename)
                        new_path = os.path.join(unmatched_dir, f"{base}__{file_hash[:8]}{ext}")
                    
                    try:
                        os.replace(old_path, new_path)
                        # Keep the original (lower) page_num in DB for tracking where it was
                        db_upsert_file(filename, new_path, os.path.getsize(new_path), file_hash, page_num=db_page_num, status="unmatched")
                        print(f"⚠️  Disappeared from page {db_page_num} (now only at {target_page_num}): {filename} -> unmatched/")
                        moved_to_unmatched += 1
                    except Exception as e:
                        print(f"⚠️ Failed to move {filename} to unmatched: {e}")
                else:
                    # Already in unmatched with correct status
                    already_unmatched += 1
            else:
                # target_page_num == db_page_num, file still at same page
                already_correct += 1
                # Still update DB if needed (in case path changed)
                is_conflict = (status == "conflict")
                expected_path = get_download_path_for_page(db_page_num, filename, is_conflict=is_conflict)
                if old_path == expected_path:
                    continue
                else:
                    # Path mismatch, update DB
                    db_upsert_file(filename, old_path, os.path.getsize(old_path), file_hash, page_num=db_page_num, status=status)
                    updated_db += 1
    
    print(f"\n✅ Organization complete:")
    print(f"   {moved} files moved")
    print(f"   {updated_db} database entries updated")
    print(f"   {already_correct} files already in correct location")
    print(f"   {skipped_with_page} files skipped (assigned to pages not yet scanned)")
    print(f"   {moved_to_unmatched} files moved to unmatched/ (disappeared from lower pages)")
    print(f"   {restored_from_unmatched} files restored from unmatched/ (found at lower pages)")
    print(f"   {stayed_in_unmatched} files stayed in unmatched/ (found only at same/higher pages)")
    print(f"   {already_unmatched} files already in unmatched/ (not found in scan)")


def main():
    global stop_flag

    state = load_state()
    url = state["current_url"]

    seen_files.update(state.get("seen_files", []))
    seen_hashes.update(state.get("seen_hashes", []))
    file_hash_map.update(state.get("file_hashes", {}))

    # If the user provided --start-page, honor that and override the saved
    # state for this run (but do not change the saved state file until progress
    # surpasses the saved page number). This allows ad-hoc restarts without
    # regressing the on-disk state file.
    if START_PAGE_OVERRIDE:
        print(f"⚠️ Start-page override in effect; starting from: {START_URL}")
        url = START_URL


    print(f"▶️  Starting from: {url}")
    print(f"♻️  Resume: {len(seen_files)} files already seen")

    # Optionally import cookies from Brave (or another Chromium-based browser).
    if USE_BROWSER_COOKIES:
        try:
            cookies = load_browser_cookies(BROWSER_NAME)
            if cookies:
                session.cookies.update(cookies)
                print(f"✅ Loaded {len(cookies)} cookies from {BROWSER_NAME}")
            else:
                print(f"⚠️ No cookies found in {BROWSER_NAME} profile")
        except ImportError as e:
            print(f"⚠️ browser_cookie3 not installed: {e}")
            print("   Install with: pip install browser_cookie3")
        except Exception as e:
            print(f"⚠️ Could not load browser cookies from {BROWSER_NAME}: {e}")
            print("   Continuing with default session headers only")
    else:
        print(f"ℹ️ Browser cookie import disabled (USE_BROWSER_COOKIES=False)")

    # If a page returns no links, it might be genuinely empty. We will continue
    # on empty pages but stop if we encounter repeated persistent 403 (forbidden)
    # Values can be overridden via CLI which updates the DEFAULT_* constants.
    MAX_EMPTY_CONSECUTIVE = DEFAULT_MAX_EMPTY_CONSECUTIVE
    MAX_FORBIDDEN_CONSECUTIVE = DEFAULT_MAX_FORBIDDEN_CONSECUTIVE

    empty_count = 0
    forbidden_count = 0
    prev_url = None

    # Install a simple SIGINT handler to set the stop flag (works with Ctrl+C)
    def _sigint_handler(signum, frame):
        global stop_flag
        print('\n⚠️ Received interrupt (SIGINT) — will finish current downloads and exit after they complete...')
        stop_flag = True

    try:
        signal.signal(signal.SIGINT, _sigint_handler)
    except Exception:
        # signal may not be available in some environments; fall back to KeyboardInterrupt handling
        pass

    try:
        while not stop_flag:
            # Extract page number from current URL for tracking
            page_num = page_from_url(url)
            global CURRENT_PAGE_NUM
            CURRENT_PAGE_NUM = page_num
            
            # Check if we've reached the end page limit
            if END_PAGE_NUM is not None and page_num > END_PAGE_NUM:
                print(f"🛑 Reached end page limit (page {END_PAGE_NUM}); stopping crawl.")
                break
            
            result = scrape_links(url, referer=prev_url, max_retries=DEFAULT_SCRAPE_MAX_RETRIES)

            if result is None:
                # Persistent 403 after retries
                forbidden_count += 1
                print(f"⚠️ Persistent 403 on {url} (forbidden count {forbidden_count}/{MAX_FORBIDDEN_CONSECUTIVE})")
                if forbidden_count >= MAX_FORBIDDEN_CONSECUTIVE:
                    print("🛑 Stopping due to repeated 403 Forbidden responses.")
                    break
                else:
                    # give some time before moving to next page
                    time.sleep(REQUEST_DELAY * 2)
            elif not result:
                # Empty but not forbidden — keep going
                empty_count += 1
                print(f"⚠️ No links found on {url} (empty count {empty_count}/{MAX_EMPTY_CONSECUTIVE}).")
                if empty_count >= MAX_EMPTY_CONSECUTIVE:
                    print("🛑 No links found for many pages — stopping.")
                    break
            else:
                # Successful page with links
                empty_count = 0
                forbidden_count = 0

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = [pool.submit(download_file, link, page_num) for link in result]
                    try:
                        # Drain completed futures. If user interrupts (KeyboardInterrupt), set
                        # stop_flag and wait for the remainder of the in-flight downloads to
                        # finish before proceeding.
                        for f in as_completed(futures):
                            try:
                                # retrieve result to propagate exceptions from download_file
                                f.result()
                            except Exception:
                                # download_file logs its own errors; ignore here
                                pass
                    except KeyboardInterrupt:
                        # User pressed Ctrl+C while processing this page; ensure we finish
                        # all downloads for the current page before exiting.
                        print('\n⚠️ Scan interrupted by user — finishing current downloads before exiting...')
                        stop_flag = True
                        from concurrent.futures import wait, ALL_COMPLETED
                        wait(futures, return_when=ALL_COMPLETED)
                        # drain remaining results
                        for f in futures:
                            try:
                                f.result()
                            except Exception:
                                pass

            save_state(url)

            prev_url = url
            url = increment_page(url)
            time.sleep(REQUEST_DELAY)
    except KeyboardInterrupt:
        # Fallback for interrupts outside the page-processing block
        print('\n⚠️ Scan interrupted by user — exiting after current work completes...')
        stop_flag = True
        # If there are still running tasks, we allow the context managers to wait on them
        # when they exit; there's nothing else to do here.


    save_state(url)
    print("\n✅ Crawl complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download DOJ Data Set files")
    parser.add_argument("--dataset", type=int, help="Dataset number (e.g., 10 for data-set-10-files)")
    parser.add_argument("--start-url", help="Starting URL (full override, ignores --dataset and --start-page)")
    parser.add_argument("--start-page", type=int, help="Start page number (constructs URL)")
    parser.add_argument("--end-page", type=int, help="End page number (stop after reaching this page)")
    parser.add_argument("--download-dir", default=DOWNLOAD_DIR, help="Download directory")
    parser.add_argument("--state-file", default=STATE_FILE, help="Path to state file")
    parser.add_argument("--csv-log", default=CSV_LOG, help="CSV log file path")
    parser.add_argument("--dry-run", action="store_true", help="Do not download files")
    parser.add_argument("--use-csv-log", action="store_true", help="Enable CSV logging (database only)")
    parser.add_argument("--no-csv-log", action="store_true", help="Disable CSV logging (database only)")
    parser.add_argument("--use-state-file", action="store_true", help="Enable crawl_state.json usage")
    parser.add_argument("--no-state-file", action="store_true", help="Disable crawl_state.json usage")
    parser.add_argument("--use-db-state", action="store_true", help="Persist crawl state in the DB")
    parser.add_argument("--no-db-state", action="store_true", help="Do not persist crawl state in the DB")
    parser.add_argument("--use-browser-cookies", action="store_true", dest="use_browser_cookies", help="Import cookies from browser")
    parser.add_argument("--no-browser-cookies", action="store_true", dest="no_browser_cookies", help="Do not import cookies from browser")
    parser.add_argument("--browser", default=BROWSER_NAME, help="Browser for cookie import (brave/chrome/firefox)")
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--request-delay", type=float, default=REQUEST_DELAY)
    parser.add_argument("--max-empty", type=int, default=DEFAULT_MAX_EMPTY_CONSECUTIVE)
    parser.add_argument("--max-forbidden", type=int, default=DEFAULT_MAX_FORBIDDEN_CONSECUTIVE)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_SCRAPE_MAX_RETRIES)
    parser.add_argument("--no-resume", action="store_true", help="Start fresh (ignore state file)")
    parser.add_argument("--conflicts-dir", help="Path (absolute) or name (relative to download dir) to store filename-conflict files")
    parser.add_argument("--unmatched-dir", help="Subfolder name under download dir to stash unmatched/leftover files (default: 'unmatched')")
    parser.add_argument("--index-db", default="downloads.db", help="Path to the downloads index SQLite DB")
    parser.add_argument("--rescan", action="store_true", help="Rescan download dir to rebuild index and exit")
    parser.add_argument("--organize", action="store_true", help="Organize existing files by page number (fetches page mapping from website, moves files, updates DB)")
    parser.add_argument("--no-fs-fallback", action="store_true", dest="no_fs_fallback", help="Do not scan filesystem if DB misses (fastest; requires up-to-date index)")
    parser.add_argument("--use-playwright", action="store_true", help="Use Playwright for all page fetches (requires browser_cookie3 + playwright)")
    parser.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback for 403 errors")
    parser.add_argument("--debug-http", action="store_true", help="Print extra HTTP diagnostics for 403s and errors")
    parser.add_argument("--use-proxy", action="store_true", help="Use system proxy settings for HTTP requests")
    parser.add_argument("--no-proxy", action="store_true", help="Ignore system proxy settings for HTTP requests")
    parser.add_argument("--use-httpx", action="store_true", help="Use httpx (HTTP/2) fallback before Playwright")
    parser.add_argument("--no-httpx", action="store_true", help="Disable httpx fallback (HTTP/2) for page fetches")
    args = parser.parse_args()

    # Apply CLI args to module-level globals
    # Priority: --start-url (full override) > --dataset + --start-page > defaults
    if args.start_url:
        START_URL = args.start_url
    else:
        # Apply --dataset if provided
        if args.dataset is not None:
            START_URL = f"https://www.justice.gov/epstein/doj-disclosures/data-set-{args.dataset}-files?page=0"
        
        # Then apply --start-page if provided
        if args.start_page is not None:
            if re.search(r"page=\d+", START_URL):
                START_URL = re.sub(r"page=\d+", f"page={args.start_page}", START_URL)
            else:
                START_URL = START_URL.rstrip("/?") + f"?page={args.start_page}"
            # Starting page override.
            # This ensures we start from it even if a saved state exists, but we will
            # not overwrite the state file until we surpass the saved page number.
            START_PAGE_OVERRIDE = True

    if args.end_page is not None:
        END_PAGE_NUM = args.end_page

    DOWNLOAD_DIR = args.download_dir
    STATE_FILE = args.state_file
    CSV_LOG = args.csv_log
    DRY_RUN = args.dry_run
    BROWSER_NAME = args.browser
    MAX_WORKERS = args.max_workers
    REQUEST_DELAY = args.request_delay
    NO_FS_FALLBACK = args.no_fs_fallback

    if args.use_state_file:
        ENABLE_STATE_FILE = True
    if args.no_state_file:
        ENABLE_STATE_FILE = False

    if args.use_csv_log:
        ENABLE_CSV_LOG = True
    if args.no_csv_log:
        ENABLE_CSV_LOG = False

    if args.use_db_state:
        ENABLE_DB_STATE = True
    if args.no_db_state:
        ENABLE_DB_STATE = False

    if args.use_browser_cookies:
        USE_BROWSER_COOKIES = True
    if args.no_browser_cookies:
        USE_BROWSER_COOKIES = False

    if args.use_httpx:
        DEFAULT_USE_HTTPX = True
    if args.no_httpx:
        DEFAULT_USE_HTTPX = False

    if args.use_proxy:
        USE_SYSTEM_PROXY = True
    if args.no_proxy:
        USE_SYSTEM_PROXY = False

    # Ensure download directory exists (in case it was overridden)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Configure conflicts directory (CLI override allowed)
    if args.conflicts_dir:
        if os.path.isabs(args.conflicts_dir):
            CONFLICT_DIR = args.conflicts_dir
        else:
            CONFLICT_DIR = os.path.join(DOWNLOAD_DIR, args.conflicts_dir)
        # If user explicitly configured a global conflicts directory, ensure it exists
        os.makedirs(CONFLICT_DIR, exist_ok=True)
    else:
        CONFLICT_DIR = os.path.join(DOWNLOAD_DIR, CONFLICTS_SUBDIR)

    # Initialize index DB and optionally rescan to rebuild it (useful after moving files)
    init_db(args.index_db)
    if args.rescan:
        rescan_and_rebuild_index(DOWNLOAD_DIR)
        print("Exiting after rescan as requested.")
        sys.exit(0)
    
    # Organize mode: reorganize existing files by page number
    if args.organize:
        ORGANIZE_MODE = True
        # Load cookies if needed for website scraping
        if USE_BROWSER_COOKIES:
            try:
                cookies = load_browser_cookies(BROWSER_NAME)
                if cookies:
                    session.cookies.update(cookies)
                    print(f"✅ Loaded {len(cookies)} cookies from {BROWSER_NAME}")
            except Exception as e:
                print(f"⚠️ Could not load browser cookies: {e}")
        
        organize_files()
        print("Exiting after organize as requested.")
        sys.exit(0)

    # Load indexed file metadata into memory to avoid filesystem scans
    load_index_into_memory()

    # Initialize part tracker so new downloads fill part_x folders
    init_part_tracker(DOWNLOAD_DIR)

    # Optional unmatched folder name (used by the sorter) — the downloader will
    # not automatically move leftovers into unmatched/, but the sorter uses it.
    if args.unmatched_dir:
        UNMATCHED_DIR_NAME = args.unmatched_dir
    else:
        UNMATCHED_DIR_NAME = "unmatched"


    DEFAULT_MAX_EMPTY_CONSECUTIVE = args.max_empty
    DEFAULT_MAX_FORBIDDEN_CONSECUTIVE = args.max_forbidden
    DEFAULT_SCRAPE_MAX_RETRIES = args.max_retries
    DEBUG_HTTP = args.debug_http
    session.trust_env = USE_SYSTEM_PROXY

    if args.no_playwright:
        DEFAULT_USE_PLAYWRIGHT = False

    if args.no_resume and ENABLE_STATE_FILE and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"🧹 Removed state file {STATE_FILE} (no-resume)")

    print("Configuration:")
    print(f"  start_url: {START_URL}")
    print(f"  end_page: {END_PAGE_NUM if END_PAGE_NUM is not None else 'unlimited'}")
    print(f"  download_dir: {DOWNLOAD_DIR}")
    print(f"  conflicts_dir: {CONFLICT_DIR}")
    print(f"  folder_structure: downloads/part_X/page_Y/ (conflicts: downloads/conflicts/page_Y/)")
    print(f"  max_pages_per_part: {MAX_PAGES_PER_PART}")
    print(f"  index_db: {args.index_db}")
    print(f"  no_fs_fallback: {NO_FS_FALLBACK}")
    print(f"  dry_run: {DRY_RUN}")
    print(f"  use_browser_cookies: {USE_BROWSER_COOKIES} ({BROWSER_NAME})")
    print(f"  playwright_enabled: {DEFAULT_USE_PLAYWRIGHT}")
    print(f"  max_workers: {MAX_WORKERS}")
    print(f"  request_delay: {REQUEST_DELAY}s")
    print(f"  max_empty: {DEFAULT_MAX_EMPTY_CONSECUTIVE}")
    print(f"  max_forbidden: {DEFAULT_MAX_FORBIDDEN_CONSECUTIVE}")
    print(f"  max_retries: {DEFAULT_SCRAPE_MAX_RETRIES}")
    print(f"  debug_http: {DEBUG_HTTP}")
    print(f"  use_system_proxy: {USE_SYSTEM_PROXY}")
    print(f"  httpx_enabled: {DEFAULT_USE_HTTPX}")
    print(f"  csv_log_enabled: {ENABLE_CSV_LOG}")
    print(f"  state_file_enabled: {ENABLE_STATE_FILE}")
    print(f"  db_state_enabled: {ENABLE_DB_STATE}")
    print(f"  download_timeouts: connect={DOWNLOAD_CONNECT_TIMEOUT}s read={DOWNLOAD_READ_TIMEOUT}s total={DOWNLOAD_TOTAL_TIMEOUT}s")

    main()
