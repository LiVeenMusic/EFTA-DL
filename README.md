# EFTA Releases Downloader

A resumable web scraper for downloading the Department of Justice Epstein Files (EFTA) publicly available datasets from justice.gov. Features multi-threaded downloads, deduplication, state tracking, and intelligent retry logic with multiple fetch strategies.

## Overview

This tool automates bulk downloading of DOJ disclosure files with:
- **Resilient downloading** — Handles network interruptions, retries with exponential backoff
- **Database tracking** — SQLite index prevents re-downloading and tracks file metadata
- **Deduplication** — Detects and handles duplicate filenames + contents across datasets
- **Smart organization** — Auto-organizes files by dataset page number into part folders
- **Multiple fetch strategies** — httpx (HTTP/2), Playwright, or requests fallbacks
- **Resume capability** — Stop and resume where you left off without data loss
- **Detects altered files** — Manages filename collisions intelligently, effectively handling files that have the same filename and same contents (duplicates), as well as files that have the same filename but different contents (redactions/modifications)
- **Creates comparison reports for conflicts** — On filename conflicts with different contents, the script can generate a readable compare PDF and a visual side-by-side compare PDF for PDFs
- **Detects missing files** — Uses the database to keep track of which filenames are supposed to be in specific page=x URLs, and when running with --organize, will move any missing files into an "unmatched" folder. (MIGHT HAVE ISSUES. ALWAYS BACKUP!)
- **Safe cancellation** — Doing CTRL + C while the script is running will not fully terminate immediately. Instead, it will finish the current page, and then safely exit so that you don't end up with corrupted files.
- **Default dataset is 12** — Yes, this is a feature. Dataset 12 is tiny, so if you make a mistake, it won't cost you ages.

## Prerequisites

- Python 3.8+
- pip package manager (if you can run "pip" in a terminal, you're good to go)

## Installation

Note: Step 1 and 2 are only needed to run 1 time. 

Step 3 might have to be run when switching between different datasets, though your mileage may vary. The reason for this is likely related to how the script loads things into RAM, and has been causing non-stop issues for me until I started rescanning between datasets.

**Make sure that each dataset has it's own folder! i.e dataset 9 would be it's own folder containing a downloads folder and a download.py script (remember --rescan if you make a new dataset folder, to create the database). Using the same folder for multiple datasets might work, but is not something I have planned for.**

### Step 0: Open a terminal
In Windows, when you have a certain folder open, you can simply click the top address bar (so that it becomes a text field), type "cmd" (without quotes) and hit Enter, and it should open a terminal window inside that folder. 

This is usually better than running scripts by double-clicking, as it means finished tasks won't fully close the terminal window.

### Step 1: Install Required Packages

```bash
pip install -U pip
pip install requests bs4 playwright httpx httpx[http2] 'httpx[cli]' pypdf PyMuPDF
```

Notes:
- `pypdf` is used to extract page text for page-aware compare reports.
- `PyMuPDF` is used to build the visual side-by-side comparison PDFs.
- If those optional packages are missing, normal downloading still works, but PDF comparison output will be reduced or skipped.

### Step 2: Install Playwright Browser

```bash
playwright install
playwright install-deps
```

This downloads and configures the Chromium browser for Playwright (used as fallback for anti-bot protection). The second command may not be necessary for your usecase, though it is when running in a Linux CLI.

### Step 3: Initialize a new Database Index

```bash
python download.py --rescan
```

This scans your download directory and builds the SQLite index (`downloads.db`). Only needed if you already have downloaded files.

## Quick Start

### Download Dataset 12 (Default)

```bash
python download.py
```

### Download a Specific Dataset

Dataset numbers refer to the URL structure (`data-set-{N}-files`):

```bash
python download.py --dataset 9
```

### Resume from a Specific Page

```bash
python download.py --dataset 9 --start-page 9210
```

The script will resume from page 9210. Use `--no-resume` or `--start-page 0` to ignore saved state and start from scratch.

### Bulk Download with High Concurrency

```bash
python download.py --dataset 9 --max-workers 50 --request-delay 0.1
```

- `--max-workers 50` — Download 50 files in parallel (be careful. Once the database and folders get populated with tens of thousands of files, it can get slower to respond)
- `--request-delay 0.1` — Wait 0.1 seconds between page fetches (Again, be careful. Most useful for organize runs, as those do not download files, only fetch pages).

### Organize Existing Files

If files are scattered in your download directory, reorganize them by page number:

```bash
python download.py --organize
```

This crawls the website to map filenames to pages, then moves local files into the correct folder structure.

Highly recommmend avoiding having to do this, as I have not been able to fully verify that it is working properly. 

(*NOTE: ALWAYS MAKE SURE TO RESCAN FIRST, IF THERE ISN'T ALREADY A .DB FILE*)

## Conflict Handling And Compare Reports

When the scraper encounters the same filename again, it handles it in three different ways:

1. **Same filename + same content**
    The file is treated as a duplicate and skipped.

2. **Same filename + different content**
    The new file is saved into the `downloads/conflicts/page_X/` folder.

3. **Conflict reports for changed PDFs and files**
    The script also generates helper comparison files next to the conflict file.

Generated files typically look like this:

```text
downloads/conflicts/page_123/
├── EFTA01234567.pdf
├── EFTA01234567_compare.pdf
└── EFTA01234567_visual_compare.pdf
```

What each file means:
- `filename.pdf` — The newly downloaded conflicting file.
- `filename_compare.pdf` — A text-based comparison report.
- `filename_visual_compare.pdf` — A visual side-by-side comparison for PDF conflicts.

The normal compare PDF now includes:
- Color-coded diff lines
- Red for removed lines
- Green for added lines
- Blue hunk headers
- Separate report pages for each changed source PDF page
- Page headers like `Page 17 changes` so you can jump directly to the page with changes

The visual compare PDF shows:
- Original PDF on one side
- Conflict PDF on the other side
- A quick visual way to inspect redactions, formatting changes, and page-level differences

## CLI Arguments Reference

Note: I have set up the default options to work well here, assuming you have completed step 0-3 of the setup! (if you get 403 errors, your IP is either blocked, or you are missing httpx/2).

### Basic Options

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--dataset` | int | 10 | Dataset number (e.g., `9`, `10`, `11`) |
| `--start-url` | string | auto | Full URL override (ignores `--dataset` and `--start-page`) |
| `--start-page` | int | 0 | Start page number (constructs URL automatically) |
| `--end-page` | int | unlimited | Stop crawling after reaching this page |
| `--download-dir` | path | `downloads/` | Where to save files |
| `--dry-run` | flag | off | Don't actually download—just simulate |

### State & Resume

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--no-resume` | flag | off | Ignore saved state and start from scratch |
| `--state-file` | path | `crawl_state.json` | File to store resume position |
| `--use-state-file` | flag | off | Enable crawl state JSON (off by default) |
| `--no-state-file` | flag | off | Disable crawl state JSON |
| `--use-db-state` | flag | on | Persist state in SQLite database |
| `--no-db-state` | flag | off | Disable database state persistence |

### Database & Indexing

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--index-db` | path | `downloads.db` | SQLite database for index & file tracking |
| `--rescan` | flag | off | Rescan download folder to rebuild DB index (then exits) |
| `--no-fs-fallback` | flag | off | Don't scan filesystem if DB lookup fails (faster, requires up-to-date index) |

### File Organization

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--organize` | flag | off | Reorganize files by page number (crawls site, moves files, updates DB) |
| `--conflicts-dir` | path | `downloads/conflicts/` | Where to move files with duplicate names |
| `--unmatched-dir` | string | `unmatched` | Subfolder name for files not found in crawl |

### Logging

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--csv-log` | path | `downloads.csv` | CSV log file path |
| `--use-csv-log` | flag | off | Enable CSV logging (disabled by default) |
| `--no-csv-log` | flag | off | Disable CSV logging |
| `--debug-http` | flag | off | Print HTTP diagnostics (useful for troubleshooting 403 errors) |

### Performance & Networking

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--max-workers` | int | 10 | Number of parallel downloads |
| `--request-delay` | float | 0.4 | Seconds to wait between page fetches |
| `--max-empty` | int | 50 | Stop after this many consecutive empty pages |
| `--max-forbidden` | int | 3 | Stop after this many consecutive 403 errors |
| `--max-retries` | int | 3 | Retry attempts for failed page fetches |
| `--use-proxy` | flag | on | Use system proxy settings |
| `--no-proxy` | flag | off | Ignore system proxy settings |

### Fetch Strategies

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--use-httpx` | flag | on | Use httpx (HTTP/2) for page fetches |
| `--no-httpx` | flag | off | Disable httpx (use requests only) |
| `--use-playwright` | flag | off | Use Playwright for all fetches (browser-based, slower but bypasses anti-bot) |
| `--no-playwright` | flag | off | Disable Playwright fallback on 403 errors |

### Browser Cookies

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--use-browser-cookies` | flag | off | Import cookies from local browser profile |
| `--no-browser-cookies` | flag | on | Don't import browser cookies |
| `--browser` | string | `firefox` | Browser to import cookies from (`firefox`, `chrome`, `brave`) |

## Folder Structure

Files are organized automatically as follows:

```
downloads/
├── part_1/
│   ├── page_0/
│   │   ├── file1.pdf
│   │   └── file2.pdf
│   └── page_1/
│       └── file3.pdf
├── part_2/
│   └── page_100/
│       └── file4.pdf
└── conflicts/
    └── page_0/
        ├── duplicate_name.pdf
        ├── duplicate_name_compare.pdf
        └── duplicate_name_visual_compare.pdf
```

- **part_X folders** — Organize pages into parts (1000 pages per part by default)
- **page_Y folders** — Each page's files in its own folder
- **conflicts/** — Files with duplicate names but different contents go here, along with generated comparison reports

## Common Workflows

### 1. Download Dataset 9 with High Speed

```bash
python download.py --dataset 9 --max-workers 50 --request-delay 0.1
```

### 2. Download Dataset 11 Up to Page 11,000

```bash
python download.py --dataset 11 --max-workers 30 --request-delay 0.2 --end-page 11000
```

### 3. Download Dataset 9 starting from a Specific Page

```bash
python download.py --dataset 9 --start-page 7520 --max-workers 40
```

### 4. Troubleshoot 403 or Network Errors

```bash
python download.py --debug-http --no-httpx --use-playwright --max-retries 5
```

This will:
- Print HTTP diagnostics
- Use Playwright (real browser) instead of httpx
- Retry up to 5 times on failures

### 5. Organize Existing Downloads

```bash
python download.py --organize
```

Crawls the website to determine each file's correct page, then moves files to their proper locations and updates the database. 

Will always choose the lowest found page number for a given file, to counter duplicates. 

If a file already has an assigned page=x number in the database, then it will keep that number, even if it becomes unmatched, which means that if you scan up to page 50, and the file is in page 51, it will not move into an unmatched folder unless you scan page 51 and the file is found to be missing.

Again, be careful with `--organize`. Make sure to back up your folder structure, just in case something goes wrong.

### 6. Force Rebuild of Database Index

```bash
python download.py --rescan
```

Useful if you manually moved files or you are starting a new download. Creates/Updates the .db file with any files found within the `downloads` folder, assigning them numbers if they already have the folder structure for it, and assigning them `unmatched` if they don't.

A `--rescan` is always necessary whenever the .db database is missing. (note: if you use arguments like `--max-workers` etc, you can simply add `--rescan` at the end. It'll still work like any normal rescan.)

## State Tracking

The script supports two methods to track progress and avoid re-downloading:

### SQLite Database (`downloads.db`)
- Tracks every downloaded file with hash, size, page number, and status
- Persistent across runs by default
- Used for fast duplicate detection
- Enabled with `--use-db-state` (default: **on**)

### JSON State File (`crawl_state.json`)
- Stores the current URL and last page fetched
- Allows resuming at exact position
- Disabled by default to avoid confusion with old state
- Enable with `--use-state-file` if you want JSON-based resume.
- Becomes slow far quicker than the SQLite database, but might be useful to have as a backup if you have an extremely fast storage drive and CPU.

## Exit Behavior

The script stops in these conditions:

1. **End of dataset** — Final page reached with no more links
2. **`--end-page` reached** — Stops after fetching the specified page
3. **Ctrl+C (graceful)** — Finishes current page, saves state, exits cleanly
4. **Consecutive empty pages** — `--max-empty` pages with no files found (default: 50). 

Due to the current way the EFTA releases work, this is actually impossible, and if you encounter this, I suggest that you make a note of the last successful page shown in the terminal, and restart from that point onwards with `--start-page`


5. **Consecutive 403 errors** — `--max-forbidden` forbidden responses (default: 3)

If you're having this issue, make sure to verify that you did steps 0-2 properly. This issue is usually resolved when HTTPX/2 is properly installed.

## Troubleshooting

### "403 Forbidden" or "Access Denied"

The justice.gov servers have anti-bot protection. Try:

```bash
python download.py --debug-http --use-playwright --max-retries 5
```

Playwright uses a real browser, bypassing most detection.

### Slow Downloads

Increase parallelism and reduce delays:

```bash
python download.py --max-workers 50 --request-delay 0.1
```

Try to avoid the server throttling you. `--max-workers` set to 10 up to 25 is reasonable, though 50 has worked for me personally.

### Database Corruption or Out-of-Sync

Rebuild the index:

```bash
python download.py --rescan
```

### Compare PDFs Are Missing

If conflict files are being saved but compare PDFs are not showing up, check these packages:

```bash
pip install pypdf PyMuPDF
```

Notes:
- Missing `pypdf` can prevent page-aware PDF text extraction for the normal compare report.
- Missing `PyMuPDF` can prevent the visual compare PDF from being created.

### Import Certificate Errors (Playwright)

Ensure you ran:

```bash
playwright install-deps
```

This installs system-level dependencies needed by Playwright.

### Files in Wrong Locations

Use organize mode to fix folder structure:

```bash
python download.py --organize
```

## Performance Tips

1. **Use httpx (HTTP/2)** — Faster than requests; enabled by default
2. **Increase `--max-workers`** — 30-50 workers is reasonable for bulk downloads
3. **Reduce `--request-delay`** — 0.05-0.1 seconds is fast but respectful
4. **Use `--no-fs-fallback`** once index is built — Skips filesystem scans
5. **Monitor with `--debug-http`** — Identify bottlenecks and errors

## File Metadata

Every downloaded file is tracked in `downloads.db` with:
- Filename
- File path
- SHA-256 hash
- Page number if applicable
- Status (downloaded, duplicate, conflict, unmatched)
- Timestamp

Use the database to:
- Detect duplicates/redactions in the dataset releases (running a normal download run from page 0 after the first time will always detect and download changed/redacted files encountered)
- Track file locations
- Resume intelligently without filesystem scans

## License & Attribution

This tool downloads publicly available DOJ disclosures. See [justice.gov](https://www.justice.gov/epstein) for source and licensing information.

## Support

For issues, questions, or improvements, check the script logs (if used) and use `--debug-http` to diagnose problems.

---

**Last Updated:** March 2026
