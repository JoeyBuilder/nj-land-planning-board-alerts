import os
import re
import json
import time
import hashlib
import pathlib
import warnings
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")
logging.getLogger("pdfminer").setLevel(logging.ERROR)  # silence FontBBox/pdfminer noise


# =========================
# CONFIG — EDIT THESE
# =========================
TARGET_SITES = [
    {"town": "Winslow", "url": "https://winslowtownship.granicus.com/ViewPublisher.php?view_id=1"},
    {"town": "Cherry Hill", "url": "https://www.chnj.gov/AgendaCenter/Planning-Board-5"},
    {"town": "Voorhees", "url": "https://voorheesnj.com/government/boards-committees/planning-board/"},
    {"town": "Voorhees", "url": "https://voorheesnj.com/government/boards-committees/zoning-board/"},
    {"town": "Mantua", "url": "https://mantuatownship.com/departments/zoning-land-use-code-enforcement/land-use-board-agenda-and-minutes/"},
    {"town": "Mullica Hill", "url": "https://harrisontwp.us/department/joint-land-use-board/"},
    {"town": "Williamstown", "url": "https://monroetownshipnj.org/5boards-and-commissions/planning-board/meeting-agendas/"},
    {"town": "Williamstown", "url": "https://monroetownshipnj.org/meeting-agendas/"},
    {"town": "Washington Township", "url": "https://www.twp.washington.nj.us/government/boards___commissions/planning_board/agendas_and_minutes.php"},
    {"town": "Washington Township", "url": "https://www.twp.washington.nj.us/government/boards___commissions/zoning_board/agendas_and_minutes.php"},
    {"town": "West Deptford", "url": "https://www.westdeptford.com/government/meeting_agendas/planning_board.php"},
    {"town": "West Deptford", "url": "https://www.westdeptford.com/government/meeting_agendas/zoning_board.php"},
    {"town": "Deptford", "url": "https://www.deptford-nj.org/government/agendas-minutes"},
    {"town": "East Greenwich", "url": "https://www.eastgreenwichnj.com/government/planning-and-zoning"},
    {"town": "Mt. Laurel", "url": "https://www.mountlaurel.com/government/meetings/planning_board_meetings.php"},
    {"town": "Mt. Laurel", "url": "https://www.mountlaurel.com/government/meetings/zoning_board_meetings.php"},
    {"town": "Medford", "url": "https://medfordtownship.com/planning-board/"},
    {"town": "Medford Lakes", "url": "https://medfordtownship.com/zoningboard/"},
    {"town": "Evesham", "url": "https://evesham-nj.org/meetings/meeting-documents/planning-board-meetings"},
    {"town": "Evesham", "url": "https://evesham-nj.org/meetings/meeting-documents/board-of-adjustment-meetings"},
    {"town": "Berlin Boro", "url": "https://www.berlinnj.org/planning-board/"},
    {"town": "Hainesport", "url": "https://www.hainesporttownship.com/joint-land-use-board"},
    {"town": "Lumberton", "url": "https://www.lumbertontwp.com/departments-services/land-development-board/"},
    {"town": "Burlington", "url": "http://twp.burlington.nj.us/content/159/82/default.aspx"},
    {"town": "Moorestown", "url": "https://www.moorestown.nj.us/AgendaCenter/Search/?term=&CIDs=3,&startDate=3/28/2024&endDate=3/28/2025&dateRange=1%20year&dateSelector=4"},
    {"town": "Delran", "url": "https://delrantownship.org/document-category/planning-board-agendas-minutes/"},
    {"town": "Delran", "url": "https://delrantownship.org/zoning-board/"},
    {"town": "Cinnaminson", "url": "https://cinnaminsonnj.org/agendas-resolutions-minutes/"},
    {"town": "Elk Township", "url": "https://elktownshipnj.gov/boards/planning-and-zoning-board-agendas/"},
    {"town": "Elk Township", "url": "https://elktownshipnj.gov/boards/planning-and-zoning-board-minutes/"},
    {"town": "Woolwich", "url": "https://woolwichtwp.org/government/woolwich-township-minutes-agendas/"},
    {"town": "Glassboro", "url": "https://drive.google.com/drive/folders/0B-l-QWJCLVkhdW52OHpZWFhLbm8?resourcekey=0-mbjKln7-ZtBHmHijO_ZIPg"},
    {"town": "Hammonton", "url": "https://www.townofhammonton.org/land-use-board/"},
    {"town": "Southampton", "url": "https://www.southamptonnj.org/government/meetings/land_development_board_.php"},
    {"town": "Eastampton", "url": "https://www.eastampton.com/meetings?field_smart_date_value_1=&field_smart_date_end_value=&combine=&department=133&boards-commissions=All"},
    {"town": "Westampton", "url": "https://www.westamptonnj.gov/node/32/agenda"},
    {"town": "Pemberton Township", "url": "https://www.pemberton-twp.com/government/minutes_ordinances/current_year_meeting_minutes.php"},
    {"town": "Shamong", "url": "https://www.shamong.net/community_county_burlington/meetingsagendasminutes/joint_land_use_board_jlub.php#outer-764sub-771"},
    {"town": "Tabernacle", "url": "https://www.tabernacle-nj.gov/departments/land_development_board/meeting_minutes.php"},
    {"town": "Swedesboro", "url": "https://ecode360.com/SW0669/documents/Minutes#category-89893453"},
    {"town": "Swedesboro", "url": "https://ecode360.com/SW0669/documents/Agendas#category-89874773"},
]

USER_AGENT = "Mozilla/5.0 (GitHubActions PlanningBoard Monitor)"

KEYWORDS = [
    "minor subdivision",
    "major subdivision",
    "final major subdivision",
    "preliminary",
    "amended preliminary",
    "amended final",
    "resolution approving",
    "resolution deeming",
    "memorialization",
]

# NJ-style parcel pattern: Block ####(.## optional) + Lot/Lots ...
BLOCK_LOT_REGEX = re.compile(
    r"\bBlock\s*\d+(?:\.\d+)?\b[\s\S]{0,120}?\bLot(?:s)?\s*[\d][\d,\s&\-\.\(\)]*",
    re.IGNORECASE,
)

# Filter links so we don't chase irrelevant PDFs
LINK_HINTS = (
    "agenda",
    "minutes",
    "planning",
    "zoning",
    "board",
    "land use",
    "land development",
    "jlu",
    "jlub",
    "joint land use",
)

# Storage
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)
SEEN_FILE = DATA_DIR / "seen_links.json"
FAILED_FILE = DATA_DIR / "dead_links.json"

# GitHub issue creation settings
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPOSITORY")


# =========================
# Helpers
# =========================
def _load_set(path: pathlib.Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return set()


def _save_set(path: pathlib.Path, s: set[str]) -> None:
    path.write_text(json.dumps(sorted(s), indent=2), encoding="utf-8")


def load_seen() -> set[str]:
    return _load_set(SEEN_FILE)


def save_seen(seen: set[str]) -> None:
    _save_set(SEEN_FILE, seen)


def load_failed() -> set[str]:
    return _load_set(FAILED_FILE)


def save_failed(failed: set[str]) -> None:
    _save_set(FAILED_FILE, failed)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
)


def canonicalize_url(u: str) -> str:
    """
    Canonicalize without breaking sites that REQUIRE query params (e.g. Revize `t=`).
    - Normalize path encoding (avoid double-encoding)
    - Preserve query params (including `t=...`)
    - Optionally de-dupe ONLY truly safe tracking params (utm_*)
    """
    parts = urlsplit(u)

    raw_path = unquote(parts.path)
    safe_path = quote(raw_path, safe="/")

    qs = parse_qsl(parts.query, keep_blank_values=True)
    qs = [(k, v) for (k, v) in qs if not k.lower().startswith("utm_")]
    new_query = urlencode(qs, doseq=True)

    return urlunsplit((parts.scheme, parts.netloc, safe_path, new_query, parts.fragment))


def normalize_url(u: str) -> str:
    return canonicalize_url(u)


def fetch_html(url: str) -> str:
    """
    Fetch HTML with:
    - retry
    - fallback to r.jina.ai proxy when basic bot blocks occur (403/406/429)
    - DNS fallback: retry without leading www.
    """
    last_err: Optional[Exception] = None

    def via_jina(u: str) -> str:
        r = SESSION.get(f"https://r.jina.ai/{u}", timeout=60, allow_redirects=True, verify=False)
        r.raise_for_status()
        return r.text

    for attempt in range(1, 4):
        try:
            r = SESSION.get(url, timeout=40, allow_redirects=True, verify=False)
            if r.status_code in (403, 406, 429):
                return via_jina(url)
            r.raise_for_status()
            return r.text

        except requests.exceptions.ConnectionError as e:
            parts = urlsplit(url)
            if parts.netloc.startswith("www."):
                alt = urlunsplit((parts.scheme, parts.netloc[4:], parts.path, parts.query, parts.fragment))
                try:
                    r2 = SESSION.get(alt, timeout=40, allow_redirects=True, verify=False)
                    if r2.status_code in (403, 406, 429):
                        return via_jina(alt)
                    r2.raise_for_status()
                    return r2.text
                except Exception as e2:
                    last_err = e2
            else:
                last_err = e

            time.sleep(2 * attempt)

        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)

    raise RuntimeError(str(last_err) if last_err else "Unknown fetch_html error")


# -------------------------
# Revize helpers (NEW)
# -------------------------
REVIZE_SLUG_CACHE: dict[str, str] = {}  # domain -> slug


def discover_revize_slug(site_url: str) -> Optional[str]:
    """
    Find a Revize slug by looking for:
      https://cms2.revize.com/revize/<slug>/
    Cache per domain.
    """
    try:
        domain = urlsplit(site_url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]

        if domain in REVIZE_SLUG_CACHE:
            return REVIZE_SLUG_CACHE[domain]

        candidates = [
            site_url,
            f"https://{domain}/",
            f"https://www.{domain}/",
        ]

        slug_re = re.compile(r"cms2\.revize\.com/revize/([A-Za-z0-9_\-]+)/", re.IGNORECASE)

        for u in candidates:
            try:
                html = fetch_html(u)
            except Exception:
                continue

            m = slug_re.search(html)
            if m:
                slug = m.group(1)
                REVIZE_SLUG_CACHE[domain] = slug
                return slug

        return None
    except Exception:
        return None


def build_revize_pdf_url(original_pdf_url: str, slug: str) -> str:
    """
    Rewrite pretty town pdf URL to Revize CMS:
      https://cms2.revize.com/revize/<slug>/<filename>.pdf?...query...
    """
    parts = urlsplit(original_pdf_url)
    filename = parts.path.split("/")[-1] or "document.pdf"
    return urlunsplit(("https", "cms2.revize.com", f"/revize/{slug}/{filename}", parts.query, ""))


# -------------------------
# Link extraction helpers
# -------------------------
def looks_like_board_doc(a_tag) -> bool:
    txt = " ".join(a_tag.stripped_strings).lower()
    if any(h in txt for h in LINK_HINTS):
        return True
    for attr in ("title", "aria-label"):
        v = (a_tag.get(attr) or "").lower()
        if any(h in v for h in LINK_HINTS):
            return True
    return False


def extract_agendacenter_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = requests.compat.urljoin(base_url, href)
        full = normalize_url(full)
        low = full.lower()

        if "/agendacenter/viewfile/" in low:
            if any(seg in low for seg in ("/agenda/", "/minutes/", "/packet/")):
                links.append(full)
                continue

        if "agendacenter" in low and "viewfile" in low and any(k in low for k in ("agenda", "minutes", "packet")):
            links.append(full)

    out: list[str] = []
    seen: set[str] = set()
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)
    return out


def extract_pdf_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    if "/agendacenter/" in base_url.lower():
        links.extend(extract_agendacenter_links(html, base_url))

    for a in soup.find_all("a", href=True):
        if not looks_like_board_doc(a):
            continue

        href = a["href"].strip()
        full = requests.compat.urljoin(base_url, href)
        full = normalize_url(full)

        low = full.lower()
        path = urlsplit(full).path.lower()

        # Granicus agenda viewer links (not direct PDFs, but still useful)
        if "agendaviewer.php" in low:
            links.append(full)
            continue

        # AgendaCenter ViewFile links (often PDF response even without .pdf)
        if "/agendacenter/viewfile/" in low and any(seg in low for seg in ("/agenda/", "/minutes/", "/packet/")):
            links.append(full)
            continue

        # Direct PDFs only
        if path.endswith(".pdf"):
            doccenter_markers = ("documentcenter", "document_center", "document%20center", "document center")
            if any(m in path for m in doccenter_markers):
                continue
            links.append(full)

    out: list[str] = []
    seen: set[str] = set()
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)
    return out


def is_viewer_page(url: str) -> bool:
    low = url.lower()
    path = urlsplit(url).path.lower()

    if path.endswith(".pdf"):
        return False
    if "/agendacenter/viewfile/" in low:
        return False

    return (path.endswith(".php") or "agendaviewer.php" in low or "viewpublisher.php" in low)


def resolve_viewer_to_pdfs(viewer_url: str) -> list[str]:
    html = fetch_html(viewer_url)
    soup = BeautifulSoup(html, "html.parser")

    found: list[str] = []

    def add(u: str) -> None:
        u = normalize_url(u)
        if urlsplit(u).path.lower().endswith(".pdf"):
            found.append(u)
            return
        low = u.lower()
        if "/agendacenter/viewfile/" in low and any(seg in low for seg in ("/agenda/", "/minutes/", "/packet/")):
            found.append(u)

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = requests.compat.urljoin(viewer_url, href)
        add(full)

    for tag in soup.find_all(["iframe", "embed"], src=True):
        src = (tag.get("src") or "").strip()
        if not src:
            continue
        full = requests.compat.urljoin(viewer_url, src)
        add(full)

    # De-dupe
    out: list[str] = []
    seen_local: set[str] = set()
    for u in found:
        if u not in seen_local:
            out.append(u)
            seen_local.add(u)

    # Light filter (optional)
    filtered: list[str] = []
    for u in out:
        ulow = u.lower()
        if any(h in ulow for h in LINK_HINTS) or "agendacenter/viewfile" in ulow:
            filtered.append(u)

    return filtered or out


# -------------------------
# Download PDF (UPDATED)
# -------------------------
def download_pdf(url: str, referer: str | None = None) -> pathlib.Path:
    url = canonicalize_url(url)

    pdf_name = f"{sha1(url)}.pdf"
    path = DATA_DIR / pdf_name
    if path.exists():
        return path

    candidates = [url]
    parts = urlsplit(url)

    # try without query
    no_query = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    if no_query != url:
        candidates.append(no_query)

    # try with/without www
    if parts.netloc and not parts.netloc.startswith("www."):
        www = urlunsplit((parts.scheme, "www." + parts.netloc, parts.path, parts.query, parts.fragment))
        candidates.append(canonicalize_url(www))
    elif parts.netloc.startswith("www."):
        bare = urlunsplit((parts.scheme, parts.netloc[4:], parts.path, parts.query, parts.fragment))
        candidates.append(canonicalize_url(bare))

    # De-dupe
    deduped = []
    seen_c = set()
    for c in candidates:
        c = canonicalize_url(c)
        if c not in seen_c:
            deduped.append(c)
            seen_c.add(c)
    candidates = deduped

    last_err: Optional[Exception] = None

    def try_candidates(url_list: list[str]) -> tuple[Optional[pathlib.Path], bool]:
        """
        Returns (saved_path_or_none, saw_non_404)
        """
        nonlocal last_err

        saw_non_404 = False
        for cand in url_list:
            try:
                headers = {
                    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
                }
                if referer:
                    headers["Referer"] = referer

                r = SESSION.get(
                    cand,
                    timeout=90,
                    allow_redirects=True,
                    verify=False,
                    headers=headers,
                    stream=True,
                )

                if r.status_code == 404:
                    # record something so we never end as "Unknown download_pdf error"
                    last_err = RuntimeError(f"404 Not Found: {cand}")
                    continue

                saw_non_404 = True
                r.raise_for_status()

                content = r.content
                ctype = (r.headers.get("Content-Type") or "").lower()

                if ("pdf" not in ctype) and (not content.startswith(b"%PDF")):
                    raise RuntimeError(f"Response did not look like a PDF (content-type={ctype})")

                path.write_bytes(content)
                return path, saw_non_404

            except Exception as e:
                last_err = e

        return None, saw_non_404

    # First attempt normal candidates
    saved, saw_non_404 = try_candidates(candidates)
    if saved:
        return saved

    # If everything 404'd, try Revize rewrite (auto-discover slug)
    if not saw_non_404:
        slug = discover_revize_slug(referer or url)
        if slug:
            revize_url = build_revize_pdf_url(url, slug)
            saved2, saw_non_404_2 = try_candidates([revize_url])
            if saved2:
                return saved2

            if (not saw_non_404_2) and last_err is None:
                last_err = RuntimeError("All download candidates returned 404 Not Found (including Revize rewrite)")

        raise RuntimeError(str(last_err) if last_err else "All download candidates returned 404 Not Found")

    # Non-404 failures (403/timeout/bad content-type/etc)
    raise RuntimeError(f"Failed to download PDF: {last_err}" if last_err else "Failed to download PDF")


# -------------------------
# PDF parsing & analysis
# -------------------------
def extract_text(pdf_path: pathlib.Path) -> str:
    parts: list[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


def analyze_text(text: str) -> dict:
    lower = text.lower()
    keyword_hits = [k for k in KEYWORDS if k in lower]

    snippets: list[str] = []
    for m in re.finditer(BLOCK_LOT_REGEX, text):
        snippet = m.group(0).replace("\n", " ").strip()
        snippets.append(snippet[:240])

    relevant = bool(keyword_hits) and bool(snippets)

    return {
        "relevant": relevant,
        "keyword_hits": keyword_hits[:15],
        "block_lot_snippets": snippets[:20],
    }


def create_github_issue(title: str, body: str) -> None:
    if not (GITHUB_TOKEN and REPO):
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPOSITORY environment variables")

    url = f"https://api.github.com/repos/{REPO}/issues"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    payload = {"title": title, "body": body}

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


# =========================
# Main
# =========================
def main():
    seen = load_seen()
    failed = load_failed()
    new_relevant_hits = []

    for site in TARGET_SITES:
        town = site["town"]
        page_url = site["url"]

        try:
            html = fetch_html(page_url)
        except Exception as e:
            print(f"[ERROR] Fetch page failed: {town} {page_url} -> {e}")
            continue

        pdf_links = extract_pdf_links(html, page_url)[:20]
        print(f"[INFO] {town}: found {len(pdf_links)} pdf link(s)")

        for link in pdf_links:
            if link in seen or link in failed:
                continue

            candidate_pdfs = [link]
            if is_viewer_page(link):
                try:
                    resolved = resolve_viewer_to_pdfs(link)
                    if resolved:
                        candidate_pdfs = resolved
                    else:
                        # Don't treat viewer as permanently dead if it just had no PDFs today
                        continue
                except Exception as e:
                    print(f"[ERROR] Viewer resolve failed: {town} {link} -> {e}")
                    continue

            for pdf_url in candidate_pdfs:
                if pdf_url in seen or pdf_url in failed:
                    continue

                try:
                    pdf_path = download_pdf(pdf_url, referer=page_url)
                    seen.add(pdf_url)  # only after successful download

                    text = extract_text(pdf_path)
                    result = analyze_text(text)

                    if result["relevant"]:
                        new_relevant_hits.append(
                            {
                                "town": town,
                                "source_page": page_url,
                                "pdf_url": pdf_url,
                                "keyword_hits": result["keyword_hits"],
                                "block_lot_snippets": result["block_lot_snippets"],
                            }
                        )

                except Exception as e:
                    msg = str(e).lower()

                    # Only mark dead on confirmed "all 404" failures
                    if "all download candidates returned 404" in msg or msg.startswith("404 not found"):
                        failed.add(pdf_url)

                    # Still log, but you won’t keep retrying true dead links forever
                    print(f"[ERROR] PDF process failed: {town} {pdf_url} -> {e}")

    save_seen(seen)
    save_failed(failed)

    if not new_relevant_hits:
        print("[INFO] No new relevant subdivision docs.")
        return

    lines = ["New subdivision-related document(s) detected.\n"]

    for hit in new_relevant_hits:
        lines.append(f"**Town:** {hit['town']}")
        lines.append(f"**Source page:** {hit['source_page']}")
        lines.append(f"**PDF:** {hit['pdf_url']}")
        if hit["keyword_hits"]:
            lines.append(f"**Keywords:** {', '.join(hit['keyword_hits'])}")
        if hit["block_lot_snippets"]:
            lines.append("**Block/Lot snippets:**")
            for s in hit["block_lot_snippets"]:
                lines.append(f"- {s}")
        lines.append("")

    body = "\n".join(lines)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    title = f"Subdivision Alert ({today}): {len(new_relevant_hits)} new document(s)"

    create_github_issue(title=title, body=body)
    print("[INFO] GitHub Issue created.")


if __name__ == "__main__":
    main()
