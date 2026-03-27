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
    {"town": "Cherry Hill", "url": "https://www.chnj.gov/113/Planning-Board"},
    {"town": "Cherry Hill", "url": "https://www.chnj.gov/117/Zoning-Board-of-Adjustment"},
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
    {"town": "Medford", "url": "https://ecode360.com/ME0295/documents/Zoning_Agendas"},
    {"town": "Medford", "url": "https://ecode360.com/ME0295/documents/Planning_Agendas"},
    {"town": "Medford Lakes", "url": "https://www.medfordlakes.com/AgendaCenter/Borough-Council-2"},
    {"town": "Evesham", "url": "https://evesham-nj.org/meetings/meeting-documents/planning-board-meetings/2026-planning-board-meeting-documents/2026-agendas-planning-board"},
    {"town": "Evesham", "url": "https://evesham-nj.org/meetings/meeting-documents/board-of-adjustment-meetings/2026-zoning-board-of-adjustment-meeting-documents/2026-agendas-zoning-board"},
    {"town": "Berlin Boro", "url": "https://www.berlinnj.org/planning-board/"},
    {"town": "Hainesport", "url": "https://www.hainesporttownship.com/node/20/agenda/2026"},
    {"town": "Lumberton", "url": "https://ecode360.com/LU1362/documents/Agendas#category-311119646"},
    {"town": "Burlington", "url": "https://twp.burlington.nj.us/content/159/82/default.aspx"},
    {"town": "Moorestown", "url": "https://www.moorestown.nj.us/AgendaCenter/Planning-Board-Meeting-Notices-Agendas-3/?"},
    {"town": "Moorestown", "url": "https://www.moorestown.nj.us/AgendaCenter/Zoning-Board-of-Adjustment-Meeting-Notic-4/?"},
    {"town": "Delran", "url": "https://delrantownship.org/document-category/planning-board-agendas-minutes/"},
    {"town": "Delran", "url": "https://delrantownship.org/zoning-board/"},
    {"town": "Cinnaminson", "url": "https://cinnaminsonnj.org/agendas-resolutions-minutes/"},
    {"town": "Elk Township", "url": "https://elktownshipnj.gov/boards/planning-and-zoning-board-agendas/"},
    {"town": "Elk Township", "url": "https://elktownshipnj.gov/boards/planning-and-zoning-board-minutes/"},
    {"town": "Woolwich", "url": "https://woolwichtwp.org/government/woolwich-township-minutes-agendas/"},
    {"town": "Glassboro", "url": "https://drive.google.com/drive/folders/1hDiaWBWSzM8mxb_rLPYdDWaatzgt9cI4?usp=drive_link"},
    {"town": "Hammonton", "url": "https://www.townofhammonton.org/land-use-board/"},
    {"town": "Southampton", "url": "https://www.southamptonnj.org/government/meetings/land_development_board_.php"},
    {"town": "Eastampton", "url": "https://www.eastampton.com/meetings/recent?boards-commissions=All&combine=&department=133&field_smart_date_end_value=&field_smart_date_value_1="},
    {"town": "Westampton", "url": "https://www.westamptonnj.gov/node/32/agenda/2026"},
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

BLOCK_LOT_REGEX = re.compile(
    r"\bBlock\s*\d+(?:\.\d+)?\b[\s\S]{0,120}?\bLot(?:s)?\s*[\d][\d,\s&\-\.\(\)]*",
    re.IGNORECASE,
)

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

BOARD_RELEVANCE_HINTS = (
    "planning",
    "zoning",
    "land use",
    "land development",
    "board",
    "agenda",
    "minutes",
    "resolution",
    "hearing",
    "application",
    "subdivision",
)

UNRELATED_DOC_HINTS = (
    "vital statistics",
    "police",
    "parks",
    "tax",
    "tax collector",
    "brush pickup",
    "opra",
    "recycling",
    "clerk",
    "animal control",
    "finance",
    "court",
    "fire",
    "ems",
)

# =========================
# Residential-only filter
# =========================
RESIDENTIAL_USE_KEYWORDS = [
    "residential", "single family", "single-family", "sf", "sfd",
    "townhouse", "town home", "multifamily", "multi-family",
    "duplex", "triplex", "quad", "apartments", "apartment",
    "condominium", "condo", "age-restricted", "55+", "senior housing",
    "cluster", "residential subdivision", "minor subdivision", "major subdivision",
    "creating", "new lot", "new lots", "building lot", "building lots",
    "infill", "dwelling", "dwelling unit", "dwelling units",
]

RESIDENTIAL_ZONE_HINTS = [
    "r-1", "r-2", "r-3", "r-4", "r-5", "rr", "res",
    "rm", "rh", "mf", "mfr", "prd", "pud", "cluster",
]

COMMERCIAL_USE_KEYWORDS = [
    "commercial", "retail", "shopping", "store", "tenant", "lease", "leased",
    "restaurant", "diner", "drive-thru", "drive thru", "fast food",
    "wawa", "7-eleven", "dunkin", "starbucks",
    "office", "medical office", "clinic", "urgent care", "bank",
    "warehouse", "distribution", "logistics", "industrial", "manufacturing",
    "self storage", "self-storage", "storage facility",
    "hotel", "motel", "gas station", "fuel", "convenience store",
    "auto", "dealership", "car wash", "oil change",
    "sign", "signage", "freestanding sign",
    "site plan",
]

COMMERCIAL_ZONE_HINTS = [
    "c-1", "c-2", "c-3", "c-4", "c1", "c2", "c3", "c4",
    "hc", "highway commercial", "cc",
    "i-1", "i-2", "i-3", "i1", "i2", "i3",
    "li", "hi", "industrial", "ip", "bp", "business park",
    "m-1", "m-2", "m1", "m2",
]

ALLOW_MIXED_USE = False  # keep residential-only by default

# =========================
# Applicant extraction (NEW)
# =========================
# This is heuristic: agendas/resolutions vary, so we try multiple patterns.
APPLICANT_PATTERNS: list[re.Pattern] = [
    # Applicant: John Doe / Applicant - John Doe
    re.compile(r"\bApplicant\s*[:\-]\s*(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})", re.IGNORECASE),
    # Applicant/Owner: ...
    re.compile(r"\bApplicant\s*/\s*Owner\s*[:\-]\s*(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})", re.IGNORECASE),
    # Owner: ...
    re.compile(r"\bOwner\s*[:\-]\s*(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})", re.IGNORECASE),
    # Developer: ...
    re.compile(r"\bDeveloper\s*[:\-]\s*(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})", re.IGNORECASE),
    # Applicant is XYZ / Applicant(s) are XYZ
    re.compile(r"\bApplicant(?:s)?\s+(?:is|are)\s+(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})", re.IGNORECASE),
    # "ABC LLC" (Applicant) - in parentheses
    re.compile(r"(?P<name>[A-Z][A-Za-z0-9&.,'’\-\s]{2,80})\s*\(\s*Applicant\s*\)", re.IGNORECASE),
]

# common suffixes to keep; we also trim trailing junk.
_APPLICANT_CLEAN_RE = re.compile(r"[\s,;:\-]+$")


# Storage
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)
SEEN_FILE = DATA_DIR / "seen_links.json"
SEEN_DOCS_FILE = DATA_DIR / "seen_docs.json"
FAILED_FILE = DATA_DIR / "dead_links.json"

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

def load_seen_docs() -> set[str]:
    return _load_set(SEEN_DOCS_FILE)

def save_seen_docs(seen_docs: set[str]) -> None:
    _save_set(SEEN_DOCS_FILE, seen_docs)

def load_failed() -> set[str]:
    return _load_set(FAILED_FILE)


def save_failed(failed: set[str]) -> None:
    _save_set(FAILED_FILE, failed)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def canonicalize_url(u: str) -> str:
    parts = urlsplit(u)
    raw_path = unquote(parts.path)
    safe_path = quote(raw_path, safe="/")
    qs = parse_qsl(parts.query, keep_blank_values=True)
    qs = [(k, v) for (k, v) in qs if not k.lower().startswith("utm_")]
    new_query = urlencode(qs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, safe_path, new_query, parts.fragment))


def normalize_url(u: str) -> str:
    return canonicalize_url(u)


def build_url_fingerprints(u: str) -> set[str]:
    normalized = canonicalize_url(u)
    parts = urlsplit(normalized)
    without_query = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return {
        f"url:{normalized}",
        f"url_noquery:{without_query}",
    }

def normalize_text_for_fingerprint(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def build_doc_fingerprint(text: str, pdf_path: pathlib.Path) -> str:
    normalized_text = normalize_text_for_fingerprint(text)
    if normalized_text:
        return f"text:{sha1(normalized_text)}"
    return f"file:{hashlib.sha1(pdf_path.read_bytes()).hexdigest()}"


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


def fetch_html(url: str) -> str:
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
# Revize helpers
# -------------------------
REVIZE_SLUG_CACHE: dict[str, str] = {}


def discover_revize_slug(site_url: str) -> Optional[str]:
    try:
        domain = urlsplit(site_url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]

        if domain in REVIZE_SLUG_CACHE:
            return REVIZE_SLUG_CACHE[domain]

        candidates = [site_url, f"https://{domain}/", f"https://www.{domain}/"]
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

def _extract_anchor_context_text(a_tag) -> str:
    parts: list[str] = []
    txt = " ".join(a_tag.stripped_strings).strip()
    if txt:
        parts.append(txt)
    for attr in ("title", "aria-label"):
        v = (a_tag.get(attr) or "").strip()
        if v:
            parts.append(v)

    parent = a_tag.parent
    if parent:
        ptxt = " ".join(parent.stripped_strings).strip()
        if ptxt:
            parts.append(ptxt[:220])

    prev_heading = a_tag.find_previous(["h1", "h2", "h3", "h4", "h5"])
    if prev_heading:
        htxt = " ".join(prev_heading.stripped_strings).strip()
        if htxt:
            parts.append(htxt)

    return " ".join(parts).lower()


def is_board_relevant_link(url: str, context_text: str) -> bool:
    low = f"{url.lower()} {context_text.lower()}"
    return any(k in low for k in BOARD_RELEVANCE_HINTS)


def looks_unrelated_doc(url: str, context_text: str) -> bool:
    low = f"{url.lower()} {context_text.lower()}"
    return any(k in low for k in UNRELATED_DOC_HINTS)



def is_pdf_source_url(url: str) -> bool:
    low = url.lower()
    path = urlsplit(url).path.lower()
    return (
        path.endswith(".pdf")
        or "/documentcenter/view/" in low
        or "/agendacenter/viewfile/" in low
    )

def is_selector_filter_page(html: str, base_url: str) -> bool:
    low_url = base_url.lower()
    soup = BeautifulSoup(html, "html.parser")

    if any(k in low_url for k in ("recent?", "filter", "department=", "category=", "documents")):
        return True
    if soup.find("form") and soup.find(["select", "input"]):
        return True
    if soup.find(["select", "option"]):
        return True
    if soup.find(attrs={"data-filter": True}) or soup.find(attrs={"data-category": True}):
        return True
    return False


def collect_link_debug_info(html: str, base_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    anchors = soup.find_all("a")
    anchor_hrefs = [a.get("href", "").strip() for a in anchors if a.get("href")]
    raw_hrefs = re.findall(r"""href\s*=\s*["']?([^"' >]+)""", html, flags=re.IGNORECASE)

    candidate_hrefs: list[str] = []
    for href in anchor_hrefs + raw_hrefs:
        if not href:
            continue
        full = normalize_url(requests.compat.urljoin(base_url, href))
        candidate_hrefs.append(full)

    first_candidates = list(dict.fromkeys(candidate_hrefs))[:20]
    pdf_like_count = sum(1 for h in candidate_hrefs if is_pdf_source_url(h))
    filtered_by_link_hints: list[str] = []
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = normalize_url(requests.compat.urljoin(base_url, href))
        if not is_pdf_source_url(full):
            continue
        if not looks_like_board_doc(a):
            filtered_by_link_hints.append(full)
            
    return {
        "anchor_count": len(anchors),
        "raw_href_count": len(raw_hrefs),
        "pdf_href_count": pdf_like_count,
        "first_candidate_hrefs": first_candidates,
        "has_iframe": bool(soup.find("iframe")),
        "has_embed": bool(soup.find("embed")),
        "has_object": bool(soup.find("object")),
        "has_script_doc_pattern": bool(
            re.search(r"(agenda|minutes|document|viewfile|\.pdf)", html, flags=re.IGNORECASE)
            and soup.find("script")
        ),
        "is_selector_filter_page": is_selector_filter_page(html, base_url),
        "filtered_by_link_hints_count": len(filtered_by_link_hints),
        "filtered_by_link_hints_examples": list(dict.fromkeys(filtered_by_link_hints))[:10],        
    }


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


def extract_pdf_links(html: str, base_url: str, relaxed: bool = False) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    effective_relaxed = relaxed

    # If we can already see PDF-like hrefs on the page, do not gate on LINK_HINTS.
    page_has_pdf_hrefs = any(
        is_pdf_source_url(normalize_url(requests.compat.urljoin(base_url, (a.get("href") or "").strip())))
        for a in soup.find_all("a", href=True)
    )
    if page_has_pdf_hrefs:
        effective_relaxed = True
    
    if "/agendacenter/" in base_url.lower():
        links.extend(extract_agendacenter_links(html, base_url))

    filtered_by_link_hints = 0
    filtered_unrelated = 0
    for a in soup.find_all("a", href=True):

        href = a["href"].strip()
        full = requests.compat.urljoin(base_url, href)
        full = normalize_url(full)

        low = full.lower()
        path = urlsplit(full).path.lower()

        context_text = _extract_anchor_context_text(a)
        board_relevant = is_board_relevant_link(full, context_text)
        unrelated = looks_unrelated_doc(full, context_text)

        # Keep board-specific PDF sources; skip obvious unrelated municipal docs.
        if is_pdf_source_url(full):
            if unrelated and not board_relevant:
                filtered_unrelated += 1
                continue
            if "/documentcenter/view/" in low and not (board_relevant or effective_relaxed):
                filtered_by_link_hints += 1
                continue
            links.append(full)
            continue

        if not effective_relaxed and not (looks_like_board_doc(a) or board_relevant):
            filtered_by_link_hints += 1
            continue
        
        if "agendaviewer.php" in low:
            links.append(full)
            continue

        if "/agendacenter/viewfile/" in low and any(seg in low for seg in ("/agenda/", "/minutes/", "/packet/")):
            links.append(full)
            continue

        if path.endswith(".pdf"):
            links.append(full)

    out: list[str] = []
    seen: set[str] = set()
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)
    if filtered_by_link_hints:
        print(f"[DEBUG] {base_url}: filtered_by=LINK_HINTS count={filtered_by_link_hints}")
    if filtered_unrelated:
        print(f"[DEBUG] {base_url}: filtered_by=UNRELATED_DOC_HINTS count={filtered_unrelated}")
    return out

def extract_intermediate_links(html: str, base_url: str, relaxed: bool = False) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = normalize_url(requests.compat.urljoin(base_url, href))
        low = full.lower()
        path = urlsplit(full).path.lower()

        if path.endswith(".pdf"):
            continue

        context_text = _extract_anchor_context_text(a)
        hint_match = looks_like_board_doc(a) or any(k in low for k in LINK_HINTS) or is_board_relevant_link(full, context_text)
        if hint_match or relaxed:
            if any(k in low for k in ("agenda", "minutes", "meeting", "document", "viewfile", "documents")):
                links.append(full)

    return list(dict.fromkeys(links))

def extract_script_document_links(html: str, base_url: str) -> list[str]:
    url_re = re.compile(
        r"""["']([^"']*(?:\.pdf(?:\?[^"']*)?|/documentcenter/view/[^"']*|/agendacenter/viewfile/[^"']*))["']""",
        re.IGNORECASE,
    )
    links: list[str] = []
    for raw in url_re.findall(html):
        full = normalize_url(requests.compat.urljoin(base_url, raw.strip()))
        if is_pdf_source_url(full):
            links.append(full)
    return list(dict.fromkeys(links))

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

    for tag in soup.find_all("object"):
        data = (tag.get("data") or "").strip()
        src = (tag.get("src") or "").strip()
        for candidate in (data, src):
            if not candidate:
                continue
            full = requests.compat.urljoin(viewer_url, candidate)
            add(full)

    script_url_re = re.compile(
        r"""["']([^"']*(?:\.pdf(?:\?[^"']*)?|/agendacenter/viewfile/[^"']*|/documentcenter/view/[^"']*))["']""",
        re.IGNORECASE,
    )
    for match in script_url_re.findall(html):
        full = requests.compat.urljoin(viewer_url, match.strip())
        add(full)
    
    out: list[str] = []
    seen_local: set[str] = set()
    for u in found:
        if u not in seen_local:
            out.append(u)
            seen_local.add(u)

    filtered: list[str] = []
    for u in out:
        ulow = u.lower()
        if any(h in ulow for h in LINK_HINTS) or "agendacenter/viewfile" in ulow:
            filtered.append(u)

    return filtered or out

def resolve_intermediate_links_to_pdfs(links: list[str], max_pages: int = 10, max_depth: int = 2) -> list[str]:
    found: list[str] = []
    queue: list[tuple[str, int]] = [(l, 0) for l in links[:max_pages]]
    visited: set[str] = set()

    while queue:
        link, depth = queue.pop(0)
        if link in visited:
            continue
        visited.add(link)
        try:
            html = fetch_html(link)
            found.extend(extract_pdf_links(html, link, relaxed=True))
            found.extend(extract_script_document_links(html, link))

            soup = BeautifulSoup(html, "html.parser")
            for tag in soup.find_all(["iframe", "embed", "object"]):
                src = (tag.get("src") or "").strip()
                data = (tag.get("data") or "").strip()
                for candidate in (src, data):
                    if not candidate:
                        continue
                    full = normalize_url(requests.compat.urljoin(link, candidate))
                    if is_pdf_source_url(full):
                        found.append(full)

            if depth < max_depth:
                nested = extract_intermediate_links(html, link, relaxed=True)
                for n in nested[:max_pages]:
                    if n not in visited:
                        queue.append((n, depth + 1))         
        except Exception:
            continue

    return list(dict.fromkeys(found))


def maybe_switch_eastampton_page(page_url: str, html: str) -> tuple[str, str]:
    low = page_url.lower()
    if "eastampton" not in low:
        return page_url, html

    soup = BeautifulSoup(html, "html.parser")
    if not ("recent" in low or "department=" in low):
        return page_url, html

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        label = " ".join(a.stripped_strings).lower()
        href_low = href.lower()
        if any(k in href_low or k in label for k in ("agendas", "minutes")) and any(
            k in href_low or k in label for k in ("planning", "zoning", "land use", "board", "agenda", "minutes")
        ):
            new_url = normalize_url(requests.compat.urljoin(page_url, href))
            if new_url != page_url:
                try:
                    return new_url, fetch_html(new_url)
                except Exception:
                    return page_url, html
    return page_url, html



# -------------------------
# Download PDF
# -------------------------
def download_pdf(url: str, referer: str | None = None) -> pathlib.Path:
    url = canonicalize_url(url)

    pdf_name = f"{sha1(url)}.pdf"
    path = DATA_DIR / pdf_name
    if path.exists():
        return path

    candidates = [url]
    parts = urlsplit(url)

    no_query = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    if no_query != url:
        candidates.append(no_query)

    low = url.lower()
    is_document_center = "/documentcenter/view/" in low
    if is_document_center:
        m = re.search(r"/documentcenter/view/(\d+)", parts.path, flags=re.IGNORECASE)
        if m:
            doc_id = m.group(1)
            for path_variant in (f"/DocumentCenter/View/{doc_id}", f"/DocumentCenter/View/{doc_id}/"):
                v = urlunsplit((parts.scheme, parts.netloc, path_variant, parts.query, ""))
                candidates.append(canonicalize_url(v))
    else:
        if parts.netloc and not parts.netloc.startswith("www."):
            www = urlunsplit((parts.scheme, "www." + parts.netloc, parts.path, parts.query, parts.fragment))
            candidates.append(canonicalize_url(www))
        elif parts.netloc.startswith("www."):
            bare = urlunsplit((parts.scheme, parts.netloc[4:], parts.path, parts.query, parts.fragment))
            candidates.append(canonicalize_url(bare))

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
        nonlocal last_err

        saw_non_404 = False
        for cand in url_list:
            try:
                headers = {"Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8"}
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
                    print(f"[DEBUG] download candidate 404: {cand}")
                    last_err = RuntimeError(f"404 Not Found: {cand}")
                    continue

                saw_non_404 = True
                r.raise_for_status()

                content = r.content
                ctype = (r.headers.get("Content-Type") or "").lower()

                if ("pdf" not in ctype) and (not content.startswith(b"%PDF")):
                    raise RuntimeError(f"Response did not look like a PDF (content-type={ctype})")

                print(f"[DEBUG] download candidate success: {cand}")                
                path.write_bytes(content)
                return path, saw_non_404

            except Exception as e:
                print(f"[DEBUG] download candidate failed: {cand} -> {e}")
                last_err = e

        return None, saw_non_404

    saved, saw_non_404 = try_candidates(candidates)
    if saved:
        return saved

    if not saw_non_404:
        slug = discover_revize_slug(referer or url)
        if slug:
            revize_url = build_revize_pdf_url(url, slug)
            saved2, _ = try_candidates([revize_url])
            if saved2:
                return saved2

        raise RuntimeError(str(last_err) if last_err else "All download candidates returned 404 Not Found")

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


def extract_applicant_names(text: str) -> list[str]:
    """
    Heuristic extraction: try several patterns. Returns unique names in order found.
    """
    names: list[str] = []
    lower = text.lower()

    # keep searches in first N chars for agendas/minutes where applicant listed up top
    # (still catches mid-doc items often, but reduces random matches)
    window = text[:25000] if len(text) > 25000 else text

    for pat in APPLICANT_PATTERNS:
        for m in pat.finditer(window):
            raw = (m.group("name") or "").strip()
            raw = _APPLICANT_CLEAN_RE.sub("", raw)
            # avoid obviously-bad captures
            if not raw or len(raw) < 3:
                continue
            # cut off at line breaks / double spaces / "Block" etc.
            raw = re.split(r"\n|  +|\bBlock\b|\bLot\b|\bResolution\b", raw, maxsplit=1)[0].strip()
            # normalize spacing
            raw = re.sub(r"\s{2,}", " ", raw).strip()

            # reject generic words
            if raw.lower() in ("n/a", "na", "none", "tbd"):
                continue
            if raw and raw not in names:
                names.append(raw)

    return names[:5]  # keep alert small


def classify_land_use(text: str) -> dict:
    lower = text.lower()

    res_hits = [k for k in RESIDENTIAL_USE_KEYWORDS if k in lower]
    com_hits = [k for k in COMMERCIAL_USE_KEYWORDS if k in lower]

    res_zone_hits = [z for z in RESIDENTIAL_ZONE_HINTS if z in lower]
    com_zone_hits = [z for z in COMMERCIAL_ZONE_HINTS if z in lower]

    res_score = len(res_hits) * 2 + len(res_zone_hits)
    com_score = len(com_hits) * 2 + len(com_zone_hits)

    if res_score == 0 and com_score == 0:
        label = "unknown"
    elif res_score >= com_score + 2:
        label = "residential"
    elif com_score >= res_score + 2:
        label = "commercial"
    else:
        label = "mixed"

    return {
        "land_use": label,
        "res_score": res_score,
        "com_score": com_score,
        "res_hits": res_hits[:25],
        "com_hits": com_hits[:25],
    }


def analyze_text(text: str) -> dict:
    lower = text.lower()
    keyword_hits = [k for k in KEYWORDS if k in lower]

    snippets: list[str] = []
    for m in re.finditer(BLOCK_LOT_REGEX, text):
        snippet = m.group(0).replace("\n", " ").strip()
        snippets.append(snippet[:240])

    base_relevant = bool(keyword_hits) and bool(snippets)

    use_info = classify_land_use(text)
    if ALLOW_MIXED_USE:
        use_ok = use_info["land_use"] in ("residential", "mixed")
    else:
        use_ok = use_info["land_use"] == "residential"

    applicants = extract_applicant_names(text)

    relevant = base_relevant and use_ok

    return {
        "relevant": relevant,
        "keyword_hits": keyword_hits[:15],
        "block_lot_snippets": snippets[:20],
        "land_use": use_info["land_use"],
        "res_score": use_info["res_score"],
        # keep com_score internally (used for classification), but you won't print signals anymore
        "com_score": use_info["com_score"],
        "applicants": applicants,
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
    seen_docs = load_seen_docs()
    failed = load_failed()
    new_relevant_hits = []

    for site in TARGET_SITES:
        town = site["town"]
        page_url = site["url"]
        town_reasons: set[str] = set()
        
        try:
            html = fetch_html(page_url)
        except Exception as e:
            print(f"[ERROR] Fetch page failed: {town} {page_url} -> {e}")
            continue

        if town == "Eastampton":
            page_url, html = maybe_switch_eastampton_page(page_url, html)
        if "drive.google.com/drive/folders/" in page_url.lower():
            town_reasons.add("unsupported_source")
            print(f"[SUMMARY] {town}: reasons={sorted(town_reasons)}")
            continue
        
        relaxed_mode = is_selector_filter_page(html, page_url)
        pdf_links = extract_pdf_links(html, page_url, relaxed=relaxed_mode)
        pdf_links.extend(extract_script_document_links(html, page_url))
        
        intermediate_links: list[str] = []
        should_follow_intermediate = (
            relaxed_mode
            or not pdf_links
            or "evesham-nj.org/meetings/meeting-documents" in page_url.lower()
        )
        if should_follow_intermediate:
            intermediate_links = extract_intermediate_links(html, page_url, relaxed=relaxed_mode)
            if intermediate_links:
                resolved_intermediate = resolve_intermediate_links_to_pdfs(intermediate_links)
                if resolved_intermediate:
                    pdf_links.extend(resolved_intermediate)
                else:
                    town_reasons.add("intermediate_page_not_followed")
                    
        pdf_links = list(dict.fromkeys(pdf_links))[:20]
        print(f"[INFO] {town}: found {len(pdf_links)} pdf link(s)")

        if not pdf_links:
            dbg = collect_link_debug_info(html, page_url)
            town_reasons.add("no_links_found")
            if dbg["pdf_href_count"] > 0:
                town_reasons.add("filtered_out")
            if dbg["anchor_count"] == 0 and dbg["has_script_doc_pattern"]:
                town_reasons.add("likely_js_rendered")            
            print(
                "[DEBUG] "
                f"{town}: anchors={dbg['anchor_count']} raw_hrefs={dbg['raw_href_count']} "
                f"pdf_hrefs={dbg['pdf_href_count']}"
            )
            print(
                "[DEBUG] "
                f"{town}: iframe={dbg['has_iframe']} embed={dbg['has_embed']} object={dbg['has_object']} "
                f"script_doc_pattern={dbg['has_script_doc_pattern']}"
            )
            print(f"[DEBUG] {town}: selector_filter_page={dbg['is_selector_filter_page']}")
            print(f"[DEBUG] {town}: first_20_candidate_hrefs={dbg['first_candidate_hrefs']}")
            print(
                f"[DEBUG] {town}: filtered_by_LINK_HINTS={dbg['filtered_by_link_hints_count']} "
                f"examples={dbg['filtered_by_link_hints_examples']}"
            )
        
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
                        continue
                except Exception as e:
                    print(f"[ERROR] Viewer resolve failed: {town} {link} -> {e}")
                    continue

            for pdf_url in candidate_pdfs:
                url_fingerprints = build_url_fingerprints(pdf_url)
                if pdf_url in seen or pdf_url in failed or any(fp in seen_docs for fp in url_fingerprints):
                    continue

                try:
                    pdf_path = download_pdf(pdf_url, referer=page_url)
                    text = extract_text(pdf_path)
                    result = analyze_text(text)

                    seen.add(pdf_url)
                    seen_docs.update(url_fingerprints)
                    doc_fingerprint = build_doc_fingerprint(text, pdf_path)
                    if doc_fingerprint in seen_docs:
                        continue
                    seen_docs.add(doc_fingerprint)

                    if result["relevant"]:
                        new_relevant_hits.append(
                            {
                                "town": town,
                                "source_page": page_url,
                                "pdf_url": pdf_url,
                                "keyword_hits": result["keyword_hits"],
                                "block_lot_snippets": result["block_lot_snippets"],
                                "land_use": result.get("land_use"),
                                "res_score": result.get("res_score"),
                                "applicants": result.get("applicants") or [],
                            }
                        )

                except Exception as e:
                    msg = str(e).lower()
                    if "all download candidates returned 404" in msg or msg.startswith("404 not found"):
                        failed.add(pdf_url)
                        town_reasons.add("download_404")
                    print(f"[ERROR] PDF process failed: {town} {pdf_url} -> {e}")
                    
        if town_reasons:
            print(f"[SUMMARY] {town}: reasons={sorted(town_reasons)}")
    
    save_seen(seen)
    save_seen_docs(seen_docs)
    save_failed(failed)

    if not new_relevant_hits:
        print("[INFO] No new relevant subdivision docs.")
        return

    lines = ["New residential subdivision-related document(s) detected.\n"]

    for hit in new_relevant_hits:
        lines.append(f"**Town:** {hit['town']}")
        lines.append(f"**Source page:** {hit['source_page']}")
        lines.append(f"**PDF:** {hit['pdf_url']}")

        # ✅ show applicant name(s)
        if hit.get("applicants"):
            lines.append(f"**Applicant/Owner:** {', '.join(hit['applicants'])}")
        else:
            lines.append("**Applicant/Owner:** (not found)")

        # ✅ keep only residential classification summary (no commercial signals)
        lines.append(f"**Land use:** {hit.get('land_use')} (res-score={hit.get('res_score')})")

        if hit["keyword_hits"]:
            lines.append(f"**Keywords:** {', '.join(hit['keyword_hits'])}")
        if hit["block_lot_snippets"]:
            lines.append("**Block/Lot snippets:**")
            for s in hit["block_lot_snippets"]:
                lines.append(f"- {s}")
        lines.append("")

    body = "\n".join(lines)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    title = f"Residential Subdivision Alert ({today}): {len(new_relevant_hits)} new document(s)"

    create_github_issue(title=title, body=body)
    print("[INFO] GitHub Issue created.")


if __name__ == "__main__":
    main()
