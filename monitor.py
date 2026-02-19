import os
import re
import json
import hashlib
import pathlib
from datetime import datetime

import requests
import pdfplumber
from bs4 import BeautifulSoup

# =========================
# CONFIG — EDIT THESE
# =========================
TARGET_PAGES = [
    # Put the EXACT page(s) that list Planning Board agendas/minutes PDFs.
    # Example: the page where you found the Dec 11, 2025 PDF link.
    "https://winslowtownship.granicus.com/ViewPublisher.php?view_id=1",
]

# If the site uses relative links, we’ll join them against the page URL.
USER_AGENT = "Mozilla/5.0 (GitHubActions PlanningBoard Monitor)"

# Keywords tuned for agendas/minutes like your Dec 11, 2025 Winslow PB agenda
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

# Where we store downloaded PDFs + the “seen links” list
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)
SEEN_FILE = DATA_DIR / "seen_links.json"

# GitHub issue creation settings
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # provided automatically by Actions
REPO = os.getenv("GITHUB_REPOSITORY")     # e.g. "username/repo"


# =========================
# Helpers
# =========================
def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2), encoding="utf-8")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=40, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def extract_pdf_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        # catch pdfs anywhere in URL (some are like ...pdf?download=1)
full = requests.compat.urljoin(base_url, href)
if ".pdf" in full.lower() or "agendaviewer.php" in full.lower():
    links.append(full)

    # de-dupe but keep order
    out = []
    seen = set()
    for l in links:
        if l not in seen:
            out.append(l)
            seen.add(l)
    return out


def download_pdf(url: str) -> pathlib.Path:
    pdf_name = f"{sha1(url)}.pdf"
    path = DATA_DIR / pdf_name
    if path.exists():
        return path

    r = requests.get(url, timeout=90, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    path.write_bytes(r.content)
    return path


def extract_text(pdf_path: pathlib.Path) -> str:
    parts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


def analyze_text(text: str) -> dict:
    """
    Returns analysis including:
    - keyword hits
    - block/lot matches (snippets)
    - relevant boolean
    """
    lower = text.lower()
    keyword_hits = [k for k in KEYWORDS if k in lower]

    snippets = []
    for m in re.finditer(BLOCK_LOT_REGEX, text):
        snippet = m.group(0).replace("\n", " ").strip()
        # keep it readable
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
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    payload = {"title": title, "body": body}

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


# =========================
# Main
# =========================
def main():
    seen = load_seen()
    new_relevant_hits = []

    for page_url in TARGET_PAGES:
        try:
            html = fetch_html(page_url)
        except Exception as e:
            print(f"[ERROR] Fetch page failed: {page_url} -> {e}")
            continue

        pdf_links = extract_pdf_links(html, page_url)
        print(f"[INFO] {page_url}: found {len(pdf_links)} pdf link(s)")

        for pdf_url in pdf_links:
            if pdf_url in seen:
                continue

            # Mark it seen early to prevent reprocessing loops
            seen.add(pdf_url)

            try:
                pdf_path = download_pdf(pdf_url)
                text = extract_text(pdf_path)
                result = analyze_text(text)

                if result["relevant"]:
                    new_relevant_hits.append({
                        "pdf_url": pdf_url,
                        "keyword_hits": result["keyword_hits"],
                        "block_lot_snippets": result["block_lot_snippets"],
                    })

            except Exception as e:
                print(f"[ERROR] PDF process failed: {pdf_url} -> {e}")

    save_seen(seen)

    if not new_relevant_hits:
        print("[INFO] No new relevant subdivision docs.")
        return

    # Build issue content
    lines = []
    lines.append("New subdivision-related document(s) detected.\n")
    for hit in new_relevant_hits:
        lines.append(f"**PDF:** {hit['pdf_url']}")
        if hit["keyword_hits"]:
            lines.append(f"**Keywords:** {', '.join(hit['keyword_hits'])}")
        if hit["block_lot_snippets"]:
            lines.append("**Block/Lot snippets:**")
            for s in hit["block_lot_snippets"]:
                lines.append(f"- {s}")
        lines.append("")  # blank line

    body = "\n".join(lines)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    title = f"Subdivision Alert ({today}): {len(new_relevant_hits)} new document(s)"

    create_github_issue(title=title, body=body)
    print("[INFO] GitHub Issue created.")


if __name__ == "__main__":
    main()
