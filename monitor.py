import os
import re
import json
import hashlib
import pathlib
import requests
import pdfplumber
from bs4 import BeautifulSoup

# ====== CONFIG ======
TARGET_PAGES = [
    # Put the specific Planning Board listing page(s) here.
    # Start with the page you manually browse to where the PDF links appear.
    "https://www.winslowtownship.com/content/3296/3734/default.aspx",
]

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)
SEEN_FILE = DATA_DIR / "seen_links.json"

# Subdivision + parcel patterns (tuned for NJ-style docs like your Dec 11, 2025 agenda)
KEYWORDS = [
    "minor subdivision",
    "major subdivision",
    "preliminary",
    "final",
    "amended preliminary",
    "resolution approving",
    "resolution deeming",
]
BLOCK_LOT_REGEX = re.compile(
    r"\bBlock\s*\d+(\.\d+)?\b.*?\bLot[s]?\s*[\d,\s&\-\.]+",
    re.IGNORECASE | re.DOTALL
)

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL")


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2), encoding="utf-8")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def extract_pdf_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" in href.lower():
            links.append(requests.compat.urljoin(base_url, href))
    # Deduplicate while preserving order
    seen = set()
    out = []
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
    r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    path.write_bytes(r.content)
    return path


def extract_text(pdf_path: pathlib.Path) -> str:
    text_parts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                text_parts.append(t)
    return "\n".join(text_parts)


def is_relevant(text: str) -> tuple[bool, dict]:
    t_lower = text.lower()
    keyword_hits = [k for k in KEYWORDS if k in t_lower]
    block_lot_hits = BLOCK_LOT_REGEX.findall(text)

    # Also collect a few explicit Block/Lot snippets for the alert
    snippet_matches = []
    for m in re.finditer(BLOCK_LOT_REGEX, text):
        snippet_matches.append(m.group(0)[:200].replace("\n", " "))

    relevant = (len(keyword_hits) > 0) and (len(snippet_matches) > 0)
    details = {
        "keyword_hits": keyword_hits[:10],
        "block_lot_snippets": snippet_matches[:10],
    }
    return relevant, details


def send_email_sendgrid(subject: str, body: str) -> None:
    if not (SENDGRID_API_KEY and ALERT_TO_EMAIL and ALERT_FROM_EMAIL):
        raise RuntimeError("Missing SENDGRID_API_KEY / ALERT_TO_EMAIL / ALERT_FROM_EMAIL env vars")

    url = "https://api.sendgrid.com/v3/mail/send"
    payload = {
        "personalizations": [{"to": [{"email": ALERT_TO_EMAIL}]}],
        "from": {"email": ALERT_FROM_EMAIL},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }
    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def main():
    seen = load_seen()
    new_hits = []

    for page_url in TARGET_PAGES:
        html = fetch_html(page_url)
        pdf_links = extract_pdf_links(html, page_url)

        for pdf_url in pdf_links:
            if pdf_url in seen:
                continue

            # Mark seen early so we don't reprocess on transient failures
            seen.add(pdf_url)

            try:
                pdf_path = download_pdf(pdf_url)
                text = extract_text(pdf_path)

                relevant, details = is_relevant(text)
                if relevant:
                    new_hits.append((pdf_url, details))
            except Exception as e:
                # Keep it seen to avoid loops; you can log if you want
                print(f"Error processing {pdf_url}: {e}")

    save_seen(seen)

    if new_hits:
        lines = []
        lines.append("New subdivision-related Planning Board document(s) found:\n")
        for pdf_url, details in new_hits:
            lines.append(f"PDF: {pdf_url}")
            if details["keyword_hits"]:
                lines.append(f"Keywords: {', '.join(details['keyword_hits'])}")
            if details["block_lot_snippets"]:
                lines.append("Block/Lot snippets:")
                for s in details["block_lot_snippets"]:
                    lines.append(f"  - {s}")
            lines.append("")  # blank line

        body = "\n".join(lines)
        send_email_sendgrid(
            subject=f"Subdivision alert: {len(new_hits)} new document(s)",
            body=body
        )
        print("Alert sent.")
    else:
        print("No new relevant docs.")


if __name__ == "__main__":
    main()
