from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl
import requests
import fitz  # PyMuPDF
import tempfile
import os
import time
import hashlib
import re
from urllib.parse import urlparse
from typing import Dict, Any
from bs4 import BeautifulSoup

app = FastAPI()

# Only allow trusted vendor domains here
ALLOWED_DOMAINS = {
    "i.dell.com",
    "dell.com",
    "www.dell.com",
    "www.delltechnologies.com",
    "delltechnologies.com",
    "hpe.com",
    "www.hpe.com",
    "arubanetworks.com",
    "www.arubanetworks.com",
    "cisco.com",
    "www.cisco.com",
}

USER_AGENT = (
    "Mozilla/5.0 (compatible; ProductFactsBot/1.0; "
    "+https://yourdomain.example/parser-info)"
)

REQUEST_TIMEOUT = 30
MAX_FILE_SIZE_MB = 25
CACHE_TTL_SECONDS = 60 * 60 * 24  # 24 hours
CACHE_DIR = "cache"

os.makedirs(CACHE_DIR, exist_ok=True)


class ParseRequest(BaseModel):
    url: HttpUrl
    sku: str


def domain_allowed(url: str) -> bool:
    hostname = urlparse(url).hostname or ""
    return hostname.lower() in ALLOWED_DOMAINS


def cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def get_cache_path(url: str) -> str:
    return os.path.join(CACHE_DIR, f"{cache_key(url)}.json")


def read_cache(url: str):
    path = get_cache_path(url)
    if not os.path.exists(path):
        return None
    age = time.time() - os.path.getmtime(path)
    if age > CACHE_TTL_SECONDS:
        return None
    with open(path, "r", encoding="utf-8") as f:
        import json
        return json.load(f)


def write_cache(url: str, data: Dict[str, Any]):
    path = get_cache_path(url)
    with open(path, "w", encoding="utf-8") as f:
        import json
        json.dump(data, f, ensure_ascii=False)


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_url(url: str) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    last_error = None

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                stream=True,
                allow_redirects=True,
            )
            response.raise_for_status()

            content_length = response.headers.get("Content-Length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > MAX_FILE_SIZE_MB:
                    raise ValueError(f"File too large: {size_mb:.2f} MB")

            return response

        except Exception as e:
            last_error = str(e)
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Failed after retries: {last_error}")


def parse_pdf_from_response(response: requests.Response) -> Dict[str, Any]:
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp.write(chunk)
            tmp_path = tmp.name

        doc = fitz.open(tmp_path)
        pages = [page.get_text("text") for page in doc]
        text = clean_text(" ".join(pages))

        return {
            "ok": True,
            "content_type": "application/pdf",
            "pages": len(doc),
            "chars": len(text),
            "text": text,
            "error": "",
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def parse_html_from_response(response: requests.Response) -> Dict[str, Any]:
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    text = clean_text(soup.get_text(separator=" "))

    return {
        "ok": True,
        "content_type": "text/html",
        "pages": 1,
        "chars": len(text),
        "text": text,
        "error": "",
    }


@app.post("/parse")
def parse_document(req: ParseRequest):
    url = str(req.url)
    target_sku = req.sku

    if not domain_allowed(url):
        return {
            "ok": False,
            "source_url": url,
            "content_type": "",
            "pages": 0,
            "chars": 0,
            "text": "",
            "cached": False,
            "error": "Domain not allowed",
        }

    cached = read_cache(url)
    if cached:
        cached["cached"] = True
        return cached

    try:
        response = fetch_url(url)
        content_type = (response.headers.get("Content-Type") or "").lower()

        if "application/pdf" in content_type or url.lower().endswith(".pdf"):
            result = parse_pdf_from_response(response)
        elif "text/html" in content_type or "application/xhtml+xml" in content_type:
            result = parse_html_from_response(response)
        else:
            result = {
                "ok": False,
                "content_type": content_type,
                "pages": 0,
                "chars": 0,
                "text": "",
                "error": f"Unsupported content type: {content_type}",
            }

        # Classification
        text = result.get("text", "")
        candidates = re.findall(r'\b[A-Z0-9]{2,}[-/][A-Z0-9]{2,}[-A-Z0-9]*\b', text.upper())
        unique_candidates = set(candidates)
        unique_candidates.discard(target_sku.upper())

        result["target_found"] = target_sku.upper() in text.upper()
        result["multi_model"] = len(unique_candidates) > 3
        result["detected_siblings"] = ', '.join(unique_candidates)
        result["target_sku"] = target_sku

        output = {
            "source_url": url,
            "cached": False,
            **result,
        }

        write_cache(url, output)
        return output

    except Exception as e:
        return {
            "ok": False,
            "source_url": url,
            "content_type": "",
            "pages": 0,
            "chars": 0,
            "text": "",
            "cached": False,
            "error": str(e),
        }
