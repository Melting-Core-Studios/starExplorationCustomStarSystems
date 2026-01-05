#!/usr/bin/env python3
from __future__ import annotations

import datetime as _dt
import json
import os
import random
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, List, Optional


USER_AGENT = "MCS-Education-CatalogBot/1.0 (+https://github.com/your-org/your-repo)"

FETCH_PLANET_WIKITEXT = os.getenv("STARTREK_FETCH_PLANET_WIKITEXT", "0").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

MAX_PLANET_WIKITEXT = int(os.getenv("STARTREK_MAX_PLANET_WIKITEXT", "300"))


def http_get_json(url: str, timeout: int = 60, retries: int = 5) -> dict:
    """
    Robust JSON fetch with retries + exponential backoff.
    Handles intermittent network issues / 429 / 5xx without failing the entire workflow.
    """
    last_err: Optional[Exception] = None

    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            return json.loads(data)

        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
            else:
                raise

        except (TimeoutError, urllib.error.URLError) as e:
            last_err = e

        sleep_s = min(30.0, (2 ** attempt) + random.random())
        time.sleep(sleep_s)

    raise RuntimeError(f"HTTP fetch failed after {retries} attempts: {url}") from last_err


def mw_api_url(api_base: str, params: dict) -> str:
    return f"{api_base}?{urllib.parse.urlencode(params)}"


def mw_category_members(
    api_base: str,
    category_title: str,
    namespace: int = 0,
    sleep_s: float = 0.15,
    limit: int = 200,
) -> List[str]:
    titles: List[str] = []
    cmcontinue: Optional[str] = None

    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_title}",
            "cmnamespace": str(namespace),
            "cmlimit": str(limit),
            "format": "json",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue

        payload = http_get_json(mw_api_url(api_base, params), timeout=60, retries=5)
        for m in payload.get("query", {}).get("categorymembers", []):
            t = m.get("title")
            if t:
                titles.append(t)

        cmcontinue = (payload.get("continue") or {}).get("cmcontinue")
        if not cmcontinue:
            break

        time.sleep(sleep_s)

    return titles


def mw_page_wikitext(api_base: str, title: str, sleep_s: float = 0.15) -> str:
    params = {
        "action": "query",
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "titles": title,
        "format": "json",
        "formatversion": "2",
    }
    payload = http_get_json(mw_api_url(api_base, params), timeout=90, retries=5)
    pages = payload.get("query", {}).get("pages", [])
    if not pages:
        return ""
    revs = pages[0].get("revisions") or []
    if not revs:
        return ""
    slots = revs[0].get("slots") or {}
    main = slots.get("main") or {}
    text = main.get("content") or ""
    time.sleep(sleep_s)
    return text


_WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")


def strip_wiki_markup(value: str) -> str:
    value = value.strip()

    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        return inner.split("|", 1)[1].strip() if "|" in inner else inner.strip()

    value = _WIKILINK_RE.sub(_repl, value)
    value = re.sub(r"\{\{[^{}]*\}\}", "", value)
    value = re.sub(r"<ref[^>]*>.*?</ref>", "", value, flags=re.DOTALL | re.IGNORECASE)
    value = re.sub(r"<[^>]+>", "", value)
    return value.strip()


def guess_system_from_planet_wikitext(wikitext: str) -> Optional[str]:
    candidates = [
        r"^\|\s*system\s*=\s*(.+)$",
        r"^\|\s*star\s*system\s*=\s*(.+)$",
        r"^\|\s*starsystem\s*=\s*(.+)$",
    ]
    for pat in candidates:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for line in wikitext.splitlines():
            m = rx.match(line)
            if m:
                val = strip_wiki_markup(m.group(1))
                val = re.sub(r"\(.*?\)", "", val).strip()
                return val or None
    return None


def build_startrek_catalog() -> dict:
    api = "https://memory-alpha.fandom.com/api.php"

    generated_at = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()

    system_titles = mw_category_members(api, category_title="Star systems")
    systems: Dict[str, dict] = {}

    for t in system_titles:
        name = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
        systems[name.lower()] = {
            "name": name,
            "category": "Single",
            "notes": (
                "Auto-generated from Memory Alpha category listings; astrophysical parameters are not asserted "
                "unless explicitly present."
            ),
            "stars": [{"name": f"{name} primary", "type": "star"}],
            "planets": [],
        }

    if not FETCH_PLANET_WIKITEXT:
        for s in systems.values():
            s["notes"] += " Planet mapping skipped (STARTREK_FETCH_PLANET_WIKITEXT=0)."
    else:
        planet_titles = mw_category_members(api, category_title="Planets")
        reads = 0
        skipped = 0

        for pt in planet_titles:
            if reads >= MAX_PLANET_WIKITEXT:
                break

            planet_name = re.sub(r"\s*\(.*?\)\s*$", "", pt).strip()
            try:
                wikitext = mw_page_wikitext(api, pt)
            except Exception:
                skipped += 1
                continue

            reads += 1
            sys_name = guess_system_from_planet_wikitext(wikitext)
            if not sys_name:
                continue

            sys_obj = systems.get(sys_name.lower())
            if not sys_obj:
                continue

            sys_obj["planets"].append({"name": planet_name})

        for s in systems.values():
            s["notes"] += f" Planet enrichment enabled: {reads} pages read, {skipped} page fetches failed."

    out_systems = sorted(systems.values(), key=lambda s: s["name"].lower())

    return {
        "meta": {
            "franchise": "Star Trek",
            "source": "Memory Alpha (MediaWiki API)",
            "generatedAt": generated_at,
            "planetEnrichment": {
                "enabled": FETCH_PLANET_WIKITEXT,
                "maxPlanetPages": MAX_PLANET_WIKITEXT,
            },
        },
        "systems": out_systems,
    }


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    catalogs_dir = base_dir / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)

    startrek = build_startrek_catalog()
    (catalogs_dir / "startrek_star_systems_catalog.json").write_text(
        json.dumps(startrek, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {catalogs_dir / 'startrek_star_systems_catalog.json'}")


if __name__ == "__main__":
    main()
