#!/usr/bin/env python3
from __future__ import annotations

import datetime as _dt
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

USER_AGENT = "MCS-Education-CatalogBot/1.0 (+https://github.com/your-org/your-repo)"


def http_get_json(url: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    return json.loads(data)


def mw_api_url(api_base: str, params: dict) -> str:
    return f"{api_base}?{urllib.parse.urlencode(params)}"


def mw_category_members(
    api_base: str,
    category_title: str,
    namespace: int = 0,
    sleep_s: float = 0.2,
    limit: int = 500,
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

        payload = http_get_json(mw_api_url(api_base, params))
        for m in payload.get("query", {}).get("categorymembers", []):
            t = m.get("title")
            if t:
                titles.append(t)

        cmcontinue = (payload.get("continue") or {}).get("cmcontinue")
        if not cmcontinue:
            break

        time.sleep(sleep_s)

    return titles


def mw_page_wikitext(api_base: str, title: str, sleep_s: float = 0.2) -> str:
    params = {
        "action": "query",
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "titles": title,
        "format": "json",
        "formatversion": "2",
    }
    payload = http_get_json(mw_api_url(api_base, params))
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
    lines = wikitext.splitlines()
    for pat in candidates:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for line in lines:
            m = rx.match(line)
            if m:
                val = strip_wiki_markup(m.group(1))
                val = re.sub(r"\(.*?\)", "", val).strip()
                return val or None
    return None


def build_startrek_catalog() -> dict:
    api = "https://memory-alpha.fandom.com/api.php"

    generated_at = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    system_titles = mw_category_members(api, category_title="Star systems")
    systems: Dict[str, dict] = {}

    for t in system_titles:
        name = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
        systems[name.lower()] = {
            "name": name,
            "category": "Single",
            "notes": "Auto-generated from Memory Alpha category listings; astrophysical parameters are not asserted unless explicitly present.",
            "stars": [{"name": f"{name} primary", "type": "star"}],
            "planets": [],
        }

    planet_titles = mw_category_members(api, category_title="Planets")
    for pt in planet_titles:
        planet_name = re.sub(r"\s*\(.*?\)\s*$", "", pt).strip()
        wikitext = mw_page_wikitext(api, pt)
        sys_name = guess_system_from_planet_wikitext(wikitext)
        if not sys_name:
            continue
        sys_obj = systems.get(sys_name.lower())
        if not sys_obj:
            continue
        sys_obj["planets"].append({"name": planet_name})

    out_systems = sorted(systems.values(), key=lambda s: s["name"].lower())

    return {
        "meta": {
            "franchise": "Star Trek",
            "source": "Memory Alpha (MediaWiki API)",
            "generatedAt": generated_at,
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
