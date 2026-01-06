#!/usr/bin/env python3
"""
Generate JPL Horizons ephemerides for MCS Education.

This script is intended to run in GitHub Actions and commit JSON outputs to the
repository so that browser-based simulations can fetch them without hitting JPL
Horizons directly (avoids CORS issues).

Target folder structure:
  Ephemeris/scripts/generate_ephemeris.py   (this script)
  Ephemeris/public/                        (generated artifacts)

Artifacts produced:
  Ephemeris/public/manifest.json
  Ephemeris/public/planets_5d.json
  Ephemeris/public/voyager1_1d.json
  Ephemeris/public/voyager1_jupiter_30m.json   (optional hi-res window)
  Ephemeris/public/voyager1_saturn_30m.json    (optional hi-res window)

Notes:
  * Horizons JSON responses can include an `error` field (and omit $$SOE/$$EOE).
    We treat that as a hard failure with a clear message.
  * We quote parameter values like the official Horizons API examples.
"""

from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "MCS-Education-EphemerisBot/1.1 (+https://github.com/)"

# Reference frame / output choices used by the simulation
CENTER = "@0"          # Solar System Barycenter
REF_SYSTEM = "ICRF"
REF_PLANE = "FRAME"
OUT_UNITS = "AU-D"     # AU and days
VEC_TABLE = "2"        # position+velocity
TIME_TYPE = "UT"

# Time spans
START_TIME = "1977-09-05 12:56:00"  # Voyager 1 launch (UTC)
# Stop time is computed at runtime (today 00:00 UTC) so the feed extends as time passes.

# Objects (SPK IDs) — Horizons major-body IDs are stable and documented.
# Sun: 10, Mercury:199, Venus:299, Earth:399, Mars:499, Jupiter:599, Saturn:699, Uranus:799, Neptune:899
MAJOR_BODIES = [
    ("Sun", 10),
    ("Mercury", 199),
    ("Venus", 299),
    ("Earth", 399),
    ("Mars", 499),
    ("Jupiter", 599),
    ("Saturn", 699),
    ("Uranus", 799),
    ("Neptune", 899),
]

VOYAGER_1_ID = -31

# Networking / rate limiting
HTTP_TIMEOUT_S = 90
RETRIES = 5
BACKOFF_S = 2.0


def q(value: str) -> str:
    """Quote a Horizons parameter value per official API examples."""
    return f"'{value}'"


def _request_horizons(params: Dict[str, str]) -> Dict:
    """GET Horizons and return parsed JSON.

    Horizons API returns HTTP 200 even for many parameter-level errors; when
    JSON output is requested it attempts to expose errors via an `error` field.
    """
    last_exc: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(
                HORIZONS_URL,
                params=params,
                timeout=HTTP_TIMEOUT_S,
                headers={"User-Agent": USER_AGENT},
            )
            r.raise_for_status()
            j = r.json()
            return j
        except Exception as e:
            last_exc = e
            if attempt >= RETRIES:
                break
            # modest exponential backoff
            time.sleep(BACKOFF_S * (2 ** (attempt - 1)))
    raise RuntimeError(f"Horizons request failed after {RETRIES} attempts: {last_exc}")


def _extract_soe_block(result_text: str) -> str:
    i0 = result_text.find("$$SOE")
    i1 = result_text.find("$$EOE")
    if i0 < 0 or i1 < 0 or i1 <= i0:
        # Provide a useful snippet for CI logs.
        snippet = result_text.strip().replace("\r", "")
        snippet = snippet[:1200] + ("…" if len(snippet) > 1200 else "")
        raise RuntimeError(
            "Horizons response missing $$SOE/$$EOE block. "
            "This almost always means Horizons returned an error message instead of an ephemeris. "
            f"First part of response:\n{snippet}"
        )
    return result_text[i0 + 5 : i1].strip("\r\n ")


def _parse_vectors_csv_block(block: str) -> Tuple[List[float], List[float]]:
    """Parse a Horizons VECTORS CSV block (VEC_TABLE=2, VEC_LABELS=NO) into packed arrays.

    Returns:
      t_jd: [JD0, JD1, ...]
      pv:   flattened [x,y,z,vx,vy,vz, x,y,z,vx,vy,vz, ...] in AU and AU/day

    Handles both formats:
      JD, CAL, X, Y, Z, VX, VY, VZ
    and (rarely):
      JD, X, Y, Z, VX, VY, VZ
    """
    t_jd: List[float] = []
    pv: List[float] = []

    for raw in block.splitlines():
        line = raw.strip()
        if not line:
            continue
        # CSV lines should start with a Julian Day number.
        if not re.match(r"^\d", line):
            continue

        parts = [p.strip() for p in line.split(",")]

        try:
            # Common: JD, CAL, X, Y, Z, VX, VY, VZ  -> 8 columns
            if len(parts) >= 8:
                jd = float(parts[0])
                x, y, z, vx, vy, vz = map(float, parts[2:8])
            # Fallback: JD, X, Y, Z, VX, VY, VZ -> 7 columns
            elif len(parts) >= 7:
                jd = float(parts[0])
                x, y, z, vx, vy, vz = map(float, parts[1:7])
            else:
                continue
        except ValueError:
            # Sometimes the calendar column includes commas if settings change; be defensive.
            continue

        t_jd.append(jd)
        pv.extend([x, y, z, vx, vy, vz])

    if len(t_jd) < 2:
        raise RuntimeError("Parsed ephemeris contains too few samples (expected >=2).")
    if len(pv) != len(t_jd) * 6:
        raise RuntimeError("Parsed ephemeris pv length mismatch.")

    return t_jd, pv


def horizons_vectors(
    command: int,
    start_time: str,
    stop_time: str,
    step_size: str,
) -> Tuple[List[float], List[float], Dict]:
    """Fetch packed vectors from Horizons."""

    params = {
        "format": "json",
        "EPHEM_TYPE": q("VECTORS"),
        "MAKE_EPHEM": q("YES"),
        "OBJ_DATA": q("NO"),
        "COMMAND": q(str(command)),
        "CENTER": q(CENTER),
        "START_TIME": q(start_time),
        "STOP_TIME": q(stop_time),
        "STEP_SIZE": q(step_size),
        "REF_SYSTEM": q(REF_SYSTEM),
        "REF_PLANE": q(REF_PLANE),
        "OUT_UNITS": q(OUT_UNITS),
        "VEC_TABLE": VEC_TABLE,
        "CSV_FORMAT": q("YES"),
        "VEC_LABELS": q("NO"),
        "VEC_DELTA_T": q("NO"),
        "VEC_CORR": q("NONE"),
        "TIME_TYPE": q(TIME_TYPE),
    }

    j = _request_horizons(params)

    # Official behavior: when json is requested, an error (if detected) is included in `error`.
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(
            f"Horizons API error for COMMAND={command}: {j['error']}\n"
            f"(Tip: check COMMAND/CENTER and quoting. This script uses the official quoted style.)"
        )

    result = j.get("result") if isinstance(j, dict) else None
    if not isinstance(result, str):
        raise RuntimeError(f"Horizons response missing `result` field for COMMAND={command}.")

    block = _extract_soe_block(result)
    t_jd, pv = _parse_vectors_csv_block(block)

    sig = j.get("signature") if isinstance(j, dict) else None
    if not isinstance(sig, dict):
        sig = {}

    return t_jd, pv, sig


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: Dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    tmp.replace(path)


def stop_time_today_00z() -> str:
    """Return 'YYYY-MM-DD 00:00:00' in UTC, for a stable daily boundary."""
    d = datetime.now(timezone.utc).date()
    return f"{d.isoformat()} 00:00:00"


@dataclass(frozen=True)
class DatasetDef:
    id: str
    file: str
    description: str


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]  # .../Ephemeris/scripts -> repo root
    out_dir = repo_root / "Ephemeris" / "public"
    ensure_dir(out_dir)

    stop_time = stop_time_today_00z()

    # Output files used by the simulation
    ds_planets = DatasetDef(
        id="planets_5d",
        file="planets_5d.json",
        description="Sun + 8 planets; barycentric state vectors sampled every 5 days (AU, AU/day).",
    )
    ds_voy = DatasetDef(
        id="voyager1_1d",
        file="voyager1_1d.json",
        description="Voyager 1 barycentric state vectors sampled daily (AU, AU/day).",
    )
    ds_hi_jup = DatasetDef(
        id="voyager1_jupiter_30m",
        file="voyager1_jupiter_30m.json",
        description="Voyager 1 hi-res window around Jupiter encounter; 30-minute sampling.",
    )
    ds_hi_sat = DatasetDef(
        id="voyager1_saturn_30m",
        file="voyager1_saturn_30m.json",
        description="Voyager 1 hi-res window around Saturn/Titan encounter; 30-minute sampling.",
    )

    # ---------------------------
    # Fetch planets (5-day cadence)
    # ---------------------------
    print(f"[1/3] Fetching major bodies (5d) from {START_TIME} to {stop_time} …", file=sys.stderr)

    planet_t: List[float] | None = None
    objects: Dict[str, Dict] = {}
    sig_any: Dict = {}

    for (name, spkid) in MAJOR_BODIES:
        print(f"  - {name} (COMMAND={spkid})", file=sys.stderr)
        t_jd, pv, sig = horizons_vectors(spkid, START_TIME, stop_time, "5 d")
        if planet_t is None:
            planet_t = t_jd
        else:
            if len(t_jd) != len(planet_t) or any(abs(a - b) > 1e-10 for a, b in zip(t_jd, planet_t)):
                raise RuntimeError(f"Time grid mismatch for {name}. Refusing to write multi-ephemeris file.")

        objects[name] = {"name": name, "spkid": spkid, "pv": pv}
        sig_any = sig_any or sig

    assert planet_t is not None

    planets_json = {
        "schema": "mcs-ephem-multi-v1",
        "t_jd": planet_t,
        "objects": objects,
        "meta": {
            "generated_at": now_utc_iso(),
            "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "signature": sig_any,
        },
    }
    write_json(out_dir / ds_planets.file, planets_json)

    # ---------------------------
    # Fetch Voyager 1 (daily)
    # ---------------------------
    print(f"[2/3] Fetching Voyager 1 (1d) from {START_TIME} to {stop_time} …", file=sys.stderr)
    t_v, pv_v, sig_v = horizons_vectors(VOYAGER_1_ID, START_TIME, stop_time, "1 d")

    voyager_json = {
        "schema": "mcs-ephem-v1",
        "t_jd": t_v,
        "pv": pv_v,
        "meta": {
            "object": {"name": "Voyager 1", "spkid": VOYAGER_1_ID},
            "generated_at": now_utc_iso(),
            "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "signature": sig_v,
        },
    }
    write_json(out_dir / ds_voy.file, voyager_json)

    # ---------------------------
    # Optional hi-res windows
    # ---------------------------
    print("[3/3] Fetching optional hi-res windows (30m) …", file=sys.stderr)

    # Jupiter encounter ~1979-03-05 (closest approach). Window chosen for visualization.
    jup_start = "1979-02-20 00:00:00"
    jup_stop = "1979-03-15 00:00:00"

    # Saturn/Titan encounter ~1980-11-12. Window chosen for visualization.
    sat_start = "1980-11-01 00:00:00"
    sat_stop = "1980-11-20 00:00:00"

    for (ds, w_start, w_stop) in [
        (ds_hi_jup, jup_start, jup_stop),
        (ds_hi_sat, sat_start, sat_stop),
    ]:
        print(f"  - {ds.id}: {w_start} to {w_stop}", file=sys.stderr)
        t_hi, pv_hi, sig_hi = horizons_vectors(VOYAGER_1_ID, w_start, w_stop, "30 m")
        hi_json = {
            "schema": "mcs-ephem-v1",
            "t_jd": t_hi,
            "pv": pv_hi,
            "meta": {
                "object": {"name": "Voyager 1", "spkid": VOYAGER_1_ID},
                "window": {"start": w_start, "stop": w_stop, "step": "30 m"},
                "generated_at": now_utc_iso(),
                "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
                "frame": {
                    "center": CENTER,
                    "ref_system": REF_SYSTEM,
                    "ref_plane": REF_PLANE,
                    "out_units": OUT_UNITS,
                    "time_type": TIME_TYPE,
                    "vec_table": VEC_TABLE,
                },
                "signature": sig_hi,
            },
        }
        write_json(out_dir / ds.file, hi_json)

    # ---------------------------
    # Manifest
    # ---------------------------
    manifest = {
        "schema": "mcs-ephem-manifest-v1",
        "generated_at": now_utc_iso(),
        "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
        "frame": {
            "center": CENTER,
            "ref_system": REF_SYSTEM,
            "ref_plane": REF_PLANE,
            "out_units": OUT_UNITS,
            "time_type": TIME_TYPE,
            "vec_table": VEC_TABLE,
        },
        "datasets": [
            {"id": ds_planets.id, "file": ds_planets.file, "description": ds_planets.description},
            {"id": ds_voy.id, "file": ds_voy.file, "description": ds_voy.description},
            {"id": ds_hi_jup.id, "file": ds_hi_jup.file, "description": ds_hi_jup.description},
            {"id": ds_hi_sat.id, "file": ds_hi_sat.file, "description": ds_hi_sat.description},
        ],
    }
    write_json(out_dir / "manifest.json", manifest)

    print("Done. Wrote Ephemeris/public/*.json", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Make CI failure actionable.
        print("\nERROR: Ephemeris generation failed.", file=sys.stderr)
        print(str(e), file=sys.stderr)
        raise
