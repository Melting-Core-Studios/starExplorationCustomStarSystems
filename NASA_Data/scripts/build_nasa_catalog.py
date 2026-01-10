import argparse, csv, json, math, os, re, time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import requests

# ESA Gaia Archive TAP synchronous endpoint.
# Used to fetch per-component astrometry (RA/Dec/parallax) for Gaia DR3 source_ids.
GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"

AU_PER_PC = 206264.80624709636  # AU per parsec
TSUN_K = 5772.0


def normalize_gaia_dr3_id(v) -> Optional[str]:
    """Return Gaia DR3 source_id as a digit-only string, or None."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return str(int(v))
    s = str(v).strip()
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    return digits if digits else None


def _delta_ra_deg(ra_deg: float, ra0_deg: float) -> float:
    """Smallest RA difference in degrees in [-180, 180]."""
    return (ra_deg - ra0_deg + 540.0) % 360.0 - 180.0


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def to_num(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def fetch_gaia_astrometry(source_ids: List[str], session: requests.Session, chunk_size=1200, tries=4) -> Dict[str, Dict[str, float]]:
    """Fetch Gaia DR3 astrometry for a list of Gaia DR3 source_ids.

    Returns: dict[source_id_str] -> {"ra":deg,"dec":deg,"parallax":mas}
    """
    out: Dict[str, Dict[str, float]] = {}
    if not source_ids:
        return out

    ids = [sid for sid in source_ids if sid]
    for part in _chunks(ids, chunk_size):
        adql = (
            "SELECT source_id, ra, dec, parallax "
            "FROM gaiadr3.gaia_source "
            f"WHERE source_id IN ({','.join(part)})"
        )
        payload = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": "csv",
            "QUERY": adql,
        }

        last = None
        for i in range(tries):
            try:
                r = session.post(GAIA_TAP_SYNC_URL, data=payload, timeout=(25, 900))
                r.raise_for_status()
                text = r.text or ""
                lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
                if not lines or "source_id" not in lines[0]:
                    raise RuntimeError("Unexpected Gaia TAP response format")

                reader = csv.DictReader(lines)
                for row in reader:
                    sid = normalize_gaia_dr3_id(row.get("source_id"))
                    if not sid:
                        continue
                    ra = to_num(row.get("ra"))
                    dec = to_num(row.get("dec"))
                    plx = to_num(row.get("parallax"))
                    if ra is None or dec is None:
                        continue
                    out[sid] = {"ra": ra, "dec": dec, "parallax": plx if plx is not None else float("nan")}
                break
            except Exception as e:
                last = e
                time.sleep(2.0 * (i + 1))
        # If Gaia fails for a chunk, keep building without it.
        _ = last

    return out


CONSTELLATION_GENITIVE = {
    "And": "Andromedae",
    "Ant": "Antliae",
    "Aps": "Apodis",
    "Aqr": "Aquarii",
    "Aql": "Aquilae",
    "Ara": "Arae",
    "Ari": "Arietis",
    "Aur": "Aurigae",
    "Boo": "Bootis",
    "Cae": "Caeli",
    "Cam": "Camelopardalis",
    "Cap": "Capricorni",
    "Car": "Carinae",
    "Cas": "Cassiopeiae",
    "Cen": "Centauri",
    "Cep": "Cephei",
    "Cet": "Ceti",
    "Cha": "Chamaeleontis",
    "Cir": "Circini",
    "CMa": "Canis Majoris",
    "CMi": "Canis Minoris",
    "Cnc": "Cancri",
    "Col": "Columbae",
    "Com": "Comae Berenices",
    "CrA": "Coronae Australis",
    "CrB": "Coronae Borealis",
    "Crt": "Crateris",
    "Cru": "Crucis",
    "Crv": "Corvi",
    "CVn": "Canum Venaticorum",
    "Cyg": "Cygni",
    "Del": "Delphini",
    "Dor": "Doradus",
    "Dra": "Draconis",
    "Equ": "Equulei",
    "Eri": "Eridani",
    "For": "Fornacis",
    "Gem": "Geminorum",
    "Gru": "Gruis",
    "Her": "Herculis",
    "Hor": "Horologii",
    "Hya": "Hydrae",
    "Hyi": "Hydri",
    "Ind": "Indi",
    "Lac": "Lacertae",
    "LMi": "Leonis Minoris",
    "Leo": "Leonis",
    "Lep": "Leporis",
    "Lib": "Librae",
    "Lup": "Lupi",
    "Lyn": "Lyncis",
    "Lyr": "Lyrae",
    "Men": "Mensae",
    "Mic": "Microscopii",
    "Mon": "Monocerotis",
    "Mus": "Muscae",
    "Nor": "Normae",
    "Oct": "Octantis",
    "Oph": "Ophiuchi",
    "Ori": "Orionis",
    "Pav": "Pavonis",
    "Peg": "Pegasi",
    "Per": "Persei",
    "Phe": "Phoenicis",
    "Pic": "Pictoris",
    "PsA": "Piscis Austrini",
    "Psc": "Piscium",
    "Pup": "Puppis",
    "Pyx": "Pyxidis",
    "Ret": "Reticuli",
    "Scl": "Sculptoris",
    "Sco": "Scorpii",
    "Sct": "Scuti",
    "Ser": "Serpentis",
    "Sex": "Sextantis",
    "Sge": "Sagittae",
    "Sgr": "Sagittarii",
    "Tau": "Tauri",
    "Tel": "Telescopii",
    "TrA": "Trianguli Australis",
    "Tri": "Trianguli",
    "Tuc": "Tucanae",
    "UMa": "Ursae Majoris",
    "UMi": "Ursae Minoris",
    "Vel": "Velorum",
    "Vir": "Virginis",
    "Vol": "Volantis",
    "Vul": "Vulpeculae",
}


def _const_genitive(abbrev):
    if not abbrev:
        return None
    key = re.sub(r"\.", "", str(abbrev))
    return CONSTELLATION_GENITIVE.get(key)


def beautify_system_name(raw):
    s = re.sub(r"\s+", " ", str("" if raw is None else raw)).strip()
    if not s:
        return s
    if re.match(r"^Proxima\s+Cen\b", s, flags=re.I):
        s = re.sub(r"^Proxima\s+Cen\b", "Proxima Centauri", s, flags=re.I)
    if re.match(r"^Alpha\s+Cen\b", s, flags=re.I):
        s = re.sub(r"^Alpha\s+Cen\b", "Alpha Centauri", s, flags=re.I)
    if re.match(r"^(GJ\s*551|Gl\s*551)\b", s, flags=re.I):
        s = "Proxima Centauri"

    def repl(m):
        n = m.group(1)
        ab = m.group(2)
        pl = m.group(3)
        g = _const_genitive(ab)
        if not g:
            return m.group(0)
        return f"{n} {g} {pl}" if pl else f"{n} {g}"

    s = re.sub(r"^(\d+)\s+([A-Za-z]{2,3})\b\s*([b-z])?\b", repl, s, flags=re.I)
    return s


def build_system_aliases(raw_name, pretty_name):
    out = set()
    r = re.sub(r"\s+", " ", str("" if raw_name is None else raw_name)).strip()
    p = re.sub(r"\s+", " ", str("" if pretty_name is None else pretty_name)).strip()
    if r:
        out.add(r)
    if p:
        out.add(p)
    if re.match(r"^(Proxima\s+Cen|Proxima\s+Centauri|GJ\s*551|Gl\s*551)\b", r, flags=re.I):
        out.update(["Proxima Centauri", "Proxima Cen", "GJ 551", "Gl 551", "Alpha Centauri"])
    if re.match(r"^(Alpha\s+Cen|Alpha\s+Centauri)\b", r, flags=re.I):
        out.update(["Alpha Centauri", "Alpha Cen", "Rigil Kentaurus"])
    b = beautify_system_name(r)
    if b and b != r:
        out.add(b)
    return list(out)


def format_exoplanet_display_name(host, pl_name_raw, letter_raw):
    """Return canonical exoplanet display name.

    Important: Planet letters should be lowercase (b, c, d...), to avoid collisions with stellar component
    designations (A, B, C...).
    """
    host_raw = ("" if host is None else str(host)).strip()
    host_name = beautify_system_name(host_raw)

    pl_name = ("" if pl_name_raw is None else str(pl_name_raw)).strip()
    if pl_name:
        # Keep as provided (no forced uppercase on terminal letter).
        return beautify_system_name(pl_name)

    letter = ("" if letter_raw is None else str(letter_raw)).strip()
    L = (letter.lower() if letter else "b")
    return (host_name + " " + L).strip() if host_name else L


def _adql_url(query: str, maxrec: int) -> str:
    # Keep commas unescaped and avoid introducing post-comma spaces.
    enc = requests.utils.quote(query, safe=",=*()'<>!+\n\t")
    enc = enc.replace("%20", "+")
    return f"https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query={enc}&format=json&maxrec={maxrec}"


def build_exo_download_url(ps_maxrec: int) -> str:
    # NOTE: ps table does not reliably expose gaia_dr3_id for all TAP variants.
    # Gaia IDs are enriched from stellarhosts instead.
    cols = ",".join(
        [
            "hostname",
            "sy_snum",
            "sy_pnum",
            "sy_dist",
            "ra",
            "dec",
            "cb_flag",
            "st_teff",
            "st_lum",
            "st_mass",
            "st_rad",
            "st_spectype",
            "pl_name",
            "pl_letter",
            "discoverymethod",
            "disc_year",
            "pul_flag",
            "ptv_flag",
            "etv_flag",
            "pl_orbper",
            "pl_orbsmax",
            "pl_rade",
            "pl_bmasse",
            "pl_dens",
            "pl_insol",
            "pl_eqt",
        ]
    )
    q = f"select {cols} from ps where default_flag=1 order by hostname asc"
    return _adql_url(q, ps_maxrec)


def build_exo_stellarhosts_url(sh_maxrec: int) -> str:
    cols = ",".join(
        [
            "sy_name",
            "hostname",
            "sy_snum",
            "sy_pnum",
            "sy_dist",
            "ra",
            "dec",
            "gaia_dr3_id",
            "cb_flag",
            "st_teff",
            "st_lum",
            "st_mass",
            "st_rad",
            "st_spectype",
        ]
    )
    q = f"select {cols} from stellarhosts where 1=1 and sy_snum>=2 order by sy_name asc, hostname asc"
    return _adql_url(q, sh_maxrec)


def fetch_json(url, session: requests.Session, tries=5):
    last = None
    for i in range(tries):
        try:
            r = session.get(url, timeout=(25, 900))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last


def _star_row_score(r: dict) -> int:
    # Prefer rows with more physical parameters.
    score = 0
    if normalize_gaia_dr3_id(r.get("gaia_dr3_id")):
        score += 6
    if to_num(r.get("st_rad")) is not None:
        score += 4
    if to_num(r.get("st_teff")) is not None:
        score += 3
    if to_num(r.get("st_lum")) is not None:
        score += 2
    if to_num(r.get("st_mass")) is not None:
        score += 2
    if r.get("st_spectype"):
        score += 1
    return score


def build_stellar_maps(sh_rows: list) -> Tuple[Dict[str, List[dict]], Dict[str, str], List[str]]:
    """Return (stars_by_system, host_to_system, gaia_ids).

    stars_by_system maps sy_name -> list of *deduplicated* stellarhosts rows.
    Deduping uses gaia_dr3_id when present, else hostname.
    """
    stars_by_system: Dict[str, Dict[str, Tuple[int, dict]]] = {}
    host_to_system: Dict[str, str] = {}
    gaia_ids: List[str] = []

    if not isinstance(sh_rows, list):
        return {}, {}, []

    for r in sh_rows:
        if not isinstance(r, dict):
            continue
        sys_name = ("" if r.get("sy_name") is None else str(r.get("sy_name"))).strip()
        host = ("" if r.get("hostname") is None else str(r.get("hostname"))).strip()
        if not sys_name:
            continue

        if host:
            host_to_system[host] = sys_name

        gid = normalize_gaia_dr3_id(r.get("gaia_dr3_id"))
        key = gid if gid else (host.lower() if host else None)
        if not key:
            continue

        sc = _star_row_score(r)
        bucket = stars_by_system.setdefault(sys_name, {})
        prev = bucket.get(key)
        if prev is None or sc > prev[0]:
            bucket[key] = (sc, r)

    # flatten + gaia list
    out_map: Dict[str, List[dict]] = {}
    seen_gaia = set()
    for sys_name, bucket in stars_by_system.items():
        rows = [t[1] for t in bucket.values()]
        out_map[sys_name] = rows
        for rr in rows:
            gid = normalize_gaia_dr3_id(rr.get("gaia_dr3_id"))
            if gid and gid not in seen_gaia:
                seen_gaia.add(gid)
                gaia_ids.append(gid)

    return out_map, host_to_system, gaia_ids


def _lum_from_log10(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        return 10 ** v
    except Exception:
        return None


def _estimate_radius_lum(teff: Optional[float], rad: Optional[float], lum: Optional[float], mass: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Fill missing (radius, lum) when possible.

    - If lum and teff present, infer radius (Stefan-Boltzmann).
    - If radius and teff present, infer lum.
    - If only teff present, use conservative main-sequence proxy for radius.
    - If only mass present, use rough main-sequence proxy for radius.
    """
    R = rad
    L = lum

    # Infer radius from lum + teff
    if (R is None or not math.isfinite(R) or R <= 0) and (L is not None and math.isfinite(L) and L > 0) and (teff is not None and math.isfinite(teff) and teff > 0):
        t = teff / TSUN_K
        if t > 0:
            R = math.sqrt(L) / (t * t)

    # Infer lum from radius + teff
    if (L is None or not math.isfinite(L) or L <= 0) and (R is not None and math.isfinite(R) and R > 0) and (teff is not None and math.isfinite(teff) and teff > 0):
        t = teff / TSUN_K
        L = (R * R) * (t ** 4)

    # Heuristic radius from teff (main-sequence-ish proxy)
    if (R is None or not math.isfinite(R) or R <= 0) and (teff is not None and math.isfinite(teff) and teff > 0):
        T = teff
        if T < 3200:
            R = 0.18
        elif T < 3700:
            R = 0.30
        elif T < 4200:
            R = 0.55
        elif T < 5200:
            R = 0.80
        elif T < 6000:
            R = 1.00
        elif T < 7500:
            R = 1.35
        elif T < 10000:
            R = 2.00
        else:
            R = 3.00

    # Heuristic radius from mass (if teff missing)
    if (R is None or not math.isfinite(R) or R <= 0) and (mass is not None and math.isfinite(mass) and mass > 0):
        m = mass
        if m < 0.43:
            R = 0.85 * m
        elif m < 2.0:
            R = m ** 0.8
        else:
            R = m ** 0.57

    # If we inferred a radius, infer lum if possible
    if (L is None or not math.isfinite(L) or L <= 0) and (R is not None and math.isfinite(R) and R > 0) and (teff is not None and math.isfinite(teff) and teff > 0):
        t = teff / TSUN_K
        L = (R * R) * (t ** 4)

    # keep None if still invalid
    if R is not None and (not math.isfinite(R) or R <= 0):
        R = None
    if L is not None and (not math.isfinite(L) or L <= 0):
        L = None

    return R, L


def _projected_pos_au(primary_ast: dict, comp_ast: dict, dist_pc: float) -> List[float]:
    ra0 = primary_ast.get("ra")
    dec0 = primary_ast.get("dec")
    ra = comp_ast.get("ra")
    dec = comp_ast.get("dec")
    if ra0 is None or dec0 is None or ra is None or dec is None:
        return [0.0, 0.0, 0.0]

    dra = _delta_ra_deg(ra, ra0)
    ddec = (dec - dec0)
    cosd = math.cos(math.radians(dec0))

    k = dist_pc * AU_PER_PC * (math.pi / 180.0)
    x = (dra * cosd) * k
    z = ddec * k
    return [x, 0.0, z]


def ingest_rows(
    rows: list,
    source_label: str,
    stars_by_system: Optional[Dict[str, List[dict]]] = None,
    host_to_system: Optional[Dict[str, str]] = None,
    gaia_astrometry: Optional[Dict[str, Dict[str, float]]] = None,
    planet_cap: int = 16,
) -> List[dict]:
    if not isinstance(rows, list):
        raise ValueError("Expected rows array")

    by_system: Dict[str, dict] = {}
    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    for r in rows:
        if not isinstance(r, dict):
            continue

        host = r.get("hostname") or r.get("pl_hostname") or ""
        host = str(host).strip()
        if not host:
            continue

        sys_key = host_to_system.get(host, host) if host_to_system else host
        sys = by_system.get(sys_key)

        if sys is None:
            teff = to_num(r.get("st_teff"))
            mass = to_num(r.get("st_mass"))
            rad = to_num(r.get("st_rad"))

            lum = None
            v = to_num(r.get("st_lum"))
            if v is not None:
                lum = _lum_from_log10(v)

            rad, lum = _estimate_radius_lum(teff, rad, lum, mass)

            snum = to_num(r.get("sy_snum"))
            snumi = int(snum) if snum is not None else None

            cat = "Single star"
            if snumi == 2:
                cat = "Binary stars"
            elif snumi is not None and snumi >= 3:
                cat = "Multi stars"

            spect = ("" if r.get("st_spectype") is None else str(r.get("st_spectype"))).upper()
            pul = 1 if to_num(r.get("pul_flag")) == 1 else 0
            ptv = 1 if to_num(r.get("ptv_flag")) == 1 else 0
            etv = 1 if to_num(r.get("etv_flag")) == 1 else 0
            if pul or ptv or etv or ("WD" in spect):
                cat = "Miscellaneous"

            sy_name_raw = sys_key
            sy_name = beautify_system_name(sy_name_raw)
            aliases = build_system_aliases(sy_name_raw, sy_name)

            # --- Build stars list from stellarhosts (deduped), else fall back to the single star from ps row.
            stars: List[dict]
            sh_list = (stars_by_system or {}).get(sy_name_raw)

            if sh_list:
                # sort so the planet-hosting star is first when possible
                def _pkey(sr: dict):
                    hn = ("" if sr.get("hostname") is None else str(sr.get("hostname"))).strip()
                    return (0 if hn.lower() == host.lower() else 1, hn)

                sh_sorted = sorted(sh_list, key=_pkey)

                # Determine distance (pc): prefer ps sy_dist, else stellarhosts sy_dist, else Gaia parallax.
                dist_pc = to_num(r.get("sy_dist"))
                if dist_pc is None and sh_sorted:
                    dist_pc = to_num(sh_sorted[0].get("sy_dist"))

                # Try Gaia parallax from the primary.
                primary_gid = normalize_gaia_dr3_id(sh_sorted[0].get("gaia_dr3_id"))
                if (dist_pc is None or dist_pc <= 0) and primary_gid and gaia_astrometry and primary_gid in gaia_astrometry:
                    plx = gaia_astrometry[primary_gid].get("parallax")
                    if plx is not None and math.isfinite(plx) and plx > 0:
                        dist_pc = 1000.0 / plx

                if dist_pc is None or not math.isfinite(dist_pc) or dist_pc <= 0:
                    dist_pc = 10.0

                primary_ast = None
                if primary_gid and gaia_astrometry:
                    primary_ast = gaia_astrometry.get(primary_gid)

                stars = []
                for idx, sr in enumerate(sh_sorted):
                    teff2 = to_num(sr.get("st_teff"))
                    mass2 = to_num(sr.get("st_mass"))
                    rad2 = to_num(sr.get("st_rad"))

                    lum2 = None
                    v2 = to_num(sr.get("st_lum"))
                    if v2 is not None:
                        lum2 = _lum_from_log10(v2)

                    rad2, lum2 = _estimate_radius_lum(teff2, rad2, lum2, mass2)

                    hn = sr.get("hostname")
                    hn = str(hn).strip() if isinstance(hn, str) and hn.strip() else (host if idx == 0 else f"{host} companion {idx+1}")

                    gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))

                    st: Dict[str, object] = {
                        "name": beautify_system_name(hn),
                        "type": "star",
                        "mass": mass2,
                        "radius": rad2,
                        "tempK": teff2,
                        "lum": lum2,
                        "gaiaDr3Id": gid,
                    }

                    # Gaia projected positions (relative to primary)
                    if gid and gaia_astrometry and primary_ast and (gid in gaia_astrometry):
                        if idx == 0:
                            st["posAU"] = [0.0, 0.0, 0.0]
                        else:
                            st["posAU"] = _projected_pos_au(primary_ast, gaia_astrometry[gid], dist_pc)
                            # Also provide a scalar separation hint for UIs that display orbit radius.
                            try:
                                x, y, z = st["posAU"]
                                st["orbitAU"] = float(math.sqrt(x * x + z * z))
                            except Exception:
                                pass

                    stars.append({k: v for k, v in st.items() if v is not None})

                # Ensure primary has orbit fields cleared if present
                if stars:
                    stars[0].pop("orbitAU", None)
            else:
                st = {
                    "name": beautify_system_name(host),
                    "type": "star",
                    "mass": mass if mass is not None else 1.0,
                    "radius": rad if rad is not None else 1.0,
                    "tempK": teff if teff is not None else TSUN_K,
                    "lum": lum,
                }
                stars = [{k: v for k, v in st.items() if v is not None}]

            cb0 = True if to_num(r.get("cb_flag")) == 1 else False

            sys = {
                "category": cat,
                "name": sy_name,
                "primaryName": sy_name,
                "syName": sy_name_raw,
                "rawName": sy_name_raw,
                "aliases": aliases,
                "circumbinary": True if cb0 else None,
                "catalogFlags": {
                    "cb": cb0,
                    "pul": bool(pul),
                    "ptv": bool(ptv),
                    "etv": bool(etv),
                    "sy_snum": snumi,
                },
                "discoveryMethods": [],
                "notes": f"Loaded from NASA Exoplanet Archive ({source_label}). Stars: {snumi if snumi is not None else len(stars)} (catalog); planets: truncated to first {planet_cap} for performance.",
                "__source": "NASA Exoplanet Archive",
                "__datasetVersion": source_label,
                "__retrievedAt": retrieved_at,
                "stars": stars,
                "planets": [],
            }
            by_system[sys_key] = sys

        if planet_cap is not None and len(sys.get("planets") or []) >= planet_cap:
            continue

        a_au = to_num(r.get("pl_orbsmax"))
        per = to_num(r.get("pl_orbper"))
        if a_au is None or per is None:
            continue

        r_e = to_num(r.get("pl_rade"))
        m_e = to_num(r.get("pl_bmasse"))
        dens = to_num(r.get("pl_dens"))
        insol = to_num(r.get("pl_insol"))
        eqt = to_num(r.get("pl_eqt"))

        pname = format_exoplanet_display_name(host, r.get("pl_name"), r.get("pl_letter"))
        disc_method = ("" if r.get("discoverymethod") is None else str(r.get("discoverymethod"))).strip() or None
        disc_year = to_num(r.get("disc_year"))

        cb = True if to_num(r.get("cb_flag")) == 1 else False
        pul = True if to_num(r.get("pul_flag")) == 1 else False
        ptv = True if to_num(r.get("ptv_flag")) == 1 else False
        etv = True if to_num(r.get("etv_flag")) == 1 else False

        if cb:
            sys["circumbinary"] = True
            sys.setdefault("catalogFlags", {})["cb"] = True
        if pul:
            sys.setdefault("catalogFlags", {})["pul"] = True
        if ptv:
            sys.setdefault("catalogFlags", {})["ptv"] = True
        if etv:
            sys.setdefault("catalogFlags", {})["etv"] = True

        planet = {
            "name": pname,
            "aAU": a_au,
            "periodDays": per,
            "radiusEarth": r_e if r_e is not None else 1.0,
            "massEarth": m_e,
            "density": dens,
            "insol": insol,
            "eqTempK": eqt,
            "discoveryMethod": disc_method,
            "discoveryYear": int(disc_year) if disc_year is not None else None,
            "circumbinary": True if cb else None,
            "detectionFlags": {"cb": cb, "pul": pul, "ptv": ptv, "etv": etv},
            "spinPeriodHours": per * 24.0,
        }
        sys["planets"].append({k: v for k, v in planet.items() if v is not None})
        if disc_method and disc_method not in sys["discoveryMethods"]:
            sys["discoveryMethods"].append(disc_method)

    return list(by_system.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ps-maxrec", type=int, required=True)
    ap.add_argument("--sh-maxrec", type=int, required=True)
    ap.add_argument("--output", type=str, required=True)
    ap.add_argument("--planet-cap", type=int, default=16)
    args = ap.parse_args()

    ps_url = build_exo_download_url(args.ps_maxrec)
    sh_url = build_exo_stellarhosts_url(args.sh_maxrec)

    sess = requests.Session()
    sess.headers.update({"User-Agent": "MCS-Education-NASA-Archive-Updater/1.1"})

    ps_rows = fetch_json(ps_url, sess)
    sh_rows = fetch_json(sh_url, sess)

    stars_by_system, host_to_system, gaia_ids = build_stellar_maps(sh_rows)

    # Gaia enrichment (best-effort).
    gaia_ast = {}
    try:
        if gaia_ids:
            gaia_ast = fetch_gaia_astrometry(gaia_ids, sess)
    except Exception:
        gaia_ast = {}

    source_label = f"TAP/ps default_flag maxrec {args.ps_maxrec}"
    systems = ingest_rows(ps_rows, source_label, stars_by_system, host_to_system, gaia_astrometry=gaia_ast, planet_cap=args.planet_cap)

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    out = {
        "meta": {
            "source": "NASA Exoplanet Archive",
            "generatedAt": now,
            "psMaxrec": args.ps_maxrec,
            "stellarHostsMaxrec": args.sh_maxrec,
            "psUrl": ps_url,
            "stellarHostsUrl": sh_url,
            "datasetVersion": source_label,
            "gaiaEnrichment": {
                "enabled": True,
                "uniqueSourceIds": len(gaia_ids),
                "resolved": len(gaia_ast),
            },
        },
        "systems": systems,
    }

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
