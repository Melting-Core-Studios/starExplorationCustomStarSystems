import argparse, csv, json, math, os, re, time
from datetime import datetime, timezone
import requests

# ESA Gaia Archive TAP synchronous endpoint.
# Used to fetch per-component astrometry (RA/Dec/parallax) for Gaia DR3 source_ids.
GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"

def normalize_gaia_dr3_id(v):
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
    # Sometimes APIs prepend labels (e.g., "Gaia DR3 ..."); keep only digits.
    digits = re.sub(r"\D+", "", s)
    return digits if digits else None

def _delta_ra_deg(ra_deg, ra0_deg):
    """Smallest RA difference in degrees in [-180, 180]."""
    d = (ra_deg - ra0_deg + 540.0) % 360.0 - 180.0
    return d

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_gaia_astrometry(source_ids, session, chunk_size=1800, tries=4):
    """Fetch Gaia DR3 astrometry for a list of Gaia DR3 source_ids.

    Returns: dict[source_id_str] -> {"ra":deg,"dec":deg,"parallax":mas}
    """
    out = {}
    if not source_ids:
        return out

    # Synchronous queries commonly enforce ~2000 row limits; keep chunks below that.
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
                r = session.post(GAIA_TAP_SYNC_URL, data=payload, timeout=(20, 900))
                r.raise_for_status()
                text = r.text or ""
                # Gaia TAP returns CSV (or a VOTable/error html). Guard CSV parse.
                if "source_id" not in text.splitlines()[0:1][0] if text.splitlines() else True:
                    # Not CSV header; treat as transient.
                    raise RuntimeError("Unexpected Gaia TAP response format")
                reader = csv.DictReader(text.splitlines())
                for row in reader:
                    sid = normalize_gaia_dr3_id(row.get("source_id"))
                    if not sid:
                        continue
                    ra = to_num(row.get("ra"))
                    dec = to_num(row.get("dec"))
                    plx = to_num(row.get("parallax"))
                    if ra is None or dec is None:
                        continue
                    out[sid] = {"ra": ra, "dec": dec, "parallax": plx}
                break
            except Exception as e:
                last = e
                time.sleep(2.0 * (i + 1))
        else:
            # Partial enrichment is acceptable; keep building the catalog.
            # (We don't raise; we just proceed without Gaia positions for this chunk.)
            pass
    return out

CONSTELLATION_GENITIVE={"And":"Andromedae","Ant":"Antliae","Aps":"Apodis","Aqr":"Aquarii","Aql":"Aquilae","Ara":"Arae","Ari":"Arietis","Aur":"Aurigae","Boo":"Bootis","Cae":"Caeli","Cam":"Camelopardalis","Cap":"Capricorni","Car":"Carinae","Cas":"Cassiopeiae","Cen":"Centauri","Cep":"Cephei","Cet":"Ceti","Cha":"Chamaeleontis","Cir":"Circini","CMa":"Canis Majoris","CMi":"Canis Minoris","Cnc":"Cancri","Col":"Columbae","Com":"Comae Berenices","CrA":"Coronae Australis","CrB":"Coronae Borealis","Crt":"Crateris","Cru":"Crucis","Crv":"Corvi","CVn":"Canum Venaticorum","Cyg":"Cygni","Del":"Delphini","Dor":"Doradus","Dra":"Draconis","Equ":"Equulei","Eri":"Eridani","For":"Fornacis","Gem":"Geminorum","Gru":"Gruis","Her":"Herculis","Hor":"Horologii","Hya":"Hydrae","Hyi":"Hydri","Ind":"Indi","Lac":"Lacertae","LMi":"Leonis Minoris","Leo":"Leonis","Lep":"Leporis","Lib":"Librae","Lup":"Lupi","Lyn":"Lyncis","Lyr":"Lyrae","Men":"Mensae","Mic":"Microscopii","Mon":"Monocerotis","Mus":"Muscae","Nor":"Normae","Oct":"Octantis","Oph":"Ophiuchi","Ori":"Orionis","Pav":"Pavonis","Peg":"Pegasi","Per":"Persei","Phe":"Phoenicis","Pic":"Pictoris","PsA":"Piscis Austrini","Psc":"Piscium","Pup":"Puppis","Pyx":"Pyxidis","Ret":"Reticuli","Scl":"Sculptoris","Sco":"Scorpii","Sct":"Scuti","Ser":"Serpentis","Sex":"Sextantis","Sge":"Sagittae","Sgr":"Sagittarii","Tau":"Tauri","Tel":"Telescopii","TrA":"Trianguli Australis","Tri":"Trianguli","Tuc":"Tucanae","UMa":"Ursae Majoris","UMi":"Ursae Minoris","Vel":"Velorum","Vir":"Virginis","Vol":"Volantis","Vul":"Vulpeculae"}

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
        out.update(["Proxima Centauri","Proxima Cen","GJ 551","Gl 551","Alpha Centauri"])
    if re.match(r"^(Alpha\s+Cen|Alpha\s+Centauri)\b", r, flags=re.I):
        out.update(["Alpha Centauri","Alpha Cen","Rigil Kentaurus"])
    b = beautify_system_name(r)
    if b and b != r:
        out.add(b)
    return list(out)

def format_exoplanet_display_name(host, pl_name_raw, letter_raw):
    host_raw = ("" if host is None else str(host)).strip()
    host_name = beautify_system_name(host_raw)
    pl_name = ("" if pl_name_raw is None else str(pl_name_raw)).strip()
    letter = ("" if letter_raw is None else str(letter_raw)).strip()
    if pl_name:
        pl_name = re.sub(r"\s+([a-z])$", lambda m: " " + m.group(1).upper(), pl_name)
        if not re.fullmatch(r"[A-Za-z]", pl_name):
            return beautify_system_name(pl_name)
    L = letter.upper() if letter else "B"
    return (host_name + " " + L).strip() if host_name else L

def to_num(x):
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def build_exo_download_url(ps_maxrec):
    where = "default_flag=1"
    q = " ".join([
        "select hostname,sy_snum,sy_pnum,sy_dist,ra,dec,gaia_dr3_id,cb_flag,st_teff,st_lum,st_mass,st_rad,st_spectype,",
        "pl_name,pl_letter,discoverymethod,disc_year,pul_flag,ptv_flag,etv_flag,",
        "pl_orbper,pl_orbsmax,pl_rade,pl_bmasse,pl_dens,pl_insol,pl_eqt",
        "from ps",
        f"where {where}",
        "order by hostname asc"
    ])
    enc = requests.utils.quote(q, safe="").replace("%20","+")
    return f"https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query={enc}&format=json&maxrec={ps_maxrec}"

def build_exo_stellarhosts_url(sh_maxrec):
    q = " ".join([
        "select sy_name,hostname,sy_snum,sy_pnum,sy_dist,ra,dec,gaia_dr3_id,cb_flag,st_teff,st_lum,st_mass,st_rad,st_spectype",
        "from stellarhosts",
        "where 1=1 and sy_snum>=2",
        "order by sy_name asc, hostname asc"
    ])
    enc = requests.utils.quote(q, safe="").replace("%20","+")
    return f"https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query={enc}&format=json&maxrec={sh_maxrec}"

def fetch_json(url, session, tries=5):
    last = None
    for i in range(tries):
        try:
            r = session.get(url, timeout=(20, 900))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last

def build_stellar_maps(sh_rows):
    stars_by_system = {}
    host_to_system = {}
    if not isinstance(sh_rows, list):
        return None, None
    for r in sh_rows:
        if not isinstance(r, dict):
            continue
        sys_name = ("" if r.get("sy_name") is None else str(r.get("sy_name"))).strip()
        host = ("" if r.get("hostname") is None else str(r.get("hostname"))).strip()
        if sys_name:
            stars_by_system.setdefault(sys_name, []).append(r)
        if sys_name and host:
            host_to_system[host] = sys_name
    return stars_by_system, host_to_system

def ingest_rows(rows, source_label, stars_by_system=None, host_to_system=None, gaia_astrometry=None, planet_cap=16):
    if not isinstance(rows, list):
        raise ValueError("Expected rows array")
    by_system = {}
    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
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
                lum = 10 ** v
            if lum is None and teff is not None and rad is not None:
                t = teff / 5772.0
                lum = (rad * rad) * (t ** 4)
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
            stars = None
            if stars_by_system and sy_name_raw in stars_by_system:
                lst = stars_by_system.get(sy_name_raw) or []
                if lst:
                    stars = []
                    # Prefer the star whose hostname matches the planetary host as the primary.
                    lst = sorted(lst, key=lambda sr: (str(sr.get('hostname') or '').strip() != host, str(sr.get('hostname') or '').strip()))
                    for i, sr in enumerate(lst):
                        teff2 = to_num(sr.get("st_teff"))
                        mass2 = to_num(sr.get("st_mass"))
                        rad2 = to_num(sr.get("st_rad"))
                        lum2 = None
                        v2 = to_num(sr.get("st_lum"))
                        if v2 is not None:
                            lum2 = 10 ** v2
                        if lum2 is None and teff2 is not None and rad2 is not None:
                            t2 = teff2 / 5772.0
                            lum2 = (rad2 * rad2) * (t2 ** 4)
                        a_au = 0.0 if i == 0 else 0.18 * i
                        p_days = 0.0 if i == 0 else (25 + i * 7 if cat == "Binary stars" else (50 + i * 12))
                        ph = 0.0 if i == 0 else 0.17 * i
                        hn = sr.get("hostname")
                        hn = str(hn).strip() if isinstance(hn, str) and hn.strip() else (host if i == 0 else f"{host} companion {i+1}")
                        gaia_id = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                        st = {
                            "name": beautify_system_name(hn),
                            "type": "star",
                            "mass": mass2,
                            "radius": rad2,
                            "tempK": teff2,
                            "lum": lum2,
                            "gaiaDr3Id": gaia_id,
                            "orbitAU": a_au,
                            "periodDays": p_days,
                            "phase": ph
                        }
                        stars.append({k:v for k,v in st.items() if v is not None})
                    if stars:
                        stars[0]["orbitAU"] = 0.0
                        stars[0]["periodDays"] = 0.0
                        stars[0]["phase"] = 0.0

                        # If Gaia astrometry is available, compute physically-based component offsets.
                        # We project separations onto the sky plane (RA/Dec) and convert to AU using system distance.
                        # 1 arcsec at 1 pc equals 1 AU.
                        if gaia_astrometry and len(stars) >= 2:
                            # Use system distance in pc when available (from PS row), else fall back to Gaia parallax.
                            dist_pc = to_num(r.get("sy_dist"))
                            # Choose a reference star with Gaia coordinates.
                            ref_idx = 0
                            if not stars[0].get("gaiaDr3Id"):
                                for j, s in enumerate(stars):
                                    if s.get("gaiaDr3Id"):
                                        ref_idx = j
                                        break
                            ref_id = stars[ref_idx].get("gaiaDr3Id")
                            ref_ast = gaia_astrometry.get(ref_id) if ref_id else None
                            if ref_ast:
                                if dist_pc is None:
                                    plx = to_num(ref_ast.get("parallax"))
                                    if plx is not None and plx > 0:
                                        dist_pc = 1000.0 / plx
                                if dist_pc is not None and dist_pc > 0:
                                    ra0 = to_num(ref_ast.get("ra"))
                                    dec0 = to_num(ref_ast.get("dec"))
                                    if ra0 is not None and dec0 is not None:
                                        dec0r = math.radians(dec0)
                                        for j, s in enumerate(stars):
                                            sid = s.get("gaiaDr3Id")
                                            ast = gaia_astrometry.get(sid) if sid else None
                                            if j == ref_idx or not ast:
                                                s["posAU"] = [0.0, 0.0, 0.0]
                                                s["posSource"] = "gaia_dr3" if ast else None
                                                continue
                                            ra = to_num(ast.get("ra"))
                                            dec = to_num(ast.get("dec"))
                                            if ra is None or dec is None:
                                                continue
                                            dra_deg = _delta_ra_deg(ra, ra0)
                                            ddec_deg = dec - dec0
                                            dx_arcsec = dra_deg * math.cos(dec0r) * 3600.0
                                            dz_arcsec = ddec_deg * 3600.0
                                            x_au = dx_arcsec * dist_pc
                                            z_au = dz_arcsec * dist_pc
                                            s["posAU"] = [x_au, 0.0, z_au]
                                            s["posSource"] = "gaia_dr3"
                                        # Ensure the reference star is at the origin.
                                        stars[ref_idx]["posAU"] = [0.0, 0.0, 0.0]
                                        stars[ref_idx]["posSource"] = "gaia_dr3"
            if stars is None:
                st = {
                    "name": beautify_system_name(host),
                    "type": "star",
                    "mass": mass if mass is not None else 1.0,
                    "radius": rad if rad is not None else 1.0,
                    "tempK": teff if teff is not None else 5772.0,
                    "lum": lum,
                    "orbitAU": 0.0,
                    "periodDays": 0.0,
                    "phase": 0.0
                }
                stars = [{k:v for k,v in st.items() if v is not None}]
            cb0 = True if to_num(r.get("cb_flag")) == 1 else False
            sys = {
                "category": cat,
                "name": sy_name,
                "primaryName": sy_name,
                "syName": sy_name_raw,
                "rawName": sy_name_raw,
                "aliases": aliases,
                "circumbinary": True if cb0 else None,
                "catalogFlags": {"cb": cb0, "pul": bool(pul), "ptv": bool(ptv), "etv": bool(etv), "sy_snum": snumi},
                "discoveryMethods": [],
                "notes": f"Loaded from NASA Exoplanet Archive ({source_label}). Stars: {snumi if snumi is not None else len(stars)} (catalog); planets: truncated to first {planet_cap} for performance.",
                "__source": "NASA Exoplanet Archive",
                "__datasetVersion": source_label,
                "__retrievedAt": retrieved_at,
                "stars": stars,
                "planets": []
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
            "spinPeriodHours": per * 24.0
        }
        sys["planets"].append({k:v for k,v in planet.items() if v is not None})
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
    sess.headers.update({"User-Agent":"MCS-Education-NASA-Archive-Updater/1.0"})
    ps_rows = fetch_json(ps_url, sess)
    sh_rows = fetch_json(sh_url, sess)
    stars_by_system, host_to_system = build_stellar_maps(sh_rows)

    # Gaia enrichment: fetch RA/Dec/parallax for the Gaia DR3 source_ids of stellar components
    # in systems that appear in the PS query results. This lets us compute physically-based
    # projected separations for multi-star systems.
    planet_system_keys = set()
    if isinstance(ps_rows, list) and host_to_system:
        for r in ps_rows:
            if not isinstance(r, dict):
                continue
            host = r.get("hostname") or r.get("pl_hostname") or ""
            host = str(host).strip()
            if not host:
                continue
            planet_system_keys.add(host_to_system.get(host, host))

    gaia_ids = set()
    if stars_by_system and planet_system_keys:
        for sys_key in planet_system_keys:
            for sr in (stars_by_system.get(sys_key) or []):
                gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                if gid:
                    gaia_ids.add(gid)
    gaia_astrometry = fetch_gaia_astrometry(sorted(gaia_ids), sess) if gaia_ids else {}
    source_label = f"TAP/ps default_flag maxrec {args.ps_maxrec}"
    systems = ingest_rows(ps_rows, source_label, stars_by_system, host_to_system, gaia_astrometry, planet_cap=args.planet_cap)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
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
                "resolved": len(gaia_astrometry)
            }
        },
        "systems": systems
    }
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",",":"))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
