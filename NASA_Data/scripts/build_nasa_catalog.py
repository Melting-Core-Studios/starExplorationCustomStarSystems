import argparse, json, math, os, re, time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import requests

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
    # Prefer the archive-provided planet name; keep the conventional lowercase planet letter
    # to avoid confusion with stellar component designations (A/B/C).
    if pl_name:
        if not re.fullmatch(r"[A-Za-z]", pl_name):
            return beautify_system_name(pl_name)
    L = (letter.lower() if letter else "b")
    return (host_name + " " + L).strip() if host_name else L

def to_num(x):
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def normalize_gaia_dr3_id(v):
    """Return a Gaia DR3 source_id as a digit-only string (or None).

    The Exoplanet Archive exposes gaia_dr3_id as a character column, so it may
    arrive as digits, whitespace-padded, or (rarely) in scientific notation.
    """
    if v is None or v == "" or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        # Floats cannot exactly represent large integers, but this is better
        # than the prior behaviour of concatenating exponent digits.
        return str(int(v))
    s = str(v).strip()
    if not s:
        return None
    if "e" in s.lower():
        try:
            d = Decimal(s)
            if not d.is_finite():
                return None
            return str(int(d))
        except (InvalidOperation, ValueError):
            return None
    digits = re.sub(r"\D+", "", s)
    return digits if digits else None

def fetch_gaia_tap_csv(query, session, tries=4):
    """Run an ADQL query against the Gaia TAP endpoint and return raw CSV text."""
    url = "https://gea.esac.esa.int/tap-server/tap/sync"
    last = None
    for i in range(tries):
        try:
            r = session.post(url, data={"REQUEST":"doQuery","LANG":"ADQL","FORMAT":"csv","QUERY":query}, timeout=(30, 900))
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last

def _parse_gaia_csv(text):
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return [], []
    header = [h.strip() for h in lines[0].split(",")]
    rows = []
    for ln in lines[1:]:
        cols = [c.strip() for c in ln.split(",")]
        if len(cols) != len(header):
            continue
        rows.append(dict(zip(header, cols)))
    return header, rows

def fetch_gaia_astrometry(source_ids, session, chunk_size=1500):
    """Fetch Gaia DR3 astrometry + basic photometry for a list of source_ids."""
    ids = [str(x) for x in source_ids if x]
    ids = list(dict.fromkeys(ids))
    out = {}
    if not ids:
        return out
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i+chunk_size]
        id_list = ",".join(chunk)
        q = " ".join([
            "select source_id, ra, dec, parallax, pmra, pmdec,",
            "phot_g_mean_mag, bp_rp",
            "from gaiadr3.gaia_source",
            f"where source_id in ({id_list})"
        ])
        text = fetch_gaia_tap_csv(q, session=session)
        _, rows = _parse_gaia_csv(text)
        for r in rows:
            sid = normalize_gaia_dr3_id(r.get("source_id"))
            if not sid:
                continue
            out[sid] = {
                "ra": to_num(r.get("ra")),
                "dec": to_num(r.get("dec")),
                "parallax": to_num(r.get("parallax")),
                "pmra": to_num(r.get("pmra")),
                "pmdec": to_num(r.get("pmdec")),
                "gmag": to_num(r.get("phot_g_mean_mag")),
                "bp_rp": to_num(r.get("bp_rp")),
            }
        time.sleep(0.12)
    return out

def fetch_gaia_astrophysical_params(source_ids, session, chunk_size=1500):
    """Fetch Gaia DR3 GSP-Phot Teff and radius where available."""
    ids = [str(x) for x in source_ids if x]
    ids = list(dict.fromkeys(ids))
    out = {}
    if not ids:
        return out
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i+chunk_size]
        id_list = ",".join(chunk)
        q = " ".join([
            "select source_id, teff_gspphot, radius_gspphot",
            "from gaiadr3.astrophysical_parameters",
            f"where source_id in ({id_list})"
        ])
        text = fetch_gaia_tap_csv(q, session=session)
        _, rows = _parse_gaia_csv(text)
        for r in rows:
            sid = normalize_gaia_dr3_id(r.get("source_id"))
            if not sid:
                continue
            out[sid] = {
                "teff_gspphot": to_num(r.get("teff_gspphot")),
                "radius_gspphot": to_num(r.get("radius_gspphot")),
            }
        time.sleep(0.12)
    return out

def build_exo_download_url(ps_maxrec):
    where = "default_flag=1"
    q = " ".join([
        "select sy_name,hostname,sy_snum,sy_pnum,sy_dist,ra,dec,gaia_dr3_id,",
        "cb_flag,st_teff,st_lum,st_mass,st_rad,st_spectype,",
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
        "select sy_name,hostname,sy_snum,sy_dist,ra,dec,gaia_dr3_id,cb_flag,",
        "st_teff,st_lum,st_mass,st_rad,st_spectype",
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

def ingest_rows(
    rows,
    source_label,
    stars_by_system=None,
    host_to_system=None,
    planet_cap=16,
    gaia_astrometry=None,
    gaia_ap=None,
):
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
            # ---- Stars (deduped) -------------------------------------------------
            AU_PER_PC = 206264.80624709636
            primary_gid = normalize_gaia_dr3_id(r.get("gaia_dr3_id"))
            dist_pc = to_num(r.get("sy_dist"))

            def _row_score(sr):
                # Prefer rows with more populated astrophysical fields.
                score = 0
                for k in ("st_teff","st_rad","st_mass","st_lum","st_spectype","gaia_dr3_id"):
                    v = sr.get(k)
                    if v is None or v == "":
                        continue
                    score += 1
                return score

            stars = []

            if stars_by_system and sy_name_raw in stars_by_system:
                lst = stars_by_system.get(sy_name_raw) or []
                if lst:
                    groups = {}
                    for sr in lst:
                        if not isinstance(sr, dict):
                            continue
                        gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                        hn0 = ("" if sr.get("hostname") is None else str(sr.get("hostname"))).strip()
                        key = gid or ("name:" + hn0.lower())
                        groups.setdefault(key, []).append(sr)

                    picked = []
                    for _, group in groups.items():
                        best = max(group, key=_row_score)
                        picked.append(best)

                    def _sort_key(sr):
                        gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                        hn = ("" if sr.get("hostname") is None else str(sr.get("hostname"))).strip()
                        is_primary = 1 if (primary_gid and gid == primary_gid) else 0
                        # Component suffix ordering if present (A/B/C...).
                        m = re.search(r"\s([A-Z])$", hn.strip())
                        comp = (ord(m.group(1)) - ord('A') + 1) if m else 999
                        return (-is_primary, comp, hn.lower())

                    picked.sort(key=_sort_key)

                    for i, sr in enumerate(picked):
                        gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                        hn = ("" if sr.get("hostname") is None else str(sr.get("hostname"))).strip()
                        if not hn:
                            hn = host if i == 0 else f"{host} companion {i+1}"

                        teff2 = to_num(sr.get("st_teff"))
                        mass2 = to_num(sr.get("st_mass"))
                        rad2 = to_num(sr.get("st_rad"))
                        lum2 = None
                        v2 = to_num(sr.get("st_lum"))
                        if v2 is not None:
                            lum2 = 10 ** v2

                        # Gaia GSP-Phot enrichment (Teff + radius).
                        if gid and gaia_ap and gid in gaia_ap:
                            ap = gaia_ap[gid]
                            if teff2 is None and ap.get("teff_gspphot") is not None:
                                teff2 = ap.get("teff_gspphot")
                            if rad2 is None and ap.get("radius_gspphot") is not None:
                                rad2 = ap.get("radius_gspphot")

                        if lum2 is None and teff2 is not None and rad2 is not None:
                            t2 = teff2 / 5772.0
                            lum2 = (rad2 * rad2) * (t2 ** 4)

                        st = {
                            "name": beautify_system_name(hn),
                            "type": "star",
                            "mass": mass2,
                            "radius": rad2,
                            "tempK": teff2,
                            "lum": lum2,
                            "gaiaDr3Id": gid,
                        }
                        stars.append({k:v for k,v in st.items() if v is not None})

            # Fallback: single-star systems or systems with no stellarhosts rows.
            if not stars:
                gid = primary_gid
                teff1 = teff
                rad1 = rad
                # Optional Gaia enrichment for single stars as well.
                if gid and gaia_ap and gid in gaia_ap:
                    ap = gaia_ap[gid]
                    if teff1 is None and ap.get("teff_gspphot") is not None:
                        teff1 = ap.get("teff_gspphot")
                    if rad1 is None and ap.get("radius_gspphot") is not None:
                        rad1 = ap.get("radius_gspphot")
                lum1 = lum
                if lum1 is None and teff1 is not None and rad1 is not None:
                    t1 = teff1 / 5772.0
                    lum1 = (rad1 * rad1) * (t1 ** 4)
                st = {
                    "name": beautify_system_name(host),
                    "type": "star",
                    "mass": mass if mass is not None else 1.0,
                    "radius": rad1 if rad1 is not None else 1.0,
                    "tempK": teff1 if teff1 is not None else 5772.0,
                    "lum": lum1,
                    "gaiaDr3Id": gid,
                }
                stars = [{k:v for k,v in st.items() if v is not None}]

            # Per-star positions from Gaia astrometry (projected on-sky separation).
            try:
                if gaia_astrometry and primary_gid and primary_gid in gaia_astrometry:
                    base = gaia_astrometry[primary_gid]
                    # If the Exoplanet Archive doesn't provide sy_dist, fall back to Gaia parallax.
                    if dist_pc is None:
                        par = base.get("parallax")
                        if par is not None and par > 0:
                            dist_pc = 1000.0 / par
                    if dist_pc is None:
                        raise ValueError("Missing distance")
                    ra0 = base.get("ra")
                    dec0 = base.get("dec")
                    if ra0 is not None and dec0 is not None:
                        k = dist_pc * AU_PER_PC * (math.pi / 180.0)
                        cdec = math.cos(math.radians(dec0))
                        for s in stars:
                            gid = s.get("gaiaDr3Id")
                            if not gid or gid not in gaia_astrometry:
                                continue
                            if gid == primary_gid:
                                s["posAU"] = [0.0, 0.0, 0.0]
                                continue
                            g = gaia_astrometry[gid]
                            ra = g.get("ra")
                            dec = g.get("dec")
                            if ra is None or dec is None:
                                continue
                            dra = (ra - ra0 + 180.0) % 360.0 - 180.0
                            ddec = (dec - dec0)
                            x = dra * cdec * k
                            z = ddec * k
                            s["posAU"] = [float(x), 0.0, float(z)]
            except Exception:
                pass
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

    # Gaia enrichment is limited to multi-star systems (stellarhosts table is
    # already filtered to sy_snum>=2). This keeps the Gaia TAP workload
    # manageable while fixing the star placement problem comprehensively.
    gaia_ids = []
    if isinstance(sh_rows, list):
        for sr in sh_rows:
            if not isinstance(sr, dict):
                continue
            gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
            if gid:
                gaia_ids.append(gid)
    gaia_ids = list(dict.fromkeys(gaia_ids))

    gaia_ast = fetch_gaia_astrometry(gaia_ids, session=sess) if gaia_ids else {}
    gaia_ap = fetch_gaia_astrophysical_params(gaia_ids, session=sess) if gaia_ids else {}

    source_label = f"TAP/ps default_flag maxrec {args.ps_maxrec}"
    systems = ingest_rows(
        ps_rows,
        source_label,
        stars_by_system,
        host_to_system,
        planet_cap=args.planet_cap,
        gaia_astrometry=gaia_ast,
        gaia_ap=gaia_ap,
    )
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
                "uniqueGaiaIds": len(gaia_ids),
                "astrometryRows": len(gaia_ast),
                "astrophysicalRows": len(gaia_ap),
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
