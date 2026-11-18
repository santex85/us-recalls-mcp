"""Fetchers + normalisers for US recall sources (openFDA, NHTSA, CPSC).

Every record is normalised to one flat schema (see FIELDS). Only product/firm-level
data is emitted: no personal contact names, phone numbers or e-mails are copied.
"""
import asyncio
import io
import logging
import time
import re
import zipfile
from datetime import date, datetime, timedelta
from urllib.parse import quote

import httpx

FIELDS = [
    "source", "agency", "recallId", "date", "initiatedDate", "title", "product", "firm",
    "firmLocation", "reason", "consequence", "classification", "status", "distribution",
    "quantity", "remedy", "codeInfo", "url", "apiUrl",
]

FDA_SOURCES = {"fda_food": "food", "fda_drug": "drug", "fda_device": "device"}
FDA_PAGE = 1000          # openFDA max limit per request
FDA_MAX_SKIP = 25000     # openFDA hard cap on skip
NHTSA_FLAT = "https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip"
NHTSA_TYPES = {"V": "Vehicle", "E": "Equipment", "C": "Child restraint", "T": "Tire"}
CPSC_API = "https://www.saferproducts.gov/RestWebServices/Recall"

_PHONE = re.compile(r"\(?\b\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b")
_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")


def _iso(yyyymmdd: str | None) -> str | None:
    s = (yyyymmdd or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return None


def _clean(text: str | None, limit: int = 2000) -> str | None:
    if not text:
        return None
    t = re.sub(r"\s+", " ", str(text)).strip()
    return (t[: limit - 1] + "…") if len(t) > limit else (t or None)


def _blank() -> dict:
    return {k: None for k in FIELDS}


class FetchError(Exception):
    pass


async def get_json(client: httpx.AsyncClient, url: str, log=None, params=None, ok404=False):
    """GET JSON with 3 short retries. Returns None for openFDA "no matches" 404; raises FetchError otherwise."""
    log = log or logging.getLogger("recalls")
    err = ""
    for attempt in range(3):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404 and ok404:
                return None  # openFDA answers 404 NOT_FOUND for "no matches"
            err = f"HTTP {r.status_code}"
            if r.status_code == 429:
                log.warning(f"Rate limited (429) on {url.split('?')[0]}")
                await asyncio.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 400 and '"Count":0' in r.text[:200]:
                return {"Count": 0, "results": []}  # NHTSA answers 400 for "no results"
            if 400 <= r.status_code < 500:
                raise FetchError(f"{err}: {r.text[:200]}")
            log.warning(f"{url.split('?')[0]} -> {err}: {r.text[:200]}")
        except (httpx.HTTPError, ValueError) as e:
            err = f"{type(e).__name__}: {e}"
            log.warning(f"{url.split('?')[0]} -> {err}")
        if attempt < 2:
            await asyncio.sleep(1 + attempt * 2)
    raise FetchError(err or "request failed")


# ---------------------------------------------------------------- openFDA
async def fetch_fda(client, source: str, d_from: date | None, d_to: date | None, keywords: list[str],
                    api_key: str | None, cap: int, log=None, include_raw: bool = False,
                    search: str | None = None) -> list[dict]:
    """openFDA enforcement reports, newest report_date first. `search` overrides the built query."""
    cat = FDA_SOURCES[source]
    if search is None:
        parts = []
        if d_from or d_to:
            parts.append(f"report_date:[{(d_from or date(2004, 1, 1)):%Y%m%d}+TO+{(d_to or date.today()):%Y%m%d}]")
        if keywords:
            terms = []
            for kw in keywords:
                q = quote(f'"{kw}"')
                terms += [f"product_description:{q}", f"recalling_firm:{q}", f"reason_for_recall:{q}"]
            parts.append("(" + "+".join(terms) + ")")
        search = "+AND+".join(parts)
    out, skip = [], 0
    while len(out) < cap and skip <= FDA_MAX_SKIP:
        limit = min(FDA_PAGE, cap - len(out))
        url = (f"https://api.fda.gov/{cat}/enforcement.json?" + (f"search={search}&" if search else "")
               + f"sort=report_date:desc&limit={limit}&skip={skip}")
        if api_key:
            url += f"&api_key={quote(api_key)}"
        data = await get_json(client, url, log, ok404=True)
        rows = (data or {}).get("results") or []
        for r in rows:
            out.append(norm_fda(source, r, include_raw))
        total = ((data or {}).get("meta") or {}).get("results", {}).get("total", 0)
        skip += len(rows)
        if not rows or skip >= total:
            break
        await asyncio.sleep(0.5)  # stay far below 240 req/min
    return out


def norm_fda(source: str, r: dict, include_raw: bool) -> dict:
    cat = FDA_SOURCES[source]
    rec = _blank()
    rn = r.get("recall_number")
    product = _clean(r.get("product_description"), 1500)
    rec.update({
        "source": source,
        "agency": "FDA",
        "recallId": rn,
        "date": _iso(r.get("report_date")),
        "initiatedDate": _iso(r.get("recall_initiation_date")),
        "title": _clean(product, 160),
        "product": product,
        "firm": _clean(r.get("recalling_firm"), 200),
        "firmLocation": ", ".join(x for x in [r.get("city"), r.get("state"), r.get("country")] if x) or None,
        "reason": _clean(r.get("reason_for_recall")),
        "classification": r.get("classification"),
        "status": " – ".join(x for x in [r.get("status"), r.get("voluntary_mandated")] if x) or None,
        "distribution": _clean(r.get("distribution_pattern"), 1000),
        "quantity": _clean(r.get("product_quantity"), 300),
        "codeInfo": _clean(r.get("code_info"), 1000),
        "url": "https://www.accessdata.fda.gov/scripts/ires/index.cfm",
        "apiUrl": f'https://api.fda.gov/{cat}/enforcement.json?search=recall_number:"{rn}"' if rn else None,
    })
    if include_raw:
        rec["raw"] = {k: r.get(k) for k in ("event_id", "product_type", "initial_firm_notification",
                                              "center_classification_date", "termination_date") if r.get(k)}
    return rec


# ---------------------------------------------------------------- NHTSA
def _nhtsa_url(camp: str) -> str:
    return f"https://www.nhtsa.gov/recalls?nhtsaId={camp}"


_FLAT_CACHE: dict = {"blob": None, "at": 0.0}
_FLAT_LOCK = asyncio.Lock()
FLAT_TTL = 6 * 3600  # NHTSA regenerates the file daily


async def fetch_nhtsa_by_date(client, d_from: date, d_to: date, log=None, include_raw: bool = False) -> list[dict]:
    """Recalls received by NHTSA in the window, from the official daily flat file (~15 MB zip, cached 6 h)."""
    log = log or logging.getLogger("recalls")
    async with _FLAT_LOCK:
        if not _FLAT_CACHE["blob"] or time.monotonic() - _FLAT_CACHE["at"] > FLAT_TTL:
            log.info("NHTSA: downloading official recalls flat file (~15 MB, updated daily)")
            blob, err = None, ""
            for attempt in range(2):
                try:
                    r = await client.get(NHTSA_FLAT, timeout=90)
                    if r.status_code == 200:
                        blob = r.content
                        break
                    err = f"HTTP {r.status_code}"
                except httpx.HTTPError as e:
                    err = f"{type(e).__name__}: {e}"
                await asyncio.sleep(3)
            if not blob:
                raise FetchError(f"NHTSA flat file: {err}")
            _FLAT_CACHE.update(blob=blob, at=time.monotonic())
        blob = _FLAT_CACHE["blob"]
    return await asyncio.to_thread(_parse_nhtsa_flat, blob, d_from, d_to, include_raw)


def _parse_nhtsa_flat(blob: bytes, d_from: date, d_to: date, include_raw: bool) -> list[dict]:
    lo, hi = f"{d_from:%Y%m%d}", f"{d_to:%Y%m%d}"
    camps: dict[str, dict] = {}
    z = zipfile.ZipFile(io.BytesIO(blob))
    with z.open(z.namelist()[0]) as fh:
        for line in io.TextIOWrapper(fh, encoding="latin-1", newline="\n"):
            p = line.rstrip("\r\n").split("\t")
            if len(p) < 23 or not (lo <= p[15] <= hi):
                continue
            camp = p[1]
            c = camps.get(camp)
            if c is None:
                c = camps[camp] = {"row": p, "vehicles": {}, "components": []}
            key = (p[2].strip(), p[3].strip())
            if p[4].strip() and p[4].strip() != "9999":
                c["vehicles"].setdefault(key, set()).add(p[4].strip())
            else:
                c["vehicles"].setdefault(key, set())
            if p[6] and p[6] not in c["components"]:
                c["components"].append(p[6])
    out = []
    for camp, c in camps.items():
        p = c["row"]
        products = []
        for (mk, md), years in c["vehicles"].items():
            ys = sorted(years)
            yr = (f"{ys[0]}" if len(ys) == 1 else f"{ys[0]}-{ys[-1]}") if ys else ""
            products.append(" ".join(x for x in (yr, mk, md) if x))
        flags = []
        if len(p) > 27 and p[27].strip().lower() == "yes":
            flags.append("DO NOT DRIVE")
        if len(p) > 28 and p[28].strip().lower() == "yes":
            flags.append("PARK OUTSIDE")
        rec = _blank()
        product = _clean("; ".join(products), 1500)
        rec.update({
            "source": "nhtsa_vehicle",
            "agency": "NHTSA",
            "recallId": camp,
            "date": _iso(p[15]),
            "initiatedDate": _iso(p[12]),  # owner notification date
            "title": _clean(f"{c['components'][0] if c['components'] else 'Recall'} – {product or ''}", 160),
            "product": product,
            "firm": _clean(p[7], 200),
            "reason": _clean(p[19]),
            "consequence": _clean(p[20]),
            "classification": "; ".join([NHTSA_TYPES.get(p[10].strip(), p[10].strip())] + flags) or None,
            "quantity": p[11].strip() or None,
            "remedy": _clean(p[21]),
            "codeInfo": _clean(", ".join(c["components"]), 500),
            "url": _nhtsa_url(camp),
            "apiUrl": f"https://api.nhtsa.gov/recalls/campaignNumber?campaignNumber={camp}",
        })
        if include_raw:
            rec["raw"] = {"initiatedBy": p[13] or None, "mfgCampaignNumber": p[5] or None,
                          "fmvss": p[18] or None, "makes": sorted({k[0] for k in c["vehicles"]})}
        out.append(rec)
    out.sort(key=lambda r: r["date"] or "", reverse=True)
    return out


def _nhtsa_api_date(s: str | None) -> str | None:
    try:
        return datetime.strptime(s or "", "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


async def fetch_nhtsa_by_vehicle(client, vehicles: list[str], log=None, include_raw: bool = False) -> list[dict]:
    out, seen = [], set()
    for v in vehicles:
        parts = [x.strip() for x in v.split("/")]
        if len(parts) != 3 or not all(parts):
            (log or logging.getLogger("recalls")).warning(f"Skipping vehicle '{v}': use Make/Model/Year, e.g. Honda/Accord/2023")
            continue
        mk, md, yr = parts
        data = await get_json(client, "https://api.nhtsa.gov/recalls/recallsByVehicle", log,
                              params={"make": mk, "model": md, "modelYear": yr})
        for r in (data or {}).get("results") or []:
            camp = r.get("NHTSACampaignNumber")
            if not camp or (camp, yr, md.upper()) in seen:
                continue
            seen.add((camp, yr, md.upper()))
            flags = [f for f, k in (("DO NOT DRIVE", "parkIt"), ("PARK OUTSIDE", "parkOutSide"),
                                    ("OVER-THE-AIR UPDATE", "overTheAirUpdate")) if r.get(k)]
            product = f"{r.get('ModelYear') or yr} {r.get('Make') or mk} {r.get('Model') or md}"
            rec = _blank()
            rec.update({
                "source": "nhtsa_vehicle",
                "agency": "NHTSA",
                "recallId": camp,
                "date": _nhtsa_api_date(r.get("ReportReceivedDate")),
                "title": _clean(f"{r.get('Component') or 'Recall'} – {product}", 160),
                "product": product,
                "firm": _clean(r.get("Manufacturer"), 200),
                "reason": _clean(r.get("Summary")),
                "consequence": _clean(r.get("Consequence")),
                "classification": "; ".join(["Vehicle"] + flags),
                "quantity": str(r["PotentialNumberofUnitsAffected"]) if r.get("PotentialNumberofUnitsAffected") else None,
                "remedy": _clean(r.get("Remedy")),
                "codeInfo": _clean(r.get("Component"), 500),
                "url": _nhtsa_url(camp),
                "apiUrl": f"https://api.nhtsa.gov/recalls/campaignNumber?campaignNumber={camp}",
            })
            if include_raw:
                rec["raw"] = {"notes": _clean(r.get("Notes"), 500)}
            out.append(rec)
        if len(vehicles) > 1:
            await asyncio.sleep(1.0)
    out.sort(key=lambda r: r["date"] or "", reverse=True)
    return out


# ---------------------------------------------------------------- CPSC
def _names(lst, key="Name", limit=600) -> str | None:
    vals = [str(x.get(key)).strip() for x in lst or [] if isinstance(x, dict) and x.get(key)]
    return _clean("; ".join(vals), limit)


async def fetch_cpsc(client, d_from: date | None, d_to: date | None, log=None, include_raw: bool = False,
                     extra: dict | None = None) -> list[dict]:
    params = {"format": "json"}
    if d_from:
        params["RecallDateStart"] = d_from.isoformat()
    if d_to:
        params["RecallDateEnd"] = d_to.isoformat()
    params.update(extra or {})
    data = await get_json(client, CPSC_API, log, params=params)
    out = [norm_cpsc(r, include_raw) for r in data or []]
    out.sort(key=lambda r: r["date"] or "", reverse=True)
    return out


def norm_cpsc(r: dict, include_raw: bool) -> dict:
    firms = _names(r.get("Manufacturers")) or _names(r.get("Importers")) or _names(r.get("Distributors"))
    units = _names(r.get("Products"), "NumberOfUnits", 200)
    sold = _names(r.get("Retailers"))
    rec = _blank()
    rec.update({
        "source": "cpsc_consumer",
        "agency": "CPSC",
        "recallId": r.get("RecallNumber") or str(r.get("RecallID")),
        "date": (r.get("RecallDate") or "")[:10] or None,
        "title": _clean(r.get("Title"), 300),
        "product": _names(r.get("Products"), limit=600),
        "firm": firms,
        "firmLocation": _names(r.get("ManufacturerCountries"), "Country", 200),
        "reason": _clean(_names(r.get("Hazards"), limit=2000)),
        "consequence": _names(r.get("Injuries"), limit=1000),
        "classification": _names(r.get("Products"), "Type", 300),
        "distribution": _clean((sold or "").replace("Sold At:", "").strip(), 1000),
        "quantity": units,
        "remedy": _names(r.get("Remedies"), limit=2000),
        "codeInfo": _names(r.get("ProductUPCs"), "UPC", 500),
        "url": r.get("URL"),
        "apiUrl": f"{CPSC_API}?format=json&RecallNumber={r.get('RecallNumber')}" if r.get("RecallNumber") else None,
    })
    # strip any stray phone numbers / e-mails (firm hotlines sometimes leak into text fields)
    for k in ("remedy", "consequence", "distribution"):
        if rec[k]:
            rec[k] = _EMAIL.sub("[email]", _PHONE.sub("[phone]", rec[k]))
    if include_raw:
        imgs = [i.get("URL") for i in r.get("Images") or [] if i.get("URL")]
        rec["raw"] = {"remedyOptions": [o.get("Option") for o in r.get("RemedyOptions") or []],
                      "image": imgs[0] if imgs else None, "lastPublishDate": (r.get("LastPublishDate") or "")[:10]}
    return rec


# ---------------------------------------------------------------- helpers ported from the actor's main.py
ALL_SOURCES = ["fda_food", "fda_drug", "fda_device", "nhtsa_vehicle", "cpsc_consumer"]


def parse_date(s, default: date) -> date:
    if s is None or str(s).strip() == "":
        return default
    s = str(s).strip()
    if s.lstrip("-").isdigit():  # relative: "-30" = 30 days ago
        return date.today() + timedelta(days=int(s))
    return date.fromisoformat(s[:10])


def matches(rec: dict, keywords: list[str]) -> bool:
    if not keywords:
        return True
    hay = " ".join(str(rec.get(k) or "") for k in ("title", "product", "firm", "reason", "codeInfo", "distribution")).lower()
    return any(k.lower() in hay for k in keywords)


def fair_take(per_source: dict[str, list[dict]], cap: int) -> list[dict]:
    """Round-robin the newest records of each source so every source is represented."""
    queues = {s: list(v) for s, v in per_source.items() if v}
    picked = []
    while queues and len(picked) < cap:
        for s in list(queues):
            if len(picked) >= cap:
                break
            picked.append(queues[s].pop(0))
            if not queues[s]:
                del queues[s]
    picked.sort(key=lambda r: r.get("date") or "", reverse=True)
    return picked
