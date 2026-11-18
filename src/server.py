"""US Recalls MCP server: FDA (food, drugs, devices), NHTSA (vehicles & equipment), CPSC (consumer products).

Read-only, official public US-government data. Product/firm-level data only: no personal contacts;
phone numbers / e-mails inside CPSC text are masked.
"""
import asyncio
import logging
import os
import re
import sys
from datetime import date, timedelta

import httpx
from mcp.server.fastmcp import FastMCP

sys.path.insert(0, os.path.dirname(__file__))
from sources import (ALL_SOURCES, FDA_SOURCES, FetchError, _blank, _clean, _nhtsa_api_date, _nhtsa_url,  # noqa: E402
                     fair_take, fetch_cpsc, fetch_fda, fetch_nhtsa_by_date, fetch_nhtsa_by_vehicle, get_json,
                     matches, parse_date)

logging.getLogger("httpx").setLevel(logging.WARNING)

UA = "us-recalls-mcp/1.0 (MCP server; reads public openFDA/NHTSA/CPSC APIs; python-httpx)"
MAX_RESULTS = 100
CALL_TIMEOUT = 150           # seconds per tool call
LIST_TEXT = 400              # long text fields are trimmed to this in list outputs; recall_details gives full text
LONG_FIELDS = ("product", "reason", "consequence", "distribution", "remedy", "codeInfo", "quantity")
CPSC_MAX_WINDOW = 366        # days, for CPSC date-window fetches without a query
OPENFDA_KEY = os.environ.get("OPENFDA_API_KEY") or None

mcp = FastMCP("us-recalls", host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8080")))


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(headers={"User-Agent": UA, "Accept": "application/json"}, follow_redirects=True,
                             timeout=httpx.Timeout(45, connect=10))


def _compact(r: dict) -> dict:
    r = dict(r)
    for k in LONG_FIELDS:
        if isinstance(r.get(k), str) and len(r[k]) > LIST_TEXT:
            r[k] = r[k][:LIST_TEXT - 1] + "…"
    return r


def _sources(v) -> tuple[list[str], list[str]]:
    if not v:
        return list(ALL_SOURCES), []
    if isinstance(v, str):
        v = v.split(",")
    alias = {"fda": list(FDA_SOURCES), "food": ["fda_food"], "drug": ["fda_drug"], "drugs": ["fda_drug"],
             "device": ["fda_device"], "devices": ["fda_device"], "nhtsa": ["nhtsa_vehicle"], "vehicle": ["nhtsa_vehicle"],
             "vehicles": ["nhtsa_vehicle"], "cpsc": ["cpsc_consumer"], "consumer": ["cpsc_consumer"]}
    good, bad = [], []
    for s in v:
        s = str(s).strip().lower()
        for x in ([s] if s in ALL_SOURCES else alias.get(s, [])):
            if x not in good:
                good.append(x)
        if s not in ALL_SOURCES and s not in alias:
            bad.append(s)
    return good or list(ALL_SOURCES), bad


async def _collect(sources: list[str], d_from: date, d_to: date, keywords: list[str], limit: int) -> dict:
    notes = []

    async def one(client, src):
        if src in FDA_SOURCES:
            recs = await fetch_fda(client, src, d_from, d_to, keywords, OPENFDA_KEY, limit)
        elif src == "nhtsa_vehicle":
            recs = await fetch_nhtsa_by_date(client, d_from, d_to)
        else:
            if keywords:  # CPSC title search is server-side (title = firm + product + hazard)
                recs, seen = [], set()
                for kw in keywords[:5]:
                    for r in await fetch_cpsc(client, d_from, d_to, extra={"RecallTitle": kw}):
                        if r["recallId"] not in seen:
                            seen.add(r["recallId"])
                            recs.append(r)
                recs.sort(key=lambda r: r["date"] or "", reverse=True)
                return recs
            lo = max(d_from, d_to - timedelta(days=CPSC_MAX_WINDOW))
            if lo != d_from:
                notes.append(f"cpsc_consumer without a query is limited to the last {CPSC_MAX_WINDOW} days of the window")
            recs = await fetch_cpsc(client, lo, d_to)
        return [r for r in recs if matches(r, keywords)]

    async with _client() as client:
        res = await asyncio.gather(*(one(client, s) for s in sources), return_exceptions=True)
    per, status = {}, {}
    for s, r in zip(sources, res):
        if isinstance(r, BaseException):
            per[s] = []
            status[s] = {"found": 0, "error": f"{type(r).__name__}: {str(r)[:200]}"}
        else:
            per[s] = r
            status[s] = {"found": len(r)}
    items = fair_take(per, limit)
    for s in sources:
        status[s]["returned"] = sum(1 for i in items if i["source"] == s)
    out = {"window": [d_from.isoformat(), d_to.isoformat()], "returned": len(items), "perSource": status,
           "recalls": [_compact(i) for i in items]}
    if notes:
        out["notes"] = notes
    return out


async def _timed(coro):
    try:
        return await asyncio.wait_for(coro, timeout=CALL_TIMEOUT)
    except asyncio.TimeoutError:
        return {"error": f"timed out after {CALL_TIMEOUT}s"}
    except FetchError as e:
        return {"error": f"upstream API error: {str(e)[:300]}"}


def _keywords(q: str | None) -> list[str]:
    return [k.strip() for k in (q or "").split(",") if k.strip()][:10]


@mcp.tool()
async def search_recalls(query: str, sources: list[str] | None = None, date_from: str | None = None,
                         date_to: str | None = None, limit: int = 25) -> dict:
    """Search official US recalls by keyword/brand across FDA, NHTSA and CPSC.

    query: text to find (case-insensitive); several terms comma-separated = ANY match, e.g. "listeria, salmonella",
      "Ford, Tesla", "peanut butter", "Amazon". Matched against: FDA product description / recalling firm / reason
      (server-side); NHTSA affected vehicles, manufacturer, defect summary, component; CPSC recall title (firm +
      product + hazard, server-side).
    sources: any of "fda_food", "fda_drug", "fda_device", "nhtsa_vehicle", "cpsc_consumer" (aliases: "fda", "nhtsa",
      "cpsc", "food", "drug", "device", "vehicle", "consumer"). Default: all five.
    date_from / date_to: "YYYY-MM-DD" or relative days ("-90" = 90 days ago, "0" = today). Default: last 365 days.
      Date = FDA enforcement report date, NHTSA report-received date, CPSC recall date.
    limit: max recalls returned (1-100, default 25); newest of every source are taken in turn so each is represented.
    Returns {"window", "returned", "perSource": {source: {found, returned, error?}}, "recalls": [record]}.
    Record fields: source, agency, recallId, date, initiatedDate, title, product, firm, firmLocation, reason,
    consequence, classification (FDA Class I/II/III; NHTSA type + DO NOT DRIVE / PARK OUTSIDE; CPSC product type),
    status, distribution, quantity, remedy, codeInfo (lot codes / UPCs / component), url (official page), apiUrl.
    Long text fields are trimmed to 400 chars; call recall_details(recallId) for full text.
    For one specific car use vehicle_recalls instead (complete history, not limited to a date window).
    """
    kws = _keywords(query)
    if not kws:
        return {"error": "query is required (use recent_recalls to browse without a keyword)"}
    srcs, bad = _sources(sources)
    try:
        d_to = parse_date(date_to, date.today())
        d_from = parse_date(date_from, d_to - timedelta(days=365))
    except ValueError as e:
        return {"error": f"bad date: {e}"}
    if d_from > d_to:
        d_from, d_to = d_to, d_from
    limit = max(1, min(int(limit or 25), MAX_RESULTS))
    r = await _timed(_collect(srcs, d_from, d_to, kws, limit))
    if bad:
        r["ignoredSources"] = bad
    return r


@mcp.tool()
async def recent_recalls(sources: list[str] | None = None, days: int = 7, limit: int = 30) -> dict:
    """Latest official US recalls from the last N days (no keyword), newest first.

    sources: any of "fda_food", "fda_drug", "fda_device", "nhtsa_vehicle", "cpsc_consumer" (or "fda", "nhtsa",
      "cpsc"); default all five.
    days: look-back window, 1-90 (default 7). Note: FDA publishes enforcement reports weekly (Wednesdays), so
      FDA may show nothing for days < 7.
    limit: max recalls (1-100, default 30); sources are interleaved so each selected source is represented.
    Returns the same structure and record fields as search_recalls.
    """
    srcs, bad = _sources(sources)
    days = max(1, min(int(days or 7), 90))
    limit = max(1, min(int(limit or 30), MAX_RESULTS))
    d_to = date.today()
    r = await _timed(_collect(srcs, d_to - timedelta(days=days), d_to, [], limit))
    if bad:
        r["ignoredSources"] = bad
    return r


@mcp.tool()
async def vehicle_recalls(make: str, model: str, year: int) -> dict:
    """Complete NHTSA recall history for one vehicle (make, model, model year), newest first.

    make: e.g. "Toyota", "Ford", "Tesla"; model: as NHTSA names it, e.g. "RAV4", "F-150", "Model 3", "Camry Hybrid";
    year: model year, e.g. 2022. Case-insensitive.
    Returns {"vehicle", "count", "recalls": [record]} with recallId (NHTSA campaign number), date (report received),
    title (component + vehicle), reason (defect summary), consequence, remedy, quantity (units affected),
    classification (incl. "DO NOT DRIVE" / "PARK OUTSIDE" / "OVER-THE-AIR UPDATE" advisories), url.
    If nothing matches, "knownModels" lists the models NHTSA has recall data for under that make/year, so the
    model name can be corrected (an empty recall list with a correct model simply means no recalls).
    Covers vehicles; tires/child seats/equipment are only in search_recalls/recent_recalls (source nhtsa_vehicle).
    """
    make, model = (make or "").strip(), (model or "").strip()
    try:
        year = int(year)
    except (TypeError, ValueError):
        return {"error": "year must be a number, e.g. 2022"}
    if not make or not model or "/" in make + model or not 1950 <= year <= date.today().year + 2:
        return {"error": "give make, model and a model year, e.g. make='Honda', model='Accord', year=2023"}

    async def run():
        async with _client() as client:
            recs = await fetch_nhtsa_by_vehicle(client, [f"{make}/{model}/{year}"])
            out = {"vehicle": f"{year} {make.upper()} {model.upper()}", "count": len(recs), "recalls": recs}
            if not recs:
                try:
                    d = await get_json(client, "https://api.nhtsa.gov/products/vehicle/models",
                                       params={"modelYear": year, "make": make, "issueType": "r"})
                    out["knownModels"] = sorted({m.get("model") for m in (d or {}).get("results") or [] if m.get("model")})[:100]
                except FetchError as e:
                    out["knownModelsError"] = str(e)
            return out

    return await _timed(run())


def _guess_source(rid: str) -> list[str]:
    rid = rid.upper()
    if re.fullmatch(r"\d{2}[VETC]\d{6}", rid):
        return ["nhtsa_vehicle"]
    m = re.fullmatch(r"([A-Z])-\d{3,5}-\d{4}", rid)
    if m:
        first = {"F": "fda_food", "D": "fda_drug", "Z": "fda_device"}.get(m.group(1))
        return [first] + [s for s in FDA_SOURCES if s != first] if first else list(FDA_SOURCES)
    if rid.isdigit():
        return ["cpsc_consumer"]
    return ["nhtsa_vehicle", *FDA_SOURCES, "cpsc_consumer"]


async def _nhtsa_campaign(client, camp: str) -> dict | None:
    d = await get_json(client, "https://api.nhtsa.gov/recalls/campaignNumber", params={"campaignNumber": camp})
    rows = (d or {}).get("results") or []
    if not rows:
        return None
    r = rows[0]
    vehicles = list(dict.fromkeys(f"{x.get('ModelYear')} {x.get('Make')} {x.get('Model')}" for x in rows))
    flags = [f for f, k in (("DO NOT DRIVE", "parkIt"), ("PARK OUTSIDE", "parkOutSide"),
                            ("OVER-THE-AIR UPDATE", "overTheAirUpdate")) if r.get(k)]
    rec = _blank()
    rec.update({
        "source": "nhtsa_vehicle", "agency": "NHTSA", "recallId": r.get("NHTSACampaignNumber") or camp,
        "date": _nhtsa_api_date(r.get("ReportReceivedDate")),
        "title": _clean(f"{r.get('Component') or 'Recall'} – {vehicles[0]}{' +' + str(len(vehicles) - 1) + ' more' if len(vehicles) > 1 else ''}", 160),
        "product": _clean("; ".join(vehicles), 3000), "firm": _clean(r.get("Manufacturer"), 200),
        "reason": _clean(r.get("Summary"), 4000), "consequence": _clean(r.get("Consequence"), 2000),
        "classification": "; ".join(["Vehicle"] + flags),
        "quantity": str(r["PotentialNumberofUnitsAffected"]) if r.get("PotentialNumberofUnitsAffected") else None,
        "remedy": _clean(r.get("Remedy"), 2000), "codeInfo": _clean(r.get("Component"), 500),
        "url": _nhtsa_url(camp), "apiUrl": f"https://api.nhtsa.gov/recalls/campaignNumber?campaignNumber={camp}",
        "affectedVehicleCount": len(vehicles),
    })
    return rec


@mcp.tool()
async def recall_details(recall_id: str, source: str | None = None) -> dict:
    """Full record for one recall by its official ID (untrimmed text, plus extra source fields in "raw").

    recall_id: FDA recall number ("F-0276-2017", "D-0123-2026", "Z-0456-2026"), NHTSA campaign number ("24V001000"),
      or CPSC recall number ("27082"). The source is inferred from the ID format.
    source: optional, to force one of "fda_food", "fda_drug", "fda_device", "nhtsa_vehicle", "cpsc_consumer".
    Returns the record (fields as in search_recalls; NHTSA adds affectedVehicleCount and lists all affected vehicles
    in product) or {"error": ...} if not found. FDA may return several records for one recall number only rarely;
    the first is returned and "otherMatches" counts the rest.
    """
    rid = (recall_id or "").strip()
    if not rid or len(rid) > 40:
        return {"error": "recall_id is required"}
    srcs = [source] if source in ALL_SOURCES else _guess_source(rid)

    async def run():
        errors = {}
        async with _client() as client:
            for s in srcs:
                try:
                    if s in FDA_SOURCES:
                        recs = await fetch_fda(client, s, None, None, [], OPENFDA_KEY, 5, include_raw=True,
                                               search=f'recall_number:"{rid.upper()}"')
                        if recs:
                            return recs[0] | ({"otherMatches": len(recs) - 1} if len(recs) > 1 else {})
                    elif s == "nhtsa_vehicle":
                        rec = await _nhtsa_campaign(client, rid.upper())
                        if rec:
                            return rec
                    else:
                        recs = await fetch_cpsc(client, None, None, include_raw=True, extra={"RecallNumber": rid})
                        if not recs and rid.isdigit():
                            recs = await fetch_cpsc(client, None, None, include_raw=True, extra={"RecallID": rid})
                        if recs:
                            return recs[0]
                except FetchError as e:
                    errors[s] = str(e)[:200]
        return {"error": f"recall '{rid}' not found", "searched": srcs} | ({"errors": errors} if errors else {})

    return await _timed(run())


from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse  # noqa: E402


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "healthy"})


if __name__ == "__main__":
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "streamable-http"))
