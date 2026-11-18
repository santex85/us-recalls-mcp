# US Recalls MCP server (FDA, NHTSA, CPSC)

An MCP server that gives AI agents live access to official US product recalls:

- **FDA**: food, drugs and medical devices, from openFDA enforcement reports
- **NHTSA**: vehicles and vehicle equipment, tires and child seats
- **CPSC**: consumer products such as toys, furniture and appliances, from SaferProducts.gov

Every recall is returned in the same flat schema, so one call can answer questions like "were there any
listeria recalls this month?", "does my 2022 RAV4 have open recalls?" or "what is recall 24V001000 about?".
No API keys are required.

It is a port of the `us-recalls-monitor` Apify actor. The normalisation is the same; the Apify, billing and
"only new" state code is removed.

## Tools

| Tool | What it does |
|---|---|
| `search_recalls` | Keyword or brand search across all five datasets in a date window (default: last 365 days) |
| `recent_recalls` | Latest recalls of the last N days (1-90), with the sources interleaved |
| `vehicle_recalls` | Complete NHTSA recall history for one make, model and year. If the model name is wrong, lists the valid model names |
| `recall_details` | Full, untrimmed record for one recall ID (FDA `F-0276-2017`, NHTSA `24V001000`, CPSC `27082`) |

### Example calls

```jsonc
// search_recalls
{"query": "listeria, salmonella", "sources": ["fda_food"], "date_from": "-90", "limit": 20}
{"query": "Ford, Tesla", "sources": ["nhtsa"], "date_from": "2026-01-01"}
{"query": "stroller", "sources": ["cpsc"]}

// recent_recalls
{"days": 7, "limit": 30}
{"sources": ["fda_drug", "fda_device"], "days": 30}

// vehicle_recalls
{"make": "Toyota", "model": "RAV4", "year": 2022}

// recall_details
{"recall_id": "24V001000"}
```

Source names: `fda_food`, `fda_drug`, `fda_device`, `nhtsa_vehicle`, `cpsc_consumer`. The aliases `fda`, `nhtsa`,
`cpsc`, `food`, `drug`, `device`, `vehicle` and `consumer` also work. Dates are `YYYY-MM-DD` or relative days
(`-30` = 30 days ago).

### Record fields

`source, agency, recallId, date, initiatedDate, title, product, firm, firmLocation, reason, consequence,
classification, status, distribution, quantity, remedy, codeInfo, url, apiUrl`. `recall_details` adds `raw`
(extra source fields) and, for NHTSA, `affectedVehicleCount`. Fields that a source does not provide are `null`.
The `date` field is the FDA enforcement report date, the NHTSA report-received date or the CPSC recall date.

## How each source is queried

| Source | search / recent | details |
|---|---|---|
| FDA | `api.fda.gov/{food,drug,device}/enforcement.json`, server-side search on product description, recalling firm and reason | `recall_number:"…"` |
| NHTSA | official daily flat file `FLAT_RCL_POST_2010.zip` (about 15 MB, cached in memory for 6 h), filtered locally by date and keyword | `api.nhtsa.gov/recalls/campaignNumber` |
| NHTSA by vehicle | `api.nhtsa.gov/recalls/recallsByVehicle`; model names come from `/products/vehicle/models` | |
| CPSC | `saferproducts.gov/RestWebServices/Recall`; keyword search is a server-side `RecallTitle` match (titles name the firm, product and hazard) | `RecallNumber` |

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python src/server.py                         # streamable-http on 0.0.0.0:8080, endpoint /mcp
PORT=8092 .venv/bin/python src/server.py
MCP_TRANSPORT=stdio .venv/bin/python src/server.py
.venv/bin/python tests/live_test.py                    # live smoke test of every tool
```

Environment: `HOST`, `PORT`, `MCP_TRANSPORT`, and optionally `OPENFDA_API_KEY` (a free key from
open.fda.gov that raises the daily openFDA quota).

## Limits

- `search_recalls` and `recent_recalls` return at most 100 records per call. Long text fields are trimmed to 400
  characters in list output; `recall_details` gives up to 2,000-4,000 characters per field.
- Every tool call has a 150 s timeout. Each HTTP request has a 45 s timeout and up to 3 attempts. openFDA pages
  are spaced 0.5 s apart.
- openFDA without a key allows 240 requests per minute and 1,000 per day per IP. Set `OPENFDA_API_KEY` for heavy use.
- The first NHTSA date-window query downloads the 15 MB flat file, which takes a few seconds. Later queries use the 6-hour cache.
- CPSC queries without a keyword are limited to the last 366 days of the window, to keep responses small.
- FDA publishes enforcement reports weekly, so the newest FDA records can be a few days old. `recent_recalls` with
  `days` < 7 may show no FDA records.
- FDA coverage is enforcement reports, which include classified recalls. Market withdrawals and safety alerts that
  are not in enforcement reports are not covered. FDA has no stable page per recall, so `url` points to the
  Enforcement Report search page and `apiUrl` links to the exact record.
- NHTSA returns HTTP 400 for "no results". The server treats that as an empty list.

## Data sources and terms

- **openFDA** enforcement APIs. The data is public domain under CC0 ([terms](https://open.fda.gov/terms/)).
  FDA disclaimer: *"Do not rely on openFDA to make decisions regarding medical care … you should assume all results
  are unvalidated."*
- **NHTSA** recalls API and the flat file at `static.nhtsa.gov`. These are US federal government works and public (17 U.S.C. §105).
- **CPSC** Recalls API at SaferProducts.gov, listed on data.gov as US public domain.

This project is not affiliated with or endorsed by the FDA, NHTSA, CPSC or any US agency. The data is provided "as is"
and can contain errors or delays. Before any safety or medical decision, check the official notice linked in `url`.

## Privacy

The server returns product- and firm-level recall data only. It never copies contact persons, and it masks phone
numbers and e-mail addresses that appear in CPSC remedy, injury or distribution text. The server keeps no logs of
queries or results.

## Use with Claude Desktop or another MCP client

Local (stdio). Clone the repo, create the venv as above, then add this to `claude_desktop_config.json`
(or your client's MCP config), with the paths changed to where you cloned it:

```json
{
  "mcpServers": {
    "us-recalls": {
      "command": "/path/to/us-recalls-mcp/.venv/bin/python",
      "args": ["/path/to/us-recalls-mcp/src/server.py"],
      "env": {"MCP_TRANSPORT": "stdio"}
    }
  }
}
```

If you have a free openFDA key, add `"OPENFDA_API_KEY": "<your key>"` to `env`.

Remote (streamable HTTP). Run `.venv/bin/python src/server.py` on a server and point any client that supports
remote MCP servers at `http://<host>:8080/mcp`. `GET /health` returns `{"status": "healthy"}`.

## License

MIT, see [LICENSE](LICENSE). The data returned by the tools stays under the terms of its sources (see above).
