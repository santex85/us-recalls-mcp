"""Live smoke test: calls every tool against the real APIs. Run: .venv/bin/python tests/live_test.py"""
import asyncio, json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import server  # noqa: E402


def show(name, r, ok):
    s = json.dumps(r, ensure_ascii=False)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {len(s)} bytes | {s[:700]}\n")
    return bool(ok)


async def timed(name, coro):
    t = time.time()
    r = await coro
    return f"{name} ({time.time()-t:.1f}s)", r


async def main():
    res = []
    tools = await server.mcp.list_tools()
    res.append(show("list_tools", [t.name for t in tools], len(tools) == 4))
    n, r = await timed("recent_recalls all 14d", server.recent_recalls(days=14, limit=10))
    res.append(show(n, {k: r[k] for k in ("window", "returned", "perSource")} | {"first": r["recalls"][:1]},
                    r["returned"] > 0 and not any("error" in v for v in r["perSource"].values())))
    n, r = await timed("search_recalls listeria", server.search_recalls("listeria", sources=["fda"], limit=5))
    res.append(show(n, {k: r[k] for k in ("window", "returned", "perSource")} | {"first": r["recalls"][:1]}, r["returned"] > 0))
    n, r = await timed("search_recalls Ford,stroller all", server.search_recalls("Ford, stroller", date_from="-180", limit=10))
    res.append(show(n, {k: r[k] for k in ("window", "returned", "perSource")} | {"titles": [(x["source"], x["title"]) for x in r["recalls"]]}, r["returned"] > 0))
    n, r = await timed("vehicle_recalls Toyota RAV4 2022", server.vehicle_recalls("Toyota", "RAV4", 2022))
    res.append(show(n, r | {"recalls": r["recalls"][:1]}, r["count"] > 0))
    n, r = await timed("vehicle_recalls bad model", server.vehicle_recalls("Toyota", "Rav-Four", 2022))
    res.append(show(n, r, r["count"] == 0 and r.get("knownModels")))
    for rid in ["F-0276-2017", "24V001000", "27082"]:
        n, r = await timed(f"recall_details {rid}", server.recall_details(rid))
        res.append(show(n, r, r.get("recallId", "").upper() == rid))
    n, r = await timed("recall_details missing", server.recall_details("Z-9999-1999"))
    res.append(show(n, r, "error" in r))
    print(f"{sum(res)}/{len(res)} passed")


asyncio.run(main())
