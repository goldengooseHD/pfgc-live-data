#!/usr/bin/env python3
"""
Pulls live PFGC, USFD, SYY quotes via finance tools and writes live_data.json.
Run with: api_credentials=["external-tools"]
"""
import asyncio
import json
import re
from datetime import datetime, timezone

async def call_tool(source_id, tool_name, arguments):
    proc = await asyncio.create_subprocess_exec(
        "external-tool", "call", json.dumps({
            "source_id": source_id, "tool_name": tool_name, "arguments": arguments,
        }),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Tool error: {stderr.decode()}")
    return json.loads(stdout.decode())

def parse_markdown_tables(content):
    """Parse markdown tables from the finance tool output."""
    quotes = {}
    lines = content.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Find header rows
        if line.startswith("| symbol |"):
            headers = [h.strip() for h in line.split("|")[1:-1]]
            i += 1  # skip separator
            if i < len(lines):
                i += 1  # skip --- row
            # Read data row
            if i < len(lines):
                data_line = lines[i].strip()
                if data_line.startswith("|"):
                    values = [v.strip() for v in data_line.split("|")[1:-1]]
                    if len(values) >= len(headers):
                        row = dict(zip(headers, values))
                        sym = row.get("symbol", "")
                        
                        def safe_float(val, default=0):
                            try:
                                return float(val.replace(",", ""))
                            except (ValueError, AttributeError):
                                return default
                        
                        quotes[sym] = {
                            "price": safe_float(row.get("price")),
                            "change": safe_float(row.get("change")),
                            "change_pct": safe_float(row.get("changesPercentage")),
                            "market_cap": safe_float(row.get("marketCap")),
                            "pe": safe_float(row.get("pe")) or None,
                            "eps": safe_float(row.get("eps")),
                            "volume": int(safe_float(row.get("volume"))),
                            "avg_volume": int(safe_float(row.get("avgVolume"))),
                            "day_low": safe_float(row.get("dayLow")),
                            "day_high": safe_float(row.get("dayHigh")),
                            "year_low": safe_float(row.get("yearLow")),
                            "year_high": safe_float(row.get("yearHigh")),
                            "prev_close": safe_float(row.get("previousClose")),
                            "open": safe_float(row.get("open")),
                            "market_status": row.get("market_status", "unknown"),
                        }
        i += 1
    return quotes

async def main():
    result = await call_tool("finance", "finance_quotes", {
        "ticker_symbols": ["PFGC", "USFD", "SYY"],
        "fields": ["price", "change", "changesPercentage", "marketCap", "pe", "eps",
                    "volume", "avgVolume", "dayLow", "dayHigh", "yearLow", "yearHigh",
                    "previousClose", "open"]
    })

    content = result.get("content", "")
    quotes = parse_markdown_tables(content)

    pfgc = quotes.get("PFGC", {})
    usfd = quotes.get("USFD", {})
    syy = quotes.get("SYY", {})

    pfgc_price = pfgc.get("price", 85.15)
    base_target = 72.50
    bear_target = 57.60
    bull_target = 98.00

    live_data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "market_status": pfgc.get("market_status", "unknown"),
        "pfgc": {
            **pfgc,
            "base_target": base_target,
            "base_return_pct": round((base_target - pfgc_price) / pfgc_price * 100, 1),
            "bear_target": bear_target,
            "bear_return_pct": round((bear_target - pfgc_price) / pfgc_price * 100, 1),
            "bull_target": bull_target,
            "bull_return_pct": round((bull_target - pfgc_price) / pfgc_price * 100, 1),
        },
        "usfd": usfd,
        "syy": syy,
        "peer_pe_spread": {
            "pfgc_pe": pfgc.get("pe"),
            "usfd_pe": usfd.get("pe"),
            "syy_pe": syy.get("pe"),
            "pfgc_premium_to_usfd_pct": round((pfgc.get("pe", 0) / usfd.get("pe", 1) - 1) * 100, 1) if usfd.get("pe") else None,
            "pfgc_premium_to_syy_pct": round((pfgc.get("pe", 0) / syy.get("pe", 1) - 1) * 100, 1) if syy.get("pe") else None,
        }
    }

    with open("/home/user/workspace/pfgc-live-data/live_data.json", "w") as f:
        json.dump(live_data, f, indent=2)

    print(f"Updated at {live_data['updated_at']}")
    print(f"PFGC: ${pfgc_price:.2f} ({pfgc.get('change_pct', 0):+.2f}%) P/E {pfgc.get('pe')}x")
    print(f"USFD: ${usfd.get('price', 0):.2f} ({usfd.get('change_pct', 0):+.2f}%) P/E {usfd.get('pe')}x")
    print(f"SYY:  ${syy.get('price', 0):.2f} ({syy.get('change_pct', 0):+.2f}%) P/E {syy.get('pe')}x")

asyncio.run(main())
