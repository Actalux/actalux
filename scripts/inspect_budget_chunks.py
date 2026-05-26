"""Dump audit-statement chunk text so budget figures can be transcribed by hand.

Read-only helper for building the verified figures in ``load_budget.py``. For
each budget document it prints the chunk that holds "Total expenditures" plus
its immediate neighbours, so the full function x fund matrix is visible verbatim.

Run (the --chunk-id form dumps one chunk + its neighbours):
  doppler run --project mac --config dev -- uv run python scripts/inspect_budget_chunks.py
  doppler run --project mac --config dev -- \
    uv run python scripts/inspect_budget_chunks.py --chunk-id 7690
"""

from __future__ import annotations

import argparse

from actalux.config import load_config
from actalux.db import get_client

# (fiscal_year, document_id, chunk_id) for the seven audited statements,
# mirroring scripts/load_budget.py.
BUDGET_CHUNKS = [
    ("2018-2019", 429, 7690),
    ("2019-2020", 428, 7588),
    ("2020-2021", 427, 7479),
    ("2021-2022", 426, 7371),
    ("2022-2023", 425, 7260),
    ("2023-2024", 424, 7154),
    ("2024-2025", 436, 7802),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunk-id", type=int, help="dump a single chunk + neighbours")
    parser.add_argument(
        "--window", type=int, default=1, help="how many neighbouring chunks each side"
    )
    args = parser.parse_args()

    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)

    if args.chunk_id is not None:
        targets = [("(ad hoc)", None, args.chunk_id)]
    else:
        targets = BUDGET_CHUNKS

    for fy, doc_id, chunk_id in targets:
        anchor = (
            client.table("chunks")
            .select("id, document_id, chunk_index, section")
            .eq("id", chunk_id)
            .single()
            .execute()
            .data
        )
        doc_id = anchor["document_id"]
        idx = anchor["chunk_index"]
        lo, hi = idx - args.window, idx + args.window
        rows = (
            client.table("chunks")
            .select("id, chunk_index, section, content")
            .eq("document_id", doc_id)
            .gte("chunk_index", lo)
            .lte("chunk_index", hi)
            .order("chunk_index")
            .execute()
            .data
        )
        print("=" * 100)
        print(f"FY {fy}  document_id={doc_id}  anchor chunk_id={chunk_id} (index {idx})")
        print("=" * 100)
        for r in rows:
            mark = " <== ANCHOR" if r["id"] == chunk_id else ""
            print(
                f"\n--- chunk_id={r['id']} index={r['chunk_index']} section={r['section']!r}{mark}"
            )
            print(r["content"])
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
