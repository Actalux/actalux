# Actalux — Task Plan

Living task list. Updated as work completes and new items surface.

Last updated: 2026-04-12.

---

## T1 — Redesign the reader pane as "summary + native source"

**Problem.** Search results currently show raw chunk content in both the
results list and the reader pane. For budget tables, TOCs, and long minutes,
this is a wall of unreadable data — useless to a parent. A parent wants to
know "what was said about X," not to read 600 words of concatenated table
cells.

**Desired shape.** Three-column layout keeps, but the columns change roles:

- **Left (results list).** Keep as list of hits, but with a *short*
  windowed snippet (~150 chars, `<mark>`-highlighted, ellipses). Not the
  full chunk. Each result is a clickable pointer into the middle column.
- **Middle (summary).** NEW. When a result is selected, show an
  AI-generated summary of "what this source says about [query]" —
  citation-first, built from the chunk plus 1-2 neighbors. Short paragraph
  or a few bullets. Every claim cites inline back to the source passage
  (`#qXXXX`). Uses Claude Sonnet, same as `/summarize` already does.
- **Right (source in native format).** Depends on document type:
  - **PDF / document:** embedded PDF viewer, jumped to the page
    containing the cited passage, passage highlighted. (See T2.)
  - **Board meeting (YouTube):** top half = embedded video cued to the
    cited timestamp; bottom half = transcript excerpt around the cited
    passage with highlight + surrounding speaker turns. (See T3.)

**Effort estimate.** Medium. Template + CSS rework, new
`/chunk/{id}/summary` endpoint, routing by document type. 1-2 sessions.

**Dependencies.** T2 for PDF viewing. T3 for YouTube. Both can stub
initially.

---

## T2 — In-context PDF viewer with passage highlight

**Problem.** The current right column shows PDF content as scraped
markdown text, which is ugly and often formatted badly (tables squashed to
one line, line breaks lost). The actual PDF looks fine. Show the PDF.

**Desired behavior.** When the user clicks a result whose source is a PDF
(most documents), the right column loads a PDF viewer (PDF.js or native
browser PDF embed), scrolled to the page containing the cited passage,
with the passage visually highlighted (overlay rectangle matching the
text's bounding box, or the phrase wrapped with an annotation).

**Technical pieces:**

1. **Track page numbers during ingestion.** Currently PyMuPDF4LLM
   collapses the PDF to markdown without preserving per-passage page
   numbers. Need to extend `ingest/to_markdown.py` (or switch to direct
   PyMuPDF text extraction) to record the source page number for each
   chunk in a new `chunks.page_number` column.
2. **Embed PDF.js in the right column.** Vendor pdf.js viewer (or use
   `mozilla/pdfjs-dist` via CDN). Pass `?page=N` to jump to the page.
3. **Highlight overlay.** Simple v1: just jump to page. v2: search the
   rendered page text for the passage and draw an overlay rectangle.
   PDF.js exposes text-layer coordinates for this.

**Effort.** Medium-large. v1 (page jump, no overlay) = half session.
v2 (with overlay) = 1-2 additional sessions. Re-ingest needed to populate
`page_number`.

**Dependencies.** None. Can ship incrementally: page-jump first, overlay
later.

---

## T3 — Board meeting reader: YouTube embed + transcript split

**Problem.** Board meeting results come from YouTube-hosted video
recordings. Showing the transcript text as a wall of prose is worse than
showing the video. Parents should be able to watch the exact moment.

**Desired behavior.** When the user clicks a result whose source is a
board meeting transcript (`source_portal = "youtube"`), the right column
splits vertically:

- **Top:** `<iframe>` embed of the YouTube video, cued via
  `?start=<seconds>` to the exact timestamp of the cited passage.
- **Bottom:** transcript excerpt — 3-5 speaker turns around the cited
  passage, with the cited turn highlighted (archival yellow + vermillion
  border, same as the core motif). Speaker names and timestamps in Plex
  Mono metadata style.

**Technical pieces:**

1. **Store YouTube video IDs** in the `documents` table (new column
   `youtube_video_id`). Already stored in `source_url` as a URL; just needs
   an extracted field. Or parse at render time.
2. **Store timestamps per chunk** for transcript chunks. ASR output
   from faster-whisper or YouTube auto-captions includes per-word or
   per-segment timestamps. Need to preserve these at ingest time (new
   `chunks.start_seconds` column).
3. **Render template.** Already sketched in design-preview.html. Port
   into `reader_pane.html` with branching on `document.source_portal`.

**Effort.** Medium. Re-ingest YouTube transcripts needed to populate
timestamps.

**Dependencies.** None.

---

## T4 — Chunk quality pass (chunker fixes + filter low-value chunks)

**Problem.** Current chunker can produce 1600-word chunks when the source
PDF has no sentence-style punctuation (budget tables, spreadsheet dumps).
TOCs, table dumps, and control-byte-corrupted chunks surface as search
results and are useless. Chunk size stats today: avg 116 words, max 336 in
a sample — but search results for "salary" return 1670w chunks, so the
distribution has a long tail.

**Fixes:**

1. **Hard-cap chunker at ~180 words.** In
   `ingest/chunker.py::_chunk_section`, when a paragraph exceeds target
   size and has no sentence boundaries, fall back to word-boundary split
   every ~150 words.
2. **Flag low-value chunks at ingestion:**
   - **TOC detector.** Many `SECTION N` / `Page N` patterns, low avg
     word length, high digit-to-word ratio.
   - **Table detector.** High number-to-word ratio, few English function
     words (`the`, `is`, `of`...), short "sentences" with no verbs.
   - **Control-byte detector.** Any `chr(< 32)` other than `\n`, `\t`, or
     the occasional `\r`. Already see some chunks with literal `\x01` etc.
3. **Add `exclude_from_search` column** on `chunks`. Mark flagged chunks.
   Filter at search time in `hybrid_search()`.
4. **Re-chunk the corpus.** `scripts/ingest.py --rechunk` run after code
   changes. 10-20 min of compute on pliny or local.

**Effort.** Medium. Chunker logic + DB migration + re-ingest.

**Dependencies.** None. Can land independently of T1–T3.

**Note.** T4 reduces the number of bad chunks, but the bigger UX fix is
T1 (AI summary in the middle column). T4 improves the underlying data;
T1 changes how the user interacts with it.

---

## T5 — Snippet windowing in the results list

**Problem.** Even with clean chunks, showing the full 200-word chunk as
the result snippet is too much. Users want a tight preview around the
matching keyword.

**Fix.** In `partials/search_results.html` (or a small helper in
`app.py`), when rendering each `.result-item .snippet`:

- Find the first occurrence of any query term in the chunk
- Extract `~150` characters centered on that match
- Wrap the match in `<mark>`
- Prepend/append ellipsis if truncated
- Fall back to first 150 chars if no match (shouldn't happen with FTS hits)

**Effort.** Small. Template helper + CSS. 30 min.

**Dependencies.** None. Can ship before or alongside T1. If T1 ships,
this becomes the left-column snippet behavior by default.

---

## T6 — Budget as a first-class page with interactive charts

**Problem.** Budget is a massive use case for Actalux. Parents want to
know "what does the district spend money on, and how has that changed?"
— not to read 600-word budget-table text dumps. Current `/topic/budget`
is just a scoped search page.

**Desired shape.** A new `/budgets` page (or `/topic/budget` redesigned)
that offers:

- **Interactive charts.** Spending by category (salaries, facilities,
  curriculum, etc.), year-over-year deltas, revenue composition (property
  tax, state, federal), fund balance over time. Users can zoom into a
  category to see line items.
- **Narrative context.** Each chart has a short explanation of what it
  shows and a link to the source document page (board-approved budget
  PDF).
- **Citation integrity maintained.** Every number shown on a chart must
  trace to a specific table in a specific board-approved budget PDF.

**Technical pieces:**

1. **Extract structured data from budget PDFs.** pdfplumber or Tabula to
   pull line-item tables from each year's adopted budget. Store as
   `budget_line_items` table: year, category, subcategory, amount,
   source_document_id, source_page.
2. **Data normalization.** Budget category names change year-over-year;
   need a reconciliation layer (manual or rule-based).
3. **Frontend charts.** Observable Plot (HTMX-friendly, no build step) or
   Chart.js. Server-rendered HTML with the chart data embedded.
4. **Narrative + citation.** Each chart reads from the DB, emits a
   summary sentence and a `#qXXXX` citation to the source page.

**Effort.** Large. Multi-session. Could be milestone 2 after T1-T5 are
solid.

**Dependencies.** T2 (PDF page jumps) makes citation drill-down from a
chart to a PDF page work well. Also benefits from having the newly-found
budget documents ingested (see T7).

---

## T7 — Ingest newly-found budget documents

**Problem.** User found more budget documents during this session; they
were stored in a different location than the main Diligent portal.

**Fix.** Locate the newly-found documents, add them to
`data/documents/`, run ingestion, verify they appear in the corpus with
correct metadata.

**Effort.** Small — 1 hour unless the new source requires a different
crawler/parser.

**Dependencies.** None. Should run soon so T6 has complete data.

---

## Priority + suggested order

Three loose phases:

### Phase 1 — Usability pivot (makes current search usable)
- **T1** redesign reader pane as summary + native source
- **T5** snippet windowing in results list
- **T4** chunker quality pass + filter low-value chunks
- **T7** ingest newly-found budget documents

### Phase 2 — Native source viewer
- **T2** PDF viewer with page jump (v1), then highlight overlay (v2)
- **T3** YouTube embed + transcript split for meeting results

### Phase 3 — Budget as its own thing
- **T6** interactive budget charts with citations

Phase 1 unlocks the product's value prop. Phase 2 polishes the reader.
Phase 3 is a standalone feature that could become the flagship demo.

---

## Cross-cutting notes

- **Citation integrity is the constraint.** Every change must preserve
  the rule that every AI-generated statement links to a verifiable source
  quote. T1's summaries must cite. T6's chart numbers must cite.
- **No editorializing.** Summaries state what the source says, not what
  the board should have done.
- **Nonpartisan.** Never mention PropO or MEC complaint. Never take
  sides.
- **Closed session content never published.**
