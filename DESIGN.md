# Design System — Actalux

Created 2026-04-12 by `/design-consultation`. Source of truth for all visual
and UI decisions. Do not deviate without explicit approval.

## Product Context

- **What this is:** A nonpartisan, nonprofit searchable archive of public
  records of the Clayton, Missouri school district. Every passage on the site
  is a verbatim quote with its source citation attached.
- **Who it's for:** Clayton parents, community members, and researchers who
  want to track board decisions without watching hours of video.
- **Space/industry:** Civic transparency / local government records. Peers:
  Aware, Hamlet, CivicSearch, MuckRock, DocumentCloud, Retriever.
- **Project type:** Web application + archive reader. FastAPI + HTMX +
  Jinja2, no JS build step.

## Aesthetic Direction

- **Direction:** Editorial / archival with modern restraint.
- **Decoration level:** Minimal. Typography and whitespace do the work. One
  accent color used sparingly, the way a highlighter marks a quote.
- **Mood:** "NYT Upshot meets The Marshall Project meets a university
  special-collections reading room, built in 2026." Literate, institutional,
  calm, trustworthy. Not bureaucratic. Not patriotic. Not AI-SaaS.
- **Reference sites visited during research:** awarenow.ai, myhamlet.com,
  civicsearch.org, muckrock.com, documentcloud.org, retriever.dog.
- **White space in the category:** Peers either read as generic AI SaaS
  (Retriever, Hamlet, Aware) or as dated nonprofits (CivicSearch, MuckRock,
  DocumentCloud). Nobody owns a serious editorial/archival aesthetic done
  modern. That is our lane.

## Typography

All fonts loaded from Google Fonts. Variable fonts used where available to
minimize request weight.

- **Display (hero headings, featured quotes, section titles):** **Fraunces**
  — variable serif, modern-editorial, enough character without being dated.
  Use roman weight 400 for headings; italic 350 for featured quotes. Do NOT
  use Fraunces italic at sizes larger than ~40px — at 72px it read as
  overwrought in v1. Use Fraunces italic only for quotes (where it means
  "this is a quote") and Fraunces roman for headings.
- **Body (document prose, quote text, lede, long-form reading):**
  **Newsreader** — variable serif, designed for on-screen reading. Literate
  but unmistakably current.
- **UI (navigation, labels, buttons, metadata):** **Geist** — modern neutral
  sans. Feels 2025-2026. Weight 400 for labels, 500 for buttons and active
  nav items, 600 for small-caps section labels.
- **Data (citations, hashes, dates, IDs, counts, timecodes):** **IBM Plex
  Mono** — institutional, tabular-nums, free. All citation metadata uses
  Plex Mono.

**Blacklist.** Never use Inter, Roboto, Arial, Helvetica, Poppins,
Montserrat, Raleway, Clash Display, Lato, Open Sans — overused and/or not
fitting. System fallbacks only if Google Fonts fails.

**Type scale.** Modular, approximate:

| Role | Font | Size | Weight | Line-height |
|---|---|---|---|---|
| Hero H1 | Fraunces | clamp(28px, 3.2vw, 40px) | 400 | 1.1 |
| Featured quote | Fraunces italic | clamp(18px, 2vw, 24px) | 400 | 1.35 |
| Section divider | Fraunces italic | 22px | 400 | 1.2 |
| Reader H3 | Fraunces | 24px | 400 | 1.2 |
| Document H1 | Fraunces | 32px | 400 | 1.15 |
| Body / lede | Newsreader | 17px | 400 | 1.55 |
| Snippet | Newsreader | 15px | 400 | 1.45 |
| UI label | Geist | 14px | 500 | 1.4 |
| Nav sub | Geist | 13px | 400 | 1.5 |
| Section-header small caps | Geist / mono | 11px | 600 | 1.4 |
| Metadata | IBM Plex Mono | 11-12px | 400 | 1.8-1.9 |
| Hash / ID | IBM Plex Mono | 10-11px | 400 | 1.6 |

## Color

Restrained palette — one accent + warm neutrals. No gradients. No purple.
No generic civic-blue. The accent is used sparingly, the way a highlighter
marks a source.

```css
:root {
  /* surfaces */
  --paper:       #FAFAF7;  /* warm off-white, primary canvas */
  --paper-warm:  #F4F2EC;  /* surface tint for asides, input bg */
  --paper-sunk:  #EFEDE5;  /* slightly deeper surface for state-divider */

  /* text */
  --ink:         #0F0F0F;  /* near-black primary text */
  --ink-soft:    #2A2A28;  /* body prose */
  --muted:       #6B6B68;  /* secondary text, metadata labels */
  --muted-soft:  #9A988F;  /* tertiary, placeholder */

  /* rules */
  --rule:        #E5E4DE;  /* hairline divider */
  --rule-strong: #C9C6BD;  /* stronger divider, input border */

  /* accent — vermillion (the "highlighter on archive" move) */
  --accent:      #C8553D;  /* primary accent, CTA, highlight chroma */
  --accent-soft: #F3DDD6;  /* highlighted-word background */

  /* citation treatment */
  --highlight:   #F4E9B0;  /* archival-yellow highlight for cited passages
                              inside source documents + transcripts */

  /* semantic (muted — institutional, not signal-light bright) */
  --success:     #3A7A52;  /* verified, ingested ok */
  --warning:     #9E7C25;  /* needs review */
  --error:       #9B3330;  /* retracted, corrected */
}
```

**Accent usage rules.**

- Vermillion (`--accent`) is the single brand color. Use for: active nav
  state, primary CTA hover, quote marks on featured quotes, active result
  left-border (3px), inline highlight-on-search (`--accent-soft`
  background).
- Cited passages inside source documents and transcripts use the archival
  yellow `--highlight` + a 3px vermillion left border. Two accents working
  together: yellow = "the passage someone cited," vermillion = "this is
  Actalux."
- Never use vermillion for body text, body links, or decorative fill.
  Typography and whitespace carry the page; accent punctuates.

**Alternate considered and rejected:** Deeper oxblood `#8B3A2A`. More
restrained, reads as more institutional. We landed on the warmer vermillion
because it actually reads as "highlighter ink" — the color means the
thing the product does.

**Dark mode:** Not in scope for MVP. When added, redesign surfaces rather
than invert (paper-dark should be warm charcoal `~#1A1815`, not pure black;
accent saturation reduces ~15%).

## Layout

**Approach:** App shell — sticky top bar, sticky left sidebar, single main
content area. Hybrid grid inside the main area for different page types.

**Shell structure.**

```
┌──────────────────────────────────────────────────────────┐
│ Actalux · Clayton MO   [ search ⌘K ]       About  GitHub │ 56px top bar
├─────────────┬────────────────────────────────────────────┤
│             │                                            │
│ Sidebar     │ Main content                               │
│ 256px       │                                            │
│ sticky      │  - Landing: tagline + lede + featured      │
│             │    quote + recently ingested               │
│ · Topics ▾  │                                            │
│   Budget    │  - Search / topic: results list (380px) +  │
│   Curric.   │    reader pane (rest) with source context  │
│   Facil.    │                                            │
│ · Meetings ▸│  - Document: 3-col — meta / prose / TOC    │
│ · Documents▸│                                            │
│             │                                            │
│ · Quick     │                                            │
│             │                                            │
└─────────────┴────────────────────────────────────────────┘
```

**Grid.**

- Sidebar: 256px fixed, sticky to viewport below top bar, scrolls
  independently.
- Top bar: 56px, sticky, `border-bottom: 1px solid var(--rule)`.
- Main: `1fr`, `min-width: 0` (critical for child overflow behavior).
- Landing content: max-width 920px, left-aligned with `padding: 48px 32px`.
- Document reading column: max-width 720px (roughly 65-75 characters per
  line, the prose target).
- Split pane: results column 380px fixed, reader column fills remaining.

**Max-width ladder.**

| Role | Max-width |
|---|---|
| Reading column (document prose) | 720px |
| Landing copy | 920px |
| Featured quote | 820px |
| Top bar content | full (padded) |

**Border radius.** 0 everywhere. This is an archive. Rounded corners are
a SaaS tell.

**Dividers.** Hairline 1px `var(--rule)` by default. Top borders on
section headers use `var(--rule-strong)` or 2px solid `var(--ink)` for
citation rails.

**Responsive.** Below 960px the sidebar collapses to a drawer (sidebar CSS
sets `display: none` at the breakpoint in the preview; real implementation
should use an off-canvas pattern with a menu button in the top bar). The
split pane stacks (results on top, reader below) below 960px.

## Spacing

**Base unit: 8px.** Density: comfortable — not spacious (this is a
research tool, not a marketing site), not compact (this is for reading).

```
2xs  4px   tight inline gaps
xs   8px   icon-to-label, kbd hint padding
sm  12px   small padding, inline gap
md  16px   default component padding
lg  24px   section internal padding, gap between related blocks
xl  32px   section side padding, main column padding
2xl 48px   section-to-section separation
3xl 64px   major page-region separation
4xl 96px   landing top padding, hero breathing room
```

## Motion

**Approach:** Minimal-functional. Motion exists to aid comprehension, not
to entertain.

- **Durations.** `micro: 50-100ms` (hover state change), `short: 120-180ms`
  (collapse/expand, fade-in), `medium: 250-300ms` (page transitions if any),
  `long: 400ms+` (avoid — feels slow on a research tool).
- **Easing.** `ease-out` for enter, `ease-in` for exit, `ease-in-out` for
  move.
- **Specific animations used.**
  - Sidebar section collapse: 180ms `max-height` transition, `ease-out`.
  - Chevron rotate: 180ms `transform`, `ease-out`.
  - Button hover: 120ms `all`, `ease-out`.
  - Result card hover: instant background swap (no transition — reading
    apps should feel responsive, not draggy).
- **No.** Parallax. Scroll-triggered reveal. Skeleton shimmer. Spring
  physics. Confetti. Any particle effect, ever.

## Interaction Patterns

### Search is always visible
Search field lives in the top bar, sticky, at 36px tall with `⌘K` keyboard
hint. Never hide it behind a disclosure. Search is the primary action;
browse is secondary.

### Quote as featured, not hero
Landing page leads with a tagline + lede that states clearly what the
product is. A single featured cited quote appears below the lede as a
taste of what the product does, not as the hero. The quote should never
crowd out the tagline — users must understand the product in one glance.

### Result → opens in reader pane to the right
Clicking a search result opens the source in a right-side reader pane
without full navigation. The reader pane shows:

- **For video/transcript results:** an embedded YouTube player cued to
  the exact timecode (`?start=<seconds>`), followed by a transcript
  excerpt with the cited passage highlighted in archival yellow +
  vermillion left border. Surrounding speaker turns are shown for
  context.
- **For document results (PDF-derived):** the relevant section of the
  document with the cited passage highlighted in the same archival
  yellow + vermillion border, with the rest of the document prose
  readable in context.
- **Citation metadata bar** at the top of the reader pane: portal,
  source, length, cue point, speaker (if transcript), hash ID. All in
  Plex Mono.
- **Reader footer controls:** "Open full", "Cite this passage", "Watch
  on YouTube" (transcripts only), "Report an error".

### Cited passage highlight
Wherever a cited passage appears inside a source document or transcript,
apply:

```css
background: var(--highlight);       /* #F4E9B0 archival yellow */
box-shadow: -3px 0 0 var(--accent);  /* vermillion left edge */
padding: 2px 4px 2px 8px;
box-decoration-break: clone;
```

This is the single most important visual motif of the site — it is the
product's promise made visible.

### Inline highlight-on-search-match
When search results contain the user's query term, wrap matches in
`<mark>` with `background: var(--accent-soft)`. Subtler than the passage
highlight — this is "we found your word" not "this is the cited passage."

### Collapsible sidebar sections
Sidebar nav groups (Topics, Meetings, Documents) are chevron-toggle
buttons. State persists in localStorage under `actalux.nav.expanded`.
Default state: expand the section the user is currently browsing,
collapse the rest. Chevron rotates -90° when collapsed.

### No modals for primary actions
Opening a document, citing, searching — none of these open a modal. The
app shell with its reader pane handles all primary flows. Modals are
reserved for destructive confirmations or external integrations only
(e.g., "Report an error" may open a modal form; subscribing to alerts
may open a modal).

## Content Accuracy (hard constraints)

These are not stylistic — they are content-policy requirements.

- **Never claim completeness of the record.** Avoid phrases like "every
  document they signed" or "the complete record." We have what is
  public, plus what we have obtained through Missouri Sunshine Law
  requests. Correct phrasing: "public records," "documents we have
  gathered," "every quote traceable to its source."
- **Every AI-generated statement cites a verbatim source quote.** No
  unconstrained summaries.
- **Nonpartisan language only.** No mention of PropO or MEC complaint.
  No editorializing.
- **Board and administration policy only.** No individual personnel,
  teachers, or students.
- **Closed-session content is never published.**

## Source Portals

`source_portal` is a free-form string on the `documents` table. Current
values plus forthcoming Sunshine Law support:

| Portal | Content | UI label |
|---|---|---|
| `diligent` | Minutes, budgets, resolutions, calendars | "Diligent" |
| `claytonschools` | Curriculum maps, LRFMP, strategic plan | "District website" |
| `youtube` | Board meeting transcripts | "YouTube" |
| `manual` | Manually added documents | "Manual" |
| `sunshine` | Records obtained via Missouri Sunshine Law request | "Sunshine request" |

Sunshine-request documents display additional provenance in the citation
rail: request date, responding agency, request ID. Consider adding a
`provenance_notes` JSONB column to `documents` if sunshine-request volume
justifies it; for now, packing structured fields into `source_url` or an
extra column is fine.

## Error-Reporting UX

"Report an error" is a first-class action wherever a quote or document is
shown. It appears:

- Next to every citation in search results and the reader pane
- In the reader footer alongside "Cite" and "Watch on YouTube"
- In the sidebar Quick section
- Below every highlighted passage inside a document view

Clicking opens a form (route: `/report?chunk=#qXXXXXXX`) that creates a
GitHub Issue via the GitHub API. This is transparency-by-design: all
corrections are publicly visible.

## Components Reference

All components follow the typography and color rules above. The preview
HTML (`/tmp/actalux-design-v3.html` as of 2026-04-12) is the canonical
visual reference for: top bar + search, collapsible sidebar, landing
layout, featured quote, result card, reader pane with video + transcript
highlight, document preview, buttons, badges, input, citation stub,
spacing scale.

### Buttons

```
.btn          — 13-14px Geist 500, 8-10px × 14-18px padding, 1px border ink, square
.btn-primary  — ink bg, paper text; hover → accent bg + accent border
.btn          — transparent bg, ink text, ink border; hover → accent text + accent border
.btn-ghost    — transparent, no border, muted text; hover → accent text
.btn-accent   — accent bg, paper text; reserved for single high-intent CTA (alerts)
```

### Badges

```
.badge         — 11px Geist 500, 3px × 9px padding, 1px border rule-strong, muted-soft text
.badge-accent  — accent-soft bg, accent text (for "Cited · 23" style counts)
.badge-mono    — 11px IBM Plex Mono, for dates and page refs inline
```

### Input

Full-width, 12px padding, 1px `--rule-strong` border, Newsreader 16px for
the actual text (not sans — inputs are where the user writes prose queries
into an archive, so they deserve serif). Focus: border color → `--ink`.

## Implementation Notes

- **CSS only.** No JS frameworks. HTMX handles all server-swap
  interactions. The only inline JS in the preview is the sidebar
  collapse toggle with localStorage persistence; keep it small and
  vanilla.
- **Font loading.** Google Fonts `<link>` in `<head>` with `preconnect`.
  Consider self-hosting for privacy if we ever have regulated users, but
  Google Fonts is fine for MVP.
- **No icon library.** Use inline SVG for the handful of icons we need
  (search, chevron, play). Each icon should be ≤20 lines of SVG path
  data. No Lucide, no Heroicons, no Font Awesome — adds weight and
  visual tell of "generic web app."
- **Accessibility.** All interactive elements have visible focus states.
  Color contrast meets WCAG AA (ink on paper = 16:1; muted on paper =
  5.5:1). The cited-passage highlight background + left-border + bold
  text combination does not rely on color alone. Keyboard navigation:
  `⌘K` jumps to search, `Tab` through results, `Enter` opens the
  selected result in the reader pane, `Esc` closes the reader pane if
  viewing as overlay.
- **Performance budget.** Landing page should be `<50KB` total payload
  excluding fonts. Fonts add ~80KB (Fraunces variable + Newsreader
  variable + Geist + Plex Mono, subset to Latin). Use `font-display:
  swap`. Lazy-load YouTube embeds until the user clicks play (use a
  poster thumbnail + play button, then swap to the real iframe on
  click).

## Decisions Log

| Date | Decision | Rationale |
|---|---|---|
| 2026-04-12 | Editorial/archival aesthetic | Peers either read as AI-SaaS (Retriever/Hamlet/Aware) or dated nonprofit (CivicSearch/MuckRock/DocumentCloud). Modern editorial is open category space. |
| 2026-04-12 | Fraunces + Newsreader + Geist + IBM Plex Mono | Differentiated from all six peers (none use this stack); each font has a specific role; all on Google Fonts; none on the overused blacklist. |
| 2026-04-12 | Vermillion `#C8553D` accent | "Highlighter on archive" metaphor literal. Departs from patriotic red/civic blue/nature green palette of peers. Oxblood `#8B3A2A` was considered and rejected as too restrained — we want the accent to read as highlight ink. |
| 2026-04-12 | App-shell layout (top bar + sidebar + main) over marketing-page layout | v1 landing page made the product slow to understand. App shell makes search instantly visible, IA permanently accessible, and reader-pane-in-context natural. |
| 2026-04-12 | Search in top bar, always visible | v1 put search below a giant hero quote. User correctly flagged that the product's purpose was slow to register. Top-bar search solves both. |
| 2026-04-12 | Quote as featured, not hero | Same v1 feedback. Featured quote is a demonstration of the product, placed below the tagline so the tagline reads first. |
| 2026-04-12 | Result opens in right-side reader pane with highlighted citation in source context | User requested. Also directly makes the citation-first value prop experiential. For transcript results, embedded YouTube cued to timecode. For document results, scroll-to-section with highlight. |
| 2026-04-12 | Collapsible sidebar with localStorage persistence | Scales to larger corpora without crowding. Default: expand the section currently being browsed. |
| 2026-04-12 | Tagline "Clayton school board meetings and public documents, made searchable — with every quote traceable to its source." | User flagged that v1's "every word / every document they signed" overclaimed. Revised to accurately describe what we have (public records + Sunshine requests) and the core moat (traceability). |
| 2026-04-12 | Sunshine requests as first-class source portal | Some records will come from Missouri Sunshine Law requests. `source_portal = "sunshine"` added; citation rail shows request metadata. |
| 2026-04-12 | No border-radius, no icon library, no gradients, no modals for primary actions | Anti-slop. Every one of these is a SaaS tell. Archive should feel like an archive. |
