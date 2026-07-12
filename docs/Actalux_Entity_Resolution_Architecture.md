# Actalux Entity Resolution Architecture

## Purpose

Entity resolution is the process of connecting many references to the
same real-world object into a single canonical representation.

The goal is to answer questions about **real-world entities and
matters**, not merely retrieve documents.

For example, all of the following may refer to the same underlying civic
matter:

-   Project 24-019
-   Hanley redevelopment
-   Ordinance 2026-17
-   Agenda Item 7C
-   7725 Hanley Road

Internally, Actalux should recognize these as one evolving object with
multiple identifiers and evidence sources.

------------------------------------------------------------------------

# Core Principle

Do not ask:

> What document contains this phrase?

Ask:

> What real-world entity or civic matter does this phrase refer to?

This distinction is what separates search from civic intelligence.

------------------------------------------------------------------------

# Canonical Entity Types

Keep the ontology intentionally small.

## Organization

-   City
-   School district
-   Department
-   Committee
-   Contractor

## Person

-   Officials
-   Staff
-   Applicants
-   Consultants

## Place

-   Address
-   Parcel
-   Facility
-   School
-   Road segment
-   Neighborhood

## Matter

The most important entity type.

Examples:

-   Development project
-   Ordinance
-   Resolution
-   Permit
-   Contract
-   Lawsuit
-   Bond issue

## Documents / Events

-   Meetings
-   Agendas
-   Minutes
-   Packets
-   Transcripts
-   Votes

------------------------------------------------------------------------

# Separate Extraction from Resolution

Pipeline:

``` text
Document / Audio / Transcript
            ↓
Entity mention extraction
            ↓
Candidate lookup
            ↓
Entity resolution
            ↓
Canonical entity graph
            ↓
Search / Timeline / Alerts / Reasoning
```

An LLM should never directly update the canonical knowledge graph in a
single step.

------------------------------------------------------------------------

# Mentions vs Canonical Entities

Every mention should be stored separately.

Example mention:

    "the Hanley project"

Canonical entity:

    matter_000184
    Hanley Road Redevelopment

This allows uncertainty to be represented explicitly.

------------------------------------------------------------------------

# Suggested Schema

``` text
entity
- id
- type
- canonical_name
- status
- created_at
- updated_at

entity_alias
- entity_id
- alias
- source_id
- confidence

entity_mention
- id
- source_document_id
- text_span
- normalized_text
- entity_type_guess
- resolved_entity_id
- confidence
- evidence_location

entity_link
- subject_entity_id
- predicate
- object_entity_id
- source_id
- confidence
```

------------------------------------------------------------------------

# Prefer Deterministic Identifiers

Always attempt deterministic matching before semantic matching.

Highest-confidence identifiers include:

-   Ordinance numbers
-   Resolution numbers
-   Permit IDs
-   Contract IDs
-   Parcel IDs
-   Addresses
-   Official email addresses
-   Organization domains
-   GIS feature IDs

Embeddings should be a fallback, not the primary mechanism.

------------------------------------------------------------------------

# Multi-Signal Resolution

Candidate entities should be scored using multiple independent signals.

  Signal                          Relative confidence
  ------------------------------- ---------------------
  Same explicit identifier        Very high
  Same parcel                     Very high
  Same normalized address         Very high
  Same ordinance/project number   Very high
  Same applicant and location     High
  Same meeting sequence           High
  Similar name                    Medium
  Nearby dates                    Medium
  Same organization               Medium
  Embedding similarity            Low--Medium

A weighted scoring model is likely sufficient for an initial
implementation.

------------------------------------------------------------------------

# Confidence Thresholds

Suggested policy:

-   Score \> 0.90 → automatic resolution
-   0.60--0.90 → review
-   \< 0.60 → create new entity

Avoid destructive merges.

Instead, represent relationships such as:

``` text
Entity A
    SAME_AS
Entity B

Status:
- proposed
- confirmed
- rejected
```

This keeps every decision reversible.

------------------------------------------------------------------------

# Timelines

Once entities are resolved, timelines become straightforward.

Example:

``` text
Hanley Road Redevelopment

2026-01-14  First appears in planning agenda
2026-02-03  Staff report published
2026-02-19  Public comments received
2026-03-12  Ordinance introduced
2026-04-02  Vote passed
2026-05-10  Permit issued
```

Every event should remain linked to the original evidence.

------------------------------------------------------------------------

# Recommended MVP

Begin with only five entity classes:

-   Organizations
-   People
-   Places
-   Matters
-   Documents / Meetings

Initially resolve only:

-   Agenda item → Matter
-   Ordinance → Matter
-   Resolution → Matter
-   Parcel / Address → Place
-   Person name → Person
-   Department → Organization

This is sufficient to track a civic issue across months of meetings.

------------------------------------------------------------------------

# Practical First Version

1.  Extract identifiers.
2.  Normalize names and addresses.
3.  Search existing entities using exact identifiers.
4.  Fall back to fuzzy matching.
5.  Fall back to embeddings.
6.  Use an LLM only for ambiguous cases.
7.  Store every mention, decision, confidence score, and citation.
8.  Provide an administrative review interface for corrections.

Human correction should improve the knowledge base over time.

------------------------------------------------------------------------

# Long-Term Vision

The long-term objective is not a search engine.

It is a continuously updated, evidence-backed model of civic reality.

Documents become observations.

Canonical entities become the persistent representation of the real
world.

Reasoning, search, alerts, timelines, and future prediction all operate
over those entities rather than over isolated documents.
