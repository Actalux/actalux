"""Proper-name glossary tooling: discover ASR manglings from authoritative records.

The corrections themselves live in the ``name_corrections`` table (see
``scripts/migrate_033_name_corrections.sql``); this package derives *candidate*
corrections by aligning Whisper transcripts to the human-prepared record.
"""
