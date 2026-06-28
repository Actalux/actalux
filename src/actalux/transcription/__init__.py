"""Word-level transcription seam: clean WhisperX (no name biasing) on a GPU.

The public surface is the provider-agnostic ``TranscriptionBackend`` port and the
``WordTranscript`` domain type (``backend``). Names are NOT biased at decode time
(an ``initial_prompt`` / ``hotwords`` regurgitates into the transcript — see the
A/B in docs/architecture/speaker-attribution.md); name canonicalization happens
downstream via the glossary. Concrete GPU adapters (e.g. ``WhisperXRunner``) live
in their own modules so importing the seam never pulls in a vendor SDK.
"""

from actalux.transcription.backend import (
    TranscriptionBackend,
    TranscriptSegment,
    Word,
    WordTranscript,
)

__all__ = ["TranscriptionBackend", "TranscriptSegment", "Word", "WordTranscript"]
