"""Speaker diarization: anonymous speaker-turn detection + alignment.

The public surface is the provider-agnostic ``DiarizationBackend`` port and the
``SpeakerTimeline`` domain type (``backend``), plus ``align`` for mapping turns
onto existing transcript chunks. Concrete GPU adapters (e.g. ``ModalRunner``)
live in their own modules so importing the seam never pulls in a vendor SDK.
"""

from actalux.diarization.backend import DiarizationBackend, SpeakerTimeline, SpeakerTurn

__all__ = ["DiarizationBackend", "SpeakerTimeline", "SpeakerTurn"]
