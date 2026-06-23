"""Shared pytest setup.

The unit tests never touch a real database — DB access is mocked throughout — but
``actalux.config.Config`` reads ``ACTALUX_SUPABASE_URL`` and ``ACTALUX_SUPABASE_KEY``
from the environment when it is constructed (the only two required settings;
everything else has a default). A handful of tests build a ``Config`` directly or
exercise a web route that does, so without these the documented bare command
``uv run python -m pytest tests/`` fails on ``KeyError`` rather than on any real
defect.

Provide inert placeholders so the suite is hermetic. ``setdefault`` never
overrides a value already in the environment, so running under
``doppler run --project mac --config dev`` uses the real credentials unchanged.
The placeholder URL/key are never dialed — the tests that build a ``Config`` mock
the database layer.
"""

import os

os.environ.setdefault("ACTALUX_SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("ACTALUX_SUPABASE_KEY", "test-anon-key")
