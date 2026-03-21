from __future__ import annotations

import time
import uuid


def make_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time() * 1000):013d}_{uuid.uuid4().hex[:12]}"
