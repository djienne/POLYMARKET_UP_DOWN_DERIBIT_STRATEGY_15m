import itertools
from datetime import datetime, timezone
from typing import Any


_counter = itertools.count(1)


def envelope(topic: str, data: Any) -> dict:
    return {
        "type": topic,
        "id": str(next(_counter)),
        "server_time": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
