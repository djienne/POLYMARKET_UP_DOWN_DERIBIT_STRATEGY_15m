import asyncio
from collections import defaultdict
from typing import Any, Callable, Awaitable


Handler = Callable[[str, Any], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, set[Handler]] = defaultdict(set)
        self._wildcard: set[Handler] = set()

    def subscribe(self, topic: str, handler: Handler) -> Callable[[], None]:
        bucket = self._wildcard if topic == "*" else self._subs[topic]
        bucket.add(handler)

        def unsubscribe() -> None:
            bucket.discard(handler)

        return unsubscribe

    async def publish(self, topic: str, data: Any) -> None:
        handlers = list(self._subs.get(topic, ())) + list(self._wildcard)
        if not handlers:
            return
        await asyncio.gather(
            *(self._safe_call(h, topic, data) for h in handlers),
            return_exceptions=False,
        )

    @staticmethod
    async def _safe_call(handler: Handler, topic: str, data: Any) -> None:
        try:
            await handler(topic, data)
        except Exception:  # noqa: BLE001
            import logging
            logging.getLogger(__name__).exception(
                "bus handler error on topic=%s", topic
            )


bus = EventBus()
