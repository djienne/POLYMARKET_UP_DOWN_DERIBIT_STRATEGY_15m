import time

from app.collector.spot_reader import RedisSpotReader, _spot_from_payload


def test_spot_reader_health_tracks_pubsub_message():
    reader = RedisSpotReader(
        redis_url="redis://example/0",
        latest_key="spot",
        channel="spot.channel",
    )
    spot = _spot_from_payload(
        {
            "price": 100.0,
            "source": "polymarket_rtds",
            "sequence": 42,
            "published_at_ms": int(time.time() * 1000),
        }
    )

    assert reader._record(spot, from_pubsub=True) is True

    health = reader.health()
    assert health["spot_sequence"] == 42
    assert health["spot_source"] == "polymarket_rtds"
    assert health["spot_last_pubsub_age_seconds"] is not None
    assert health["spot_age_seconds"] is not None


def test_spot_reader_latest_recomputes_age(monkeypatch):
    now = 1_800_000_000.0
    monkeypatch.setattr(time, "time", lambda: now)
    reader = RedisSpotReader(
        redis_url="redis://example/0",
        latest_key="spot",
        channel="spot.channel",
    )
    published_ms = int((time.time() - 2.0) * 1000)
    reader._record(
        _spot_from_payload(
            {
                "price": 100.0,
                "source": "polymarket_rtds",
                "sequence": 43,
                "published_at_ms": published_ms,
            }
        )
    )

    assert reader.latest.age_seconds == 2.0
    now += 10.0
    assert reader.latest.age_seconds == 12.0
    assert reader.latest.sequence == 43
