from datetime import UTC, datetime

import pytest

from arti.versions import GitCommit, SemVer, String, Timestamp, _Source


def test_GitCommit() -> None:
    assert GitCommit(sha="test").sha == "test"
    assert len(GitCommit().sha) == 40  # Length of git sha


@pytest.mark.parametrize(
    ("major", "minor", "patch", "fingerprint_fields"),
    [
        (0, 0, 0, {"major", "minor", "patch"}),
        (0, 0, 1, {"major", "minor", "patch"}),
        (0, 1, 0, {"major", "minor", "patch"}),
        (0, 1, 1, {"major", "minor", "patch"}),
        # Major versions >=1 fingerprint the major alone
        (1, 0, 0, {"major"}),
        (1, 0, 1, {"major"}),
        (1, 1, 0, {"major"}),
        (1, 1, 1, {"major"}),
        (2, 0, 0, {"major"}),
        (2, 5, 5, {"major"}),
    ],
)
def test_SemVer(major: int, minor: int, patch: int, fingerprint_fields: set[str]) -> None:
    assert set(SemVer._arti_fingerprint_fields_) == {"_arti_type_", "major", "minor", "patch"}
    version = SemVer(major=major, minor=minor, patch=patch)
    assert set(version._arti_fingerprint_fields_) == {"_arti_type_", *fingerprint_fields}


def test__Source() -> None:
    class P:
        version: String = _Source()

    src = "    class P:\n        version: String = _Source()\n"
    assert P.version.value == P().version.value == src

    class P2:
        version = _Source()

    assert P.version.fingerprint != P2.version.fingerprint


def test_Timestamp() -> None:
    default = Timestamp()
    assert default.dt.tzinfo is not None
    explicit = Timestamp(dt=datetime.now(tz=UTC))
    assert explicit.dt.tzinfo is not None
    # Check the default is ~now.
    assert (explicit.dt - default.dt).total_seconds() < 1

    # Confirm naive datetime are not supported
    with pytest.raises(ValueError, match="Timestamp requires a timezone-aware datetime!"):
        Timestamp(dt=datetime.now())
