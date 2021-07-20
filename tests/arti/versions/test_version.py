from datetime import datetime, timezone

import pytest

from arti.versions.core import GitCommit, SemVer, String, Timestamp, Version, _Source


def test_GitCommit() -> None:
    assert GitCommit(sha="test").sha == "test"
    assert GitCommit(sha="test").fingerprint == 8581389452482819506
    assert len(GitCommit().sha) == 40  # Length of git sha


@pytest.mark.parametrize(
    ["major", "minor", "patch", "fingerprint"],
    (
        (0, 0, 0, -4875916080982812485),
        (0, 0, 1, -6022020963282911891),
        (0, 1, 0, -612532240571011082),
        (0, 1, 1, -1388070919761090296),
        # Major versions >=1 fingerprint the major alone
        (1, 0, 0, -9142586270102516767),
        (1, 0, 1, -9142586270102516767),
        (1, 1, 0, -9142586270102516767),
        (1, 1, 1, -9142586270102516767),
        (2, 0, 0, 6920640749119438759),
        (2, 5, 5, 6920640749119438759),
    ),
)
def test_SemVer(major: int, minor: int, patch: int, fingerprint: int) -> None:
    assert SemVer(major=major, minor=minor, patch=patch).fingerprint == fingerprint


def test__Source() -> None:
    class P:
        version: Version = _Source()

    assert isinstance(P.version, String)
    assert P.version.value == "    class P:\n        version: Version = _Source()\n"
    assert P.version.fingerprint == -4528092110694557253
    assert P().version.fingerprint == -4528092110694557253

    class P2:
        version = _Source()

    assert P.version.fingerprint != P2.version.fingerprint


def test_String() -> None:
    assert String(value="ok").fingerprint == 5227454011934222951


def test_Timestamp() -> None:
    d = datetime.now(tz=timezone.utc)
    assert Timestamp(dt=d).fingerprint == round(d.timestamp())
    # Check the default is ~now.
    key, now = Timestamp().fingerprint.key, round(datetime.now(tz=timezone.utc).timestamp())
    assert key is not None
    assert now - 1 <= key <= now + 1
    # Confirm naive datetime are not supported
    with pytest.raises(ValueError, match="Timestamp requires a timezone-aware datetime"):
        Timestamp(dt=datetime.now())


def test_Version() -> None:
    # Version sets an @abstractmethod, so ABC catches it before our Model._abstract_ validator.
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class Version with abstract method fingerprint",
    ):
        Version()  # type: ignore
