from arti.internal import version


def test_version() -> None:
    assert version is not None
