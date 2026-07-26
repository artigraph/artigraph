import logging
import runpy
from pathlib import Path

import pytest


def test_spend_example(caplog: pytest.LogCaptureFixture) -> None:
    example = Path(__file__).parents[1] / "docs" / "examples" / "spend" / "demo.py"

    with caplog.at_level(logging.INFO):
        runpy.run_path(str(example), run_name="__main__")

    assert "Total Spend: 42.0" in caplog.messages
