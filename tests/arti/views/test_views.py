import pickle
from unittest.mock import mock_open, patch

import pytest
from pydantic import ValidationError

from arti.artifacts.core import Artifact
from arti.formats.pickle import Pickle
from arti.storage.local import LocalFile
from arti.types.core import Int64
from arti.views.core import View
from arti.views.python import Python


def test_View() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        View()


def test_Python_View() -> None:
    a = Artifact()
    a.storage = LocalFile(path="/tmp/foo.pkl")
    a.format = Pickle()

    with patch("builtins.open", mock_open(read_data=pickle.dumps(1))) as mock_file:
        view = Python.read(a)
        mock_file.assert_called_with("/tmp/foo.pkl", "rb")
        assert view.data == 1

    with patch("builtins.open", mock_open()) as mock_file:
        view.write(a)
        mock_file.assert_called_with("/tmp/foo.pkl", "wb")

    view.validate(Int64())
