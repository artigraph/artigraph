from __future__ import annotations

import string
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from functools import partial
from typing import Any, Optional

import parse

from arti.fingerprints import Fingerprint
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, InputFingerprints, PartitionKey


class Placeholder:
    def __init__(self, key: str) -> None:
        self._key = key


class FormatPlaceholder(Placeholder):
    def __format__(self, spec: str) -> str:
        result = self._key
        if spec:
            result += f":{spec}"
        return "{" + result + "}"

    def __getitem__(self, key: Any) -> FormatPlaceholder:
        self._key = f"{self._key}[{key}]"
        return self

    def __getattr__(self, attr: str) -> FormatPlaceholder:
        self._key = f"{self._key}.{attr}"
        return self


# Used to convert things like `/{date_key.Y[1970]` to `/{date_key.Y` so we can format in
# *real* partition key values.
class StripIndexPlaceholder(FormatPlaceholder):
    def __getitem__(self, key: Any) -> StripIndexPlaceholder:
        return self


class WildcardPlaceholder(Placeholder):
    def __init__(self, key: str, key_types: CompositeKeyTypes):
        super().__init__(key)
        if key not in key_types:
            raise ValueError(f"No '{key}' partition key found, expected one of {tuple(key_types)}")
        self._key_type = key_types[key]
        self._attribute: Optional[str] = None  # What field/key_component is being accessed

    @classmethod
    def with_key_types(
        cls, key_types: Mapping[str, type[PartitionKey]]
    ) -> Callable[[str], WildcardPlaceholder]:
        return partial(cls, key_types=key_types)

    def _err_if_no_attribute(self) -> None:
        if self._attribute is None:
            example = sorted(self._key_type.key_components)[0]
            raise ValueError(
                f"'{self._key}' cannot be used directly in a partition path; access one of the key components (eg: '{self._key}.{example}')."
            )

    def __getattr__(self, name: str) -> WildcardPlaceholder:
        if self._attribute is not None:
            raise ValueError(
                f"'{self._key}.{self._attribute}.{name}' cannot be used in a partition path; only immediate '{self._key}' attributes (such as '{self._attribute}') can be used."
            )
        if name not in self._key_type.key_components:
            raise AttributeError(
                f"'{self._key_type.__name__}' has no field or key component '{name}'"
            )
        self._attribute = name
        return self

    def __getitem__(self, key: Any) -> Any:
        self._err_if_no_attribute()
        # Pass through "hard coded" partition parts, eg: "{date.Y:2021}" -> format_spec="2021".
        #
        # TODO: Should we validate the key against the key_type/attribute type?
        return key

    def __str__(self) -> str:
        self._err_if_no_attribute()
        return "*"


# NOTE: We might pre-populate this with the Graph tags so the hard coded templates are already available (rather than
# being filled in with a WildcardPlaceholder).
class FormatDict(dict[str, Any]):
    def __init__(
        self,
        placeholder_type: Callable[[str], Placeholder],
        /,
        *args: Iterable[tuple[str, Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.placeholder_type = placeholder_type

    def __missing__(self, key: str) -> Placeholder:
        return self.placeholder_type(key)


def partial_format(spec: str, **kwargs: Any) -> str:
    return string.Formatter().vformat(spec, (), FormatDict(FormatPlaceholder, **kwargs))


# This is hacky...
def strip_partition_indexes(spec: str) -> str:
    return string.Formatter().vformat(spec, (), FormatDict(StripIndexPlaceholder))


def spec_to_wildcard(spec: str, key_types: Mapping[str, type[PartitionKey]]) -> str:
    return string.Formatter().vformat(
        spec.replace("{input_fingerprint}", "*"),
        (),
        FormatDict(WildcardPlaceholder.with_key_types(key_types)),
    )


def extract_placeholders(
    *,
    error_on_no_match: bool = True,
    key_types: Mapping[str, type[PartitionKey]],
    parser: parse.Parser,
    path: str,
    spec: str,
) -> Optional[tuple[Fingerprint, CompositeKey]]:
    parsed_value = parser.parse(path)
    if parsed_value is None:
        if error_on_no_match:
            raise ValueError(f"Unable to parse '{path}' with '{spec}'.")
        return None
    input_fingerprint = Fingerprint.empty()
    key_components = defaultdict[str, dict[str, str]](dict)
    for k, v in parsed_value.named.items():
        if k == "input_fingerprint":
            assert isinstance(v, str)
            input_fingerprint = Fingerprint.from_int(int(v))
            continue
        key, component = k.split(".")
        # parsing a string like "{date.Y[1970]}" will return a dict like {'1970': '1970'}.
        if isinstance(v, dict):
            v = tuple(v)[0]
        key_components[key][component] = v

    keys = {
        key: key_types[key].from_key_components(**components)
        for key, components in key_components.items()
    }
    if set(keys) != set(key_types):
        raise ValueError(
            f"Expected to find partition keys for {sorted(key_types)}, only found {sorted(keys)}. Is the partitioning spec ('{spec}') complete?"
        )
    return input_fingerprint, frozendict(keys)


def parse_spec(
    paths: set[str],
    *,
    input_fingerprints: InputFingerprints = InputFingerprints(),
    key_types: Mapping[str, type[PartitionKey]],
    spec: str,
) -> Mapping[str, tuple[Fingerprint, CompositeKey]]:
    parser = parse.compile(spec, case_sensitive=True)
    path_placeholders = (
        (path, placeholders)
        for path in paths
        if (
            placeholders := extract_placeholders(
                parser=parser, path=path, spec=spec, key_types=key_types
            )
        )
        is not None
    )
    return {
        path: (input_fingerprint, keys)
        for (path, (input_fingerprint, keys)) in path_placeholders
        if input_fingerprints.get(keys, Fingerprint.empty()) == input_fingerprint
    }
