from __future__ import annotations

import string
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Iterable, Literal, Optional, overload

import parse

from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, PartitionKey


class WildcardPlaceholder:
    def __init__(self, name: str, key_type: type[PartitionKey]):
        self._name = name
        self._key_type = key_type
        self._valid_attributes = set(key_type.__fields__) | key_type.key_components
        self._attribute: Optional[str] = None  # What field/key_component is being accessed

    def _err_if_no_attribute(self) -> None:
        if self._attribute is None:
            example = sorted(self._key_type.key_components)[0]
            raise ValueError(
                f"'{self._name}' cannot be used directly in a partition path; access one of the key components (eg: '{self._name}.{example}')."
            )

    def __getattr__(self, name: str) -> WildcardPlaceholder:
        if self._attribute is not None:
            raise ValueError(
                f"'{self._name}.{self._attribute}.{name}' cannot be used in a partition path; only immediate '{self._name}' attributes (such as '{self._attribute}') can be used."
            )
        if name not in self._valid_attributes:
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
class WildcardFormatDict(dict[str, Any]):
    def __init__(
        self,
        key_types: Mapping[str, type[PartitionKey]],
        /,
        *args: Iterable[tuple[str, Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.key_types = key_types

    def __missing__(self, key: str) -> WildcardPlaceholder:
        if key not in self.key_types:
            raise ValueError(
                f"No '{key}' partition key found, expected one of {tuple(self.key_types)}"
            )
        return WildcardPlaceholder(name=key, key_type=self.key_types[key])


def spec_to_wildcard(spec: str, key_types: Mapping[str, type[PartitionKey]]) -> str:
    return string.Formatter().vformat(spec, (), WildcardFormatDict(key_types))


@overload
def extract_partition_keys(
    *,
    error_on_no_match: Literal[False],
    key_types: Mapping[str, type[PartitionKey]],
    parser: parse.Parser,
    path: str,
    spec: str,
) -> Optional[CompositeKey]:
    ...


@overload
def extract_partition_keys(
    *,
    error_on_no_match: Literal[True] = True,
    key_types: Mapping[str, type[PartitionKey]],
    parser: parse.Parser,
    path: str,
    spec: str,
) -> CompositeKey:
    ...


def extract_partition_keys(
    *,
    error_on_no_match: bool = True,
    key_types: Mapping[str, type[PartitionKey]],
    parser: parse.Parser,
    path: str,
    spec: str,
) -> Optional[CompositeKey]:
    parsed_value = parser.parse(path)
    if parsed_value is None:
        if error_on_no_match:
            raise ValueError(f"Unable to parse '{path}' with '{spec}'.")
        return None
    key_components = defaultdict[str, dict[str, str]](dict)
    for k, v in parsed_value.named.items():
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
            f"Expected to find partition keys for {sorted(key_types)}, only found {sorted(key)}. Is the partitioning spec ('{spec}') complete?"
        )
    return frozendict(keys)


def parse_partition_keys(
    paths: set[str], *, spec: str, key_types: Mapping[str, type[PartitionKey]]
) -> Mapping[str, CompositeKey]:
    parser = parse.compile(spec, case_sensitive=True)
    return {
        path: extract_partition_keys(parser=parser, path=path, spec=spec, key_types=key_types)
        for path in paths
    }
