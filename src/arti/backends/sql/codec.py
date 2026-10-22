from __future__ import annotations

import base64
import importlib
import json
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, time
from enum import Enum
from pathlib import PurePath
from typing import Any, cast

from arti.internal.mappings import TypedBox
from arti.internal.models import Model

CODEC_VERSION = 1


class MetadataCodecError(ValueError):
    """Raised when persisted Artigraph metadata cannot be encoded or decoded."""


def _type_path(type_: type[Any]) -> str:
    module = type_.__module__
    qualname = type_.__qualname__
    path = f"{module}:{qualname}"
    if module == "__main__" or "<locals>" in qualname:
        raise MetadataCodecError(
            f"{path} is not importable. Persisted Artigraph types and producers must be defined at module scope in an importable module."
        )
    try:
        imported = _import_type(path)
    except MetadataCodecError as error:
        raise MetadataCodecError(
            f"{path} is not importable. Persisted Artigraph types and producers must be available from their module path."
        ) from error
    if imported is not type_:
        raise MetadataCodecError(
            f"{path} resolves to a different object. Persisted Artigraph types and producers must be bound to their module-qualified name."
        )
    return path


def _import_type(path: Any) -> type[Any]:
    if not isinstance(path, str):
        raise MetadataCodecError(f"Invalid persisted type path: {path!r}")
    module_name, separator, qualname = path.partition(":")
    if not separator or not module_name or not qualname or "<locals>" in qualname:
        raise MetadataCodecError(f"Invalid persisted type path: {path!r}")
    try:
        value: Any = importlib.import_module(module_name)
        for part in qualname.split("."):
            value = getattr(value, part)
    except (AttributeError, ImportError, ValueError) as error:
        raise MetadataCodecError(f"Unable to import persisted type {path!r}") from error
    if not isinstance(value, type):
        raise MetadataCodecError(f"Persisted path {path!r} does not resolve to a type")
    return value


def encode_document(value: Model) -> dict[str, Any]:
    """Encode a Model as structured, import-addressable JSON metadata."""
    return {"codec_version": CODEC_VERSION, "value": _encode(value)}


def decode_document[ModelT: Model](
    document: Mapping[str, Any], expected_type: type[ModelT]
) -> ModelT:
    """Decode and validate structured metadata as ``expected_type``."""
    if document.get("codec_version") != CODEC_VERSION:
        raise MetadataCodecError(
            f"Unsupported metadata codec version: {document.get('codec_version')!r}"
        )
    if set(document) != {"codec_version", "value"}:
        raise MetadataCodecError(f"Unexpected metadata document keys: {set(document)}")
    try:
        value = _decode(document["value"])
    except MetadataCodecError:
        raise
    except (TypeError, ValueError) as error:
        # Catch malformed values that pass the structural checks, eg: unhashable set items.
        raise MetadataCodecError("Invalid persisted metadata") from error
    if not isinstance(value, expected_type):
        raise MetadataCodecError(
            f"Expected persisted {expected_type.__name__}, got {type(value).__name__}"
        )
    return value


def _encode(value: Any) -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        # Tag floats rather than emitting JSON numbers: Postgres JSONB normalizes numbers (eg:
        # `1e+16` reads back as an int and `-0.0` as `0.0`) and JSON has no inf/nan.
        return {"kind": "float", "value": repr(value)}
    if isinstance(value, type):
        return {"kind": "type", "type": _type_path(value)}
    if isinstance(value, Model):
        model_type = type(value)
        fields = {
            name: _encode(getattr(value, name))
            for name, field in model_type.model_fields.items()
            if not field.exclude
        }
        state: dict[str, Any] = {}
        from arti.storage import Storage

        if isinstance(value, Storage):
            try:
                key_types = value.key_types
            except ValueError:
                key_types = None
            state["storage_key_types"] = _encode(key_types)
        return {
            "kind": "model",
            "type": _type_path(model_type),
            "fields": fields,
            "fields_set": sorted(value.model_fields_set),
            "state": state,
        }
    if isinstance(value, TypedBox):
        box = cast(TypedBox[Any], value)
        return {
            "kind": "typed_box",
            "type": _type_path(cast(type[Any], type(box))),
            "items": [[key, _encode(item)] for key, item in box.walk()],
        }
    if isinstance(value, Enum):
        return {"kind": "enum", "type": _type_path(type(value)), "name": value.name}
    if isinstance(value, datetime):
        return {"kind": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"kind": "date", "value": value.isoformat()}
    if isinstance(value, time):
        return {"kind": "time", "value": value.isoformat()}
    if isinstance(value, bytes):
        return {"kind": "bytes", "value": base64.b64encode(value).decode("ascii")}
    if isinstance(value, PurePath):
        return {"kind": "path", "type": _type_path(type(value)), "value": str(value)}
    if isinstance(value, int):
        return {"kind": "int", "type": _type_path(type(value)), "value": int(value)}
    if isinstance(value, Mapping):
        mapping = cast(Mapping[Any, Any], value)
        return {
            "kind": "mapping",
            "type": _type_path(cast(type[Any], type(mapping))),
            "items": [[_encode(key), _encode(item)] for key, item in mapping.items()],
        }
    if isinstance(value, tuple):
        return {
            "kind": "tuple",
            "items": [_encode(item) for item in cast(tuple[Any, ...], value)],
        }
    if isinstance(value, list):
        return {"kind": "list", "items": [_encode(item) for item in cast(list[Any], value)]}
    if isinstance(value, frozenset | set):
        items = cast(frozenset[Any] | set[Any], value)
        return {
            "kind": "frozenset" if isinstance(value, frozenset) else "set",
            # Set order follows the process' hash seed, so sort to keep documents canonical.
            "items": sorted(
                (_encode(item) for item in items),
                key=lambda item: json.dumps(item, sort_keys=True),
            ),
        }
    raise MetadataCodecError(
        f"Unsupported persisted metadata value {value!r} ({type(value).__module__}:{type(value).__qualname__})"
    )


def _require_mapping(value: Any, *, kind: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MetadataCodecError(f"Expected {kind} metadata to be a mapping")
    return cast(Mapping[str, Any], value)


def _require_items(value: Any, *, kind: str) -> list[Any]:
    if not isinstance(value, list):
        raise MetadataCodecError(f"Expected {kind} items to be a list")
    return cast(list[Any], value)


def _decode(value: Any) -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    data = _require_mapping(value, kind="encoded")
    kind = data.get("kind")
    if not isinstance(kind, str):
        raise MetadataCodecError("Encoded metadata is missing a string 'kind'")
    if kind == "model":
        model_type = _import_type(cast(str, data.get("type")))
        if not issubclass(model_type, Model):
            raise MetadataCodecError(f"Persisted type {model_type} is not an Artigraph Model")
        raw_fields = _require_mapping(data.get("fields"), kind="model fields")
        unknown = set(raw_fields) - set(model_type.model_fields)
        if unknown:
            raise MetadataCodecError(f"Unknown fields for {model_type.__name__}: {unknown}")
        fields = {name: _decode(item) for name, item in raw_fields.items()}
        raw_fields_set = _require_items(data.get("fields_set"), kind="model fields_set")
        fields_set = set[str]()
        for field_name in raw_fields_set:
            if not isinstance(field_name, str) or field_name not in model_type.model_fields:
                raise MetadataCodecError(
                    f"Invalid field in persisted {model_type.__name__} fields_set: {field_name!r}"
                )
            fields_set.add(field_name)
        try:
            # Run the public constructor first so current model validators reject incompatible
            # metadata. Then construct from the stored values without reapplying class-level
            # defaults that are already represented in the full field set.
            model_type(**fields)
            model = model_type.model_construct(
                _fields_set=fields_set,
                **fields,
            )
        except (TypeError, ValueError) as error:
            raise MetadataCodecError(f"Unable to restore {model_type.__name__}") from error
        state = _require_mapping(data.get("state"), kind="model state")
        from arti.storage import Storage

        if isinstance(model, Storage):
            storage = cast(Storage[Any], model)
            if set(state) != {"storage_key_types"}:
                raise MetadataCodecError(
                    f"Unexpected persisted {model_type.__name__} state: {set(state)}"
                )
            storage._key_types = _decode(  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
                state["storage_key_types"]
            )
        elif state:
            raise MetadataCodecError(
                f"Unexpected persisted {model_type.__name__} state: {set(state)}"
            )
        return cast(Model, model)
    if kind == "typed_box":
        box_type = _import_type(cast(str, data.get("type")))
        if not issubclass(box_type, TypedBox):
            raise MetadataCodecError(f"Persisted type {box_type} is not a TypedBox")
        items = _require_items(data.get("items"), kind=kind)
        build_box = cast(Callable[[Mapping[str, Any]], TypedBox[Any]], box_type)
        box = build_box({cast(str, key): _decode(item) for key, item in items})
        box._status.root = "closed"  # pyright: ignore[reportPrivateUsage]
        return box
    if kind == "mapping":
        mapping_type = _import_type(cast(str, data.get("type")))
        if not issubclass(mapping_type, Mapping):
            raise MetadataCodecError(f"Persisted type {mapping_type} is not a Mapping")
        items = _require_items(data.get("items"), kind=kind)
        build_mapping = cast(Callable[[Iterable[tuple[Any, Any]]], Mapping[Any, Any]], mapping_type)
        return build_mapping((_decode(key), _decode(item)) for key, item in items)
    if kind in ("tuple", "list", "set", "frozenset"):
        items = [_decode(item) for item in _require_items(data.get("items"), kind=kind)]
        return {"tuple": tuple, "list": list, "set": set, "frozenset": frozenset}[kind](items)
    if kind == "enum":
        enum_type = _import_type(cast(str, data.get("type")))
        if not issubclass(enum_type, Enum):
            raise MetadataCodecError(f"Persisted type {enum_type} is not an Enum")
        try:
            return enum_type[cast(str, data.get("name"))]
        except KeyError as error:
            raise MetadataCodecError(f"Unknown persisted {enum_type.__name__} member") from error
    if kind == "type":
        return _import_type(cast(str, data.get("type")))
    if kind == "int":
        int_type = _import_type(cast(str, data.get("type")))
        if not issubclass(int_type, int):
            raise MetadataCodecError(f"Persisted type {int_type} is not an int")
        return int_type(cast(int, data.get("value")))
    if kind == "float":
        float_value = data.get("value")
        if not isinstance(float_value, str):
            raise MetadataCodecError(f"Invalid persisted float: {float_value!r}")
        try:
            return float(float_value)
        except ValueError as error:
            raise MetadataCodecError(f"Invalid persisted float: {float_value!r}") from error
    if kind == "bytes":
        try:
            return base64.b64decode(cast(str, data.get("value")), validate=True)
        except (TypeError, ValueError) as error:
            raise MetadataCodecError("Invalid persisted bytes") from error
    if kind in ("date", "datetime", "time"):
        constructors = {
            "date": date.fromisoformat,
            "datetime": datetime.fromisoformat,
            "time": time.fromisoformat,
        }
        try:
            return constructors[kind](cast(str, data.get("value")))
        except (TypeError, ValueError) as error:
            raise MetadataCodecError(f"Invalid persisted {kind}") from error
    if kind == "path":
        path_type = _import_type(cast(str, data.get("type")))
        if not issubclass(path_type, PurePath):
            raise MetadataCodecError(f"Persisted type {path_type} is not a path")
        return path_type(cast(str, data.get("value")))
    raise MetadataCodecError(f"Unknown persisted metadata kind: {kind!r}")


def metadata_type_path(type_: type[Any]) -> str:
    """Return and validate the stable import path used for persisted types."""
    return _type_path(type_)


__all__ = [
    "CODEC_VERSION",
    "MetadataCodecError",
    "decode_document",
    "encode_document",
    "metadata_type_path",
]
