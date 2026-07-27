# Artifacts

An **artifact** is Artigraph's contract for a data product. It says what the data represents, how it
is serialized, where it lives, and what human or quality metadata is attached to it.

```python
from arti import Artifact, ArtifactStorage, Format, Type
from arti.formats.json import JSON
from arti.storage.local import LocalFile
from arti.types import Float64


class TotalSpend(Artifact):
    type: Type = Float64()
    format: Format = JSON()
    storage: ArtifactStorage = LocalFile()
```

## Artifact fields

- `type` describes the logical shape of the data: scalar, struct, collection, partitions, and field
  types.
- `format` describes how the data is serialized, such as JSON or Parquet.
- `storage` describes where bytes are stored, such as a local file path template.
- `annotations` attach domain metadata that should travel with the artifact definition.
- `statistics` are intended for measured characteristics and quality signals.

## Raw and produced artifacts

Raw artifacts point at data that already exists or is written by the user. Produced artifacts are the
outputs of producers. Produced artifact storage should include Artigraph's input fingerprint template,
`{input_fingerprint}`, so that multiple versions can remain available at the same time:

```python
LocalFile(path="/tmp/produced/{input_fingerprint}/total.json")
```

That version-preserving behavior is the foundation for reproducible snapshots: a corrected input can
produce a new output without deleting the output that belonged to an older graph snapshot.
