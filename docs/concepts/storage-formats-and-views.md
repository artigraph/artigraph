# Storage, formats, and views

Artigraph separates three concerns that are often tangled together in data workflows.

## Storage

Storage decides **where bytes live**. Local files, cloud objects, database tables, or future storage
implementations can expose partitions through the same Artigraph storage model.

```python
from arti.storage.local import LocalFile

storage = LocalFile(path="data/raw/{date.iso}.json")
```

Produced artifacts normally include `{input_fingerprint}` in their storage template so Artigraph can
keep multiple output versions live at the same time.

## Formats

Formats decide **how bytes are serialized**. JSON is minimal and convenient. Parquet is a better fit
for Pandas/DataFrame workflows and ecosystem tools.

```python
from arti.formats.json import JSON
from arti.formats.parquet import Parquet
```

## Views

Views decide **what Python object the caller reads or writes**. A single logical artifact can be
written as one view and read as another when adapters are registered.

The local Parquet example writes and reads Pandas `DataFrame` values while preserving Artigraph's
artifact, partition, and snapshot metadata around those files.
