# Artifact

A data structure that describes data in the Artigraph ecosystem.

An Artifact is comprised of three key elements:
- `type`: spec of the data's structure, such as data types, nullable, etc.
- `format`: the data's serialized format, such as CSV, Parquet, database native, etc.
- `storage`: the data's persistent storage system, such as blob storage, database native, etc.

In addition to the core elements, an Artifact can be tagged with additional `Annotations` (to associate it with human knowledge) and `Statistics` (to track derived characteristics over time).
