def patch_TopologicalSorter_class_getitem() -> None:
    """Patch adding TopologicalSorter.__class_getitem__ to support subscription.

    TopologicalSorter is considered Generic in typeshed (hence mypy expects a type arg),
    but is not at runtime.

    This has been fixed for 3.11+ (https://github.com/python/cpython/pull/28714).
    """
    from graphlib import TopologicalSorter
    from types import GenericAlias

    if not hasattr(TopologicalSorter, "__class_getitem__"):  # pragma: no cover
        TopologicalSorter.__class_getitem__ = classmethod(GenericAlias)  # type: ignore
