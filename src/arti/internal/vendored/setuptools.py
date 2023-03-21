""" Vendored copy of find_namespace_packages from setuptools[1].

Adding setuptools as a package dependency can be a bit messy, sometimes triggering an
uninstall/reinstall on the _user_ side or other issues.

Type hints have been added to pass `mypy` strict mode.

1: https://github.com/pypa/setuptools/tree/c65e3380b0a18c92a0fc2d2b770b17cfaaec054b
"""
from __future__ import annotations

import os
from collections.abc import Callable, Generator, Iterable
from fnmatch import fnmatchcase


# From: https://github.com/pypa/setuptools/blob/c65e3380b0a18c92a0fc2d2b770b17cfaaec054b/setuptools/_distutils/util.py#L166
def convert_path(pathname: str) -> str:  # pragma: no cover
    """Return 'pathname' as a name that will work on the native filesystem,
    i.e. split it on '/' and put it back together again using the current
    directory separator.  Needed because filenames in the setup script are
    always supplied in Unix style, and have to be converted to the local
    convention before we can actually use them in the filesystem.  Raises
    ValueError on non-Unix-ish systems if 'pathname' either starts or
    ends with a slash.
    """
    if os.sep == "/":
        return pathname
    if not pathname:
        return pathname
    if pathname[0] == "/":
        raise ValueError("path '%s' cannot be absolute" % pathname)
    if pathname[-1] == "/":
        raise ValueError("path '%s' cannot end with '/'" % pathname)

    paths = pathname.split("/")
    while "." in paths:
        paths.remove(".")
    if not paths:
        return os.curdir
    return os.path.join(*paths)


# From: https://github.com/pypa/setuptools/blob/c65e3380b0a18c92a0fc2d2b770b17cfaaec054b/setuptools/__init__.py#L39-L119
class PackageFinder:
    """
    Generate a list of all Python packages found within a directory
    """

    @classmethod
    def find(
        cls, where: str = ".", exclude: Iterable[str] = (), include: Iterable[str] = ("*",)
    ) -> list[str]:
        """Return a list all Python packages found within directory 'where'
        'where' is the root directory which will be searched for packages.  It
        should be supplied as a "cross-platform" (i.e. URL-style) path; it will
        be converted to the appropriate local path syntax.
        'exclude' is a sequence of package names to exclude; '*' can be used
        as a wildcard in the names, such that 'foo.*' will exclude all
        subpackages of 'foo' (but not 'foo' itself).
        'include' is a sequence of package names to include.  If it's
        specified, only the named packages will be included.  If it's not
        specified, all found packages will be included.  'include' can contain
        shell style wildcard patterns just like 'exclude'.
        """

        return list(
            cls._find_packages_iter(
                convert_path(where),
                cls._build_filter("ez_setup", "*__pycache__", *exclude),
                cls._build_filter(*include),
            )
        )

    @classmethod
    def _find_packages_iter(
        cls, where: str, exclude: Callable[[str], bool], include: Callable[[str], bool]
    ) -> Generator[str, None, None]:
        """
        All the packages found in 'where' that pass the 'include' filter, but
        not the 'exclude' filter.
        """
        for root, dirs, _files in os.walk(where, followlinks=True):
            # Copy dirs to iterate over it, then empty dirs.
            all_dirs = dirs[:]
            dirs[:] = []

            for dir in all_dirs:
                full_path = os.path.join(root, dir)
                rel_path = os.path.relpath(full_path, where)
                package = rel_path.replace(os.path.sep, ".")

                # Skip directory trees that are not valid packages
                if "." in dir or not cls._looks_like_package(full_path):
                    continue

                # Should this package be included?
                if include(package) and not exclude(package):
                    yield package

                # Keep searching subdirectories, as there may be more packages
                # down there, even if the parent was excluded.
                dirs.append(dir)

    @staticmethod
    def _looks_like_package(path: str) -> bool:
        """Does a directory look like a package?"""
        return os.path.isfile(os.path.join(path, "__init__.py"))

    @staticmethod
    def _build_filter(*patterns: str) -> Callable[[str], bool]:
        """
        Given a list of patterns, return a callable that will be true only if
        the input matches at least one of the patterns.
        """
        return lambda name: any(fnmatchcase(name, pat=pat) for pat in patterns)


class PEP420PackageFinder(PackageFinder):
    @staticmethod
    def _looks_like_package(path: str) -> bool:
        return True


find_packages = PackageFinder.find
find_namespace_packages = PEP420PackageFinder.find
