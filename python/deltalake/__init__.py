import atexit
import os
import warnings
from typing import Optional, Union

from deltalake import table
from deltalake._internal import (
    _NANOSECOND_TIMESTAMPS as _INTERNAL_NANOSECOND_TIMESTAMPS,
)
from deltalake._internal import (
    TableFeatures,
    Transaction,
    __version__,
    _configure_file_list_cache_from_env,
    disable_file_list_cache,
    rust_core_version,
)
from deltalake._internal import (
    configure_file_list_cache as _configure_file_list_cache,
)
from deltalake._internal import (
    init_tracing as _init_tracing,
)
from deltalake._internal import (
    shutdown_tracing as _shutdown_tracing,
)
from deltalake.query import QueryBuilder
from deltalake.schema import DataType, Field, Schema, VariantType
from deltalake.table import DeltaTable, Metadata, TableProperty
from deltalake.transaction import CommitProperties, PostCommitHookProperties
from deltalake.writer import (
    BloomFilterProperties,
    ColumnProperties,
    WriterProperties,
    convert_to_deltalake,
    write_deltalake,
)


def init_tracing(endpoint: Optional[str] = None) -> None:
    """
    Initialize OpenTelemetry tracing for delta-rs operations.

    Args:
        endpoint: The OTLP HTTP endpoint URL. If not provided, uses the
            OTEL_EXPORTER_OTLP_ENDPOINT environment variable or defaults to
            "http://localhost:4318/v1/traces"

    Raises:
        RuntimeError: If tracing initialization fails

    Note:
        - Tracing will be automatically shut down when the Python process exits
        - Use OTEL_EXPORTER_OTLP_HEADERS environment variable for authentication
          (format: "key1=value1,key2=value2")
        - Use RUST_LOG environment variable to control log level filtering
          (e.g., "info", "debug", "deltalake=debug")

    Example:
        Basic usage with defaults:
        ```python
        import deltalake

        deltalake.init_tracing()
        deltalake.write_deltalake("my_table", data)
        dt = deltalake.DeltaTable("my_table")
        ```

        Using custom endpoint:
        ```python
        import deltalake

        deltalake.init_tracing(endpoint="http://localhost:4318/v1/traces")
        ```

        Using environment variables for authentication:
        ```python
        import os
        import deltalake

        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://api.honeycomb.io/v1/traces"
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = "x-honeycomb-team=your-api-key"
        os.environ["RUST_LOG"] = "deltalake=debug"
        deltalake.init_tracing()
        ```
    """
    _init_tracing(endpoint)
    atexit.register(_shutdown_tracing)


def configure_file_list_cache(
    memory_bytes: int,
    directory: Optional[Union[str, "os.PathLike[str]"]] = None,
    disk_bytes: Optional[int] = None,
) -> None:
    """
    Turn on the cache of the active file list for all tables of this process.

    A `DeltaTable` does not keep the list of active files in memory. Methods such as
    `file_uris()` and `get_add_actions()` read the Delta log on each call. With this
    cache, the first call for a table stores the list, and a later call reads it from
    the cache. A later call on a newer table version reads only the new commits. This
    also applies to a new `DeltaTable` instance of the same table.

    The cache serves reads of the complete file list. Reads that filter the files,
    such as queries and the scans of `merge`, `update`, and `delete`, do not use it.

    Args:
        memory_bytes: Memory that the cache can use for entries.
        directory: Directory for cache entries on disk. Entries on disk stay available
            after a restart. Without a directory, the cache keeps a table only when its
            file list fits in `memory_bytes`. A file list needs about 600 bytes for
            each file.
        disk_bytes: Disk space that the cache can use in `directory`. The default is
            8 GiB. The cache keeps 16 files open for each GiB.

    Note:
        - Only one process at a time can use a directory.
        - The directory holds file paths and column statistics (minimum and maximum
          values) of the tables in plain form.
        - The cache can also be turned on with environment variables:
          `DELTA_FILE_LIST_CACHE_CAPACITY_BYTES` (memory, turns the cache on),
          `DELTA_CACHE_DIR` (the cache uses the `file-list` directory below it), and
          `DELTA_FILE_LIST_CACHE_DISK_CAPACITY_BYTES`.

    Examples:
        ```python
        import deltalake

        deltalake.configure_file_list_cache(
            memory_bytes=64 * 1024 * 1024,
            directory="/var/cache/deltalake",
        )
        dt = deltalake.DeltaTable("my_table")
        dt.file_uris()  # reads the log and fills the cache
        dt.file_uris()  # reads the cache
        ```
    """
    _configure_file_list_cache(
        memory_bytes,
        None if directory is None else os.fspath(directory),
        disk_bytes,
    )


try:
    _configure_file_list_cache_from_env()
except Exception as err:
    warnings.warn(
        f"The file list cache is off, the configuration from the environment failed: {err}",
        RuntimeWarning,
        stacklevel=2,
    )


# Support nanosecond timestamps:
_NANOSECOND_TIMESTAMPS: bool = False


def enable_nanosecond_timestamps() -> None:
    """
    Enable experimental support for nanosecond timestamp primitive data types.

    This takes effect process-wide.

    Note that this feature may be changed or removed in a future release
    without being considered a breaking change.
    """
    if not _INTERNAL_NANOSECOND_TIMESTAMPS:
        raise RuntimeError(
            "Extension wasn't compiled with nanosecond-timestamps Cargo feature"
        )

    if _nanosecond_timestamps_enabled():
        # Already called, no work necessary.
        return

    global _NANOSECOND_TIMESTAMPS
    _NANOSECOND_TIMESTAMPS = True
    table.SUPPORTED_WRITER_FEATURES.add("timestampNanos")
    table.SUPPORTED_READER_FEATURES.add("timestampNanos")

    from deltalake._internal import _set_cast_nanos_timestamps_to_micros

    _set_cast_nanos_timestamps_to_micros(False)


def _disable_nanosecond_timestamps() -> None:
    """
    Disable nanosecond timestamps, for unit tests.
    """
    if not _nanosecond_timestamps_enabled():
        return

    global _NANOSECOND_TIMESTAMPS
    _NANOSECOND_TIMESTAMPS = False
    table.SUPPORTED_WRITER_FEATURES.remove("timestampNanos")
    table.SUPPORTED_READER_FEATURES.remove("timestampNanos")

    from deltalake._internal import _set_cast_nanos_timestamps_to_micros

    _set_cast_nanos_timestamps_to_micros(True)


def _nanosecond_timestamps_enabled() -> bool:
    """
    Return whether nanosecond timestamps are enabled.
    """
    return _NANOSECOND_TIMESTAMPS


__all__ = [
    "BloomFilterProperties",
    "ColumnProperties",
    "CommitProperties",
    "DataType",
    "DeltaTable",
    "Field",
    "Metadata",
    "PostCommitHookProperties",
    "QueryBuilder",
    "Schema",
    "TableFeatures",
    "TableProperty",
    "Transaction",
    "VariantType",
    "WriterProperties",
    "__version__",
    "configure_file_list_cache",
    "convert_to_deltalake",
    "disable_file_list_cache",
    "enable_nanosecond_timestamps",
    "init_tracing",
    "rust_core_version",
    "write_deltalake",
]
