# Loading a Delta Table

A [DeltaTable][deltalake.table.DeltaTable] represents the state of a
delta table at a particular version. This includes which files are
currently part of the table, the schema of the table, and other metadata
such as creation time.

{{ code_example('delta_table', 'get_table_info', ['DeltaTable'])}}

Depending on your storage backend, you could use the `storage_options`
parameter to provide some configuration.

Configuration is defined for specific backends

- [s3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants),
- [azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants),
- [gcs options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants).

=== "Python"
    ```python
    storage_options = {"AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY":"THE_AWS_SECRET_ACCESS_KEY"}
    dt = DeltaTable("../rust/tests/data/delta-0.2.0", storage_options=storage_options)
    ```

=== "Rust"
    ```rust
    let storage_options = HashMap::from_iter(vec![
        ("AWS_ACCESS_KEY_ID".to_string(), "THE_AWS_ACCESS_KEY_ID".to_string()),
        ("AWS_SECRET_ACCESS_KEY".to_string(), "THE_AWS_SECRET_ACCESS_KEY".to_string()),
    ]);
    let table = open_table_with_storage_options("../rust/tests/data/delta-0.2.0", storage_options).await?;
    ```

The configuration can also be provided via the environment, and the
basic service provider is derived from the URL being used. We try to
support many of the well-known formats to identify basic service
properties.

=== "S3"

    ```
    s3://\<bucket\>/\<path\>
    s3a://\<bucket\>/\<path\>
    ```

    Note that `delta-rs` does not read credentials from a local `.aws/config` or `.aws/creds` file. Credentials can be accessed from environment variables, ec2 metadata, profiles or web identity. You can also pass credentials to `storage_options` using `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

=== "Azure"

    ```
    az://\<container\>/\<path\>
    adl://\<container\>/\<path\>
    abfs://\<container\>/\<path\>
    ```

=== "GCS"

    ```
    gs://\<bucket\>/\<path\>
    ```


## Loading and active-file metadata

Loading a `DeltaTable` reads the protocol, the metadata, and the list of log files of
the table version. It does not read the active files or their statistics. An operation
that needs the active files reads them from the log at that time. Appends and schema
changes do not need them, so their memory use does not grow with the number of files
in the table.

Methods that return the active files, such as `file_uris()` and `get_add_actions()`
in Python or `DeltaTable::get_file_uris` in Rust, read the log on each call. In Rust,
`DeltaTable::eager_snapshot` returns an `EagerSnapshot`, which holds the active files
in memory and gives synchronous access to them through `log_data()`. Keep the
`EagerSnapshot` if you need the file list more than once. Rust append-only callers can
also use `BlindDeltaTable`.

The Rust `DeltaTableConfig` loading configuration has been removed. In Python,
`without_files`, `skip_stats`, and `log_buffer_size` are deprecated compatibility
arguments and are ignored with a warning when supplied with non-default values.
`without_files=True` is not necessary: table loading does not read the active files.
The deprecated `table_config` property reports the effective loading defaults, not the
ignored arguments. Table properties do not replace these loading options.

Serialized tables written by earlier releases can still be read. A serialized table
that contains the active files keeps them in memory after it is read. A serialized
table without them reads them from the log when an operation needs them.

## Caching the active file list

Methods that return the complete list of active files read the Delta log on each call.
For a process that calls them many times, or that keeps running while the table gets new
commits, you can turn on a cache for the file list. The cache is off by default.

=== "Python"

    ```python
    import deltalake

    deltalake.configure_file_list_cache(
        memory_bytes=64 * 1024 * 1024,
        directory="/var/cache/deltalake",
    )

    dt = deltalake.DeltaTable("<path/to/table>")
    dt.file_uris()  # reads the log and fills the cache
    dt.file_uris()  # reads the cache
    ```

=== "Rust"

    ```rust
    use deltalake::kernel::file_list_cache::{FileListCacheConfig, install_file_list_cache};

    // Needs the `delta-cache` feature.
    let config = FileListCacheConfig::new(64 * 1024 * 1024).with_directory("/var/cache/deltalake");
    install_file_list_cache(config).await?;
    ```

The cache applies to all tables of the process, also to new `DeltaTable` instances of a
table. It stores the file list of one table version. A read at a newer version reads only
the commits after the stored version.

The cache serves `file_uris()`, `get_add_actions()`, `DeltaTable::eager_snapshot`, and the
internal reads of the complete file list, for example in the conflict check of a commit.
Reads that filter the files do not use it. These are queries and the scans of `merge`,
`update`, and `delete`.

| Setting | Python and Rust | Environment variable |
| --- | --- | --- |
| Memory for cache entries | `memory_bytes` | `DELTA_FILE_LIST_CACHE_CAPACITY_BYTES`. The cache is on when this is set. |
| Directory for entries on disk | `directory` | `DELTA_CACHE_DIR`. The cache uses the `file-list` directory below it. |
| Disk space, 8 GiB by default | `disk_bytes` | `DELTA_FILE_LIST_CACHE_DISK_CAPACITY_BYTES` |

Things to know:

- The memory setting limits the cache entries that stay in memory. It does not limit the
  memory of a call that returns the file list.
- Without a directory, the cache keeps a table only when its file list fits in the memory
  setting. A file list needs about 600 bytes for each file.
- Only one process at a time can use a directory. Entries on disk stay available after a
  restart.
- The cache keeps 16 files open for each GiB of disk space.
- The directory holds file paths and column statistics (minimum and maximum values) of
  the tables in plain form.
- Catalog-managed tables do not use the cache.
- The Python package reads the environment variables at import. A Rust program calls
  `FileListCacheConfig::from_env` and `install_file_list_cache`.

## Verify Table Existence

You can check whether or not a Delta table exists at a particular path by using
the `DeltaTable.is_deltatable()` method.

=== "Python"

    ```python
    from deltalake import DeltaTable

    table_path = "<path/to/valid/table>"
    DeltaTable.is_deltatable(table_path)
    # True

    invalid_table_path = "<path/to/nonexistent/table>"
    DeltaTable.is_deltatable(invalid_table_path)
    # False

    bucket_table_path = "<path/to/valid/table/in/bucket>"
    storage_options = {
        "AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY": "THE_AWS_SECRET_ACCESS_KEY",
        ...
    }
    DeltaTable.is_deltatable(bucket_table_path, storage_options)
    # True
    ```

=== "Rust"

    ```rust
    let table_path = "<path/to/valid/table>";
    let builder = deltalake::DeltaTableBuilder::from_uri(table_path);
    builder.build()?.verify_deltatable_existence().await?;
    // true

    let invalid_table_path = "<path/to/nonexistent/table>";
    let builder = deltalake::DeltaTableBuilder::from_uri(invalid_table_path);
    builder.build()?.verify_deltatable_existence().await?;
    // false

    let bucket_table_path = "<path/to/valid/table/in/bucket>";
    let storage_options = HashMap::from_iter(vec![
        ("AWS_ACCESS_KEY_ID".to_string(), "THE_AWS_ACCESS_KEY_ID".to_string()),
        ("AWS_SECRET_ACCESS_KEY".to_string(), "THE_AWS_SECRET_ACCESS_KEY".to_string()),
    ]);
    let builder = deltalake::DeltaTableBuilder::from_uri(bucket_table_path).with_storage_options(storage_options);
    builder.build()?.verify_deltatable_existence().await?;
    // true
    ```


## Custom Storage Backends

While delta always needs its internal storage backend to work and be
properly configured, in order to manage the delta log, it may sometimes
be advantageous - and is common practice in the arrow world - to
customize the storage interface used for reading the bulk data.

`deltalake` will work with any storage compliant with `pyarrow.fs.FileSystem`, however the root of the filesystem has to be adjusted to point at the root of the Delta table. We can achieve this by wrapping the custom filesystem into a `pyarrow.fs.SubTreeFileSystem`.

```python
import pyarrow.fs as fs
from deltalake import DeltaTable

path = "<path/to/table>"
filesystem = fs.SubTreeFileSystem(path, fs.LocalFileSystem())

dt = DeltaTable(path)
ds = dt.to_pyarrow_dataset(filesystem=filesystem)
```

When using the pyarrow factory method for file systems, the normalized
path is provided on creation. In case of S3 this would look something
like:

```python
import pyarrow.fs as fs
from deltalake import DeltaTable

table_uri = "s3://<bucket>/<path>"
raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)

dt = DeltaTable(table_uri)
ds = dt.to_pyarrow_dataset(filesystem=filesystem)
```

## Time Travel

To load previous table states, you can provide the version number you
wish to load:

=== "Python"
    ```python
    >>> dt = DeltaTable("/rust/tests/data/simple_table", version=2)
    ```
=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/rust/tests/data/simple_table").unwrap();
    let mut table = open_table(delta_path).await?;
    table.load_version(1).await?;
    ```


Once you've loaded a table, you can also change versions using either a
version number or datetime string:

=== "Python"
    ```python
    >>> dt.load_version(1)
    >>> dt.load_with_datetime("2021-11-04 00:05:23.283+00:00")
    ```
=== "Rust"
    ```rust
    table.load_version(1).await?;
    table.load_with_datetime("2021-11-04 00:05:23.283+00:00".parse().unwrap()).await?;
    ```
!!! warning

    Previous table versions may not exist if they have been vacuumed, in
    which case an exception will be thrown. See [Vacuuming
    tables](managing-tables.md#vacuuming-tables) for more information.

## Load table from Unity Catalog

You may load a Delta Table from your Databricks or Open Source Unity Catalog using a `uc://` URL of format `uc://<catalog-name>.<db-name>.<table-name>`. Example as follows:

=== "Python"

    ```python
    import os
    from deltalake import DeltaTable

    # Set your Unity Catalog workspace URL in the
    # DATABRICKS_WORKSPACE_URL environment variable.
    os.environ["DATABRICKS_WORKSPACE_URL"] = "https://adb-1234567890.XX.azuredatabricks.net"

    # Set your Unity Catalog access token in the
    # DATABRICKS_ACCESS_TOKEN environment variable.
    os.environ["DATABRICKS_ACCESS_TOKEN"] = "<unity-catalog-access-token-here>"

    # Your Unity Catalog name here
    catalog_name = "unity"

    # Your UC database name here
    db_name = "my_database"

    # Your table name in the UC database here
    table_name = "my_table"

    # Full UC URL in required format
    uc_full_url = f"{catalog_name}.{db_name}.{table_name}"

    # This should load the valid delta table at specified location,
    # and you can start using it.
    dt = DeltaTable(uc_full_url)
    dt.is_deltatable(dt.table_uri)
    # True
    ```

### Environment variables

The following environment variables are supported when loading tables using a `uc://` URL:

#### Connection Configuration

| Environment Variable | Description |
|---------------------|-------------|
| `DATABRICKS_HOST` | Databricks workspace host URL |
| `DATABRICKS_WORKSPACE_URL` | Alternative workspace URL variable |
| `UNITY_WORKSPACE_URL` | Unity-prefixed workspace URL |

#### Authentication Credentials

| Environment Variable | Description |
|---------------------|-------------|
| `DATABRICKS_TOKEN` | Access token for Databricks authentication|
| `DATABRICKS_ACCESS_TOKEN` | Alternative access token variable |
| `UNITY_ACCESS_TOKEN` | Unity-prefixed access token variable |

#### OAuth / Service Principal Authentication

| Environment Variable | Description |
|---------------------|-------------|
| `DATABRICKS_AUTHORITY_HOST` | Authority host for OAuth flows |
| `DATABRICKS_AUTHORITY_ID` | Authority (tenant) ID for OAuth flows |
| `DATABRICKS_CLIENT_ID` | Service principal client ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal client secret |

 or

| Environment Variable | Description |
|---------------------|-------------|
| `UNITY_AUTHORITY_HOST` | Unity-prefixed authority host |
| `UNITY_AUTHORITY_ID` | Unity-prefixed authority ID |
| `UNITY_CLIENT_ID` | Unity-prefixed client ID |
| `UNITY_CLIENT_SECRET` | Unity-prefixed client secret |

#### Azure Managed Identity Authentication

| Environment Variable | Description |
|---------------------|-------------|
| `DATABRICKS_FEDERATED_TOKEN_FILE` | File containing token for Azure AD workload identity federation |
| `DATABRICKS_MSI_ENDPOINT` | Endpoint to request managed identity token |
| `DATABRICKS_MSI_RESOURCE_ID` | MSI resource ID for managed identity |
| `DATABRICKS_OBJECT_ID` | Object ID for managed identity authentication |
| `DATABRICKS_USE_AZURE_CLI` | Use Azure CLI for acquiring access token |

or

| Environment Variable | Description |
|---------------------|-------------|
| `UNITY_FEDERATED_TOKEN_FILE` | Unity-prefixed federated token file |
| `UNITY_MSI_ENDPOINT` | Unity-prefixed MSI endpoint |
| `UNITY_MSI_RESOURCE_ID` | Unity-prefixed MSI resource ID |
| `UNITY_OBJECT_ID` | Unity-prefixed object ID |
| `UNITY_USE_AZURE_CLI` | Unity-prefixed Azure CLI flag |

#### Additional Settings

| Environment Variable | Description |
|---------------------|-------------|
| `UNITY_ALLOW_HTTP_URL` | Allow HTTP URLs (e.g., http://localhost:8080) for testing |
