import contextlib
import http.server
import json
import os
import shutil
import socket
import subprocess
import sysconfig
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Generator

import pytest
from arro3.core import Table

from deltalake import DeltaTable, Schema, UnityCatalog, write_deltalake


def _api_url(server: str, path: str) -> str:
    return f"{server}/api/2.1/unity-catalog/{path}"


def _post(server: str, path: str, body: dict) -> dict:
    request = urllib.request.Request(
        _api_url(server, path),
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read() or b"{}")


def _create_managed_table(server: str, catalog: str, root: Path, schema: dict) -> None:
    """Create the catalog-managed table `<catalog>.default.testtable`.

    As an engine does: the server gives the location and the protocol of a staging table,
    the first commit goes to the log, and then the table takes the place of the staging table.
    """
    _post(server, "catalogs", {"name": catalog, "storage_root": root.as_uri()})
    _post(server, "schemas", {"name": "default", "catalog_name": catalog})
    api = f"delta/v1/catalogs/{catalog}/schemas/default"
    staging = _post(server, f"{api}/staging-tables", {"name": "testtable"})
    protocol, properties = staging["required-protocol"], staging["required-properties"]
    now = int(time.time() * 1000)
    commit = [
        {
            "commitInfo": {
                "inCommitTimestamp": now,
                "timestamp": now,
                "operation": "CREATE TABLE",
                "operationParameters": {},
            }
        },
        {
            "protocol": {
                "minReaderVersion": protocol["min-reader-version"],
                "minWriterVersion": protocol["min-writer-version"],
                "readerFeatures": protocol["reader-features"],
                "writerFeatures": protocol["writer-features"],
            }
        },
        {
            "metaData": {
                "id": staging["table-id"],
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema),
                "partitionColumns": [],
                "configuration": properties,
                "createdTime": now,
            }
        },
    ]
    location = urllib.parse.urlparse(staging["location"]).path
    log = Path(urllib.request.url2pathname(location)) / "_delta_log"
    log.mkdir(parents=True)
    (log / f"{0:020}.json").write_text(
        "".join(json.dumps(action) + "\n" for action in commit)
    )
    _post(
        server,
        f"{api}/tables",
        {
            "name": "testtable",
            "location": staging["location"],
            "table-type": "MANAGED",
            "columns": schema,
            "protocol": protocol,
            "properties": properties,
            "last-commit-timestamp-ms": now,
        },
    )


@contextlib.contextmanager
def _recording_proxy(
    server: str,
) -> Generator[tuple[str, list[tuple[str, str]]], None, None]:
    """A proxy of `server` and the method and the token of each request that it gets.

    The server does not check the token, so the proxy shows which token a request has.
    """
    requests: list[tuple[str, str]] = []

    class Proxy(http.server.BaseHTTPRequestHandler):
        def forward(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            token = self.headers.get("Authorization", "").removeprefix("Bearer ")
            requests.append((self.command, token))
            request = urllib.request.Request(
                server + self.path,
                data=body or None,
                headers={k: v for k, v in self.headers.items() if k.lower() != "host"},
                method=self.command,
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    status, payload = response.status, response.read()
            except urllib.error.HTTPError as error:
                status, payload = error.code, error.read()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = forward

        def log_message(self, format: str, *args: object) -> None:
            pass

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{proxy.server_port}", requests
    finally:
        proxy.shutdown()
        proxy.server_close()


@pytest.fixture(scope="module")
def unity_catalog_server(tmp_path_factory) -> Generator[str, None, None]:
    """Start `uc-server` and return its URL.

    The dependency group `unitycatalog` installs `uc-server`.
    """
    uc_server = shutil.which("uc-server") or shutil.which(
        "uc-server", path=sysconfig.get_path("scripts")
    )
    assert uc_server, "uc-server is not installed: uv pip install --group unitycatalog"

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = f"http://127.0.0.1:{port}"

    process = subprocess.Popen(
        [uc_server],
        env={
            **os.environ,
            "UC_SERVER_BIND": f"127.0.0.1:{port}",
            "UC_SERVER_DATA_DIR": str(tmp_path_factory.mktemp("uc-server")),
            "UC_DB_URL": "memory",
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                urllib.request.urlopen(_api_url(server, "catalogs"), timeout=1).close()
                break
            except OSError:
                assert process.poll() is None, "uc-server stopped"
                assert time.monotonic() < deadline, "uc-server did not start"
                time.sleep(0.05)
        yield server
    finally:
        process.terminate()
        process.wait()


def _register_external_table(server: str, catalog: str, location: str) -> None:
    """Register the table at `location` as `<catalog>.default.testtable`."""
    _post(server, "catalogs", {"name": catalog})
    _post(server, "schemas", {"name": "default", "catalog_name": catalog})
    _post(
        server,
        "tables",
        {
            "name": "testtable",
            "catalog_name": catalog,
            "schema_name": "default",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": location,
        },
    )


@pytest.mark.unitycatalog
@pytest.mark.pyarrow
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_uc_read_deltatable(
    unity_catalog_server: str,
    existing_sample_table: DeltaTable,
    sample_table: Table,
    monkeypatch,
):
    """Test delta table reads using Unity Catalog URL (uc://)"""
    import pyarrow as pa

    _register_external_table(
        unity_catalog_server, "unity", existing_sample_table.table_uri
    )

    monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", unity_catalog_server)
    # The server does not check the token, but the client needs one.
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "123456")
    monkeypatch.setenv("UNITY_ALLOW_HTTP_URL", "true")

    deprecated = pytest.warns(
        DeprecationWarning, match="`uc://` table URI is deprecated"
    )
    with deprecated:
        dt = DeltaTable("uc://unity.default.testtable")
    with deprecated:
        assert dt.is_deltatable(dt.table_uri), True
    expected = pa.table(sample_table).to_pydict()
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected

    with deprecated:
        write_deltalake("uc://unity.default.testtable", sample_table, mode="append")


@pytest.mark.unitycatalog
@pytest.mark.pyarrow
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_from_catalog(
    unity_catalog_server: str,
    existing_sample_table: DeltaTable,
    sample_table: Table,
):
    import pyarrow as pa

    _register_external_table(
        unity_catalog_server, "by_name", existing_sample_table.table_uri
    )
    # The server does not check the token, but the client needs one.
    catalog = UnityCatalog(unity_catalog_server, token="123456")

    dt = DeltaTable.from_catalog(catalog, "by_name.default.testtable")

    # The table has the storage location that the catalog gives.
    assert dt.table_uri == existing_sample_table.table_uri
    expected = pa.table(sample_table).to_pydict()
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected

    write_deltalake(dt, sample_table, mode="append")
    assert dt.version() == 1
    assert (
        DeltaTable.from_catalog(catalog, "by_name.default.testtable", 0).version() == 0
    )

    with pytest.raises(Exception, match="`catalog.schema.table`"):
        DeltaTable.from_catalog(catalog, "by_name.default")
    with pytest.raises(Exception, match="missing"):
        DeltaTable.from_catalog(catalog, "by_name.default.missing")


@pytest.mark.unitycatalog
def test_unity_catalog_token_type():
    with pytest.raises(TypeError, match="token must be a str or a callable"):
        UnityCatalog("http://127.0.0.1:1", token=1)  # type: ignore[arg-type]


@pytest.mark.unitycatalog
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_from_catalog_token_from_callable(
    unity_catalog_server: str,
    existing_sample_table: DeltaTable,
):
    """The catalog asks the callable for the access token of each request"""
    _register_external_table(
        unity_catalog_server, "tokens", existing_sample_table.table_uri
    )
    calls = []

    def access_token() -> str:
        calls.append(1)
        return "123456"

    catalog = UnityCatalog(unity_catalog_server, token=access_token)
    assert calls == []

    dt = DeltaTable.from_catalog(catalog, "tokens.default.testtable")

    # One request for the table and one for its credentials.
    assert len(calls) == 2
    assert dt.version() == existing_sample_table.version()


@pytest.mark.unitycatalog
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_from_catalog_token_callable_fails(unity_catalog_server: str):
    def access_token() -> str:
        raise RuntimeError("no token today")

    catalog = UnityCatalog(unity_catalog_server, token=access_token)

    with pytest.raises(Exception, match="the token callable failed.*no token today"):
        DeltaTable.from_catalog(catalog, "tokens.default.testtable")


@pytest.mark.unitycatalog
@pytest.mark.integration
@pytest.mark.timeout(timeout=60, method="thread")
def test_from_catalog_managed_table_sends_the_current_token(
    unity_catalog_server: str, tmp_path: Path, sample_table: Table
):
    """Each request for a catalog-managed table has the token that the callable gives then"""
    schema = json.loads(Schema.from_arrow(sample_table.schema).to_json())
    _create_managed_table(unity_catalog_server, "managed", tmp_path, schema)
    given: list[str] = []

    def access_token() -> str:
        given.append(f"token-{len(given)}")
        return given[-1]

    with _recording_proxy(unity_catalog_server) as (proxy, requests):
        catalog = UnityCatalog(proxy, token=access_token)
        dt = DeltaTable.from_catalog(catalog, "managed.default.testtable")
        opened = len(requests)
        write_deltalake(dt, sample_table, mode="append")

    assert dt.version() == 1
    assert dt.file_uris()
    # Each request has its own token.
    assert sorted(token for _, token in requests) == sorted(given)
    # The commit goes to the catalog with a newer token than the tokens of the open.
    commits = [token for method, token in requests[opened:] if method == "POST"]
    assert commits and set(commits).isdisjoint(given[:opened])
