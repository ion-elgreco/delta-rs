import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

import deltalake
from deltalake import DeltaTable, write_deltalake


@pytest.fixture
def file_list_cache(tmp_path: pathlib.Path):
    deltalake.configure_file_list_cache(
        memory_bytes=8 * 1024 * 1024,
        directory=tmp_path / "cache",
        disk_bytes=128 * 1024 * 1024,
    )
    yield
    deltalake.disable_file_list_cache()


def _append(path: pathlib.Path, start: int) -> None:
    data = Table(
        {
            "id": Array(
                list(range(start, start + 3)),
                ArrowField("id", type=DataType.int64(), nullable=True),
            )
        }
    )
    write_deltalake(path, data, mode="append")


def _add_actions(dt: DeltaTable) -> list[dict]:
    rows = dt.get_add_actions().to_struct_array().to_pylist()
    return sorted(rows, key=lambda row: row["path"])


def test_file_list_cache_returns_the_same_files(
    tmp_path: pathlib.Path, file_list_cache: None
):
    path = tmp_path / "table"
    for start in range(0, 15, 3):
        _append(path, start)

    deltalake.disable_file_list_cache()
    expected_uris = sorted(DeltaTable(path).file_uris())
    expected_actions = _add_actions(DeltaTable(path))
    deltalake.configure_file_list_cache(
        memory_bytes=8 * 1024 * 1024,
        directory=tmp_path / "cache-2",
        disk_bytes=128 * 1024 * 1024,
    )

    # The first read fills the cache, the second read uses it.
    for _ in range(2):
        dt = DeltaTable(path)
        assert sorted(dt.file_uris()) == expected_uris
        assert _add_actions(dt) == expected_actions

    # A newer version, with a new file and a removed file.
    _append(path, 15)
    DeltaTable(path).delete("id < 3")
    dt = DeltaTable(path)
    cached_uris = sorted(dt.file_uris())
    cached_actions = _add_actions(dt)

    deltalake.disable_file_list_cache()
    dt = DeltaTable(path)
    assert cached_uris == sorted(dt.file_uris())
    assert cached_actions == _add_actions(dt)
    assert len(cached_uris) == 5


def test_file_list_cache_rejects_a_bad_directory(tmp_path: pathlib.Path):
    not_a_directory = tmp_path / "file"
    not_a_directory.write_text("x")

    with pytest.raises(Exception):
        deltalake.configure_file_list_cache(
            memory_bytes=1024 * 1024, directory=not_a_directory / "cache"
        )
