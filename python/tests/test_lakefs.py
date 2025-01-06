import os
import uuid
from datetime import timedelta

import pyarrow as pa
import pytest

from deltalake import DeltaTable, TableFeatures
from deltalake._internal import Field, PrimitiveType
from deltalake.exceptions import DeltaError, DeltaProtocolError
from deltalake.table import CommitProperties
from deltalake.writer import write_deltalake
from tests.test_alter import _sort_fields


@pytest.fixture
def lakefs_path() -> str:
    return os.path.join("lakefs://bronze/main", str(uuid.uuid4()))


@pytest.fixture
def lakefs_storage_options():
    return {
        "endpoint": "http://127.0.0.1:8000",
        "allow_http": "true",
        "access_key_id": "LAKEFSID",
        "secret_access_key": "LAKEFSKEY",
    }


def test_create(lakefs_path: str, sample_data: pa.Table, lakefs_storage_options):
    dt = DeltaTable.create(
        lakefs_path,
        sample_data.schema,
        mode="error",
        storage_options=lakefs_storage_options,
    )
    last_action = dt.history(1)[0]

    with pytest.raises(DeltaError):
        dt = DeltaTable.create(
            lakefs_path,
            sample_data.schema,
            mode="error",
            storage_options=lakefs_storage_options,
        )

    assert last_action["operation"] == "CREATE TABLE"
    with pytest.raises(DeltaError):
        dt = DeltaTable.create(
            lakefs_path,
            sample_data.schema,
            mode="append",
            storage_options=lakefs_storage_options,
        )

    dt = DeltaTable.create(
        lakefs_path,
        sample_data.schema,
        mode="ignore",
        storage_options=lakefs_storage_options,
    )
    assert dt.version() == 0

    dt = DeltaTable.create(
        lakefs_path,
        sample_data.schema,
        mode="overwrite",
        storage_options=lakefs_storage_options,
    )
    assert dt.version() == 1

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "CREATE OR REPLACE TABLE"


def test_delete(lakefs_path: str, sample_data: pa.Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by=["bool"],
        storage_options=lakefs_storage_options,
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()
    dt.delete(commit_properties=commit_properties)

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert dt.version() == old_version + 1
    assert last_action["userName"] == "John Doe"

    dataset = dt.to_pyarrow_dataset()
    assert dataset.count_rows() == 0
    assert len(dt.files()) == 0


# TODO: figure out how to run multiple commit operations :S
@pytest.mark.skip
def test_optimize_min_commit_interval(
    lakefs_path: str, sample_data: pa.Table, lakefs_storage_options
):
    print(lakefs_path)
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()

    dt.optimize.z_order(["date32", "timestamp"], min_commit_interval=timedelta(0))

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 5


def test_optimize(lakefs_path: str, sample_data: pa.Table, lakefs_storage_options):
    print(lakefs_path)
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )
    write_deltalake(
        lakefs_path,
        sample_data,
        partition_by="utf8",
        mode="append",
        storage_options=lakefs_storage_options,
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()

    dt.optimize.z_order(["date32", "timestamp"])

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 1


# TODO: delete file from LakeFS and commit deletion
@pytest.mark.skip
def test_repair_with_dry_run(lakefs_path, sample_data, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    os.remove(dt.file_uris()[0])

    metrics = dt.repair(dry_run=True)
    last_action = dt.history(1)[0]

    assert len(metrics["files_removed"]) == 1
    assert metrics["dry_run"] is True
    assert last_action["operation"] == "WRITE"


# TODO: delete file from LakeFS and commit deletion
@pytest.mark.skip
def test_repair_wo_dry_run(lakefs_path, sample_data, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    os.remove(dt.file_uris()[0])

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    metrics = dt.repair(dry_run=False, commit_properties=commit_properties)
    last_action = dt.history(1)[0]

    assert len(metrics["files_removed"]) == 1
    assert metrics["dry_run"] is False
    assert last_action["operation"] == "FSCK"
    assert last_action["userName"] == "John Doe"


def test_add_constraint(lakefs_path, sample_table: pa.Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    dt.alter.add_constraint({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraint({"check_price": "price < 0"})

    with pytest.raises(DeltaProtocolError):
        data = pa.table(
            {
                "id": pa.array(["1"]),
                "price": pa.array([-1], pa.int64()),
                "sold": pa.array(list(range(1)), pa.int32()),
                "deleted": pa.array([False] * 1),
            }
        )
        write_deltalake(
            lakefs_path,
            data,
            engine="rust",
            mode="append",
            storage_options=lakefs_storage_options,
        )


def test_drop_constraint(lakefs_path, sample_table: pa.Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    dt.alter.add_constraint({"check_price": "price >= 0"})
    dt.alter.drop_constraint(name="check_price")
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DROP CONSTRAINT"
    assert dt.version() == 2


def test_set_table_properties(
    lakefs_path, sample_table: pa.Table, lakefs_storage_options
):
    write_deltalake(
        lakefs_path,
        sample_table,
        mode="append",
        engine="rust",
        storage_options=lakefs_storage_options,
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {"delta.enableChangeDataFeed": "true"}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 4


def test_add_feautres(lakefs_path, sample_table: pa.Table, lakefs_storage_options):
    write_deltalake(lakefs_path, sample_table, storage_options=lakefs_storage_options)
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    dt.alter.add_feature(
        feature=[
            TableFeatures.ChangeDataFeed,
            TableFeatures.DeletionVectors,
            TableFeatures.ColumnMapping,
        ],
        allow_protocol_versions_increase=True,
    )
    protocol = dt.protocol()

    assert sorted(protocol.reader_features) == sorted(  # type: ignore
        ["columnMapping", "deletionVectors"]
    )
    assert sorted(protocol.writer_features) == sorted(  # type: ignore
        [
            "changeDataFeed",
            "columnMapping",
            "deletionVectors",
        ]
    )  # type: ignore


def test_merge(lakefs_path, sample_table: pa.Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_table, mode="append", storage_options=lakefs_storage_options
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)

    source_table = pa.table(
        {
            "id": pa.array(["5"]),
            "weight": pa.array([105], pa.int32()),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        commit_properties=commit_properties,
    ).when_matched_delete().execute()

    nrows = 4
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


def test_restore(
    lakefs_path,
    sample_data: pa.Table,
    lakefs_storage_options,
):
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    old_version = dt.version()
    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.restore(1, commit_properties=commit_properties)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1


def test_add_column(lakefs_path, sample_data: pa.Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_data, mode="append", storage_options=lakefs_storage_options
    )
    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    current_fields = dt.schema().fields

    new_fields_to_add = [
        Field("foo", PrimitiveType("integer")),
        Field("bar", PrimitiveType("float")),
    ]

    dt.alter.add_columns(new_fields_to_add)
    new_fields = dt.schema().fields

    assert _sort_fields(new_fields) == _sort_fields(
        [*current_fields, *new_fields_to_add]
    )

@pytest.fixture()
def sample_table_update():
    nrows = 5
    return pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int64()),
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
            "deleted": pa.array([False] * nrows),
        }
    )

def test_update(lakefs_path, sample_table_update: pa.Table, lakefs_storage_options):
    write_deltalake(
        lakefs_path, sample_table_update, mode="append", storage_options=lakefs_storage_options
    )

    dt = DeltaTable(lakefs_path, storage_options=lakefs_storage_options)
    nrows = 5
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int64()),
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
            "deleted": pa.array([False, False, False, False, True]),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.update(
        updates={"deleted": "True"},
        predicate="price > 3",
        commit_properties=commit_properties,
    )

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert last_action["userName"] == "John Doe"
    assert result == expected