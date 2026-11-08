# Use the kernel `ColumnTrie` for nested column paths

Found while working on DROP COLUMNS (#4732, branch `feat/alter-drop-columns`). That branch prunes
`delta.dataSkippingStatsColumns` with `delta_kernel::column_trie::ColumnTrie`. No other code in
delta-rs uses it; each place below does its own path handling.

## The kernel helpers

- `delta_kernel::column_trie::ColumnTrie` (kernel `column_trie.rs`). It is behind the kernel
  `internal-api` feature, which delta-rs already turns on in the root `Cargo.toml`.
  - `ColumnTrie::from_columns(&[ColumnName])`, `insert(&ColumnName)`
  - `contains_prefix_of(path)`: true when a column in the trie is the path itself or one of its
    parents. This is the prefix match Spark does for `dataSkippingStatsColumns`.
  - `is_terminal(path)`: true when the path itself is in the trie.
  - The kernel uses it to build the stats schema from `dataSkippingStatsColumns`
    (`scan/data_skipping/stats_schema/column_filter.rs`).
- `ColumnName` parsing and printing:
  - `ColumnName::parse_column_name_list(s)` reads a comma-separated list. An empty string gives
    an empty list.
  - `ColumnName: FromStr` reads one name.
  - `Display` puts backticks around parts where needed, so the output parses back to the same
    column.

## Bug: the writer ignores nested `dataSkippingStatsColumns` entries

`crates/core/src/writer/stats.rs`, `stats_from_metadata`. The main write path uses it through
`create_add` in `crates/core/src/datafile/writer/partition.rs`. It re-parses each entry with
sqlparser, joins the parts with `"."`, then compares with the parquet column's
`ColumnDescriptor::name()`. That is only the leaf name.

Checked on upstream main 878742f74 by reading the raw `add.stats` JSON after writing a table with
columns `a` and `s{a,b}`:

| `delta.dataSkippingStatsColumns` | `minValues` written |
|---|---|
| `s.a` | `{}` (no stats at all) |
| `a` | `{"a": 1, "s": {"a": 10}}` (the nested `s.a` matches too, because its leaf is also named `a`) |
| `s` | `{}` (a struct entry is not expanded to its fields) |

- **Hidden in `get_add_actions`:** `get_add_actions(flatten=True)` reads stats through the
  kernel's stats schema. It shows a `min.s.a` column filled with nulls. To see what was written,
  read the log JSON.
- **Effect:** nested stats columns get no data skipping. Results are still correct.

Repro:

```python
import glob, json, tempfile
import pyarrow as pa
from deltalake import write_deltalake

for cfg in ["s.a", "a", "s"]:
    d = tempfile.mkdtemp()
    t = pa.table({"a": [1, 2], "s": [{"a": 10, "b": 20}, {"a": 11, "b": 21}]})
    write_deltalake(d, t, configuration={"delta.dataSkippingStatsColumns": cfg})
    for line in open(glob.glob(f"{d}/_delta_log/*.json")[0]):
        action = json.loads(line)
        if "add" in action:
            print(cfg, json.loads(action["add"]["stats"])["minValues"])
```

## Where delta-rs handles nested column paths today

| Where | How the path is stored | How it is matched |
|---|---|---|
| `kernel/snapshot/stats_projection.rs`, `filter_schema_by_paths` | `BTreeSet<ColumnName>` | Hand-written recursive prefix filter: groups paths by first part, keeps the whole field if a path ends there, otherwise recurses with the rest. This is what `contains_prefix_of` does. |
| same file, `schema_contains_path` / `physicalize_column_path` | `ColumnName` | Walks the schema part by part through structs. `physicalize_column_path` ignores case. |
| `delta_datafusion/data_validation.rs` (NOT NULL checks) | `ColumnName` | Walks the schema part by part. `col_expr` builds `ident(first).field(..)`, so dots in names are safe. |
| `kernel/schema/schema.rs`, `get_invariants` / `get_generated_columns` | `String` joined with `"."` | Plain strings, so a top-level `a.b` and the nested path `a` → `b` look the same. |
| `operations/optimize.rs`, z-order (`validate_zorder_column` and the sort expression) | `String` | `split('.')`, so z-order cannot address a top-level name that contains a dot. |
| `kernel/arrow/extract.rs`, `extract_and_cast_opt` | `String` | `split('.')`, with special handling for maps. |
| `writer/stats.rs`, `stats_from_metadata` | `String` | Leaf-name comparison (the bug above). |
| `operations/mod.rs`, `get_num_idx_cols_and_stats_columns`; `writer/record_batch.rs` | `String` | Turns the kernel-parsed `ColumnName` list back into strings. The fallback without a table config does `split(',')` on the raw property, which breaks on quoted names. |

## TODO

- [ ] Fix the writer stats bug: match the parquet leaf paths with
      `ColumnTrie::contains_prefix_of(col.path().parts())` against the kernel-parsed
      `Vec<ColumnName>`. Under column mapping, the comparison needs physical names. Add a test
      that reads the raw `add.stats` JSON, not `get_add_actions`.
- [ ] Pass the stats columns to the writer as `Vec<ColumnName>` instead of strings
      (`get_num_idx_cols_and_stats_columns`, `WriterStatsConfig`, `writer/record_batch.rs`). Drop
      the `split(',')` fallback in favor of `ColumnName::parse_column_name_list`.
- [ ] `stats_projection.rs` `filter_schema_by_paths`: check whether `ColumnTrie` can replace the
      hand-written recursion.
- [ ] `get_invariants` / `get_generated_columns`: return the path as a `ColumnName` instead of a
      dot-joined `String`. Then the invariant check in `operations/drop_column.rs` can use
      `ColumnTrie` too.
- [ ] Z-order columns: parse with `ColumnName::from_str` instead of `split('.')`, so
      backtick-quoted names that contain dots work. Build the expression the same way as
      `col_expr` in `data_validation.rs`.
- [ ] `kernel/arrow/extract.rs`: check the callers. If they only pass internal names (for
      example `add.stats`), `split('.')` is fine.
- [ ] Remove this file when done.
