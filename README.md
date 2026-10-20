# Northwind Case Study

A hands-on Spark performance case study on Databricks using a Singapore vending-machine dataset. A nightly "restock manifest" job returns correct answers but does more work than it needs to. These notebooks investigate why: they read query plans with `explain()`, then compare how filters, projections, joins and table layouts change the work Spark does.

The scenario has one standout machine, `M1004`, which sells far more than the rest. It is used as the running example throughout.

## Repository Contents

| File | Description |
|------|-------------|
| `North_wind_execution.ipynb` | Loads the data and runs the experiments: plan reading, lazy evaluation, narrow vs wide operations, joins, partitioning, `OPTIMIZE` and liquid clustering. |
| `North_wind_review.ipynb` | A review pass over the same tables: aggregations, joins, file-layout checks and plan inspection. |

## Tech Stack

Apache Spark / PySpark, Databricks (Unity Catalog, Volumes), Delta Lake.

## Data and Tables

`vends.csv` and `machines.csv` are read from a Unity Catalog Volume into Delta tables in `databricksbootcamp.northwindcasestudy`. The CSV files are not included in this repo.

| Table | How it is created | Purpose |
|-------|-------------------|---------|
| `vends` | CSV read, `saveAsTable` | Vend transactions (`machine_id`, `vend_date`, `amount`, `category`, `payment_type`, ...) |
| `machines` | CSV read, `saveAsTable` | Small machine dimension (`machine_id`, `location_type`, ...) |
| `vends_by_date` | `partitionBy("vend_date")` | Partition pruning test |
| `vends_clustered` | `clusterBy("machine_id", "vend_date")` + `OPTIMIZE` | Liquid clustering test |
| `vends_small` | `repartition(64)` | Small-files test |
| `vends_layout` | `partitionBy("machine_id")` | Layout comparison |
| `check_extract` | Full `select("*")` write | File-layout check |

## `North_wind_execution.ipynb`

**What it does, and what it shows**

1. **Loads the data.** Reads both CSVs and saves them as Delta tables.
2. **Reads query plans.** Runs `explain()`, `explain(True)` and `explain("formatted")` on a filter plus `groupBy().sum()` for `M1004`. This shows the logical plan (what was asked) vs the physical plan (how Spark runs it), read bottom to top.
3. **Tests lazy evaluation.** Builds a `filter` + `select` chain and only calls `count()` at the end. Transformations return instantly and the cost lands at the action.
4. **Compares narrow and wide operations.** `filter`, `select` and `withColumn` are checked against `groupBy`, `orderBy` and `distinct`. The wide ones add an `Exchange` (shuffle) to the plan.
5. **Checks filter and column behaviour.** A date filter with `select("machine_id", "amount")`, and `select` on its own. The plan shows the filter at the scan and only the needed columns being read.
6. **Compares join strategies.** `vends` joined to `machines` with the default strategy, with `hint("merge")`, and with `broadcast(machines)`. The default and broadcast joins avoid shuffling the large table. The `merge` hint forces a sort-merge join that shuffles and sorts both sides.
7. **Tests partition pruning.** Writes `vends_by_date` with `partitionBy("vend_date")` and filters on one date. The plan shows the filter applied as a partition filter.
8. **Tests small files.** Runs `DESCRIBE DETAIL` on `vends`, then `OPTIMIZE`, then `DESCRIBE DETAIL` again to compare the file count.
9. **Tests liquid clustering.** Writes `vends_clustered` with `clusterBy("machine_id", "vend_date")`, runs `OPTIMIZE`, and inspects the result.
10. **Runs the combined query.** A May 2026 date-range query joined to `machines` and grouped by `location_type`, run two ways: on the raw table with a `merge` hint, and on the clustered table with a broadcast join.

## `North_wind_review.ipynb`

**What it does, and what it shows**

1. **Sets up the schema.** `USE databricksbootcamp.northwindcasestudy` and loads `vends` and `machines`.
2. **Checks the heavy machine.** Counts rows for `M1004`, and sums `amount` per machine joined to `machines` with the `merge` hint. One machine dominating a `groupBy` is the uneven-work example.
3. **Runs a single-day total.** Sums `amount` for 2026-05-15 on the unpartitioned table.
4. **Compares write layouts.** Writes `check_extract`, `vends_small` (`repartition(64)`) and `vends_layout` (`partitionBy("machine_id")`), and runs `DESCRIBE DETAIL` on each to see how file count and layout change.
5. **Repeats the date-range join.** May 2026 filter with the `merge` hint, grouped by `location_type`, to compare against the broadcast and clustered version in the execution notebook.
6. **Runs extra checks.** Distinct `payment_type`, the lazy-chain `count()`, and a `groupBy("machine_id", "vend_date")` with a date filter, inspected with `explain()`.

## What I Learned

- **Lazy evaluation:** a long chain of transformations is cheap to build; work only runs at an action like `count()` or a write.
- **Narrow vs wide:** `groupBy`, `orderBy`, `distinct` and joins shuffle data. `filter` and `select` do not.
- **Plans over guesses:** `explain()` shows what Spark actually runs, including filters pushed to the scan and only the required columns being read.
- **Read less:** select only needed columns and filter early. Partitioning on `vend_date` lets a one-day query skip other dates' folders.
- **File layout:** many small files add overhead. `OPTIMIZE` compacts them, which `DESCRIBE DETAIL` confirms through the file count.
- **Liquid clustering vs partitioning:** clustering on `machine_id` and `vend_date` supports filtering on several columns without splitting the table into many folders.
- **Joins:** broadcasting the small `machines` table avoids shuffling `vends`. Forcing `hint("merge")` shuffles and sorts both sides and is the costlier choice.
- **Skew:** a single dominant machine (`M1004`) makes grouped work uneven.

## How to Run

1. Create the catalog and schema `databricksbootcamp.northwindcasestudy`, or change the names in the notebooks.
2. Create a Volume named `northwindcasestudyvolume` and upload `vends.csv` and `machines.csv`.
3. Import both notebooks into Databricks.
4. Run `North_wind_execution.ipynb` first (it creates the tables), then `North_wind_review.ipynb`.
