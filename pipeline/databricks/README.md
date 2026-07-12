# Databricks run

`tarn_rollup_databricks.py` is a Databricks notebook in source format (the `# COMMAND ----------`
separators are how Databricks serialises notebooks — import it directly, or paste it into a
new Python notebook).

It runs **the same Stage-1 job** as `pipeline/rollup.py`: sessionization with window
functions, the two-stage distinct aggregation that `bench/spark_opt.json` benchmarks, and the
broadcast red-team join. Nothing about the logic changes for Databricks; only the paths and
the session do.

## To run it (this is the part Claude cannot do for you)

1. Create a free Databricks workspace (Free Edition).
2. Upload `data/sample/auth_sample.csv.gz` and `data/sample/redteam_sample.csv.gz` to a Volume
   (or DBFS). A serverless / single-node cluster is plenty for the ~100k-event slice.
3. Import `tarn_rollup_databricks.py` as a notebook, point the three widgets at your paths,
   and **Run All**.
4. Export the executed notebook — **File → Export → IPython Notebook** — and commit it here as
   `tarn_rollup_databricks.ipynb`, *with the output cells intact*. The outputs are the
   evidence; a notebook with the cells cleared proves nothing.

## What this artifact does and does not claim

**Does:** the pipeline runs unmodified on a managed Spark cluster, and the physical plan there
shows the same `BroadcastHashJoin` and absence of `Expand` that the local benchmark relies on.

**Does not:** any statement about scale on Databricks. This runs the ~100k-event CI slice on a
free workspace, once. The 1.05B-row numbers in `bench/` come from the local Docker runs and
say so. The honest claim is *"also executed on Databricks and validated"* — resist the urge to
grow it into anything larger, because a screener who asks "what cluster size?" will get an
answer that does not support a bigger story.
