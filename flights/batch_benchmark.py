"""On-demand F16 destination experiment; creates and drops only its own database."""

import base64
import json
import os
import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    rows = int(os.getenv("BENCH_ROWS", "25000"))
    if not 1000 <= rows <= 100000:
        raise ValueError("BENCH_ROWS must be 1000..100000")
    mode = os.getenv("BENCH_PAYLOAD", "high_entropy")
    if mode not in ("high_entropy", "repeated_text"):
        raise ValueError("invalid BENCH_PAYLOAD")
    db = "cdc_batch_bench_" + uuid.uuid4().hex[:12]
    con = duckdb.connect("md:")
    con.execute(f'CREATE DATABASE "{db}"')
    results = []
    try:
        for width in (1024, 10240):
            for target_mb in (1, 10, 50):
                prefix = f'"{db}".main.'
                con.execute(f"CREATE OR REPLACE TABLE {prefix}log (id BIGINT, payload VARCHAR)")
                con.execute(f"CREATE OR REPLACE TABLE {prefix}mirror (id BIGINT, payload VARCHAR)")
                batch_rows = max(1, target_mb * 1024 * 1024 // width)
                append_s = parquet_s = merge_s = 0.0
                with tempfile.TemporaryDirectory() as directory:
                    for first in range(0, rows, batch_rows):
                        path = str(Path(directory) / f"batch_{first}.parquet")
                        count = min(batch_rows, rows - first)
                        if mode == "high_entropy":
                            payload = [
                                base64.b64encode(os.urandom(width)).decode()[:width]
                                for _ in range(count)
                            ]
                        else:
                            payload = [
                                (
                                    f"record:{first + i};"
                                    + "customer updated account details; " * width
                                )[:width]
                                for i in range(count)
                            ]
                        table = pa.table({"id": range(first, first + count), "payload": payload})
                        start = time.perf_counter()
                        pq.write_table(table, path)
                        parquet_s += time.perf_counter() - start
                        start = time.perf_counter()
                        con.execute(
                            f"INSERT INTO {prefix}log SELECT * FROM read_parquet(?)", [path]
                        )
                        append_s += time.perf_counter() - start
                        start = time.perf_counter()
                        con.execute(
                            f"MERGE INTO {prefix}mirror m USING "
                            f"(SELECT * FROM {prefix}log WHERE id>=? AND id<?) s "
                            "ON m.id=s.id WHEN MATCHED THEN UPDATE SET payload=s.payload "
                            "WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.payload)",
                            [first, first + count],
                        )
                        merge_s += time.perf_counter() - start
                assert con.execute(f"SELECT count(*) FROM {prefix}log").fetchone()[0] == rows
                assert con.execute(f"SELECT count(*) FROM {prefix}mirror").fetchone()[0] == rows
                result = dict(
                    payload_mode=mode,
                    width=width,
                    batch_mb=target_mb,
                    rows=rows,
                    parquet_s=parquet_s,
                    append_s=append_s,
                    merge_s=merge_s,
                    append_rows_s=rows / append_s,
                    merge_rows_s=rows / merge_s,
                )
                results.append(result)
                print(json.dumps(result), flush=True)
        print("CDC_BENCH_RESULT=" + json.dumps(results), flush=True)
    finally:
        con.execute(f'DROP DATABASE "{db}"')
        con.close()


if __name__ == "__main__":
    main()
