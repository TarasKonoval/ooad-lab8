#!/usr/bin/env python3
"""Run every statement of a sql/ file through Athena (workgroup lab8) via the aws CLI.

For each statement it saves the result CSV and a stats line (Data scanned, run time)
into results/<file>[_<n>].csv / .stats.txt. Source scripts/aws-env.sh first.
Usage: python3 scripts/athena.py sql/02_count.sql
"""
import json, pathlib, re, subprocess, sys, time

WORKGROUP = "lab8"
RESULTS = pathlib.Path(__file__).resolve().parent.parent / "results"


def aws(*args):
    proc = subprocess.run(["aws", *args, "--output", "json"], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"aws {args[0]} {args[1]} failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout) if proc.stdout.strip() else None


def statements(sql):
    sql = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in sql.split(";") if s.strip()]


def run(stmt):
    qid = aws("athena", "start-query-execution", "--work-group", WORKGROUP, "--query-string", stmt)["QueryExecutionId"]
    while True:
        q = aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return q
        time.sleep(1)


def main(path):
    path = pathlib.Path(path)
    stmts = statements(path.read_text())
    RESULTS.mkdir(exist_ok=True)
    for i, stmt in enumerate(stmts, 1):
        name = path.stem if len(stmts) == 1 else f"{path.stem}_{i}"
        q = run(stmt)
        st = q["Statistics"]
        state = q["Status"]["State"]
        scanned = st.get("DataScannedInBytes", 0)
        stats = (f"query: {name}\nstate: {state}\nexecution_id: {q['QueryExecutionId']}\n"
                 f"data_scanned_bytes: {scanned}\ndata_scanned_mb: {scanned / 1024**2:.2f}\n"
                 f"engine_time_ms: {st.get('EngineExecutionTimeInMillis')}\n"
                 f"total_time_ms: {st.get('TotalExecutionTimeInMillis')}\n")
        if state != "SUCCEEDED":
            stats += f"error: {q['Status'].get('StateChangeReason')}\n"
        (RESULTS / f"{name}.stats.txt").write_text(stats)
        out = q.get("ResultConfiguration", {}).get("OutputLocation", "")
        if state == "SUCCEEDED" and out.endswith(".csv"):
            subprocess.run(["aws", "s3", "cp", out, str(RESULTS / f"{name}.csv"), "--only-show-errors"], check=True)
        print(stats)
        if state != "SUCCEEDED":
            sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1])
