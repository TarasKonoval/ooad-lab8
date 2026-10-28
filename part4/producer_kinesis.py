#!/usr/bin/env python3
"""Replay one day of payments into the Kinesis stream (Part 4). Same replay as Part 3.

The reading, sorting by receivedAt, slicing and --fail-rate come from part3/producer.py;
only the sending changes: boto3 put_records instead of a Kafka producer.

Run with the Part 3 virtualenv (it has confluent-kafka for the import, plus boto3):
    source scripts/aws-env.sh
    part3/.venv/bin/python part4/producer_kinesis.py --start 17:00 --limit 18000
"""
import argparse
import json
import pathlib
import sys
import time

import boto3

# Make part3/ importable and reuse its replay functions.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "part3"))
from producer import inject_failures, load_events, parse_time  # noqa: E402

MAX_BATCH = 500      # put_records accepts at most 500 records per call
FLUSH_EVERY = 0.5    # seconds: send what we have at least twice a second


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_file = pathlib.Path(__file__).resolve().parent.parent / "part3/data/events-2026-10-01.jsonl"
    p.add_argument("--file", default=str(default_file), help="one day of events (JSON Lines)")
    p.add_argument("--stream", default="ooad-lab8-payments", help="Kinesis stream name")
    p.add_argument("--rate", type=float, default=20.0, help="target payments per second")
    p.add_argument("--start", default="00:00", help="skip events received before this UTC time, HH:MM")
    p.add_argument("--limit", type=int, default=0, help="send at most this many events (0 = all)")
    p.add_argument("--fail-rate", type=float, default=0.0,
                   help="share of payments (0..1) whose success is turned into a failure")
    p.add_argument("--seed", type=int, default=42, help="random seed for --fail-rate")
    return p.parse_args()


def send(kinesis, stream, batch, stats):
    """put_records one batch; retry only the records Kinesis rejected (e.g. throttling)."""
    for attempt in range(5):
        resp = kinesis.put_records(StreamName=stream, Records=batch)
        if resp["FailedRecordCount"] == 0:
            stats["sent"] += len(batch)
            return
        # The response lists results in the same order as the request; failed ones have an ErrorCode.
        failed = [rec for rec, res in zip(batch, resp["Records"]) if "ErrorCode" in res]
        stats["sent"] += len(batch) - len(failed)
        stats["retried"] += len(failed)
        batch = failed
        time.sleep(0.2 * 2 ** attempt)              # back off: 0.2, 0.4, 0.8 ... s
    stats["failed"] += len(batch)
    print(f"gave up on {len(batch)} records")


def main():
    args = parse_args()
    events = load_events(args.file, args.start, args.limit)
    if not events:
        raise SystemExit("no events in the selected slice")
    if args.fail_rate > 0:
        changed = inject_failures(events, args.fail_rate, args.seed)
        print(f"fail-rate {args.fail_rate:.0%}: turned {changed} success events into failures")

    # Same speed-up as Part 3: target payments/s divided by the real payments/s in the slice.
    first_t = parse_time(events[0]["receivedAt"])
    real_span = max(parse_time(events[-1]["receivedAt"]) - first_t, 1.0)
    payments = len({e["invoiceId"] for e in events})
    speedup = args.rate / (payments / real_span)
    print(f"replaying {len(events)} events of {payments} payments, "
          f"{events[0]['receivedAt']} .. {events[-1]['receivedAt']}, "
          f"speed-up x{speedup:.1f}, expected run {real_span / speedup:.0f} s")

    kinesis = boto3.client("kinesis")               # region and credentials from the environment
    stats = {"sent": 0, "retried": 0, "failed": 0}
    batch = []
    wall_start = time.monotonic()
    last_flush = next_report = wall_start
    for e in events:
        due = wall_start + (parse_time(e["receivedAt"]) - first_t) / speedup
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)

        batch.append({
            # Firehose concatenates records as they are, so we end each one with a newline
            # to get valid JSON Lines in S3.
            "Data": (json.dumps(e, ensure_ascii=False) + "\n").encode("utf-8"),
            # The partition key picks the shard (with one shard, all go to it) and keeps the
            # events of one payment in order, as the Kafka key did in Part 3.
            "PartitionKey": e["invoiceId"],
        })

        now = time.monotonic()
        if len(batch) >= MAX_BATCH or now - last_flush >= FLUSH_EVERY:
            send(kinesis, args.stream, batch, stats)
            batch, last_flush = [], now
        if now >= next_report + 10:
            next_report = now
            print(f"[{now - wall_start:6.1f}s] sent {stats['sent']} events, at receivedAt {e['receivedAt']}")

    if batch:
        send(kinesis, args.stream, batch, stats)
    elapsed = time.monotonic() - wall_start
    print(f"done in {elapsed:.1f} s: sent {stats['sent']}, retried {stats['retried']}, "
          f"failed {stats['failed']}, {payments / elapsed:.1f} payments/s")


if __name__ == "__main__":
    main()
