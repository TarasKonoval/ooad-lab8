#!/usr/bin/env python3
"""Replay one day of payment webhook events into the Redpanda topic `payments`.

Events are sent in the order they reached the payment system (receivedAt), so
events that were late in the dataset are also late in the stream. Time is sped
up so that about --rate payments per second are sent, keeping the gaps between
events proportional to the real ones.

Example (3 minutes from 17:00 UTC, the evening peak):
    python producer.py --start 17:00 --limit 10800
Experiment 3 (make 30% of the payments fail):
    python producer.py --start 17:00 --limit 10800 --fail-rate 0.3
"""
import argparse
import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer


def parse_time(iso):
    """'2026-10-01T08:15:22Z' -> Unix seconds (float). Python 3.11+ accepts the Z suffix."""
    return datetime.fromisoformat(iso).timestamp()


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", default="data/events-2026-10-01.jsonl", help="one day of events (JSON Lines)")
    p.add_argument("--bootstrap", default="localhost:19092", help="Kafka/Redpanda address")
    p.add_argument("--topic", default="payments")
    p.add_argument("--rate", type=float, default=20.0, help="target payments per second")
    p.add_argument("--start", default="00:00", help="skip events received before this UTC time, HH:MM")
    p.add_argument("--limit", type=int, default=0, help="send at most this many events (0 = all)")
    p.add_argument("--fail-rate", type=float, default=0.0,
                   help="share of payments (0..1) whose success is turned into a failure")
    p.add_argument("--seed", type=int, default=42, help="random seed for --fail-rate, so runs repeat")
    return p.parse_args()


def load_events(path, start_hhmm, limit):
    """Read the file, sort by receivedAt, and keep the slice starting at start_hhmm."""
    with open(path, encoding="utf-8") as f:
        events = [json.loads(line) for line in f if line.strip()]
    # Sort by processing time: the order in which a webhook endpoint would have received them.
    events.sort(key=lambda e: e["receivedAt"])
    # receivedAt is 'YYYY-MM-DDTHH:MM:SSZ', so characters 11..15 are 'HH:MM'.
    events = [e for e in events if e["receivedAt"][11:16] >= start_hhmm]
    return events[:limit] if limit else events


def inject_failures(events, fail_rate, seed):
    """Turn the success event of a random share of payments into a failure (experiment 3)."""
    rng = random.Random(seed)
    invoices = sorted({e["invoiceId"] for e in events})
    # Decide once per payment, so both copies of a duplicated event get the same status.
    failing = {inv for inv in invoices if rng.random() < fail_rate}
    changed = 0
    for e in events:
        if e["invoiceId"] in failing and e["status"] == "success":
            e["status"] = "failure"
            e["failureReason"] = "Injected failure (experiment)"
            e["finalAmount"] = 0          # finalAmount is non-zero only on success
            changed += 1
    return changed


def main():
    args = parse_args()
    events = load_events(args.file, args.start, args.limit)
    if not events:
        raise SystemExit("no events in the selected slice")

    if args.fail_rate > 0:
        changed = inject_failures(events, args.fail_rate, args.seed)
        print(f"fail-rate {args.fail_rate:.0%}: turned {changed} success events into failures")

    # Speed-up factor: how much faster than real time we must go to reach --rate payments/s.
    first_t = parse_time(events[0]["receivedAt"])
    last_t = parse_time(events[-1]["receivedAt"])
    payments = len({e["invoiceId"] for e in events})
    real_span = max(last_t - first_t, 1.0)          # seconds of real time in the slice
    speedup = args.rate / (payments / real_span)    # target rate / real rate
    print(f"replaying {len(events)} events of {payments} payments, "
          f"{events[0]['receivedAt']} .. {events[-1]['receivedAt']}, "
          f"speed-up x{speedup:.1f}, expected run {real_span / speedup:.0f} s")

    # Small batching (linger) is enough at this rate; acks=all waits for the broker to store each event.
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5, "acks": "all"})
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        # Called by producer.poll()/flush() once the broker confirms (or rejects) a message.
        if err:
            stats["failed"] += 1
            print(f"delivery failed: {err}")
        else:
            stats["delivered"] += 1

    wall_start = time.monotonic()
    next_report = wall_start + 10
    for i, e in enumerate(events):
        # When this event should be sent: its real offset from the first event, compressed by speedup.
        due = wall_start + (parse_time(e["receivedAt"]) - first_t) / speedup
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)

        producer.produce(
            args.topic,
            key=e["invoiceId"],     # same key -> same partition -> events of one payment stay in order
            value=json.dumps(e, ensure_ascii=False).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)            # serve delivery callbacks without blocking

        now = time.monotonic()
        if now >= next_report:
            elapsed = now - wall_start
            print(f"[{elapsed:6.1f}s] sent {i + 1} events, at receivedAt {e['receivedAt']}, "
                  f"{(i + 1) / elapsed:.1f} events/s")
            next_report += 10

    producer.flush()                # wait until every message is confirmed
    elapsed = time.monotonic() - wall_start
    print(f"done in {elapsed:.1f} s: delivered {stats['delivered']}, failed {stats['failed']}, "
          f"{payments / elapsed:.1f} payments/s")


if __name__ == "__main__":
    main()
