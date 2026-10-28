#!/usr/bin/env python3
"""Read payment events from Redpanda and watch the failure share per payment method.

Pipeline for every message:
  1. drop duplicates (the same invoiceId + status seen before);
  2. put the event into its one-minute tumbling window by EVENT time (modifiedDate);
  3. reject it as late if that window is already closed;
  4. move the watermark (the latest event time seen so far) forward;
  5. close every window whose end + allowed lateness is behind the watermark,
     print one line for it and an ALERT for every method whose failure share is above 10%.

Example:
    python consumer.py --group lab8-alerts
Experiment 4 (no waiting for late events):
    python consumer.py --group lab8-zero --lateness 0
"""
import argparse
import json
import signal
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

WINDOW = 60                  # window length in seconds (one-minute tumbling windows)
FINAL = ("success", "failure")  # only these statuses say how a payment ended


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:19092", help="Kafka/Redpanda address")
    p.add_argument("--topic", default="payments")
    p.add_argument("--group", default="lab8-alerts", help="consumer group id")
    p.add_argument("--lateness", type=int, default=120,
                   help="seconds to keep a window open after it ends, waiting for late events")
    p.add_argument("--threshold", type=float, default=0.10, help="alert when failure share is above this")
    p.add_argument("--min-payments", type=int, default=20,
                   help="alert only if the method had at least this many finished payments in the window "
                        "(3 failures out of 25 is 12%%, but at ~6%% normal failure that happens by chance)")
    p.add_argument("--from-beginning", action="store_true",
                   help="new group starts at the oldest message (default: only new messages)")
    p.add_argument("--idle-exit", type=float, default=0,
                   help="stop after this many seconds without messages (0 = run until Ctrl+C)")
    return p.parse_args()


def to_epoch(iso):
    """'2026-10-01T17:03:12Z' -> Unix seconds (int)."""
    return int(datetime.fromisoformat(iso).timestamp())


def hhmm(epoch):
    """Unix seconds -> 'HH:MM' in UTC, for printing window bounds."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%M")


def new_window():
    # events: all accepted events in the window; methods: paymentMethod -> [successes, failures]
    return {"events": 0, "methods": defaultdict(lambda: [0, 0])}


def stop(signum, frame):
    """On SIGTERM (kill <pid>) shut down the same way as on Ctrl+C."""
    raise KeyboardInterrupt


def main():
    args = parse_args()
    signal.signal(signal.SIGTERM, stop)
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,                      # consumers with the same id share the partitions
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",  # only for a group with no saved offset
        "enable.auto.commit": True,                  # commit stored offsets to the broker every few seconds
        # Do NOT mark a message as done when poll() returns it; we call store_offsets() after
        # processing it. Otherwise a stop right after poll() commits a message we never processed
        # (at-most-once). With this, such a message is read again after restart (at-least-once).
        "enable.auto.offset.store": False,
    })
    # Print which partitions this consumer got (useful for the two-consumer experiment).
    consumer.subscribe([args.topic],
                       on_assign=lambda c, parts: print(f"assigned partitions {[p.partition for p in parts]}"),
                       on_revoke=lambda c, parts: print(f"revoked partitions {[p.partition for p in parts]}"))

    seen = set()             # (invoiceId, status) pairs already processed: grows without limit (see LAB8)
    windows = {}             # window start (epoch) -> window state; only open windows are kept
    watermark = 0            # the latest event time seen so far
    totals = defaultdict(int)  # counters for the final summary

    def close_window(start, note=""):
        """Print one line for a finished window and its alerts, then forget it."""
        w = windows.pop(start)
        parts, alerts = [], []
        for method in sorted(w["methods"]):
            ok, failed = w["methods"][method]
            finished = ok + failed
            share = failed / finished if finished else 0.0
            parts.append(f"{method} {failed}/{finished} {share:.0%}")
            if finished >= args.min_payments and share > args.threshold:
                alerts.append(f"{method} {share:.0%}")
        totals["windows"] += 1
        totals["alerts"] += len(alerts)
        line = (f"window {hhmm(start)}-{hhmm(start + WINDOW)} events {w['events']:3d} | "
                + " | ".join(parts)
                + f" | late so far {totals['late']}{note}")
        if alerts:
            line += "  ALERT: " + ", ".join(alerts)
        print(line, flush=True)

    def process(e):
        """Steps 1-5 for one event."""
        nonlocal watermark
        totals["received"] += 1

        # 1. Deduplicate: one payment has each status at most once.
        key = (e["invoiceId"], e["status"])
        if key in seen:
            totals["duplicates"] += 1
            return
        seen.add(key)

        # 2. Event time and its window: [start, start + 60).
        t = to_epoch(e["modifiedDate"])
        start = t - t % WINDOW

        # 3. Late: its window ended more than `lateness` seconds before the watermark,
        #    so it was already closed and printed. We can only count such an event.
        if start + WINDOW + args.lateness <= watermark:
            totals["late"] += 1
            return

        # Add the event to its window.
        w = windows.setdefault(start, new_window())
        w["events"] += 1
        totals["accepted"] += 1
        if e["status"] in FINAL:
            w["methods"][e["paymentMethod"]][0 if e["status"] == "success" else 1] += 1

        # 4. Watermark = the latest event time seen (it never moves back).
        watermark = max(watermark, t)

        # 5. Close every window that the watermark has passed by more than `lateness`.
        for s in sorted(windows):
            if s + WINDOW + args.lateness <= watermark:
                close_window(s)

    idle = 0.0
    try:
        while True:
            msg = consumer.poll(1.0)                 # wait up to 1 s for the next message
            if msg is None:
                idle += 1.0
                if args.idle_exit and idle >= args.idle_exit:
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {msg.error()}")
                continue
            idle = 0.0
            process(json.loads(msg.value()))
            consumer.store_offsets(msg)              # only now this message counts as consumed
    except KeyboardInterrupt:
        pass
    finally:
        # Windows still open have not waited for late events; print them, marked as partial.
        for s in sorted(windows):
            close_window(s, note=" (closed at shutdown)")
        consumer.close()                             # commit offsets and leave the group
        print(f"summary: received {totals['received']}, duplicates {totals['duplicates']}, "
              f"late rejected {totals['late']}, accepted {totals['accepted']}, "
              f"windows {totals['windows']}, alerts {totals['alerts']}")


if __name__ == "__main__":
    main()
