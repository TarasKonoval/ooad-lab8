"""Lambda on the Kinesis stream: failure share per paymentMethod in 60-second tumbling windows.

Windowing is done by the event source mapping (tumbling_window_in_seconds = 60), not by us:
- Lambda groups the records by their ARRIVAL time in Kinesis (processing time) into 60 s windows.
- Within a window it may call us many times (one call per batch). Whatever we return as
  "state" is handed back to us in the next call for the same window.
- When the window ends, Lambda makes one last call with isFinalInvokeForWindow = true. That is
  where we compute the shares and publish an alert.

Note the difference from Part 3: these windows use processing time (when the record reached
Kinesis), not event time (modifiedDate). There is no watermark and no allowed lateness: a late
event simply counts in the window in which it arrived. In exchange, AWS keeps the state for us
and we need no Redis or DynamoDB. With one shard there is exactly one window at a time.
"""
import base64
import json
import os

import boto3

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
THRESHOLD = float(os.environ.get("THRESHOLD", "0.10"))
MIN_PAYMENTS = int(os.environ.get("MIN_PAYMENTS", "20"))

sns = boto3.client("sns")   # created once per container and reused across calls


def load_state(event):
    """The state we returned last time in this window, or an empty one at a window's start.

    We keep everything in one JSON string under the key "json", because Lambda expects the
    state to be a map of strings. Its size limit is 1 MB per window.
    """
    raw = (event.get("state") or {}).get("json")
    if raw:
        return json.loads(raw)
    # methods: paymentMethod -> [successes, failures]; seen: "invoiceId|status" keys in this window
    return {"events": 0, "duplicates": 0, "methods": {}, "seen": []}


def handler(event, context):
    state = load_state(event)
    seen = set(state["seen"])                       # set for fast lookups; saved back as a list

    for record in event["Records"]:
        # Kinesis delivers the bytes we sent, base64-encoded; each record is one JSON line.
        e = json.loads(base64.b64decode(record["kinesis"]["data"]))

        # Drop duplicates, as in Part 3. Only within this window: the set starts empty each window.
        key = f'{e["invoiceId"]}|{e["status"]}'
        if key in seen:
            state["duplicates"] += 1
            continue
        seen.add(key)
        state["events"] += 1

        # Count only the statuses that say how a payment ended.
        if e["status"] in ("success", "failure"):
            counts = state["methods"].setdefault(e["paymentMethod"], [0, 0])
            counts[0 if e["status"] == "success" else 1] += 1

    state["seen"] = sorted(seen)

    if event.get("isFinalInvokeForWindow"):
        finish_window(event, state)
        return {"state": {}}                         # the next window starts empty

    # Not the end of the window yet: hand the running totals back to Lambda.
    return {"state": {"json": json.dumps(state)}}


def finish_window(event, state):
    """Compute the failure share per method, log one line, and publish to SNS if needed."""
    window = event.get("window", {})                 # {"start": "...Z", "end": "...Z"}
    parts, alerts = [], []
    for method, (ok, failed) in sorted(state["methods"].items()):
        finished = ok + failed
        share = failed / finished if finished else 0.0
        parts.append(f"{method} {failed}/{finished} {share:.0%}")
        if finished >= MIN_PAYMENTS and share > THRESHOLD:
            alerts.append(f"{method}: {failed} of {finished} payments failed ({share:.0%})")

    # One compact line per window in CloudWatch Logs, like the Part 3 consumer.
    line = (f'window {window.get("start")}..{window.get("end")} events {state["events"]} '
            f'dup {state["duplicates"]} | ' + " | ".join(parts))
    if event.get("isWindowTerminatedEarly"):
        line += " (terminated early: state over 1 MB)"
    if alerts:
        line += "  ALERT: " + "; ".join(alerts)
    print(line)

    if alerts:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="lab8 ALERT: payment failure share above 10%",   # max 100 characters
            Message=(f'Window {window.get("start")} .. {window.get("end")} (arrival time)\n\n'
                     + "\n".join(alerts)
                     + f'\n\nAll methods: {" | ".join(parts)}'),
        )
