# Part 4 — Streaming in the cloud: results

Pipeline: `producer_kinesis.py` → Kinesis Data Stream (1 shard) → (a) Firehose → S3 `raw/dt=…/` → Athena `lab8.payments_stream`, and (b) a Lambda with 60 s tumbling windows → SNS email. The diagram is in `part4/diagram/pipeline.svg` and `.png`, and the infrastructure is in `part4/infra`.

## Runs (2026-10-28, 20:42–20:47 UTC)

| Run | Replay of 2026-10-01 | Events | Fail rate | Result |
|---|---|---|---|---|
| 1 (`run1_normal_producer.log`) | 17:00–18:26 | 9,000 | none | 150 s, 20.0 payments/s, 0 retries |
| 2 (`run2_fail30_producer.log`) | 18:30–19:39 | 9,000 | 30% (836 successes turned into failures) | 150 s, 20.0 payments/s, 0 retries |

## (a) Firehose → S3 → Athena
- **Delivery:** 6 files and 7.5 MB in `raw/dt=2026-10-28/`, and nothing in `raw-errors/`. `dt` is the day Firehose received the data, not the payment day.
- **Totals (`22_stream_totals.csv`):** **18,000 events = 18,000 sent**, 5,990 payments and 185 exact duplicates.
- **Per minute (`21_stream_per_minute.csv`):** 158 event-time minutes, with duplicates removed. The failure share was **5.69%** from 17:00 to 18:30 (169 of 2,971 finished payments) and **33.48%** from 18:30 on (991 of 2,960). The jump happens exactly at 18:30.
- **Cost of the queries:** 7.2 MB and 14.4 MB scanned, under 1 s each.

## (b) Lambda → SNS
Window lines are in `lambda_windows.log`. Each 60 s window holds about 28 event-minutes, because the replay runs 28× faster than real time.

| Window (arrival time) | Events | Card failure | Alert |
|---|---|---|---|
| 20:42 (partial, the run started at 20:42:24) | 1,895 | 3% | – |
| 20:43 | 3,531 | 5% | – |
| 20:44 (the failure run started at 20:44:55) | 3,673 | 9% | – |
| 20:45 | 3,723 | 35% | **ALERT** for apple_pay, card and google_pay |
| 20:46 | 3,331 | 34% | **ALERT** for all four methods |
| 20:47 (last 27 s, about 1,670 events) | – | – | not logged |

- **Emails:** 2 alert emails were published.
- **Steady shares:** with about 600 card payments per window, the share is steady (3–9% when normal), so there were no false alerts. Part 3 had about 25 per window and 13 false alerts.
- **The last window was never reported:** the 20:47 window got no final invocation, even minutes after the producer stopped. As observed, a processing-time window is only closed when more records reach the shard, so the last window of a burst stays open until traffic resumes. In Part 3 the consumer had to wait for the watermark in the same way, but it printed open windows at shutdown.

## Compared with Part 3 (Redpanda)
- **Time:**
  - Part 3 used event-time windows with a watermark and allowed lateness, and rejected 1.7% of events as late.
  - Here the windows use arrival time: nothing is ever late, but a late event counts in the window it arrived in.
  - The Athena query uses event time again, so the batch road corrects what the real-time road approximates.
- **State:**
  - Part 3 kept the duplicate set and open windows in process memory, and lost them on restart.
  - Here Lambda carries the window state between invocations, up to 1 MB per window. The duplicate check only works within one window.
- **Scaling:**
  - Part 3 scaled through consumer groups and partitions.
  - Here the stream scales by shards, and the event source mapping runs one Lambda per shard.
- **Operations:** nothing to run locally, and storage to S3 comes free with Firehose.
- **Cost:**
  - Part 3 cost $0.
  - Here the shard is $0.015/h while it exists, and Firehose, Lambda, SNS, S3 and Athena cost less than $0.01 for this run.
  - In total, about $0.02–0.05 depending on how long the stream stays up before `terraform destroy`.
