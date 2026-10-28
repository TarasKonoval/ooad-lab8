# Part 3 — Local streaming with Redpanda: results

Setup: Redpanda v24.2.7 and Console v2.7.2 (`part3/docker-compose.yml`), topic `payments` with 3 partitions.
- **Input:** each run replays 2026-10-01 from 17:00 UTC (the evening peak). That is 10,800 events of 3,588 payments, sorted by `receivedAt`, key `invoiceId`, sent 28.7× faster than real time at 20 payments/s, so a run takes 179 s.
- **Consumer:** drops duplicates by `(invoiceId, status)` and uses one-minute tumbling windows by `modifiedDate`.
  - A window closes when the watermark (the latest event time seen) passes `window_end + lateness`.
  - It alerts when a method's failure share is above 10% and that method had at least `--min-payments` finished payments in the window.

## Baseline (normal run, lateness 120 s)
Logs: `baseline_producer.log`, `baseline_consumer.log`.
- **Delivery:** 10,800 events delivered and 10,800 received. 107 duplicates were dropped (1.0%) and 183 events rejected as late (1.7%), leaving 10,510 accepted in 87 windows.
- **Failure share:** 5.84% overall (204 of 3,493 finished payments).
- **Alerts:** 22 of 87 windows alerted with `--min-payments 10` (`first_run_min10_consumer.log`) and **13 with `--min-payments 20`**, now the default.
- **Why so many alerts:** about 6% of payments fail normally, and a one-minute window holds only 5–30 payments per method, so a share above 10% happens by chance. Only `card` ever reaches 20 finished payments in a minute (about 24 at peak), so with min 20 in practice only card is monitored.

## Experiment 1 — consumer stopped for a minute
Logs: `exp1_*`. The group snapshots are `exp1_group_while_stopped.txt` and `exp1_group_after.txt`.
- **Run:** consumer `g1` ran for 60 s, was stopped with SIGTERM, stayed down for 60 s, then restarted with the same group.
- **Lag:** while it was down, the group's lag grew to 3,235 messages.
- **Nothing lost:** the restarted consumer continued from the committed offsets and brought the lag back to 0. Received: 3,363 + 7,437 = **10,800 = produced**.
- **First attempt lost a message** (`attempt1_exp1_*`):
  - That run received only 10,799 events.
  - Auto-commit marks a message as consumed as soon as `poll()` returns it, so the message being handled at the stop was committed without being processed (at-most-once).
  - The consumer now calls `store_offsets()` after processing each message (at-least-once).
- **What the restart lost — the in-memory state:**
  - *Open windows:* windows 17:26–17:29 were still open at the stop, so they were printed "closed at shutdown" with partial counts. The restarted consumer printed them again with only the events that arrived after the restart (1, 1 and 14 events), so each window was split in two.
  - *Duplicate set:* it was lost too, but no duplicate slipped through (31 + 76 = 107, the same as the baseline), because a duplicate arrives close to its original.
  - *Watermark:* this was the real damage, **2,247 events rejected as late** after the restart, compared with 183 in the baseline. The client read the backlog one partition at a time (2,011 messages from partition 0, then 1,224 from partition 1), so partition 0 alone pushed the single watermark about 30 event-minutes ahead.
  - *The fix:* keep a watermark per partition and use the minimum, as Flink does, and keep the state in Flink checkpoints or Redis rather than in process memory.

## Experiment 2 — two consumers in one group
Logs: `exp2_*`. Group snapshots: `exp2_group_two_members.txt` and `exp2_group_after_rebalance.txt`, plus the Console screenshot.
- **Partition split:** with both consumers in group `g2`, the `range` balancer gave consumer A partitions [0, 1] and consumer B partition [2]. The group was `Stable` with 2 members.
- **Rebalance:** when B was stopped, A had [0, 1] revoked and was then assigned [0, 1, 2]. The group stayed `Stable` with 1 member and A continued from B's committed offset on partition 2, so no messages were skipped.
- **Received:** B got 5,434 events before stopping and A got 12,867.
- **A side effect for windowed analytics:**
  - Each consumer sees only the payments in its own partitions, so every window was printed by both consumers, each with part of the counts.
  - B, with one third of the traffic, never reached 20 card payments in a window and raised 0 alerts.
  - For a global failure share, the per-consumer counts would need a second aggregation step, or the windows would need to be keyed by `paymentMethod`, as a keyed stream in Flink would do.

## Experiment 3 — 30% of payments made to fail
Logs: `exp3_fail30_*`.
- **Injected:** `--fail-rate 0.3` turned 998 success events into failures. The overall failure share rose from 5.84% to **33.70%**.
- **Alerts:** **59 of 87 windows alerted, vs 13 in the baseline**. These are exactly the windows where card had at least 20 finished payments. Card's share in them had a median of 33% and a minimum of 14%, so every one crossed 10%. Smaller methods never reach the min-20 sample.
- **Unchanged:** duplicates (107) and late events (183) were the same as the baseline, because only statuses changed.

## Experiment 4 — no waiting for late events (`--lateness 0`)
Logs: `exp4_lateness0_consumer.log`. It read the same producer run as the baseline, in its own group, so the input was identical.
- **Late rejections:** **303 events rejected as late, vs 183 with 120 s**. That is 120 more events thrown away, and 10,390 accepted instead of 10,510.
- **Window numbers:**
  - 56 of 87 windows got fewer events (up to 5 fewer), and the mean dropped from 120.8 to 119.4 events per window.
  - 40 method-window failure counts changed. For example, 17:29 had 151 events in the baseline and 148 at lateness 0.
  - The overall failure share stayed about the same (5.86% vs 5.84%), and alerting windows went from 13 to 12.
- **Why the difference is small:** in this dataset most late events are 2–10 minutes late, so 120 s of waiting already rejects most of them. Lateness 0 only adds the ones that are 0–2 minutes late.
- **The trade-off:** waiting longer gives more complete windows but later alerts. Lateness 0 gives instant results that are slightly wrong. Rejected events would usually go to a side output or a correction job instead of being dropped.
