-- Part 4: did the stream arrive? Per minute of event time (modifiedDate), after removing
-- exact duplicates: events, finished payments and the failure share.
-- The two producer runs replayed 17:00-18:26 (normal) and 18:30-19:39 (30% failures)
-- of 2026-10-01, so the share should jump from ~6% to ~34% at 18:30.
WITH distinct_events AS (
  SELECT DISTINCT * FROM lab8.payments_stream WHERE dt = '2026-10-28'
)
SELECT
  date_trunc('minute', from_iso8601_timestamp(modifiedDate)) AS event_minute,
  count(*)                                                   AS events,
  count_if(status IN ('success', 'failure'))                 AS finished,
  count_if(status = 'failure')                               AS failed,
  round(100.0 * count_if(status = 'failure')
        / nullif(count_if(status IN ('success', 'failure')), 0), 1) AS failure_pct
FROM distinct_events
GROUP BY 1
ORDER BY 1;
