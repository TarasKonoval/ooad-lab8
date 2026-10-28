-- Step 6: events that reached us more than 60 s after they happened,
-- counted on the raw events after removing exact duplicates.
WITH distinct_events AS (
  SELECT DISTINCT * FROM lab8.payment_events
)
SELECT
  count_if(date_diff('second', from_iso8601_timestamp(modifiedDate),
                               from_iso8601_timestamp(receivedAt)) > 60) AS late_events,
  count(*) AS all_events,
  round(100.0 * count_if(date_diff('second', from_iso8601_timestamp(modifiedDate),
                                             from_iso8601_timestamp(receivedAt)) > 60)
        / count(*), 3) AS late_pct
FROM distinct_events;
