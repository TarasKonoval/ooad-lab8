-- Scale answers: events, exact duplicates, unique payments, lateness.
-- Lateness = receivedAt - modifiedDate, measured on events after removing exact duplicates.
-- The last two columns are sanity checks: duplicates that differ only in dt, and events
-- whose dt is not the date of createdDate (both expected to be 0).
WITH raw AS (
  SELECT * FROM lab8.payment_events
),
distinct_events AS (
  SELECT DISTINCT * FROM raw
),
lag AS (
  SELECT date_diff('second', from_iso8601_timestamp(modifiedDate),
                             from_iso8601_timestamp(receivedAt)) AS lag_s
  FROM distinct_events
)
SELECT
  (SELECT count(*) FROM raw)                                   AS total_events,
  (SELECT count(*) FROM distinct_events)                       AS distinct_events,
  (SELECT count(*) FROM raw) - (SELECT count(*) FROM distinct_events) AS exact_duplicates,
  (SELECT count(DISTINCT invoiceId) FROM raw)                  AS unique_invoices,
  (SELECT count_if(lag_s > 60) FROM lag)                       AS late_over_60s,
  (SELECT round(100.0 * count_if(lag_s > 60) / count(*), 3) FROM lag)   AS late_over_60s_pct,
  (SELECT count_if(lag_s > 3600) FROM lag)                     AS late_over_1h,
  (SELECT round(100.0 * count_if(lag_s > 3600) / count(*), 4) FROM lag) AS late_over_1h_pct,
  (SELECT max(lag_s) FROM lag)                                 AS max_lag_s,
  (SELECT count(*) FROM distinct_events)
    - (SELECT count(*) FROM (SELECT DISTINCT invoiceId, status, failureReason, amount,
         finalAmount, ccy, reference, createdDate, modifiedDate, receivedAt, merchantId,
         merchantName, mcc, customerId, paymentMethod, device FROM raw)) AS dups_across_dt,
  (SELECT count_if(substr(createdDate, 1, 10) <> dt) FROM raw) AS dt_not_created_date;
