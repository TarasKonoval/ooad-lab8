-- Step 5: payments per hour of the day, by createdDate in Europe/Kyiv local time
-- (at_timezone handles the switch from EEST to EET on 2026-10-25).
SELECT
  hour(at_timezone(from_iso8601_timestamp(createdDate), 'Europe/Kyiv')) AS hour_kyiv,
  count(*) AS payments
FROM lab8.payments_parquet
GROUP BY 1
ORDER BY 1;
