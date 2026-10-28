-- Way B: step-3 turnover on the crawler's table. Each payment has exactly one success
-- event, so removing exact duplicates is enough here (no row_number needed).
WITH distinct_events AS (
  SELECT DISTINCT * FROM lab8_crawler.payments
)
SELECT
  merchantId,
  merchantName,
  CASE ccy WHEN 980 THEN 'UAH' WHEN 840 THEN 'USD' WHEN 978 THEN 'EUR'
           ELSE CAST(ccy AS varchar) END AS currency,
  count(*)                                AS payments,
  CAST(sum(finalAmount) / 100.0 AS decimal(18, 2)) AS turnover
FROM distinct_events
WHERE status = 'success'
GROUP BY merchantId, merchantName, ccy
ORDER BY currency, turnover DESC;
