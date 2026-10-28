-- Part 4: totals of what came through the stream, to compare with what the producer sent.
SELECT
  count(*)                                AS events,
  count(DISTINCT invoiceId)               AS payments,
  count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM lab8.payments_stream WHERE dt = '2026-10-28')) AS exact_duplicates,
  min(receivedAt)                         AS first_received,
  max(receivedAt)                         AS last_received,
  count(DISTINCT "$path")                 AS files
FROM lab8.payments_stream
WHERE dt = '2026-10-28';
