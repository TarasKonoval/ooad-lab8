-- Way B: does the crawler's table work as-is? Count, partitions and failureReason fill rate.
SELECT
  count(*)                          AS events,
  count(DISTINCT dt)                AS partitions,
  count(failureReason)              AS with_failure_reason,
  count_if(status = 'failure')      AS failure_events,
  max(amount)                       AS max_amount
FROM lab8_crawler.payments;
