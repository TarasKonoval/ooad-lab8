-- Part 2, Way A, step 2: one row per invoiceId, its latest status.
-- DISTINCT first removes exact duplicate webhook deliveries, then row_number()
-- keeps the event with the latest modifiedDate (receivedAt breaks ties).
CREATE OR REPLACE VIEW lab8.payments AS
WITH distinct_events AS (
  SELECT DISTINCT
    invoiceId, status, failureReason, amount, finalAmount, ccy, reference,
    createdDate, modifiedDate, receivedAt, merchantId, merchantName, mcc,
    customerId, paymentMethod, device, dt
  FROM lab8.payment_events
),
ranked AS (
  SELECT *,
    row_number() OVER (PARTITION BY invoiceId
                       ORDER BY modifiedDate DESC, receivedAt DESC) AS rn
  FROM distinct_events
)
SELECT
  invoiceId, status, failureReason, amount, finalAmount, ccy, reference,
  createdDate, modifiedDate, receivedAt, merchantId, merchantName, mcc,
  customerId, paymentMethod, device, dt
FROM ranked
WHERE rn = 1;
