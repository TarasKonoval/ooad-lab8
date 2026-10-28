-- Part 2, Way A: external table over the raw JSON Lines copy of payments/.
CREATE DATABASE IF NOT EXISTS lab8;

CREATE EXTERNAL TABLE IF NOT EXISTS lab8.payment_events (
  invoiceId     string,
  status        string,
  failureReason string,
  amount        bigint,
  finalAmount   bigint,
  ccy           int,
  reference     string,
  createdDate   string,   -- ISO-8601 UTC, parse with from_iso8601_timestamp()
  modifiedDate  string,   -- event time
  receivedAt    string,   -- processing time
  merchantId    string,
  merchantName  string,
  mcc           int,
  customerId    string,
  paymentMethod string,
  device        string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ooad-lab8-669977209400/payments/'
TBLPROPERTIES ('classification' = 'json');

MSCK REPAIR TABLE lab8.payment_events;
