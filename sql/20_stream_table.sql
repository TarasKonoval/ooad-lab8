-- Part 4: table over the files Firehose wrote from the Kinesis stream.
-- Same columns and SerDe as payment_events. dt here is the day Firehose received the data
-- (arrival time), not the payment day. Partition projection computes the partitions from
-- the dt range, so no MSCK REPAIR is needed when Firehose writes a new day.
CREATE EXTERNAL TABLE IF NOT EXISTS lab8.payments_stream (
  invoiceId     string,
  status        string,
  failureReason string,
  amount        bigint,
  finalAmount   bigint,
  ccy           int,
  reference     string,
  createdDate   string,
  modifiedDate  string,
  receivedAt    string,
  merchantId    string,
  merchantName  string,
  mcc           int,
  customerId    string,
  paymentMethod string,
  device        string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ooad-lab8-669977209400/raw/'
TBLPROPERTIES (
  'projection.enabled'        = 'true',
  'projection.dt.type'        = 'date',
  'projection.dt.format'      = 'yyyy-MM-dd',
  'projection.dt.range'       = '2026-10-01,NOW',
  'storage.location.template' = 's3://ooad-lab8-669977209400/raw/dt=${dt}/'
);
