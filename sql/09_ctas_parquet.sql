-- Step 7: write the deduplicated payments to Parquet under curated/, partitioned by dt.
CREATE TABLE lab8.payments_parquet
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ooad-lab8-669977209400/curated/payments/',
  partitioned_by = ARRAY['dt']
) AS
SELECT * FROM lab8.payments;
