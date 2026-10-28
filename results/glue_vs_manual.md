# Part 2, Way B — Glue Crawler vs hand-written schema

Crawler `lab8-payments-crawler` (on demand, no schedule) over `s3://ooad-lab8-669977209400/payments/`,
role `ooad-lab8-glue-crawler` (AWSGlueServiceRole + read-only on `payments/`), database `lab8_crawler`.
One run: 90.6 s, 1 table created (`lab8_crawler.payments`), 31 partitions. Setup: `scripts/glue_crawler.sh`.

## Schemas side by side

| Field | Hand-written `lab8.payment_events` | Crawler `lab8_crawler.payments` |
|---|---|---|
| invoiceId | string | string |
| status | string | string |
| failureReason | string | string (last column) |
| amount | **bigint** | **int** |
| finalAmount | **bigint** | **int** |
| ccy | int | int |
| reference | string | string |
| createdDate | string | string |
| modifiedDate | string | string |
| receivedAt | string | string |
| merchantId | string | string |
| merchantName | string | string |
| mcc | int | int |
| customerId | string | string |
| paymentMethod | string | string |
| device | string | string |
| **dt** (partition) | string | string |
| SerDe | org.openx.data.jsonserde.JsonSerDe | org.openx.data.jsonserde.JsonSerDe |
| Table name | chosen: `payment_events` | taken from the folder: `payments` |

The column order also differs. The crawler follows the key order of the JSON records (`invoiceId, amount, ccy, …`) and puts `failureReason` at the end. Column names are lowercased in both tables because the Glue/Hive catalog is case-insensitive.

## Answers

**Types for amount, finalAmount, ccy, mcc and the dates.**
- `amount`, `finalAmount`, `ccy` and `mcc` all became `int`.
- `createdDate`, `modifiedDate` and `receivedAt` became `string`. The crawler doesn't detect ISO-8601 timestamps in JSON, so the hand-written table made the same choice.
- `int` works on this data: the largest amount is 8,004,389 kopiyky. But it tops out at 2,147,483,647, about ₴21.5 M per payment. One bigger payment in a future file would overflow. That's why the hand-written table uses `bigint`.
- `sum()` returns `bigint` in both tables, so the turnover totals are the same either way.

**Was dt recognised as a partition?** Yes. The folders use the `dt=YYYY-MM-DD` naming, so the crawler made `dt` a partition key of type `string` and registered all 31 partitions (2026-10-01 to 2026-10-31). No `MSCK REPAIR` was needed.

**What happened to failureReason?**
- The key exists only on failed events. The crawler combined the schemas of all the records it sampled, so the field is still there as a nullable `string` column, placed last.
- There are 60,779 non-null values, exactly the number of `failure` events. Every other row is NULL.
- If the crawler had only sampled successful events, the column would have been missing until a later run found it. A crawler's schema depends on what it sees.

**Does it work as-is?** Yes.

| Query | Result | Scanned / time |
|---|---|---|
| `10_crawler_count`: COUNT(*), partitions, failureReason | 3,030,197 events, 31 partitions, 60,779 failureReason values, all 60,779 on `failure` events | 1,202.29 MB / 1.83 s |
| `11_crawler_turnover`: step-3 turnover | identical to `05_turnover_json.csv` | 1,202.29 MB / 1.98 s |

One catalog statistic is misleading: the crawler wrote `recordCount = 1,202,922` into the table properties. That figure comes from sampling. The real count is 3,030,197.

**Which schema is more convenient, and why are hand-written schemas still common?**
- **For getting started, the crawler is more convenient.** You don't type any DDL, partitions register themselves, and it can pick up new columns and new `dt=` folders on later runs.
- **For queries you rely on, the hand-written schema is better.**
  - Types are an explicit decision: `bigint` for money, and the right names, column order and comments.
  - The schema doesn't change when the sample changes. A crawler run can add columns, widen or change types, or split one table into several when files differ. That breaks views and dashboards with no warning.
  - It costs nothing to keep. A crawler costs money per run, and its catalog statistics are guesses.
  - The DDL lives in git and gets reviewed like code.
  - Where the data comes from: the producer already knows the schema; a crawler only guesses it from samples.
- **A common middle ground:** use a crawler once to draft the schema, then freeze it as DDL. Load new partitions with `MSCK REPAIR` / `ALTER TABLE ADD PARTITION` or partition projection, not a scheduled crawler.

## Cost
- **Crawler:** $0.44 per DPU-hour, billed per second with a 10-minute minimum per run. This 91 s run is billed as 10 minutes, so about $0.07 per DPU. Glue doesn't show the DPU count for a crawler run, so the exact charge will only appear in Cost Explorer, about a day later.
- **Athena:** 2 × 1.2 GB scanned is about $0.012 at $5 per TB.
- **Data Catalog:** 2 databases, 4 tables or views and 62 partitions are well inside the free tier (1 M objects).
