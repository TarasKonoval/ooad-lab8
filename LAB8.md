A ride-hailing app looks like one product, but its data takes two very different roads: one that answers in seconds and one that answers overnight. In this lab you read a real architecture of that kind, then build both roads yourself on a payments dataset of about three million events: batch analytics with Athena and Glue on AWS, and stream processing with Redpanda on your laptop.

The dataset is already generated and waiting in S3. You do not write a generator; you work with the data.

What you submit
Upload files (up to 5) and add a short online text with your answers.

Part 2 — the queries you wrote in Athena, with their results (screenshots or exported CSV), and the answers to the questions in What to submit for Part 2.
Part 3 — the output of your consumer for each experiment, and what changed in the numbers.
Part 4 — the diagram of your own cloud pipeline, a screenshot of an Athena query over the data that came through the stream, and a short comparison with Part 3.
Any language model may help you write the code; you must be able to explain every line of it in class.

Part 0 — Why process data at all
Raw events mean nothing on their own. One GPS ping, one order, one payment becomes valuable only after it is processed together with millions of others.
Uklon is a good end-to-end example because it has both kinds of processing: streaming (matching a driver, surge pricing, tracking a ride second by second) and batch (reports, analytics for product managers, training models).
The scale is roughly 470 thousand rides and 580 million GPS pings a day (an estimate from the lecture).
Part 1 — Uklon's data architecture: the path of one ride
The path of one ride through Uklon's data platform on AWS

Read the diagram by the numbered steps:

Entry. The passenger and driver apps talk to an Application Load Balancer. Driver GPS takes a separate road: a persistent MQTT connection to AWS IoT Core, not an HTTP request every 3 seconds. 580 million pings a day through API Gateway would cost tens of thousands of dollars a month.
Microservices. Orders, matching and payments run on ECS Fargate: no servers to manage, automatic scaling, a constant load. The main database is Amazon Aurora.
The event stream. GPS pings, service events and changes in the database (through DMS change data capture) all land in Kinesis Data Streams.
Real time. Apache Flink computes surge pricing per district and raises alerts. Driver positions live in Redis, surge multipliers in DynamoDB, and matching reads both. The push to the passenger goes through SNS.
The data lake. Firehose writes the raw events to S3 in batches. A Lambda function reacts to every new file: validation and checks.
Batch. A nightly Glue job turns the raw data into Parquet. The Glue Data Catalog holds the schema, Athena runs SQL over it, and Amazon Quick shows dashboards.
ML. SageMaker trains ETA and demand models on the data in S3, and the services call the deployed models.
Questions:

What happens if the real-time road falls behind by 10 minutes? And the batch road by a day?
Why does GPS not go through Lambda?
Which parts of this diagram would a small startup drop first, and what would it lose?
Part 2 — Batch on AWS: payments in S3
The Uklon diagram is a case to discuss. With your own hands you work on a universal example: payments in an online service, where every row is one transaction. You read the same data from S3 in two ways: directly with Athena, and through a Glue Crawler.

The dataset
Every row is one webhook event: a change in the status of a payment. One payment produces 2–3 events: created → processing → success or failure. About 1% of events arrive twice, and about 2% arrive late, 1 to 10 minutes after they happened.

Field Example Meaning
invoiceId p2_9ZgpZVsl3 the payment; all its events share it
status failure created, processing, success or failure
failureReason Insufficient funds only on failed payments
amount, finalAmount 45000, 0 in minor units (kopiyky, cents); finalAmount is non-zero only on success
ccy 980 ISO 4217 currency: 980 UAH, 840 USD, 978 EUR
reference 84d0070e… the merchant's own order id
createdDate 2026-10-01T08:15:22Z when the payment was created
modifiedDate 2026-10-01T08:15:31Z when this status happened (event time)
receivedAt 2026-10-01T08:15:32Z when the event reached us (processing time)
merchantId, merchantName, mcc m-002, Supermarket, 5411 who was paid, and the merchant category code
customerId, paymentMethod, device u-0012345, apple_pay, ios who paid, and how
The fields from invoiceId to reference, and both dates, follow the format of monobank acquiring webhooks. The rest were added for analytics: a real webhook does not carry them, the merchant's backend would.

The data covers October 2026: one million payments, about three million events, 1.2 GB of JSON Lines, partitioned by day:

s3://ucu-ooad-lab8-payments/payments/dt=2026-10-01/events.jsonl, … dt=2026-10-31/

The bucket is public for reading. Copy the whole payments/ prefix into a bucket of your own with the AWS CLI, in the same region (us-east-1): copying inside one region is free, and from then on your table, your crawler and your results all live in your account.

The real source this imitates: monobank. Ukrainian banks do not offer a full streaming API like Kafka, but they do have webhooks: an event for every transaction, which is a stream of events in practice.

The personal API (api.monobank.ua/docs): the webhook sends a StatementItem with id, time (Unix), description, mcc, hold, amount and operationAmount in minor units, currencyCode, commissionRate, cashbackAmount, balance (field reference).
Acquiring (api.monobank.ua/docs/acquiring.html): a webhook on every change in an invoice's status, with invoiceId, status, amount, ccy, finalAmount, createdDate, modifiedDate, reference, and a signature in the X-Sign header.
LiqPay and WayForPay have a similar callback mechanism.
Way A — a table and queries in Athena
Create the table by hand and read the JSON straight from S3. First deduplicate; every other query then reads the deduplicated view.

The table. An external table payment_events over your copy of payments/, with the JSON SerDe, every field from the table above, and dt as the partition column. Load the partitions and check that a COUNT(\*) returns about three million.
Deduplication. A view payments that keeps exactly one row per invoiceId: the one with the latest modifiedDate. Remove the exact duplicates first.
Turnover. Turnover by merchant and currency, for successful payments only, in hryvnias (or dollars, euros), not in minor units.
Failure rate. The share of failed payments per payment method and device, highest first.
Peak hours. The number of payments per hour of the day.
Late events. How many events arrived more than 60 seconds after they happened (receivedAt minus modifiedDate), out of how many.
Parquet. Write the deduplicated payments into Parquet, partitioned by dt, under a curated/ prefix of your bucket (a CTAS query). Run queries 3–5 against the Parquet table too.
Way B — Glue Crawler
Run a Glue Crawler over your payments/ prefix and compare the schema it creates with the one you wrote.

Which types did it choose for amount, ccy and the dates? Did it recognise dt as a partition?
What happened to failureReason, which only some events have?
Which schema is more convenient for queries, and why are hand-written schemas still common?
What to submit for Part 2
The answers about scale: how many unique invoiceId, how many duplicates, which share of events was more than an hour late.
The results of queries 2–6.
The Data scanned figure and the run time of queries 3–5 on JSON and on Parquet. How many times cheaper did it get?
An explanation: why DISTINCT comes before row_number(), and what breaks without deduplication.
Part 3 — Streaming locally: Redpanda + Python
The same payments, but in real time and on your laptop, for free.

Local streaming: replay one day of payments through Redpanda

Build it yourself; a language model may write the code with you.

Redpanda. Run Redpanda (Kafka-compatible) and Redpanda Console with Docker Compose. Create a payments topic with 3 partitions. The Console shows the messages at localhost:8080.
Producer. Take one day of events from the dataset and replay it into the topic in the order of receivedAt, about 20 payments a second. Late events therefore arrive later, just as in production. Use invoiceId as the message key, so all events of one payment land in one partition.
Consumer. Drop duplicates, then compute the share of failure per paymentMethod in one-minute windows of event time (modifiedDate), and print an alert when it is above 10%. Close a window 2 minutes after it ends, to wait for late events.
Experiments:

Stop the consumer for a minute and start it again: no events are lost, because Kafka keeps a log.
Run two consumers in one consumer group and watch in the Console how they split the partitions.
Make the producer fail 30% of payments and watch the alert fire.
Set the waiting time for late events to zero and count how many events the consumer now rejects as late. What changes in the window numbers?
The set of events your consumer has already seen grows without limit. A production service keeps it in Redis, or in Flink's state, with a TTL. Be ready to discuss this in class.

Part 4 — Independent task: streaming in the cloud
Rebuild the pipeline from the diagram below on your own. You may take the same payments dataset, or another service or product of your choice (delivery, music streaming, IoT sensors and so on).

Streaming in the cloud: Kinesis, Firehose, S3, Athena, and an alert through SNS

Submit:

the diagram of your pipeline (if your product is different, explain where its data comes from);
your producer's code;
a screenshot of an Athena query over the data that came through the stream;
a short explanation: how your cloud pipeline differs from local Redpanda, and how much it cost.
Important: delete the Kinesis stream and the Firehose stream when you finish, so that you do not pay for shard hours.

What we discuss in class
Event time versus processing time: which one your windows use, and what happens to an event that is late.
Why the same raw data is kept twice, as JSON and as Parquet.
When a nightly batch is enough and when the business needs seconds.
What a managed service (Kinesis, Flink, Glue) gives you over running Kafka and Spark yourself, and what it costs.
