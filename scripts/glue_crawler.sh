#!/usr/bin/env bash
# Part 2, Way B: Glue role, database lab8_crawler and an on-demand crawler over payments/.
# Run after: source scripts/aws-env.sh
set -euo pipefail
B=ooad-lab8-669977209400
ACCOUNT=669977209400
ROLE=ooad-lab8-glue-crawler
TAGS='Key=PROJECT_NAME,Value=ooad-lab8'

aws iam create-role --role-name "$ROLE" --tags "$TAGS" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name "$ROLE" --policy-name read-payments --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Effect\": \"Allow\", \"Action\": \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::$B/payments/*\"},
    {\"Effect\": \"Allow\", \"Action\": \"s3:ListBucket\", \"Resource\": \"arn:aws:s3:::$B\",
     \"Condition\": {\"StringLike\": {\"s3:prefix\": [\"payments\", \"payments/*\"]}}}
  ]}"

aws glue create-database --database-input '{"Name":"lab8_crawler","Description":"Tables created by the lab8 Glue crawler"}'

# IAM is eventually consistent: retry until Glue can assume the new role.
for i in $(seq 1 12); do
  if aws glue create-crawler --name lab8-payments-crawler \
      --role "arn:aws:iam::$ACCOUNT:role/$ROLE" \
      --database-name lab8_crawler \
      --targets "{\"S3Targets\":[{\"Path\":\"s3://$B/payments/\"}]}" \
      --tags PROJECT_NAME=ooad-lab8 2>/dev/null; then break; fi
  sleep 5
done

aws glue start-crawler --name lab8-payments-crawler
echo "crawler started"
