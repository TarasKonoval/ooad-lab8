#!/usr/bin/env bash
# Source this file: exports AWS credentials from ~/spry/.env without printing them.
ENV_FILE="${LAB8_ENV_FILE:-$HOME/spry/.env}"
while IFS='=' read -r key value; do
  case "$key" in
    AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AWS_REGION)
      value="${value%\"}"; value="${value#\"}"
      [ -n "$value" ] && export "$key=$value"
      ;;
  esac
done < "$ENV_FILE"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_PAGER=""
