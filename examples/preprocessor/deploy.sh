#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

if [[ ! -e .envrc ]]; then
  echo ".envrc doesn't exist" >&2
  exit 1
fi

source .envrc

export SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com"

read -p "Create new service account ($SERVICE_ACCOUNT_NAME)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gcloud iam service-accounts create \
    $SERVICE_ACCOUNT_NAME \
    --project $GCP_PROJECT \
    --description "Used by Cloud Functions which executes a quickstart example of go.nownabe.dev/bqloader"

  gcloud projects add-iam-policy-binding \
    $GCP_PROJECT \
    --member serviceAccount:$SERVICE_ACCOUNT \
    --role roles/bigquery.jobUser
fi

read -p "Create new storage bucket ($SOURCE_BUCKET)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gsutil mb -p $GCP_PROJECT -l US-EAST1 gs://$SOURCE_BUCKET
  gsutil iam ch serviceAccount:${SERVICE_ACCOUNT}:roles/storage.objectViewer gs://$SOURCE_BUCKET
fi

read -p "Create new BigQuery dataset ($BIGQUERY_DATASET_ID)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  bq --location us-east1 mk --dataset \
    --description "Used for a quickstart example of go.nownabe.dev/bqloader" \
    $BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID
fi

read -p "Create new BigQuery table ($BIGQUERY_TABLE_ID)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  bq mk --table  \
    --description "Used for a quickstart example of go.nownabe.dev/bqloader" \
    --require_partition_filter \
    --time_partitioning_field date \
    --time_partitioning_type MONTH \
    $BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID.$BIGQUERY_TABLE_ID schema.json

  envsubst < policy.json.tpl > policy.json
  bq set-iam-policy \
    $BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID.$BIGQUERY_TABLE_ID policy.json
fi

read -p "Deploy Function ($FUNCTION_NAME)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gcloud functions deploy $FUNCTION_NAME \
    --project $GCP_PROJECT \
    --region us-east1 \
    --runtime go113 \
    --trigger-resource $SOURCE_BUCKET \
    --trigger-event google.storage.object.finalize \
    --entry-point BQLoad \
    --service-account $SERVICE_ACCOUNT \
    --set-env-vars "BIGQUERY_PROJECT_ID=$BIGQUERY_PROJECT_ID,BIGQUERY_DATASET_ID=$BIGQUERY_DATASET_ID,BIGQUERY_TABLE_ID=$BIGQUERY_TABLE_ID" \
    --set-env-vars "SLACK_TOKEN=$SLACK_TOKEN,SLACK_CHANNEL=$SLACK_CHANNEL"
fi

gsutil cp source_202009.csv gs://$SOURCE_BUCKET/preprocessor/source_202009.csv
