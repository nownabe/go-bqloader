#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

if [[ ! -e .envrc ]]; then
  echo ".envrc doesn't exist" >&2
  exit 1
fi

# shellcheck disable=SC1091
source .envrc

export SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT.iam.gserviceaccount.com"

read -rp "Create new service account ($SERVICE_ACCOUNT_NAME)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gcloud iam service-accounts create \
    "$SERVICE_ACCOUNT_NAME" \
    --project "$GCP_PROJECT" \
    --description "An example of go.nownabe.dev/bqloader"

  gcloud projects add-iam-policy-binding \
    "$GCP_PROJECT" \
    --member "serviceAccount:$SERVICE_ACCOUNT" \
    --role roles/bigquery.jobUser
fi

read -rp "Create new storage bucket ($SOURCE_BUCKET)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gsutil mb -p "$GCP_PROJECT" -l US-EAST1 "gs://$SOURCE_BUCKET"
  gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT}:roles/storage.objectViewer" "gs://$SOURCE_BUCKET"
fi

read -rp "Create new BigQuery dataset ($BIGQUERY_DATASET_ID)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  bq --location us-east1 mk --dataset \
    --description "An example of go.nownabe.dev/bqloader" \
    "$BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID"
fi

tables=("sbi_sec" "sbi_bank" "smbc_card")
for table in "${tables[@]}"; do
  read -rp "Create new BigQuery table ($table)? [y/N]: " yn
  if [[ $yn = [yY] ]]; then
    bq mk --table  \
      --description "An example of go.nownabe.dev/bqloader" \
      "$BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID.$table" \
      "$table.json"

    envsubst < policy.json.tpl > policy.json
    bq set-iam-policy \
      "$BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET_ID.$table" policy.json
  fi
done

read -rp "Deploy Function ($FUNCTION_NAME)? [y/N]: " yn
if [[ $yn = [yY] ]]; then
  gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    --project "$GCP_PROJECT"

  gcloud functions deploy "$FUNCTION_NAME" \
    --project "$GCP_PROJECT" \
    --region us-east1 \
    --runtime go113 \
    --trigger-resource "$SOURCE_BUCKET" \
    --trigger-event google.storage.object.finalize \
    --entry-point BQLoad \
    --service-account "$SERVICE_ACCOUNT" \
    --set-env-vars "BIGQUERY_PROJECT_ID=$BIGQUERY_PROJECT_ID,BIGQUERY_DATASET_ID=$BIGQUERY_DATASET_ID" \
    --set-env-vars "SLACK_TOKEN=$SLACK_TOKEN,SLACK_CHANNEL=$SLACK_CHANNEL"
fi
