#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

if [[ ! -e .envrc ]]; then
  echo ".envrc doesn't exist" >&2
  exit 1
fi

source .envrc

gsutil cp *.csv gs://$SOURCE_BUCKET/csv/
