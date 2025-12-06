#!/usr/bin/env bash
set -e
MODULE=$1
shift || true

case "$MODULE" in
  ingest) python notebooks/01_ingest_bronze.py "$@" ;;
  transform) python notebooks/02_transform_silver.py "$@" ;;
  curate) python notebooks/03_curate_gold.py "$@" ;;
  manifest) python notebooks/04_manifest.py "$@" ;;
  dq) python notebooks/05_dqut.py "$@" ;;
  *) echo "Usage: $0 {ingest|transform|curate|manifest|dq}" ; exit 1 ;;
esac
