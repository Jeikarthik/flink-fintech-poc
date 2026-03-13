#!/bin/bash
set -e

cd /opt/feast/feature_repo
mkdir -p data

echo "Applying Feast definitions..."
feast apply

echo "Feast setup complete."
