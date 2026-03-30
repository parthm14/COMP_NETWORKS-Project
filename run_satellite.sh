#!/usr/bin/env bash
# Run on Machine 2 (satellite relay)
cd "$(dirname "$0")"
python3 satellite/satellite.py --config config.json
