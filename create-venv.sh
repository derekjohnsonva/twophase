#!/bin/bash
set -e
python3 -m venv .
source bin/activate
pip install wheel
pip install grpcio==1.26.0 grpcio-tools==1.26.0 parameterized
