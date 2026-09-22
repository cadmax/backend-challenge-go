#!/bin/sh
set -eu
rm -f /run/wager-aws/ready
python3 /opt/wager-bootstrap/provision.py
touch /run/wager-aws/ready
