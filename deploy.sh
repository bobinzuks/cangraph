#!/bin/bash
set -euo pipefail

VPS="root@147.93.113.37"
REMOTE_DIR="/opt/cangraph"

echo "==> Syncing files to VPS..."
rsync -avz --exclude __pycache__ --exclude .env --exclude '*.pyc' \
    ./ "${VPS}:${REMOTE_DIR}/"

echo "==> Installing systemd service..."
ssh "${VPS}" "cp ${REMOTE_DIR}/cangraph.service /etc/systemd/system/ && \
    systemctl daemon-reload && \
    systemctl enable cangraph && \
    systemctl restart cangraph"

echo "==> Updating Caddy config..."
ssh "${VPS}" "if ! grep -q 'cangraph.ca' /etc/caddy/Caddyfile 2>/dev/null; then \
    cat ${REMOTE_DIR}/Caddyfile >> /etc/caddy/Caddyfile && \
    systemctl reload caddy; \
    echo 'Caddy config added'; \
else \
    echo 'Caddy config already present'; \
fi"

echo "==> Deployed to https://cangraph.ca"
