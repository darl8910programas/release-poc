#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# Smoke test: dispara N pedidos en paralelo y verifica que todos
# terminen publicados. Útil para probar concurrencia del relay.
# Uso:   ./scripts/smoke-test.sh [cantidad]
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:5000}"
TOTAL="${1:-100}"
PARALLELISM="${PARALLELISM:-20}"

echo "Disparando $TOTAL pedidos a $API_BASE con paralelismo $PARALLELISM..."

post_pedido() {
  local i=$1
  local clienteId
  clienteId=$(uuidgen)
  curl -sS -X POST "$API_BASE/api/pedidos" \
    -H "Content-Type: application/json" \
    -d "{\"clienteId\":\"$clienteId\",\"total\":$((RANDOM % 1000 + 1)).50}" \
    -o /dev/null -w "%{http_code}\n"
}

export -f post_pedido
export API_BASE

seq 1 "$TOTAL" | xargs -P "$PARALLELISM" -I{} bash -c 'post_pedido {}' | sort | uniq -c

echo ""
echo "Esperando 3s para que el relay procese..."
sleep 3

echo ""
echo "Estado del Outbox:"
curl -sS "$API_BASE/api/pedidos/outbox/stats" | jq .
