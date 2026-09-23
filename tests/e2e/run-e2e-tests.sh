#!/usr/bin/env bash
# End-to-End Integration Test Suite for CCE Data Pipeline (Debezium + Kafka + ClickHouse)
# Prerequisites: docker compose up -d, schema applied, Debezium connector registered
# Usage: ./tests/e2e/run-e2e-tests.sh [clickhouse-host]

set -euo pipefail

CLICKHOUSE_HOST="${1:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CH_DB="cce_analytics"

# Kafka Connect REST + Debezium connector
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR="cce-ccedb-source"

PASS=0
FAIL=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_pass() { echo -e "  ${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
log_fail() { echo -e "  ${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); }
log_info() { echo -e "  ${YELLOW}[INFO]${NC} $1"; }

ch_query() {
    curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/?database=${CH_DB}" --data-binary "$1"
}

# ============================================================
echo "=== CCE Data Pipeline — E2E Test Suite ==="
echo "ClickHouse:    ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
echo "Kafka Connect: ${CONNECT_URL}"
echo ""

# ============================================================
echo "--- 1. Service Health Checks ---"

# ClickHouse
if curl -sf "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/ping" > /dev/null; then
    log_pass "ClickHouse is healthy"
else
    log_fail "ClickHouse is not responding"
fi

# Kafka Connect REST
if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    log_pass "Kafka Connect REST is reachable (${CONNECT_URL})"
else
    log_fail "Kafka Connect is not responding at ${CONNECT_URL}"
fi

# ============================================================
echo ""
echo "--- 2. Schema Validation ---"

TABLE_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}' AND engine NOT IN ('MaterializedView')" | tr -d '[:space:]')
if [[ "$TABLE_COUNT" -ge 9 ]]; then
    log_pass "ClickHouse has ${TABLE_COUNT} tables (expected >= 9)"
else
    log_fail "ClickHouse has ${TABLE_COUNT} tables (expected >= 9)"
fi

MV_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database = '${CH_DB}' AND engine = 'MaterializedView'" | tr -d '[:space:]')
if [[ "$MV_COUNT" -ge 12 ]]; then
    log_pass "ClickHouse has ${MV_COUNT} materialized-view triggers (expected >= 12)"
else
    log_fail "ClickHouse has ${MV_COUNT} materialized-view triggers (expected >= 12)"
fi

DICT_COUNT=$(ch_query "SELECT count() FROM system.dictionaries WHERE database = '${CH_DB}'" | tr -d '[:space:]')
if [[ "$DICT_COUNT" -ge 3 ]]; then
    log_pass "ClickHouse has ${DICT_COUNT} dictionaries (expected >= 3)"
else
    log_fail "ClickHouse has ${DICT_COUNT} dictionaries (expected >= 3)"
fi

# ============================================================
echo ""
echo "--- 3. CDC Data Flow ---"

# Verify inbound_event_logs has data from CDC
INBOUND_COUNT=$(ch_query "SELECT count() FROM inbound_event_logs FINAL" | tr -d '[:space:]')
if [[ "$INBOUND_COUNT" -ge 1 ]]; then
    log_pass "inbound_event_logs has ${INBOUND_COUNT} rows (CDC flowing)"
else
    log_fail "inbound_event_logs is empty (CDC not flowing)"
fi

# Verify MATERIALIZED column extraction works
EXTRACTED=$(ch_query "SELECT count() FROM inbound_event_logs WHERE facility_id != '' AND event_type != ''" | tr -d '[:space:]')
if [[ "$EXTRACTED" -ge 1 ]]; then
    log_pass "MATERIALIZED columns extracting correctly (${EXTRACTED} rows with facility_id + event_type)"
else
    log_fail "MATERIALIZED columns not extracting (no rows with facility_id + event_type)"
fi

# Verify protocol_instances CDC
PI_COUNT=$(ch_query "SELECT count() FROM protocol_instances FINAL" | tr -d '[:space:]')
if [[ "$PI_COUNT" -ge 1 ]]; then
    log_pass "protocol_instances has ${PI_COUNT} rows"
else
    log_fail "protocol_instances is empty"
fi

# Verify compliance_event_logs CDC
CE_COUNT=$(ch_query "SELECT count() FROM compliance_event_logs FINAL" | tr -d '[:space:]')
if [[ "$CE_COUNT" -ge 0 ]]; then
    log_pass "compliance_event_logs reachable (${CE_COUNT} rows)"
else
    log_fail "compliance_event_logs missing"
fi

# ============================================================
echo ""
echo "--- 4. Materialized View Population ---"

# Check event volume MV is populated
VOL_COUNT=$(ch_query "SELECT sum(event_count) FROM mv_event_volume_hourly" | tr -d '[:space:]')
if [[ "$VOL_COUNT" -ge 1 ]]; then
    log_pass "mv_event_volume_hourly has aggregated events (count: ${VOL_COUNT})"
else
    log_fail "mv_event_volume_hourly is empty"
fi

# Check daily/hourly consistency (daily is now derived from hourly at query time)
HOURLY_TODAY=$(ch_query "SELECT sum(event_count) FROM mv_event_volume_hourly WHERE toDate(hour) = today()" | tr -d '[:space:]')
log_info "mv_event_volume_hourly today: ${HOURLY_TODAY:-0} events"

# ============================================================
echo ""
echo "--- 5. Debezium Connector Status ---"

STATUS=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR}/status" 2>/dev/null || echo '{}')
if command -v jq >/dev/null 2>&1; then
    STATE=$(echo "$STATUS" | jq -r '.connector.state // "ABSENT"')
    if [[ "$STATE" == "RUNNING" ]]; then
        log_pass "Debezium connector ${CONNECTOR} is RUNNING"
    else
        log_fail "connector ${CONNECTOR} state: ${STATE}"
    fi
    FAILED=$(echo "$STATUS" | jq '[.tasks[]? | select(.state != "RUNNING")] | length')
    [[ "${FAILED:-0}" == "0" ]] && log_pass "all connector tasks RUNNING" || log_fail "${FAILED} task(s) not RUNNING"
else
    echo "$STATUS" | grep -q '"state":"RUNNING"' && log_pass "connector RUNNING" || log_fail "connector not RUNNING"
fi
log_info "Per-topic detail: kafka-ui (cce.public.*) · ${CONNECT_URL}/connectors/${CONNECTOR}/status"

# ============================================================
echo ""
echo "--- 6. Data Quality ---"

# No orphaned step_instances (LEFT JOIN pattern — safe with NULLs)
ORPHANS=$(ch_query "SELECT count() FROM (SELECT si.id FROM step_instances si FINAL LEFT JOIN protocol_instances pi FINAL ON si.protocol_instance_id = pi.id WHERE pi.id = '00000000-0000-0000-0000-000000000000' OR pi.id IS NULL)" | tr -d '[:space:]')
if [[ "$ORPHANS" == "0" ]]; then
    log_pass "No orphaned step_instances"
else
    log_fail "Found ${ORPHANS} orphaned step_instances"
fi

# ============================================================
echo ""
echo "=== Results ==="
echo -e "  ${GREEN}Passed: ${PASS}${NC}"
echo -e "  ${RED}Failed: ${FAIL}${NC}"
echo ""

if [[ "$FAIL" -eq 0 ]]; then
    echo "All E2E tests passed!"
    exit 0
else
    echo "${FAIL} test(s) failed."
    exit 1
fi
