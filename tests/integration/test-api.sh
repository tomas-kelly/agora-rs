#!/usr/bin/env bash
set -euo pipefail

# Integration test for Food Preferences API
# Requires: API_URL, USER_POOL_ID, CLIENT_ID, USERNAME, PASSWORD env vars

: "${API_URL:?Set API_URL to the deployed API Gateway endpoint}"
: "${USER_POOL_ID:?Set USER_POOL_ID}"
: "${CLIENT_ID:?Set CLIENT_ID}"
: "${USERNAME:?Set USERNAME (email)}"
: "${PASSWORD:?Set PASSWORD}"

echo "=== Authenticating ==="
AUTH_RESULT=$(aws cognito-idp initiate-auth \
  --auth-flow USER_PASSWORD_AUTH \
  --client-id "$CLIENT_ID" \
  --auth-parameters USERNAME="$USERNAME",PASSWORD="$PASSWORD" \
  --query 'AuthenticationResult.IdToken' --output text)
TOKEN="$AUTH_RESULT"
USER_ID=$(echo "$TOKEN" | cut -d. -f2 | base64 -d 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin)['sub'])")

echo "User ID: $USER_ID"
BASE="${API_URL}users/${USER_ID}/preferences"

echo "=== POST: Create preference ==="
CREATE_RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE" \
  -H "Authorization: $TOKEN" -H "Content-Type: application/json" \
  -d '{"food_name":"sushi","category":"japanese","rating":5,"tags":["seafood"],"notes":"love it"}')
CREATE_CODE=$(echo "$CREATE_RESP" | tail -1)
CREATE_BODY=$(echo "$CREATE_RESP" | sed '$d')
echo "Status: $CREATE_CODE"
[ "$CREATE_CODE" = "201" ] || { echo "FAIL: expected 201"; exit 1; }
echo "$CREATE_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['schemaVersion']=='1.0','missing schemaVersion'"
PREF_ID=$(echo "$CREATE_BODY" | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['preferenceId'])")
echo "Created preferenceId: $PREF_ID"

echo "=== GET: Single preference ==="
GET_SINGLE_RESP=$(curl -s -w "\n%{http_code}" "$BASE/$PREF_ID" -H "Authorization: $TOKEN")
GET_SINGLE_CODE=$(echo "$GET_SINGLE_RESP" | tail -1)
GET_SINGLE_BODY=$(echo "$GET_SINGLE_RESP" | sed '$d')
echo "Status: $GET_SINGLE_CODE"
[ "$GET_SINGLE_CODE" = "200" ] || { echo "FAIL: expected 200"; exit 1; }
echo "$GET_SINGLE_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['schemaVersion']=='1.0';assert d['data']['food_name']=='sushi'"

echo "=== GET: List preferences ==="
GET_LIST_RESP=$(curl -s -w "\n%{http_code}" "$BASE" -H "Authorization: $TOKEN")
GET_LIST_CODE=$(echo "$GET_LIST_RESP" | tail -1)
GET_LIST_BODY=$(echo "$GET_LIST_RESP" | sed '$d')
echo "Status: $GET_LIST_CODE"
[ "$GET_LIST_CODE" = "200" ] || { echo "FAIL: expected 200"; exit 1; }
echo "$GET_LIST_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['schemaVersion']=='1.0'"

echo "=== PUT: Update preference ==="
PUT_RESP=$(curl -s -w "\n%{http_code}" -X PUT "$BASE/$PREF_ID" \
  -H "Authorization: $TOKEN" -H "Content-Type: application/json" \
  -d '{"food_name":"sushi deluxe","rating":4}')
PUT_CODE=$(echo "$PUT_RESP" | tail -1)
PUT_BODY=$(echo "$PUT_RESP" | sed '$d')
echo "Status: $PUT_CODE"
[ "$PUT_CODE" = "200" ] || { echo "FAIL: expected 200"; exit 1; }
echo "$PUT_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['schemaVersion']=='1.0'"

echo "=== DELETE: Remove preference ==="
DEL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE/$PREF_ID" \
  -H "Authorization: $TOKEN")
echo "Status: $DEL_CODE"
[ "$DEL_CODE" = "204" ] || { echo "FAIL: expected 204"; exit 1; }

echo "=== GET after DELETE: Verify 404 ==="
GET_AFTER_DEL_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/$PREF_ID" -H "Authorization: $TOKEN")
echo "Status: $GET_AFTER_DEL_CODE"
[ "$GET_AFTER_DEL_CODE" = "404" ] || { echo "FAIL: expected 404 after delete"; exit 1; }

echo "=== Verify 401 without token ==="
NO_AUTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE")
echo "Status: $NO_AUTH_CODE"
[ "$NO_AUTH_CODE" = "401" ] || { echo "FAIL: expected 401"; exit 1; }

echo "=== Verify 403 with wrong userId ==="
WRONG_BASE="${API_URL}users/wrong-user-id/preferences"
WRONG_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$WRONG_BASE" -H "Authorization: $TOKEN")
echo "Status: $WRONG_CODE"
[ "$WRONG_CODE" = "403" ] || { echo "FAIL: expected 403"; exit 1; }

echo "=== ALL TESTS PASSED ==="
