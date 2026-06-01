#!/usr/bin/env bash
set -euo pipefail

# Integration test for Food Preferences API (with Cognito auth)
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
BASE="${API_URL}users/${USER_ID}/preferences/favorite-meal"

echo "=== PUT: Set favorite meal ==="
PUT_RESP=$(curl -s -w "\n%{http_code}" -X PUT "$BASE" \
  -H "Authorization: $TOKEN" -H "Content-Type: application/json" \
  -d '{"value":"sushi"}')
PUT_CODE=$(echo "$PUT_RESP" | tail -1)
PUT_BODY=$(echo "$PUT_RESP" | sed '$d')
echo "Status: $PUT_CODE"
[ "$PUT_CODE" = "200" ] || { echo "FAIL: expected 200, got $PUT_CODE"; exit 1; }
echo "$PUT_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['data']['value']=='sushi','wrong value'"

echo "=== GET: Retrieve favorite meal ==="
GET_RESP=$(curl -s -w "\n%{http_code}" "$BASE" -H "Authorization: $TOKEN")
GET_CODE=$(echo "$GET_RESP" | tail -1)
GET_BODY=$(echo "$GET_RESP" | sed '$d')
echo "Status: $GET_CODE"
[ "$GET_CODE" = "200" ] || { echo "FAIL: expected 200, got $GET_CODE"; exit 1; }
echo "$GET_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['data']['value']=='sushi','wrong value'"

echo "=== GET: 401 without token ==="
NO_AUTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE")
echo "Status: $NO_AUTH_CODE"
[ "$NO_AUTH_CODE" = "401" ] || { echo "FAIL: expected 401, got $NO_AUTH_CODE"; exit 1; }

echo "=== PUT: Overwrite favorite meal ==="
PUT2_RESP=$(curl -s -w "\n%{http_code}" -X PUT "$BASE" \
  -H "Authorization: $TOKEN" -H "Content-Type: application/json" \
  -d '{"value":"ramen"}')
PUT2_CODE=$(echo "$PUT2_RESP" | tail -1)
echo "Status: $PUT2_CODE"
[ "$PUT2_CODE" = "200" ] || { echo "FAIL: expected 200, got $PUT2_CODE"; exit 1; }

echo "=== GET: Verify overwrite ==="
GET2_RESP=$(curl -s -w "\n%{http_code}" "$BASE" -H "Authorization: $TOKEN")
GET2_CODE=$(echo "$GET2_RESP" | tail -1)
GET2_BODY=$(echo "$GET2_RESP" | sed '$d')
echo "Status: $GET2_CODE"
[ "$GET2_CODE" = "200" ] || { echo "FAIL: expected 200, got $GET2_CODE"; exit 1; }
echo "$GET2_BODY" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['data']['value']=='ramen','overwrite failed'"

echo "=== GET: 403 for wrong userId ==="
WRONG_BASE="${API_URL}users/wrong-user-id/preferences/favorite-meal"
WRONG_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: $TOKEN" "$WRONG_BASE")
echo "Status: $WRONG_CODE"
[ "$WRONG_CODE" = "403" ] || { echo "FAIL: expected 403, got $WRONG_CODE"; exit 1; }

echo "=== ALL TESTS PASSED ==="
