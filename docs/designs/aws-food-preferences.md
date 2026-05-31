# AWS Food Preferences Service — Architecture & Deployment

> **⚠️ THIS IS A HYPOTHETICAL DESIGN EXERCISE.**
> No real AWS infrastructure is created, deployed, or referenced.
> All account IDs, ARNs, and endpoints are fictional placeholders.

## Overview

A serverless CRUD service for managing user food preferences, built on AWS
managed services: API Gateway + Lambda + DynamoDB with Cognito auth, deployed
via AWS CDK.

## Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Compute | Lambda (Node 20, 256MB, 10s timeout) | Serverless, no idle cost, scales to zero |
| Storage | DynamoDB (on-demand, PITR enabled) | Single-digit-ms latency, no capacity planning |
| Auth | Cognito User Pool + JWT authorizer | Managed auth, no custom token validation in Lambda |
| IaC | AWS CDK (TypeScript) | Type-safe, single-command deploy |
| Routing | Single Lambda, event-based dispatch | Fewer cold starts, simpler deployment |
| Validation | Zod schemas | Runtime type safety, clear error messages |
| IDs | ULID | Sortable, unique, no coordination needed |

## Data Model

Table: `FoodPreferences` (on-demand billing, point-in-time recovery)

| Attribute | Type | Key |
|-----------|------|-----|
| userId | String | Partition key |
| preferenceId | String (ULID) | Sort key |
| food_name | String (1-200 chars) | — |
| category | String (optional) | — |
| rating | Number 1-5 (optional) | — |
| tags | String[] max 10 (optional) | — |
| notes | String max 1000 (optional) | — |
| createdAt | String (ISO 8601) | — |
| updatedAt | String (ISO 8601) | — |

## API Surface

| Method | Path | Description | Status |
|--------|------|-------------|--------|
| POST | /users/{userId}/preferences | Create preference | 201 |
| GET | /users/{userId}/preferences | List user preferences | 200 |
| PUT | /users/{userId}/preferences/{preferenceId} | Update preference | 200 |
| DELETE | /users/{userId}/preferences/{preferenceId} | Delete preference | 204 |

All endpoints require Cognito JWT. userId in path must match token `sub` claim.

## Security

- Cognito JWT authorizer at API Gateway — unauthenticated traffic never reaches Lambda
- userId path validation against token sub claim in Lambda
- DynamoDB partition key isolation (userId) — storage-layer tenant separation
- No `dynamodb:Scan` permission granted
- Zod input validation before any DB write
- HTTPS only (API Gateway does not expose HTTP)
- Rate limiting: 1000 req/s account-level, per-user 100 req/s burst 200

## Observability

- Lambda: structured JSON logs with requestId, userId, operation, duration
- X-Ray tracing on API Gateway and Lambda
- CloudWatch alarm: Lambda errors > 1% over 5 minutes
- CloudWatch alarm: API Gateway p95 latency > 200ms over 5 minutes
- DynamoDB consumed capacity metrics auto-published

## Deployment

### Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js 20+
- CDK CLI (`npm install -g aws-cdk`)

### Deploy

```bash
cd infra/cdk
npm install
cd ../../src && npm install && cd ../infra/cdk
npx cdk deploy --context env=dev
```

### Destroy

```bash
cd infra/cdk
npx cdk destroy --context env=dev
```

Note: DynamoDB table has `RETAIN` removal policy. Manual deletion required after stack destroy.

### Integration Tests

```bash
export API_URL=<stack output ApiUrl>
export USER_POOL_ID=<stack output UserPoolId>
export CLIENT_ID=<stack output UserPoolClientId>
export USERNAME=<test user email>
export PASSWORD=<test user password>
bash tests/integration/test-api.sh
```

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Cold start > 200ms p95 | Monitor; add provisioned concurrency if needed |
| Cost at scale (>25M req/month) | Switch to provisioned capacity with auto-scaling |
| Free-text food_name | Add taxonomy/controlled vocabulary later if needed |

---

*This document is a design exercise. No AWS resources exist or will be created.*
