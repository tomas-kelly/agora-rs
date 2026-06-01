import { describe, it, expect, vi } from 'vitest';
import { APIGatewayProxyEvent } from 'aws-lambda';

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: vi.fn() }) },
  PutCommand: vi.fn(),
  GetCommand: vi.fn(),
}));
vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: vi.fn(() => ({})) }));
vi.mock('../shared/dynamo-client', () => ({
  docClient: { send: vi.fn() },
  TABLE_NAME: 'test-table',
}));

function makeEvent(overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  return {
    httpMethod: 'GET',
    resource: '/users/{userId}/preferences/favorite-meal',
    pathParameters: { userId: 'user-123' },
    body: null,
    headers: {},
    multiValueHeaders: {},
    isBase64Encoded: false,
    queryStringParameters: null,
    multiValueQueryStringParameters: null,
    stageVariables: null,
    requestContext: {} as any,
    path: '',
    ...overrides,
  } as APIGatewayProxyEvent;
}

describe('index router', () => {
  it('returns 405 for unsupported method', async () => {
    const { handler } = await import('../index');
    const event = makeEvent({ httpMethod: 'DELETE', resource: '/users/{userId}/preferences/favorite-meal' });
    const res = await handler(event);
    expect(res.statusCode).toBe(405);
  });

  it('returns 405 for unknown resource', async () => {
    const { handler } = await import('../index');
    const event = makeEvent({ httpMethod: 'GET', resource: '/unknown' });
    const res = await handler(event);
    expect(res.statusCode).toBe(405);
  });
});
