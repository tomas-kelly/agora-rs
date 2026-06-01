import { describe, it, expect, vi, beforeEach } from 'vitest';
import { APIGatewayProxyEvent } from 'aws-lambda';

const mockSend = vi.fn();
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: vi.fn() }) },
  GetCommand: vi.fn(),
}));
vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: vi.fn(() => ({})) }));
vi.mock('../shared/dynamo-client', () => ({
  docClient: { send: (...args: any[]) => mockSend(...args) },
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
    requestContext: { authorizer: { claims: { sub: 'user-123' } } } as any,
    path: '',
    ...overrides,
  } as APIGatewayProxyEvent;
}

describe('get-favorite-meal', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 200 with value when found', async () => {
    mockSend.mockResolvedValue({ Item: { userId: 'user-123', preferenceKey: 'favorite-meal', value: 'pizza', updatedAt: '2026-01-01T00:00:00.000Z' } });
    const { handleGetFavoriteMeal } = await import('../get-favorite-meal');
    const event = makeEvent();
    const res = await handleGetFavoriteMeal(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.data.value).toBe('pizza');
    expect(body.data.key).toBe('favorite-meal');
    expect(body.data.userId).toBe('user-123');
  });

  it('returns 404 when not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const { handleGetFavoriteMeal } = await import('../get-favorite-meal');
    const event = makeEvent();
    const res = await handleGetFavoriteMeal(event);
    expect(res.statusCode).toBe(404);
    const body = JSON.parse(res.body);
    expect(body.title).toBe('Not Found');
  });

  it('returns 401 when no authorizer claims present', async () => {
    const { handleGetFavoriteMeal } = await import('../get-favorite-meal');
    const event = makeEvent({ requestContext: {} as any });
    const res = await handleGetFavoriteMeal(event);
    expect(res.statusCode).toBe(401);
  });

  it('returns 403 when sub does not match path userId', async () => {
    const { handleGetFavoriteMeal } = await import('../get-favorite-meal');
    const event = makeEvent({ requestContext: { authorizer: { claims: { sub: 'attacker' } } } as any });
    const res = await handleGetFavoriteMeal(event);
    expect(res.statusCode).toBe(403);
  });
});
