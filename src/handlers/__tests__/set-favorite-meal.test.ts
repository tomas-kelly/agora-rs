import { describe, it, expect, vi, beforeEach } from 'vitest';
import { APIGatewayProxyEvent } from 'aws-lambda';

const mockSend = vi.fn();
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: vi.fn() }) },
  PutCommand: vi.fn(),
}));
vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: vi.fn(() => ({})) }));
vi.mock('../shared/dynamo-client', () => ({
  docClient: { send: (...args: any[]) => mockSend(...args) },
  TABLE_NAME: 'test-table',
}));

function makeEvent(overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  return {
    httpMethod: 'PUT',
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

describe('set-favorite-meal', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 200 with stored item on valid input', async () => {
    mockSend.mockResolvedValue({});
    const { handleSetFavoriteMeal } = await import('../set-favorite-meal');
    const event = makeEvent({ body: JSON.stringify({ value: 'sushi' }) });
    const res = await handleSetFavoriteMeal(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.data.value).toBe('sushi');
    expect(body.data.key).toBe('favorite-meal');
    expect(body.data.userId).toBe('user-123');
    expect(body.data.updatedAt).toBeDefined();
  });

  it('returns 400 on empty value', async () => {
    const { handleSetFavoriteMeal } = await import('../set-favorite-meal');
    const event = makeEvent({ body: JSON.stringify({ value: '' }) });
    const res = await handleSetFavoriteMeal(event);
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 on missing body', async () => {
    const { handleSetFavoriteMeal } = await import('../set-favorite-meal');
    const event = makeEvent({ body: null });
    const res = await handleSetFavoriteMeal(event);
    expect(res.statusCode).toBe(400);
  });

  it('returns 401 when no authorizer claims present', async () => {
    const { handleSetFavoriteMeal } = await import('../set-favorite-meal');
    const event = makeEvent({ body: JSON.stringify({ value: 'tacos' }), requestContext: {} as any });
    const res = await handleSetFavoriteMeal(event);
    expect(res.statusCode).toBe(401);
  });

  it('returns 403 when sub does not match path userId', async () => {
    const { handleSetFavoriteMeal } = await import('../set-favorite-meal');
    const event = makeEvent({
      body: JSON.stringify({ value: 'tacos' }),
      requestContext: { authorizer: { claims: { sub: 'other-user' } } } as any,
    });
    const res = await handleSetFavoriteMeal(event);
    expect(res.statusCode).toBe(403);
  });
});
