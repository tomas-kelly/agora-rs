import { describe, it, expect, vi, beforeEach } from 'vitest';
import { APIGatewayProxyEvent } from 'aws-lambda';

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: vi.fn() }) },
  PutCommand: vi.fn(),
  QueryCommand: vi.fn(),
  UpdateCommand: vi.fn(),
  DeleteCommand: vi.fn(),
  GetCommand: vi.fn(),
}));
vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: vi.fn(() => ({})) }));

const mockSend = vi.fn();
vi.mock('../shared/dynamo-client', () => ({
  docClient: { send: (...args: any[]) => mockSend(...args) },
  TABLE_NAME: 'test-table',
}));

function makeEvent(overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  return {
    httpMethod: 'POST',
    resource: '/users/{userId}/preferences',
    pathParameters: { userId: 'user-123' },
    body: null,
    headers: {},
    multiValueHeaders: {},
    isBase64Encoded: false,
    queryStringParameters: null,
    multiValueQueryStringParameters: null,
    stageVariables: null,
    requestContext: {
      authorizer: { claims: { sub: 'user-123' } },
    } as any,
    path: '',
    ...overrides,
  } as APIGatewayProxyEvent;
}

describe('create-preference', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 201 with schemaVersion envelope', async () => {
    mockSend.mockResolvedValue({});
    const { handleCreate } = await import('../create-preference');
    const event = makeEvent({ body: JSON.stringify({ food_name: 'pizza', rating: 4 }) });
    const res = await handleCreate(event);
    expect(res.statusCode).toBe(201);
    const body = JSON.parse(res.body);
    expect(body.schemaVersion).toBe('1.0');
    expect(body.data.food_name).toBe('pizza');
    expect(body.data.preferenceId).toBeDefined();
  });

  it('returns 400 on invalid body', async () => {
    const { handleCreate } = await import('../create-preference');
    const event = makeEvent({ body: JSON.stringify({ food_name: '' }) });
    const res = await handleCreate(event);
    expect(res.statusCode).toBe(400);
  });

  it('returns 403 when sub != userId', async () => {
    const { handleCreate } = await import('../create-preference');
    const event = makeEvent({
      body: JSON.stringify({ food_name: 'tacos' }),
      requestContext: { authorizer: { claims: { sub: 'other-user' } } } as any,
    });
    const res = await handleCreate(event);
    expect(res.statusCode).toBe(403);
  });
});

describe('get-preference (single)', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns item with schemaVersion when found', async () => {
    mockSend.mockResolvedValue({ Item: { userId: 'user-123', preferenceId: 'p1', food_name: 'sushi' } });
    const { handleGetPreference } = await import('../get-preference');
    const event = makeEvent({
      httpMethod: 'GET',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'p1' },
    });
    const res = await handleGetPreference(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.schemaVersion).toBe('1.0');
    expect(body.data.food_name).toBe('sushi');
  });

  it('returns 404 when item not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const { handleGetPreference } = await import('../get-preference');
    const event = makeEvent({
      httpMethod: 'GET',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'p1' },
    });
    const res = await handleGetPreference(event);
    expect(res.statusCode).toBe(404);
  });
});

describe('get-preferences (list)', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 200 with schemaVersion', async () => {
    mockSend.mockResolvedValue({ Items: [{ userId: 'user-123', food_name: 'sushi' }] });
    const { handleGetPreferences } = await import('../get-preferences');
    const event = makeEvent({ httpMethod: 'GET', body: null });
    const res = await handleGetPreferences(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.schemaVersion).toBe('1.0');
    expect(body.data.items).toHaveLength(1);
  });
});

describe('update-preference', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 200 with schemaVersion', async () => {
    mockSend.mockResolvedValue({ Attributes: { userId: 'user-123', food_name: 'ramen' } });
    const { handleUpdate } = await import('../update-preference');
    const event = makeEvent({
      httpMethod: 'PUT',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'pref-1' },
      body: JSON.stringify({ food_name: 'ramen' }),
    });
    const res = await handleUpdate(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.schemaVersion).toBe('1.0');
    expect(body.data.food_name).toBe('ramen');
  });

  it('returns 403 when sub != userId', async () => {
    const { handleUpdate } = await import('../update-preference');
    const event = makeEvent({
      httpMethod: 'PUT',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'pref-1' },
      body: JSON.stringify({ food_name: 'ramen' }),
      requestContext: { authorizer: { claims: { sub: 'other-user' } } } as any,
    });
    const res = await handleUpdate(event);
    expect(res.statusCode).toBe(403);
  });
});

describe('delete-preference', () => {
  beforeEach(() => { mockSend.mockReset(); });

  it('returns 204 on success', async () => {
    mockSend.mockResolvedValue({});
    const { handleDelete } = await import('../delete-preference');
    const event = makeEvent({
      httpMethod: 'DELETE',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'pref-1' },
    });
    const res = await handleDelete(event);
    expect(res.statusCode).toBe(204);
  });

  it('returns 404 when item not found', async () => {
    const err = new Error('condition') as any;
    err.name = 'ConditionalCheckFailedException';
    mockSend.mockRejectedValue(err);
    const { handleDelete } = await import('../delete-preference');
    const event = makeEvent({
      httpMethod: 'DELETE',
      resource: '/users/{userId}/preferences/{preferenceId}',
      pathParameters: { userId: 'user-123', preferenceId: 'pref-1' },
    });
    const res = await handleDelete(event);
    expect(res.statusCode).toBe(404);
  });
});

describe('index router', () => {
  it('returns 405 for unsupported method', async () => {
    const { handler } = await import('../index');
    const event = makeEvent({ httpMethod: 'PATCH', resource: '/users/{userId}/preferences' });
    const res = await handler(event);
    expect(res.statusCode).toBe(405);
  });
});
