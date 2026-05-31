import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractAndValidateUserId } from './shared/auth';
import { PreferenceInputSchema } from './shared/schema';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleCreate(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = event.pathParameters?.userId;
    if (!userId) throw errorResponse(400, 'Bad Request', 'Missing userId path parameter.');
    extractAndValidateUserId(event, userId);

    const body = JSON.parse(event.body || '{}');
    const parsed = PreferenceInputSchema.safeParse(body);
    if (!parsed.success) {
      throw errorResponse(400, 'Validation Error', parsed.error.issues.map(i => i.message).join('; '));
    }

    const preferenceId = ulid();
    const item = { userId, preferenceId, ...parsed.data, createdAt: new Date().toISOString() };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));

    return formatSuccessResponse(201, item);
  } catch (err) {
    return formatErrorResponse(err);
  }
}
