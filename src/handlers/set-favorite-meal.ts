import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractUserId } from './shared/auth';
import { FavoriteMealSchema } from './shared/schema';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleSetFavoriteMeal(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = extractUserId(event);

    if (!event.body) throw errorResponse(400, 'Bad Request', 'Missing request body.');
    const body = JSON.parse(event.body);
    const parsed = FavoriteMealSchema.safeParse(body);
    if (!parsed.success) {
      throw errorResponse(400, 'Validation Error', parsed.error.issues.map(i => i.message).join('; '));
    }

    const updatedAt = new Date().toISOString();
    const item = { userId, preferenceKey: 'favorite-meal', value: parsed.data.value, updatedAt };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));

    return formatSuccessResponse(200, { userId, key: 'favorite-meal', value: parsed.data.value, updatedAt });
  } catch (err) {
    return formatErrorResponse(err);
  }
}
