import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractUserId } from './shared/auth';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleGetFavoriteMeal(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = extractUserId(event);

    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { userId, preferenceKey: 'favorite-meal' },
    }));

    if (!result.Item) {
      throw errorResponse(404, 'Not Found', 'Favorite meal not set for this user.');
    }

    return formatSuccessResponse(200, { userId, key: 'favorite-meal', value: result.Item.value, updatedAt: result.Item.updatedAt });
  } catch (err) {
    return formatErrorResponse(err);
  }
}
