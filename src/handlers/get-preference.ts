import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractAndValidateUserId } from './shared/auth';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleGetPreference(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = event.pathParameters?.userId;
    const preferenceId = event.pathParameters?.preferenceId;
    if (!userId || !preferenceId) throw errorResponse(400, 'Bad Request', 'Missing path parameters.');
    extractAndValidateUserId(event, userId);

    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { userId, preferenceId },
    }));

    if (!result.Item) {
      throw errorResponse(404, 'Not Found', 'Preference not found.');
    }

    return formatSuccessResponse(200, result.Item);
  } catch (err) {
    return formatErrorResponse(err);
  }
}
