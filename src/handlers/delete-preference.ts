import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DeleteCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractAndValidateUserId } from './shared/auth';
import { errorResponse, formatErrorResponse } from './shared/errors';

export async function handleDelete(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = event.pathParameters?.userId;
    const preferenceId = event.pathParameters?.preferenceId;
    if (!userId || !preferenceId) throw errorResponse(400, 'Bad Request', 'Missing path parameters.');
    extractAndValidateUserId(event, userId);

    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { userId, preferenceId },
      ConditionExpression: 'attribute_exists(userId) AND attribute_exists(preferenceId)',
    }));

    return { statusCode: 204, headers: {}, body: '' };
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      return formatErrorResponse(errorResponse(404, 'Not Found', 'Preference not found.'));
    }
    return formatErrorResponse(err);
  }
}
