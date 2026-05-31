import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { QueryCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractAndValidateUserId } from './shared/auth';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleGetPreferences(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = event.pathParameters?.userId;
    if (!userId) throw errorResponse(400, 'Bad Request', 'Missing userId path parameter.');
    extractAndValidateUserId(event, userId);

    const limit = Math.min(Number(event.queryStringParameters?.limit) || 100, 100);
    const exclusiveStartKey = event.queryStringParameters?.nextToken
      ? JSON.parse(Buffer.from(event.queryStringParameters.nextToken, 'base64url').toString())
      : undefined;

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: 'userId = :uid',
      ExpressionAttributeValues: { ':uid': userId },
      Limit: limit,
      ExclusiveStartKey: exclusiveStartKey,
    }));

    const response: Record<string, unknown> = { items: result.Items || [] };
    if (result.LastEvaluatedKey) {
      response.nextToken = Buffer.from(JSON.stringify(result.LastEvaluatedKey)).toString('base64url');
    }

    return formatSuccessResponse(200, response);
  } catch (err) {
    return formatErrorResponse(err);
  }
}
