import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, TABLE_NAME } from './shared/dynamo-client';
import { extractAndValidateUserId } from './shared/auth';
import { PreferenceInputSchema } from './shared/schema';
import { errorResponse, formatErrorResponse } from './shared/errors';
import { formatSuccessResponse } from './shared/response';

export async function handleUpdate(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  try {
    const userId = event.pathParameters?.userId;
    const preferenceId = event.pathParameters?.preferenceId;
    if (!userId || !preferenceId) throw errorResponse(400, 'Bad Request', 'Missing path parameters.');
    extractAndValidateUserId(event, userId);

    const body = JSON.parse(event.body || '{}');
    const parsed = PreferenceInputSchema.safeParse(body);
    if (!parsed.success) {
      throw errorResponse(400, 'Validation Error', parsed.error.issues.map(i => i.message).join('; '));
    }

    const attrs: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    const parts: string[] = [];
    const data = parsed.data;

    parts.push('food_name = :fn'); values[':fn'] = data.food_name; attrs['#ua'] = 'updatedAt';
    if (data.category !== undefined) { parts.push('category = :cat'); values[':cat'] = data.category; }
    if (data.rating !== undefined) { parts.push('rating = :r'); values[':r'] = data.rating; }
    if (data.tags !== undefined) { parts.push('tags = :t'); values[':t'] = data.tags; }
    if (data.notes !== undefined) { parts.push('notes = :n'); values[':n'] = data.notes; }
    parts.push('#ua = :ua'); values[':ua'] = new Date().toISOString();

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { userId, preferenceId },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: attrs,
      ExpressionAttributeValues: values,
      ConditionExpression: 'attribute_exists(userId) AND attribute_exists(preferenceId)',
      ReturnValues: 'ALL_NEW',
    }));

    return formatSuccessResponse(200, result.Attributes);
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      return formatErrorResponse(errorResponse(404, 'Not Found', 'Preference not found.'));
    }
    return formatErrorResponse(err);
  }
}
