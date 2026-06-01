import { APIGatewayProxyEvent } from 'aws-lambda';
import { errorResponse } from './errors';

export function extractUserId(event: APIGatewayProxyEvent): string {
  const pathUserId = event.pathParameters?.userId;
  if (!pathUserId) throw errorResponse(400, 'Bad Request', 'Missing userId path parameter.');

  const sub = event.requestContext.authorizer?.claims?.sub;
  if (!sub) throw errorResponse(401, 'Unauthorized', 'Missing authentication token.');
  if (sub !== pathUserId) throw errorResponse(403, 'Forbidden', 'Authenticated user does not match path userId.');

  return sub;
}
