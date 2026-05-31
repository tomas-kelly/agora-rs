import { APIGatewayProxyEvent } from 'aws-lambda';
import { errorResponse } from './errors';

export function extractAndValidateUserId(event: APIGatewayProxyEvent, pathUserId: string): string {
  const sub = event.requestContext.authorizer?.claims?.sub;
  if (!sub || sub !== pathUserId) {
    throw errorResponse(403, 'Forbidden', 'User ID in path does not match authenticated user.');
  }
  return sub;
}
