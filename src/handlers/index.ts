import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { handleCreate } from './create-preference';
import { handleGetPreferences } from './get-preferences';
import { handleGetPreference } from './get-preference';
import { handleUpdate } from './update-preference';
import { handleDelete } from './delete-preference';
import { formatErrorResponse, errorResponse } from './shared/errors';

export async function handler(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  const method = event.httpMethod;
  const resource = event.resource;

  if (resource === '/users/{userId}/preferences') {
    if (method === 'POST') return handleCreate(event);
    if (method === 'GET') return handleGetPreferences(event);
  }
  if (resource === '/users/{userId}/preferences/{preferenceId}') {
    if (method === 'GET') return handleGetPreference(event);
    if (method === 'PUT') return handleUpdate(event);
    if (method === 'DELETE') return handleDelete(event);
  }

  return formatErrorResponse(errorResponse(405, 'Method Not Allowed', `${method} ${resource} is not supported.`));
}
