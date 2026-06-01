import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { handleSetFavoriteMeal } from './set-favorite-meal';
import { handleGetFavoriteMeal } from './get-favorite-meal';
import { formatErrorResponse, errorResponse } from './shared/errors';

export async function handler(event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> {
  const method = event.httpMethod;
  const resource = event.resource;

  if (resource === '/users/{userId}/preferences/favorite-meal') {
    if (method === 'PUT') return handleSetFavoriteMeal(event);
    if (method === 'GET') return handleGetFavoriteMeal(event);
  }

  return formatErrorResponse(errorResponse(405, 'Method Not Allowed', `${method} ${resource} is not supported.`));
}
