import { APIGatewayProxyResult } from 'aws-lambda';

export function formatSuccessResponse(statusCode: number, data: unknown): APIGatewayProxyResult {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ schemaVersion: '1.0', data }),
  };
}
