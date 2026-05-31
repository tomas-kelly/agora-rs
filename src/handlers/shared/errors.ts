export interface RFC7807Error {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export function errorResponse(status: number, title: string, detail: string, instance?: string): RFC7807Error & Error {
  const err = new Error(detail) as Error & RFC7807Error;
  err.type = `https://api.foodpreferences.example/errors/${title.toLowerCase().replace(/\s+/g, '-')}`;
  err.title = title;
  err.status = status;
  err.detail = detail;
  err.instance = instance;
  return err;
}

export function formatErrorResponse(err: unknown) {
  if (err && typeof err === 'object' && 'status' in err) {
    const e = err as RFC7807Error;
    return {
      statusCode: e.status,
      headers: { 'Content-Type': 'application/problem+json' },
      body: JSON.stringify({ type: e.type, title: e.title, status: e.status, detail: e.detail, instance: e.instance }),
    };
  }
  return {
    statusCode: 500,
    headers: { 'Content-Type': 'application/problem+json' },
    body: JSON.stringify({ type: 'about:blank', title: 'Internal Server Error', status: 500, detail: 'An unexpected error occurred.' }),
  };
}
