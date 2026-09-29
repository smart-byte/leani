export interface LeaniErrorBody {
  error: {
    code: string;
    message: string;
    retryable: boolean;
    details?: Record<string, unknown> | null;
    requestId: string;
  };
}

export class LeaniError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryable: boolean;
  readonly details?: Record<string, unknown> | null;
  readonly requestId?: string;

  constructor(
    message: string,
    options: {
      status: number;
      code: string;
      retryable: boolean;
      details?: Record<string, unknown> | null;
      requestId?: string;
      cause?: unknown;
    },
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "LeaniError";
    this.status = options.status;
    this.code = options.code;
    this.retryable = options.retryable;
    this.details = options.details;
    this.requestId = options.requestId;
  }
}

/**
 * A request that failed below HTTP: the connection was refused or reset, the
 * deadline passed, the response body broke off, or a stream went silent. It
 * has no HTTP status (`status` is 0), is always retryable, and keeps the
 * underlying failure as `cause`.
 */
export class TransportError extends LeaniError {
  constructor(message: string, cause?: unknown) {
    super(message, { status: 0, code: "transport", retryable: true, cause });
    this.name = "TransportError";
  }
}

export async function responseError(response: Response): Promise<LeaniError> {
  let body: LeaniErrorBody | undefined;
  try {
    body = (await response.json()) as LeaniErrorBody;
  } catch {
    // Fall through to the status-derived error without exposing response text.
  }
  return errorFromBody(response.status, body);
}

export function errorFromBody(status: number, body: unknown): LeaniError {
  const error = (body as Partial<LeaniErrorBody> | null)?.error;
  return new LeaniError(
    typeof error?.message === "string" ? error.message : `Leani returned HTTP ${status}`,
    {
      status,
      code: typeof error?.code === "string" ? error.code : "internal",
      retryable: typeof error?.retryable === "boolean"
        ? error.retryable
        : status === 408 || status === 429 || status >= 500,
      details: error?.details && typeof error.details === "object" ? error.details : undefined,
      requestId: typeof error?.requestId === "string" ? error.requestId : undefined,
    },
  );
}
