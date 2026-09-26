import { LeaniError, TransportError } from "./errors.ts";
import type { FetchLike } from "./index.ts";

/**
 * Resolve `path` under `base`, including its path: a leading slash, as in a
 * capabilities `basePath`, stays under a prefixed base URL. A proxy may decode
 * a segment before it resolves dot segments, so a segment that decodes to `.`
 * or `..`, or contains a separator, raw or percent-encoded, or an encoded
 * percent sign, is refused before any request. So are control characters,
 * which URL parsing drops. No Leani ID contains any of them.
 */
export function resolveUnder(base: URL, path: string): URL {
  if (/^[a-z][a-z0-9+.-]*:/i.test(path) || path.startsWith("//")) {
    throw new TypeError("request path must not contain an origin or URL scheme");
  }
  if (/[\u0000-\u001f\u007f]/.test(path)) {
    throw new TypeError("request path must not contain control characters");
  }
  const relative = path.replace(/^\/+/, "");
  const pathname = relative.split(/[?#]/, 1)[0]!;
  for (const segment of pathname.split("/")) {
    if (/\\|%2f|%5c|%25/i.test(segment)) {
      throw new TypeError(
        "request path must not contain a backslash, an encoded slash or backslash, or an encoded percent sign",
      );
    }
    const decoded = decodeSegment(segment);
    if (decoded === "." || decoded === "..") {
      throw new TypeError(
        'request path must not contain a "." or ".." segment, such as an ID of "." or ".."',
      );
    }
  }
  const url = new URL(relative, base);
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) {
    throw new TypeError("request path must resolve under the configured Leani base URL");
  }
  return url;
}

function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    throw new TypeError("request path contains an invalid percent-encoding");
  }
}

/** The caller's signal, if any, bounded by a request deadline. */
export function withDeadline(
  signal: AbortSignal | undefined,
  timeoutMs: number,
): AbortSignal {
  const deadline = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, deadline]) : deadline;
}

/**
 * Send one request without following redirects. A failure below HTTP is a
 * retryable TransportError unless `caller` aborted it. A redirect is a
 * non-retryable LeaniError: its target is not the configured node, and
 * following it could hand the node's credentials to another origin.
 */
export async function send(
  fetchImpl: FetchLike,
  url: URL,
  init: RequestInit,
  caller: AbortSignal | undefined,
): Promise<Response> {
  let response: Response;
  try {
    response = await fetchImpl(url, { ...init, redirect: "manual" });
  } catch (error) {
    throw transportFailure(error, caller, "Leani request failed");
  }
  // A custom fetch may follow the redirect anyway; its response then says so.
  if (
    response.redirected ||
    response.type === "opaqueredirect" ||
    (response.status >= 300 && response.status < 400)
  ) {
    void response.body?.cancel().catch(() => undefined);
    const location =
      response.headers.get("location") ??
      (response.redirected && response.url ? response.url : null);
    throw new LeaniError(
      `Leani answered with a redirect${location ? ` to ${location}` : ""}, which the SDK does not follow; configure the final URL`,
      {
        status: response.status,
        code: "redirect_refused",
        retryable: false,
        details: location ? { location } : undefined,
      },
    );
  }
  return response;
}

/** Read a response body as text; a body that breaks off is a TransportError. */
export async function readText(
  response: Response,
  caller: AbortSignal | undefined,
): Promise<string> {
  try {
    return await response.text();
  } catch (error) {
    throw transportFailure(error, caller, "Leani response broke off");
  }
}

/** Read a JSON response body. Malformed JSON is not a transport failure. */
export async function readJson<T>(
  response: Response,
  caller: AbortSignal | undefined,
): Promise<T> {
  return JSON.parse(await readText(response, caller)) as T;
}

/**
 * Yield a streaming body's chunks until it ends or `signal` aborts, calling
 * `onChunk` for each. A read that fails otherwise is a TransportError.
 */
export async function* readChunks(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal | undefined,
  onChunk?: () => void,
): AsyncGenerator<Uint8Array> {
  const reader = body.getReader();
  const cancel = () => {
    void reader.cancel().catch(() => undefined);
  };
  signal?.addEventListener("abort", cancel, { once: true });
  try {
    while (!signal?.aborted) {
      const result = await reader.read().catch((error: unknown) => {
        if (signal?.aborted) {
          return { done: true as const, value: undefined };
        }
        throw new TransportError(`Leani stream broke off: ${describe(error)}`, error);
      });
      if (result.done || signal?.aborted) {
        return;
      }
      onChunk?.();
      yield result.value;
    }
  } finally {
    signal?.removeEventListener("abort", cancel);
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export interface IdleWatchdog {
  /** Start or restart the countdown, as each received chunk does. */
  arm(): void;
  /** Pause while nothing is being read, such as while a record is handled. */
  disarm(): void;
}

/** Longer timer delays overflow and fire at once. */
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1;

/** Call `onIdle` once a stream stays silent for `timeoutMs` while armed. */
export function idleWatchdog(timeoutMs: number, onIdle: () => void): IdleWatchdog {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const disarm = () => {
    clearTimeout(timer);
    timer = undefined;
  };
  return {
    arm: () => {
      disarm();
      timer = setTimeout(onIdle, Math.min(timeoutMs, MAX_TIMER_DELAY_MS));
    },
    disarm,
  };
}

/** `parts`, `length` bytes in all, followed by `tail`, as one array. */
export function concatBytes(
  parts: Uint8Array[],
  length: number,
  tail: Uint8Array,
): Uint8Array {
  if (parts.length === 0) {
    return tail;
  }
  const joined = new Uint8Array(length + tail.length);
  let offset = 0;
  for (const part of [...parts, tail]) {
    joined.set(part, offset);
    offset += part.length;
  }
  return joined;
}

/** A caller's own abort as it is, and any other failure as a TransportError. */
function transportFailure(
  error: unknown,
  caller: AbortSignal | undefined,
  message: string,
): unknown {
  if (caller?.aborted || error instanceof LeaniError) {
    return error;
  }
  return new TransportError(`${message}: ${describe(error)}`, error);
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
