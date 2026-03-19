export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

const DEMO_KEY = process.env.NEXT_PUBLIC_DEMO_WRITE_KEY || "";

type FetchJSONOptions = RequestInit & {
  write?: boolean;
};

type APIErrorResponse = {
  error?: {
    code?: string;
    message?: string;
    details?: unknown;
  };
};

export async function fetchJSON<T>(
  path: string,
  opts: FetchJSONOptions = {}
): Promise<T> {
  const { write = false, headers, ...rest } = opts;

  const finalHeaders = new Headers(headers);

  if (rest.body !== undefined && rest.body !== null) {
    if (!finalHeaders.has("Content-Type")) {
      finalHeaders.set("Content-Type", "application/json");
    }
  }

  if (write && DEMO_KEY) {
    finalHeaders.set("X-Demo-Key", DEMO_KEY);
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...rest,
    headers: finalHeaders,
    cache: "no-store",
  });

  const contentType = res.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!res.ok) {
    let message = `Request failed (${res.status})`;

    if (isJson) {
      const data = (await res.json().catch(() => null)) as APIErrorResponse | null;
      if (data?.error?.message) {
        message = data.error.message;
      }
    }

    throw new Error(message);
  }

  if (!isJson) {
    return null as T;
  }

  return (await res.json()) as T;
}