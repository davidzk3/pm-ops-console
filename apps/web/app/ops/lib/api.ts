export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

const DEMO_KEY = process.env.NEXT_PUBLIC_DEMO_WRITE_KEY || "";

type FetchJSONOptions = RequestInit & {
  write?: boolean;
};

export async function fetchJSON<T>(
  path: string,
  opts: FetchJSONOptions = {}
): Promise<T> {
  const { write, headers, ...rest } = opts;

  const finalHeaders: Record<string, string> = {
    ...(headers as Record<string, string> | undefined),
  };

  // only set content-type when we actually send a body
  if (rest.body !== undefined && rest.body !== null) {
    finalHeaders["Content-Type"] = "application/json";
  }

  if (write && DEMO_KEY) {
    // must match your FastAPI dependency name
    finalHeaders["X-Demo-Key"] = DEMO_KEY;
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...rest,
    headers: finalHeaders,
    cache: "no-store",
  });

  const contentType = res.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!res.ok) {
    const data = isJson ? await res.json().catch(() => null) : null;
    throw new Error(data?.error?.message || `Request failed (${res.status})`);
  }

  return (isJson ? await res.json() : (null as any)) as T;
}
