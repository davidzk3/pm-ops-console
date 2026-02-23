export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

const DEMO_KEY = process.env.NEXT_PUBLIC_DEMO_WRITE_KEY || "";

type FetchJSONOptions = RequestInit & {
  write?: boolean; // true for POST/PUT/PATCH/DELETE that require the key
};

export async function fetchJSON<T>(
  path: string,
  opts: FetchJSONOptions = {}
): Promise<T> {
  const { write, headers, ...rest } = opts;

  const finalHeaders: Record<string, string> = {
    ...(headers as Record<string, string> | undefined),
  };

  // Only set content-type if we're sending a body
  if (rest.body !== undefined && !finalHeaders["Content-Type"]) {
    finalHeaders["Content-Type"] = "application/json";
  }

  if (write && DEMO_KEY) {
    finalHeaders["X-Demo-Key"] = DEMO_KEY;
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...rest,
    headers: finalHeaders,
    cache: "no-store",
  });

  if (!res.ok) {
    const data = await res.json().catch(() => null);
    throw new Error(data?.error?.message || `Request failed (${res.status})`);
  }

  return res.json();
}
