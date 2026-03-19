export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

// Read tokens from env (browser visible)
const DEMO_BEARER =
  process.env.NEXT_PUBLIC_OPS_BEARER_TOKEN || "";

const DEMO_KEY =
  process.env.NEXT_PUBLIC_DEMO_WRITE_KEY || "";

type FetchJSONOptions = RequestInit & {
  write?: boolean; // true for POST/PUT/PATCH/DELETE that require auth
};

export async function fetchJSON<T>(
  path: string,
  opts: FetchJSONOptions = {}
): Promise<T> {
  const { write, headers, ...rest } = opts;

  const finalHeaders: Record<string, string> = {
    ...(headers as Record<string, string> | undefined),
  };

  // Set JSON content type automatically
  if (rest.body !== undefined && !finalHeaders["Content-Type"]) {
    finalHeaders["Content-Type"] = "application/json";
  }

  // Attach auth header for write requests
  if (write) {
    if (DEMO_BEARER) {
      finalHeaders["Authorization"] = `Bearer ${DEMO_BEARER}`;
    } else if (DEMO_KEY) {
      finalHeaders["Authorization"] = `Bearer ${DEMO_KEY}`;
    } else {
      console.warn(
        "No NEXT_PUBLIC_OPS_BEARER_TOKEN configured. Write request has no auth."
      );
    }
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...rest,
    headers: finalHeaders,
    cache: "no-store",
  });

  if (!res.ok) {
    const data = await res.json().catch(() => null);

    throw new Error(
      data?.detail?.message ||
        data?.error?.message ||
        `Request failed (${res.status})`
    );
  }

  return res.json();
}

export async function fetchIntegrityHistory(marketId: string) {
  return fetchJSON(
    `/ops/markets/integrity/history?market_id=${encodeURIComponent(marketId)}`
  );
}