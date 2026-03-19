import { LaunchCandidate, SnapshotResponse, SocialCandidate } from "@/lib/types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

const OPS_API_TOKEN = process.env.OPS_API_TOKEN || "";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      ...(OPS_API_TOKEN ? { Authorization: `Bearer ${OPS_API_TOKEN}` } : {}),
    },
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status} ${res.statusText}: ${text}`);
  }

  return res.json();
}

export async function getLaunchCandidates(limit = 10): Promise<LaunchCandidate[]> {
  return apiFetch<LaunchCandidate[]>(`/ops/launch/candidates?limit=${limit}`);
}

export async function getSocialCandidates(limit = 10): Promise<SocialCandidate[]> {
  return apiFetch<SocialCandidate[]>(`/ops/social/candidates?limit=${limit}`);
}

export async function getMarketSnapshot(
  marketId: string
): Promise<SnapshotResponse> {
  return apiFetch<SnapshotResponse>(`/ops/markets/${marketId}/snapshot`);
}