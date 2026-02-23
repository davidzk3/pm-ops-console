import Link from "next/link";
import OpsInboxClient from "./OpsInboxClient";

export type Flag = {
  flag_code: string;
  severity: number;
  details: Record<string, any>;
};

export type MarketRow = {
  market_id: string;
  protocol: string;
  chain: string;
  title: string;
  category: string | null;
  day: string;

  volume: number;
  trades: number;
  unique_traders: number;

  spread_median: number | null;
  depth_2pct_median: number | null;
  concentration_hhi: number | null;

  health_score: number | null;
  risk_score: number | null;

  has_manual_override?: boolean;
  flags: Flag[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

async function fetchInbox(): Promise<MarketRow[]> {
  const res = await fetch(`${API_BASE}/ops/inbox`, { cache: "no-store" });
  if (!res.ok) return [];
  const data = await res.json();

  // harden: ensure flags is always an array
  return (Array.isArray(data) ? data : []).map((r: any) => ({
    ...r,
    flags: Array.isArray(r?.flags) ? r.flags : [],
  }));
}

export default async function OpsInboxPage() {
  const rows = await fetchInbox();

  return (
    <main className="p-8 max-w-6xl">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">Ops Inbox</h1>
          <p className="text-sm text-gray-500 mt-1">Triage markets. Create incidents and interventions quickly.</p>
        </div>

        <Link href="/" className="text-sm text-gray-600 hover:underline">
          Home
        </Link>
      </div>

      {rows.length ? (
        <OpsInboxClient rows={rows} />
      ) : (
        <div className="mt-6 border rounded-2xl overflow-hidden bg-white">
          <div className="p-6 text-sm text-gray-500">
            Inbox is empty. Confirm the API is running and returning rows at{" "}
            <code className="px-1 py-0.5 border rounded bg-white">{`${API_BASE}/ops/inbox`}</code>.
          </div>
        </div>
      )}
    </main>
  );
}
