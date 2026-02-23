import Link from "next/link";

type Flag = {
  flag_code: string;
  severity: number;
  details: Record<string, any>;
};

type MarketRow = {
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

async function fetchInbox(): Promise<MarketRow[]> {
  const res = await fetch("http://127.0.0.1:8000/ops/inbox", { cache: "no-store" });
  if (!res.ok) return [];
  return res.json();
}

function fmtNumber(x: any) {
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toLocaleString();
  return String(x);
}

function flagPill(sev: number) {
  if (sev >= 4) return "bg-red-100 text-red-800 border-red-300";
  if (sev === 3) return "bg-yellow-100 text-yellow-800 border-yellow-300";
  return "bg-gray-100 text-gray-700 border-gray-300";
}

export default async function OpsInboxPage() {
  const rows = await fetchInbox();

  return (
    <main className="p-8 max-w-6xl">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">Ops Inbox</h1>
          <p className="text-sm text-gray-500 mt-1">Today view, ranked by risk score</p>
        </div>
        <div className="text-xs text-gray-500">
          API{" "}
          <code className="px-1 py-0.5 border rounded bg-white">/ops/inbox</code>
        </div>
      </div>

      {rows.length === 0 ? (
        <p className="text-sm text-gray-500 mt-6">Inbox is empty.</p>
      ) : (
        <div className="mt-6 space-y-3">
          {rows.map((m) => (
            <Link
              key={m.market_id}
              href={`/ops/${m.market_id}`}
              className="block border rounded-2xl p-4 hover:bg-gray-50 transition"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="text-lg font-medium truncate">{m.title}</div>
                  <div className="text-xs text-gray-500 mt-1">
                    <span className="font-mono">{m.market_id}</span>
                    {" | "}
                    {m.protocol}
                    {" | "}
                    {m.chain}
                    {" | "}
                    {m.category ?? "uncategorized"}
                    {" | "}
                    {m.day}
                  </div>

                  <div className="flex flex-wrap gap-2 mt-3">
                    {m.has_manual_override ? (
                      <span className="text-xs px-2 py-0.5 rounded-full border bg-blue-50 text-blue-800 border-blue-200">
                        manual override
                      </span>
                    ) : null}

                    {Array.isArray(m.flags) && m.flags.length
                      ? m.flags.map((f, i) => (
                          <span
                            key={`${m.market_id}-${f.flag_code}-${i}`}
                            className={`text-xs px-2 py-0.5 rounded-full border ${flagPill(
                              f.severity
                            )}`}
                          >
                            {f.flag_code} ({f.severity})
                          </span>
                        ))
                      : (
                        <span className="text-xs px-2 py-0.5 rounded-full border bg-gray-50 text-gray-600">
                          no flags
                        </span>
                      )}
                  </div>
                </div>

                <div className="shrink-0 grid grid-cols-2 gap-2 text-right">
                  <div className="border rounded-xl px-3 py-2">
                    <div className="text-xs text-gray-500">risk</div>
                    <div className="text-lg font-semibold">{fmtNumber(m.risk_score)}</div>
                  </div>
                  <div className="border rounded-xl px-3 py-2">
                    <div className="text-xs text-gray-500">health</div>
                    <div className="text-lg font-semibold">{fmtNumber(m.health_score)}</div>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-6 gap-2 mt-4 text-sm">
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">volume</div>
                  <div className="font-medium">{fmtNumber(m.volume)}</div>
                </div>
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">trades</div>
                  <div className="font-medium">{fmtNumber(m.trades)}</div>
                </div>
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">traders</div>
                  <div className="font-medium">{fmtNumber(m.unique_traders)}</div>
                </div>
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">spread</div>
                  <div className="font-medium">{fmtNumber(m.spread_median)}</div>
                </div>
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">depth 2pct</div>
                  <div className="font-medium">{fmtNumber(m.depth_2pct_median)}</div>
                </div>
                <div className="border rounded-xl p-3">
                  <div className="text-xs text-gray-500">hhi</div>
                  <div className="font-medium">{fmtNumber(m.concentration_hhi)}</div>
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </main>
  );
}
