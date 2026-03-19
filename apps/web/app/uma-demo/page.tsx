import Link from "next/link";
import {
  mapSnapshotToDemoViewModel,
  type SnapshotResponse,
} from "./lib/demoMapper";

const API_BASE = "http://127.0.0.1:8000";
const DEFAULT_MARKET_ID = "m_74294b4d75";

const DEMO_MARKETS = [
  {
    id: "m_74294b4d75",
    label: "Primary Review Case",
    shortTitle: "Arsenal EPL winner",
    commentary:
      "Best end to end case. This market has the strongest structural coverage and is the clearest example of a reviewer support workflow with timeline and prior review context.",
  },
  {
    id: "m_c60517e0a0",
    label: "Healthy Baseline",
    shortTitle: "Jon Stewart nomination",
    commentary:
      "Baseline healthy case. This market represents a more organic structure with broader participation and no major structural alert.",
  },
  {
    id: "m_001d3d1b65",
    label: "Thin Market",
    shortTitle: "Minnesota Timberwolves WCF",
    commentary:
      "Thin market case. This demonstrates why low activity or structurally thin conditions can produce fragile market signals that deserve extra caution.",
  },
];

async function fetchSnapshot(marketId: string): Promise<SnapshotResponse | null> {
  try {
    const res = await fetch(
      `${API_BASE}/ops/markets/${encodeURIComponent(marketId)}/snapshot`,
      { cache: "no-store" },
    );
    if (!res.ok) return null;
    return (await res.json()) as SnapshotResponse;
  } catch {
    return null;
  }
}

function badgeClass(label: string) {
  const v = String(label || "").toLowerCase();

  if (v.includes("manual") || v.includes("escalate")) {
    return "border-red-200 bg-red-50 text-red-800";
  }
  if (v.includes("closer") || v.includes("caution") || v.includes("elevated")) {
    return "border-amber-200 bg-amber-50 text-amber-800";
  }
  if (v.includes("standard")) {
    return "border-yellow-200 bg-yellow-50 text-yellow-800";
  }
  return "border-emerald-200 bg-emerald-50 text-emerald-800";
}

function protocolBadgeClass(protocol: string) {
  const p = String(protocol || "").toLowerCase();
  if (p === "polymarket") return "border-blue-200 bg-blue-50 text-blue-800";
  return "border-gray-200 bg-gray-50 text-gray-700";
}

function metricCard(label: string, value: string) {
  const isLongValue = value.length > 28;

  return (
    <div className="rounded-xl border bg-gray-50 p-3">
      <div className="text-xs text-gray-500">{label}</div>
      <div
        className={`mt-1 font-medium text-gray-900 ${
          isLongValue ? "text-xs leading-5" : "text-sm"
        }`}
      >
        {value}
      </div>
    </div>
  );
}

function formatMiniBarValue(label: string, value: number | null) {
  if (value === null || Number.isNaN(value)) return "—";
  if (label.toLowerCase() === "spread") return value.toFixed(3);
  return value.toFixed(1);
}

function resolveVerificationPosture(resolutionPosture: string): string {
  const v = String(resolutionPosture || "").toLowerCase();

  if (v.includes("unresolved")) {
    return "Event unresolved, verify against official final result";
  }

  if (v.includes("verify whether the event has concluded")) {
    return "Verify event completion before accepting proposal context";
  }

  return "Requires official source verification";
}

function MiniBar({
  label,
  value,
  max = 100,
  tone = "bg-slate-900",
}: {
  label: string;
  value: number | null;
  max?: number;
  tone?: string;
}) {
  const safe = value === null || Number.isNaN(value) ? null : value;
  const pct =
    safe === null ? 0 : Math.max(0, Math.min(100, (safe / max) * 100));

  return (
    <div className="rounded-xl border bg-white p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs text-gray-500">{label}</div>
        <div className="text-xs text-gray-700">
          {formatMiniBarValue(label, safe)}
        </div>
      </div>
      <div className="mt-2 h-2 rounded-full bg-gray-100">
        <div
          className={`h-2 rounded-full ${tone}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

type PageProps = {
  searchParams?: Promise<{
    market_id?: string;
  }>;
};

export default async function UmaDemoPage({ searchParams }: PageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const selectedMarketId =
    resolvedSearchParams?.market_id &&
    DEMO_MARKETS.some((m) => m.id === resolvedSearchParams.market_id)
      ? resolvedSearchParams.market_id
      : DEFAULT_MARKET_ID;

  const selectedMarketMeta =
    DEMO_MARKETS.find((m) => m.id === selectedMarketId) ?? DEMO_MARKETS[0];

  const snapshot = await fetchSnapshot(selectedMarketId);

  if (!snapshot) {
    return (
      <main className="max-w-6xl p-8">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold">UMA Resolution Support Demo</h1>
            <p className="mt-1 text-sm text-gray-500">
              Structural context for reviewing flagged prediction market proposals.
            </p>
          </div>

          <Link href="/ops" className="text-sm text-gray-600 hover:underline">
            Back to Ops
          </Link>
        </div>

        <section className="mt-6 rounded-2xl border bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold">Demo market selector</h2>
          <p className="mt-1 text-sm text-gray-500">
            Pick one of the predefined structural cases for the interview demo.
          </p>

          <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {DEMO_MARKETS.map((market) => (
              <Link
                key={market.id}
                href={`/uma-demo?market_id=${market.id}`}
                className="rounded-2xl border bg-gray-50 p-4 transition hover:bg-white hover:shadow-sm"
              >
                <div className="text-xs text-gray-500">{market.label}</div>
                <div className="mt-1 text-sm font-semibold text-gray-900">
                  {market.shortTitle}
                </div>
                <div className="mt-2 text-xs font-mono text-gray-500">
                  {market.id}
                </div>
                <p className="mt-3 text-sm text-gray-600">{market.commentary}</p>
              </Link>
            ))}
          </div>
        </section>

        <div className="mt-6 rounded-2xl border bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold">Snapshot unavailable</h2>
          <p className="mt-2 text-sm text-gray-600">
            We could not load the selected demo market snapshot right now.
          </p>
          <div className="mt-4 text-sm">
            <span className="text-gray-500">Endpoint: </span>
            <code className="rounded border bg-gray-50 px-1 py-0.5 break-all">
              {`${API_BASE}/ops/markets/${selectedMarketId}/snapshot`}
            </code>
          </div>
        </div>
      </main>
    );
  }

  const vm = mapSnapshotToDemoViewModel(snapshot);
  const timeline = vm.timeline ?? [];
  const verificationPosture = resolveVerificationPosture(
    vm.verification.resolutionPosture,
  );

  return (
    <main className="max-w-7xl p-8">
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="inline-flex rounded-full border border-purple-200 bg-purple-50 px-2.5 py-1 text-xs font-medium text-purple-800">
              INTERVIEW DEMO
            </span>
            <span
              className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${protocolBadgeClass(
                vm.header.protocol,
              )}`}
            >
              {vm.header.protocol}
            </span>
            <span className="inline-flex rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-xs font-medium text-gray-700">
              {vm.header.chain}
            </span>
            <span className="inline-flex rounded-full border border-indigo-200 bg-indigo-50 px-2.5 py-1 text-xs font-medium text-indigo-800">
              Optimistic oracle context
            </span>
          </div>

          <h1 className="mt-3 text-3xl font-semibold tracking-tight">
            {vm.header.title}
          </h1>
          <p className="mt-2 max-w-4xl text-sm text-gray-600">
            {vm.header.subtitle}
          </p>
          <div className="mt-3 inline-flex rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs text-amber-900">
            {vm.header.structuralOnlyNote}
          </div>
        </div>

        <div className="flex items-center gap-3">
          <Link href="/ops" className="text-sm text-gray-600 hover:underline">
            Ops
          </Link>
          <Link
            href={`/ops/${vm.header.marketId}`}
            className="text-sm text-gray-600 hover:underline"
          >
            Market page
          </Link>
        </div>
      </div>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-lg font-semibold">Demo cases</h2>
            <p className="mt-1 text-sm text-gray-500">
              Fixed structural cases chosen to show how reviewer posture changes across different market conditions.
            </p>
          </div>

          <div className="text-xs text-gray-500">
            Selected case:{" "}
            <span className="font-medium text-gray-700">
              {selectedMarketMeta.label}
            </span>
          </div>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {DEMO_MARKETS.map((market) => {
            const isSelected = market.id === selectedMarketId;

            return (
              <Link
                key={market.id}
                href={`/uma-demo?market_id=${market.id}`}
                className={`rounded-2xl border p-4 transition ${
                  isSelected
                    ? "border-slate-900 bg-slate-50 shadow-sm"
                    : "bg-gray-50 hover:bg-white hover:shadow-sm"
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs text-gray-500">{market.label}</div>
                  {isSelected ? (
                    <span className="inline-flex rounded-full border border-slate-300 bg-white px-2 py-0.5 text-[10px] font-medium text-slate-700">
                      active
                    </span>
                  ) : null}
                </div>

                <div className="mt-1 text-sm font-semibold text-gray-900">
                  {market.shortTitle}
                </div>

                <div className="mt-2 text-xs font-mono text-gray-500">
                  {market.id}
                </div>

                <p className="mt-3 text-sm text-gray-600">{market.commentary}</p>
              </Link>
            );
          })}
        </div>
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-lg font-semibold">Proposal Verification Context</h2>
            <p className="mt-1 text-sm text-gray-500">
              Reviewer oriented guidance for rule interpretation and event verification.
            </p>
          </div>
        </div>

        <div className="mt-5 grid gap-4 xl:grid-cols-3">
          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Rule summary</div>
            <p className="mt-2 text-sm text-gray-900">
              {vm.verification.ruleSummary}
            </p>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Primary verification sources</div>
            <ul className="mt-2 space-y-2 text-sm text-gray-900">
              {vm.verification.primarySources.map((item) => (
                <li key={item}>• {item}</li>
              ))}
            </ul>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Resolution posture</div>
            <p className="mt-2 text-sm text-gray-900">
              {vm.verification.resolutionPosture}
            </p>

            <div className="mt-4 text-xs text-gray-500">Ambiguity notes</div>
            <ul className="mt-2 space-y-2 text-sm text-gray-900">
              {vm.verification.ambiguityNotes.map((item) => (
                <li key={item}>• {item}</li>
              ))}
            </ul>
          </div>
        </div>

        <div className="mt-4 rounded-xl border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
          {vm.verification.note}
        </div>
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-lg font-semibold">Resolution Assessment</h2>
            <p className="mt-1 text-sm text-gray-500">
              Reviewer summary of verification posture, structural caution, context confidence, and next action.
            </p>
          </div>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Verification posture</div>
            <div className="mt-2 text-sm font-medium text-gray-900">
              {verificationPosture}
            </div>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Structural caution</div>
            <div className="mt-2 text-sm font-medium text-gray-900">
              {vm.reviewContext.cautionLevel}
            </div>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Context confidence</div>
            <div className="mt-2 text-sm font-medium text-gray-900">
              {vm.reviewContext.contextConfidence}
            </div>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Recommended operator action</div>
            <div className="mt-2 text-sm font-medium text-gray-900">
              {vm.reviewContext.recommendedAction}
            </div>
          </div>
        </div>

        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
          Structural context is informative but not determinative. Final reviewer judgment should rely on official market rules, trusted evidence sources, and the proposal’s actual resolution claim.
        </div>
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div className="min-w-0">
            <h2 className="text-xl font-semibold">{vm.header.marketTitle}</h2>
            <div className="mt-2 text-xs text-gray-500">
              <span className="font-mono">{vm.header.marketId}</span>
              {" | "}
              review status: {vm.header.reviewWindowStatus}
            </div>
          </div>

          <div
            className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${badgeClass(
              vm.reviewContext.cautionLabel,
            )}`}
          >
            {vm.reviewContext.cautionLabel}
          </div>
        </div>

        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
          <div className="text-xs text-slate-500">Case commentary</div>
          <p className="mt-1">{selectedMarketMeta.commentary}</p>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Recommended Action</div>
            <div className="mt-2 text-lg font-semibold text-gray-900">
              {vm.reviewContext.recommendedAction}
            </div>
            <p className="mt-2 text-sm text-gray-600">
              Use this as reviewer posture, not as final resolution logic.
            </p>
          </div>

          <div className="rounded-2xl border bg-gray-50 p-4">
            <div className="text-xs text-gray-500">Primary Structural Drivers</div>
            <div className="mt-3 flex flex-wrap gap-2">
              {vm.reviewContext.rationale.length ? (
                vm.reviewContext.rationale.map((item) => (
                  <span
                    key={item}
                    className="inline-flex rounded-full border bg-white px-2.5 py-1 text-xs text-gray-700"
                  >
                    {item}
                  </span>
                ))
              ) : (
                <span className="text-sm text-gray-500">No drivers available.</span>
              )}
            </div>
          </div>
        </div>
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-lg font-semibold">Market Structure Summary</h2>
            <p className="mt-1 text-sm text-gray-500">
              Immediate structural context to determine whether the market looked robust, thin, or fragile near the time of review.
            </p>
          </div>

          <div className="text-xs text-gray-500">
            Primary reason:{" "}
            <span className="font-medium text-gray-700">
              {vm.structure.primaryReason}
            </span>
          </div>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
          {metricCard("Spread median", vm.structure.spreadMedian)}
          {metricCard("Health score", vm.structure.healthScore)}
          {metricCard("Risk score", vm.structure.riskScore)}
          {metricCard("Regime", vm.structure.regime)}
          {metricCard("Regime reason", vm.structure.regimeReason)}
          {metricCard("Integrity band", vm.structure.integrityBand)}
          {metricCard("Review priority", vm.structure.reviewPriority)}
          {metricCard("Integrity score", vm.structure.integrityScore)}
          {metricCard("Liquidity health", vm.structure.liquidityHealthScore)}
          {metricCard("Market quality", vm.structure.marketQualityScore)}
        </div>

        {vm.structure.note ? (
          <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
            {vm.structure.note}
          </div>
        ) : null}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-lg font-semibold">Time Deterioration Timeline</h2>
            <p className="mt-1 text-sm text-gray-500">
              A market can look acceptable overall but deteriorate close to resolution. This view helps the reviewer inspect drift over time.
            </p>
          </div>

          <div className="text-xs text-gray-500">
            Points available: {timeline.length}
          </div>
        </div>

        {timeline.length ? (
          <div className="mt-5 grid gap-4 md:grid-cols-3">
            {timeline.map((point, idx) => (
              <div
                key={`${point.label}-${idx}`}
                className="rounded-2xl border bg-gray-50 p-4"
              >
                <div className="mb-3 flex items-center justify-between">
                  <div className="text-sm font-medium text-gray-900">
                    {point.label}
                  </div>
                  <div className="text-xs text-gray-500">daily snapshot</div>
                </div>

                <div className="space-y-3">
                  <MiniBar
                    label="Spread"
                    value={point.spread}
                    max={0.05}
                    tone="bg-sky-600"
                  />
                  <MiniBar
                    label="Health"
                    value={point.health}
                    max={1}
                    tone="bg-emerald-600"
                  />
                  <MiniBar
                    label="Risk"
                    value={point.risk}
                    max={100}
                    tone="bg-amber-500"
                  />
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            Timeline data is not available for this market.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="text-sm text-gray-600">
          Structural context supports triage and reviewer caution, but final resolution still depends on market rules and trusted evidence.
        </div>
      </section>
    </main>
  );
}