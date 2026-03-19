import Link from "next/link";
import IntegrityTrendCell from "./components/IntegrityTrendCell";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

type PipelineStatus = {
  build_id: string;

  latest_universe_day: string | null;
  latest_microstructure_day: string | null;
  latest_features_day: string | null;
  latest_trader_behavior_day: string | null;
  latest_market_regime_day: string | null;
  latest_resolution_features_day: string | null;

  universe_rows_latest: number;
  microstructure_rows_latest: number;
  features_rows_latest: number;
  trader_behavior_rows_latest: number;
  market_regime_rows_latest: number;
  resolution_features_rows_latest: number;

  latest_trade_ts: string | null;
  latest_bbo_ts: string | null;

  health: string;
};

type MarketRegimeV2Row = {
  market_id: string;
  day: string;
  title: string | null;
  url: string | null;
  regime: string | null;
  regime_reason: string | null;

  market_quality_score: number | null;
  liquidity_health_score: number | null;
  concentration_risk_score: number | null;
  whale_volume_share: number | null;

  trades: number | null;
  unique_traders: number | null;
  trader_count: number | null;
};

type TraderTopRow = {
  market_id: string;
  title: string | null;
  trader_id: string;
  day: string;

  trades: number;
  buy_trades: number;
  sell_trades: number;

  volume: number | null;
  avg_trade_size: number | null;
  buy_ratio: number | null;
  market_volume_share: number | null;
  active_minutes: number | null;

  is_large_participant: boolean;
  is_one_sided: boolean;
  is_high_frequency: boolean;
};

type RiskRadarRow = {
  market_id: string;
  day: string;
  title: string | null;
  url: string | null;

  risk_score: number | null;
  risk_tier: string | null;
  primary_risk_reason: string | null;
  dominant_role: string | null;

  needs_operator_review: boolean;

  regime: string | null;

  trades: number | null;
  unique_traders: number | null;
  trader_count: number | null;
};

type IntegrityRow = {
  market_id: string;
  day: string;
  title: string | null;
  url: string | null;
  category: string | null;

  regime: string | null;
  regime_reason: string | null;

  trades: number | null;
  unique_traders: number | null;

  market_quality_score: number | null;
  liquidity_health_score: number | null;
  concentration_risk_score: number | null;
  whale_volume_share: number | null;

  radar_risk_score: number | null;
  manipulation_score: number | null;
  manipulation_signal: string | null;

  whale_role_share: number | null;
  speculator_role_share: number | null;
  neutral_role_share: number | null;
  possible_farmer_count: number | null;

  integrity_score: number | null;
  integrity_band: string | null;
  review_priority: string | null;
  primary_reason: string | null;
  needs_operator_review: boolean;
};

type IntegrityHistoryResponse = {
  market_id: string;
  points: Array<{
    day: string;
    market_id: string;
    integrity_score: number | null;
    integrity_band: string | null;
    radar_risk_score: number | null;
    manipulation_score: number | null;
    regime: string | null;
    regime_reason: string | null;
    whale_role_share: number | null;
    speculator_role_share: number | null;
    neutral_role_share: number | null;
    trades: number | null;
    unique_traders: number | null;
  }>;
  count: number;
};

type ClusterRow = {
  key: string;
  regime: string;
  band: string;
  signal: string;
  category: string;
  count: number;
  avgIntegrity: number;
  avgManipulation: number;
  avgRadarRisk: number;
  reviewCount: number;
};

type ManipulationRow = {
  market_id: string;
  day: string;
  title: string | null;
  url: string | null;

  manipulation_score: number | null;
  risk_tier: string | null;
  primary_signal: string | null;
  signal_labels: string[];

  needs_operator_review: boolean;

  trades: number | null;
  unique_traders: number | null;

  buy_volume_share: number | null;
  sell_volume_share: number | null;

  largest_trader_share: number | null;
  top2_trader_share: number | null;

  avg_trade_size: number | null;
  median_trade_size: number | null;
};

async function fetchJSON<T>(url: string, fallback: T): Promise<T> {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return fallback;
    return (await res.json()) as T;
  } catch {
    return fallback;
  }
}

async function fetchPipelineStatus(): Promise<PipelineStatus | null> {
  return fetchJSON<PipelineStatus | null>(`${API_BASE}/ops/pipeline/status`, null);
}

async function fetchRegimes(day: string): Promise<MarketRegimeV2Row[]> {
  return fetchJSON<MarketRegimeV2Row[]>(
    `${API_BASE}/ops/markets/regimes/v2?day=${encodeURIComponent(day)}&limit=20`,
    [],
  );
}

async function fetchTopTraders(day: string): Promise<TraderTopRow[]> {
  return fetchJSON<TraderTopRow[]>(
    `${API_BASE}/ops/traders/top?day=${encodeURIComponent(day)}&limit=20`,
    [],
  );
}

async function fetchRiskRadar(day: string): Promise<RiskRadarRow[]> {
  return fetchJSON<RiskRadarRow[]>(
    `${API_BASE}/ops/markets/risk-radar?day=${encodeURIComponent(day)}&limit=20`,
    [],
  );
}

async function fetchManipulation(day: string): Promise<ManipulationRow[]> {
  return fetchJSON<ManipulationRow[]>(
    `${API_BASE}/ops/markets/manipulation?day=${encodeURIComponent(day)}&limit=20`,
    [],
  );
}

async function fetchIntegrity(day: string): Promise<IntegrityRow[]> {
  return fetchJSON<IntegrityRow[]>(
    `${API_BASE}/ops/markets/integrity?day=${encodeURIComponent(day)}&limit=50`,
    [],
  );
}

async function fetchIntegrityHistory(
  marketId: string,
): Promise<IntegrityHistoryResponse | null> {
  return fetchJSON<IntegrityHistoryResponse | null>(
    `${API_BASE}/ops/markets/integrity/history?market_id=${encodeURIComponent(marketId)}`,
    null,
  );
}

function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined) return "—";
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: digits,
  }).format(value);
}

function formatPercent(value: number | null | undefined, digits = 1) {
  if (value === null || value === undefined) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

function regimeBadgeClass(regime: string | null | undefined) {
  switch (regime) {
    case "organic_market":
      return "border border-emerald-200 bg-emerald-50 text-emerald-700";
    case "whale_dominated":
      return "border border-amber-200 bg-amber-50 text-amber-700";
    case "farming_dominated":
      return "border border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700";
    case "liquidity_collapse":
      return "border border-rose-200 bg-rose-50 text-rose-700";
    case "thin_market":
      return "border border-slate-200 bg-slate-100 text-slate-700";
    case "inactive":
      return "border border-gray-200 bg-gray-100 text-gray-700";
    default:
      return "border border-blue-200 bg-blue-50 text-blue-700";
  }
}

function riskTierBadge(tier: string | null | undefined) {
  switch (tier) {
    case "critical":
      return "border-red-700 bg-red-600 text-white";
    case "high":
      return "border-red-300 bg-red-100 text-red-800";
    case "medium":
      return "border-yellow-300 bg-yellow-100 text-yellow-800";
    case "low":
      return "border-green-300 bg-green-100 text-green-800";
    default:
      return "border-gray-200 bg-gray-100 text-gray-700";
  }
}

function integrityBandBadge(band: string | null | undefined) {
  switch (band) {
    case "strong":
      return "border-green-200 bg-green-100 text-green-800";
    case "stable":
      return "border-emerald-200 bg-emerald-100 text-emerald-800";
    case "fragile":
      return "border-yellow-200 bg-yellow-100 text-yellow-800";
    case "review":
      return "border-orange-200 bg-orange-100 text-orange-800";
    case "critical":
      return "border-red-200 bg-red-100 text-red-800";
    default:
      return "border-gray-200 bg-gray-100 text-gray-700";
  }
}

function heatColor(score: number | null | undefined) {
  const v = typeof score === "number" ? score : -1;

  if (v >= 85) return "border-green-300 bg-green-500/20";
  if (v >= 70) return "border-emerald-300 bg-emerald-500/20";
  if (v >= 50) return "border-yellow-300 bg-yellow-500/20";
  if (v >= 30) return "border-orange-300 bg-orange-500/20";
  if (v >= 0) return "border-red-300 bg-red-500/20";
  return "border-gray-200 bg-gray-100";
}

function signalBadgeClass(signal: string | null | undefined) {
  switch (signal) {
    case "one_sided_price_push":
      return "border-red-200 bg-red-100 text-red-800";
    case "thin_market_dislocation":
      return "border-orange-200 bg-orange-100 text-orange-800";
    case "none":
      return "border-gray-200 bg-gray-100 text-gray-700";
    default:
      return "border-blue-200 bg-blue-100 text-blue-800";
  }
}

function clusterCellClass(row: ClusterRow) {
  if (row.reviewCount >= 3 || row.avgManipulation >= 0.7) {
    return "border-red-200 bg-red-50";
  }
  if (row.reviewCount >= 1 || row.avgIntegrity < 55) {
    return "border-orange-200 bg-orange-50";
  }
  if (row.avgIntegrity < 70) {
    return "border-yellow-200 bg-yellow-50";
  }
  return "border-emerald-200 bg-emerald-50";
}

function healthBadgeClass(health: string | null | undefined) {
  if (health === "ok") {
    return "border border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  return "border border-amber-200 bg-amber-50 text-amber-700";
}

function buildReviewQueue(rows: IntegrityRow[]) {
  return rows
    .filter((r) => r.needs_operator_review)
    .sort((a, b) => {
      const ai = a.integrity_score ?? 0;
      const bi = b.integrity_score ?? 0;
      return ai - bi;
    })
    .slice(0, 20);
}

function buildClusterMap(rows: IntegrityRow[]): ClusterRow[] {
  const groups = new Map<
    string,
    {
      regime: string;
      band: string;
      signal: string;
      category: string;
      count: number;
      integritySum: number;
      manipulationSum: number;
      radarSum: number;
      reviewCount: number;
    }
  >();

  for (const row of rows) {
    const regime = row.regime ?? "unknown";
    const band = row.integrity_band ?? "unknown";
    const signal = row.manipulation_signal ?? "none";
    const category = row.category ?? "uncategorized";

    const key = `${regime}__${band}__${signal}__${category}`;

    if (!groups.has(key)) {
      groups.set(key, {
        regime,
        band,
        signal,
        category,
        count: 0,
        integritySum: 0,
        manipulationSum: 0,
        radarSum: 0,
        reviewCount: 0,
      });
    }

    const g = groups.get(key)!;
    g.count += 1;
    g.integritySum += row.integrity_score ?? 0;
    g.manipulationSum += row.manipulation_score ?? 0;
    g.radarSum += row.radar_risk_score ?? 0;

    if (row.needs_operator_review) g.reviewCount += 1;
  }

  return Array.from(groups.entries())
    .map(([key, g]) => ({
      key,
      regime: g.regime,
      band: g.band,
      signal: g.signal,
      category: g.category,
      count: g.count,
      avgIntegrity: g.count ? g.integritySum / g.count : 0,
      avgManipulation: g.count ? g.manipulationSum / g.count : 0,
      avgRadarRisk: g.count ? g.radarSum / g.count : 0,
      reviewCount: g.reviewCount,
    }))
    .sort((a, b) => {
      if (b.reviewCount !== a.reviewCount) return b.reviewCount - a.reviewCount;
      if (b.count !== a.count) return b.count - a.count;
      return a.avgIntegrity - b.avgIntegrity;
    });
}

function buildIntegrityHeatmap(rows: IntegrityRow[]) {
  const regimes = [
    "organic_market",
    "whale_dominated",
    "farming_dominated",
    "thin_market",
    "inactive",
    "unknown",
  ];

  const bands = ["strong", "stable", "fragile", "review", "critical"];

  const map: Record<string, Record<string, { count: number; score: number }>> = {};

  regimes.forEach((r) => {
    map[r] = {};
    bands.forEach((b) => {
      map[r][b] = { count: 0, score: 0 };
    });
  });

  rows.forEach((r) => {
    const regime = r.regime ?? "unknown";
    const band = r.integrity_band ?? "critical";

    if (!map[regime]) {
      map[regime] = {};
      bands.forEach((b) => {
        map[regime][b] = { count: 0, score: 0 };
      });
    }

    map[regime][band].count += 1;
    map[regime][band].score += r.integrity_score ?? 0;
  });

  return map;
}

function MetricCard({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <div className="rounded-2xl border bg-white p-4 shadow-sm">
      <div className="text-xs uppercase tracking-wide text-gray-500">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-gray-900">{value}</div>
    </div>
  );
}

function KeyValue({
  label,
  value,
}: {
  label: string;
  value: string | null | undefined;
}) {
  return (
    <div className="flex items-center justify-between gap-3 py-2">
      <span className="text-sm text-gray-500">{label}</span>
      <span className="text-right text-sm font-medium text-gray-900">{value ?? "—"}</span>
    </div>
  );
}

function MarketLinkCell({
  marketId,
  title,
  subtitle,
}: {
  marketId: string;
  title: string | null | undefined;
  subtitle?: string | null;
}) {
  return (
    <Link
      href={`/ops/${encodeURIComponent(marketId)}`}
      className="group block rounded-lg px-2 py-1 -mx-2 -my-1 transition-colors hover:bg-gray-50"
    >
      <div className="font-medium text-gray-900 group-hover:text-black">
        {title ?? marketId}
      </div>
      <div className="mt-1 text-xs text-gray-500">{subtitle ?? marketId}</div>
    </Link>
  );
}

export default async function OpsPage() {
  const status = await fetchPipelineStatus();

  if (!status) {
    return (
      <main className="max-w-7xl p-8">
        <div className="flex items-end justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold">Ops Console</h1>
            <p className="mt-1 text-sm text-gray-500">
              Pipeline status is unavailable. Confirm the API is running.
            </p>
          </div>

          <Link href="/" className="text-sm text-gray-600 hover:underline">
            Home
          </Link>
        </div>

        <div className="mt-6 rounded-2xl border bg-white p-6 text-sm text-gray-500">
          Expected endpoint:{" "}
          <code className="rounded border bg-gray-50 px-1 py-0.5">
            {`${API_BASE}/ops/pipeline/status`}
          </code>
        </div>
      </main>
    );
  }

  const effectiveDay =
    status.latest_market_regime_day ||
    status.latest_microstructure_day ||
    status.latest_universe_day;

  const regimes = effectiveDay ? await fetchRegimes(effectiveDay) : [];
  const radar = effectiveDay ? await fetchRiskRadar(effectiveDay) : [];
  const manipulation = effectiveDay ? await fetchManipulation(effectiveDay) : [];
  const integrity = effectiveDay ? await fetchIntegrity(effectiveDay) : [];
  const traders = effectiveDay ? await fetchTopTraders(effectiveDay) : [];

  const clusters = buildClusterMap(integrity).slice(0, 18);
  const heatmap = buildIntegrityHeatmap(integrity);
  const reviewQueue = buildReviewQueue(integrity);

  const integrityRows = integrity.slice(0, 20);

  const flaggedMarketsCount = reviewQueue.length;
  const criticalMarketsCount = integrity.filter(
    (row) => row.integrity_band === "critical",
  ).length;

  const integrityHistoryEntries = await Promise.all(
    integrityRows.map(async (row) => {
      const history = await fetchIntegrityHistory(row.market_id);
      return [row.market_id, history] as const;
    }),
  );

  const integrityHistoryMap = new Map(integrityHistoryEntries);

  return (
    <main className="max-w-7xl p-8">
      <div className="mb-6 rounded-2xl border border-gray-800 bg-gray-950 px-4 py-3 text-sm text-gray-100 shadow-sm">
        <div className="flex flex-wrap items-center gap-3">
          <span className="inline-flex rounded-full bg-emerald-500/20 px-2.5 py-1 text-xs font-medium text-emerald-300">
            OPS
          </span>

          <span className="text-gray-400">DAY</span>
          <span className="font-medium">{effectiveDay ?? "—"}</span>

          <span className="text-gray-600">|</span>

          <span className="text-gray-400">BUILD</span>
          <span className="font-medium">{status.build_id}</span>

          <span className="text-gray-600">|</span>

          <span className="text-gray-400">FLAGGED</span>
          <span className="font-medium">{flaggedMarketsCount}</span>

          <span className="text-gray-600">|</span>

          <span className="text-gray-400">TRADE TS</span>
          <span className="font-medium">{status.latest_trade_ts ?? "—"}</span>

          <span className="text-gray-600">|</span>

          <span className="text-gray-400">BBO TS</span>
          <span className="font-medium">{status.latest_bbo_ts ?? "—"}</span>

          <span className="ml-auto inline-flex rounded-full bg-white/10 px-2.5 py-1 text-xs font-medium text-white">
            {status.health}
          </span>
        </div>
      </div>

      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-3xl font-semibold tracking-tight">
            Prediction Market Ops Console
          </h1>
          <p className="mt-1 text-sm text-gray-500">
            Structural surveillance across market regimes, manipulation signals,
            integrity scoring, and participant behavior.
          </p>

          <div className="mt-4 flex flex-wrap gap-3">
            <span className="inline-flex rounded-full border bg-white px-3 py-1 text-xs font-medium text-gray-700">
              Active markets: {integrity.length}
            </span>
            <span className="inline-flex rounded-full border bg-white px-3 py-1 text-xs font-medium text-gray-700">
              Flagged markets: {flaggedMarketsCount}
            </span>
            <span className="inline-flex rounded-full border bg-white px-3 py-1 text-xs font-medium text-gray-700">
              Critical markets: {criticalMarketsCount}
            </span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <span
            className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-medium ${healthBadgeClass(
              status.health,
            )}`}
          >
            Health: {status.health}
          </span>

          <Link href="/" className="text-sm text-gray-600 hover:underline">
            Home
          </Link>
        </div>
      </div>

      <div className="mt-8 grid gap-4 sm:grid-cols-2 xl:grid-cols-6">
        <MetricCard label="Universe" value={status.universe_rows_latest} />
        <MetricCard label="Microstructure" value={status.microstructure_rows_latest} />
        <MetricCard label="Features" value={status.features_rows_latest} />
        <MetricCard label="Trader Behavior" value={status.trader_behavior_rows_latest} />
        <MetricCard label="Market Regime" value={status.market_regime_rows_latest} />
        <MetricCard
          label="Resolution Features"
          value={status.resolution_features_rows_latest}
        />
      </div>

      <div className="mt-8 grid gap-6 xl:grid-cols-[1.2fr_1fr]">
        <section className="rounded-2xl border bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold">Pipeline Status</h2>
              <p className="mt-1 text-sm text-gray-500">
                Shared analytics day and freshness across the stack.
              </p>
            </div>

            <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
              {status.build_id}
            </span>
          </div>

          <div className="mt-6 grid gap-6 md:grid-cols-2">
            <div className="rounded-2xl border bg-gray-50 p-4">
              <div className="text-sm font-medium text-gray-700">Latest Analytics Day</div>
              <div className="mt-3 divide-y">
                <KeyValue label="Universe" value={status.latest_universe_day} />
                <KeyValue label="Microstructure" value={status.latest_microstructure_day} />
                <KeyValue label="Features" value={status.latest_features_day} />
                <KeyValue
                  label="Trader Behavior"
                  value={status.latest_trader_behavior_day}
                />
                <KeyValue label="Market Regime" value={status.latest_market_regime_day} />
                <KeyValue label="Resolution" value={status.latest_resolution_features_day} />
              </div>
            </div>

            <div className="rounded-2xl border bg-gray-50 p-4">
              <div className="text-sm font-medium text-gray-700">Freshest Raw Data</div>
              <div className="mt-3 divide-y">
                <KeyValue label="Latest Trade TS" value={status.latest_trade_ts} />
                <KeyValue label="Latest BBO TS" value={status.latest_bbo_ts} />
                <KeyValue label="Dashboard Day" value={effectiveDay} />
              </div>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold">Operator Notes</h2>
          <div className="mt-4 space-y-3 text-sm text-gray-600">
            <div className="rounded-xl border bg-gray-50 p-4">
              Regime classification is now powered by <span className="font-medium">v2</span>,
              which separates thin markets from true whale dominated markets.
            </div>
            <div className="rounded-xl border bg-gray-50 p-4">
              Trade behavior coverage remains smaller than microstructure coverage, so
              some markets still have rich liquidity signals but limited participant
              detail.
            </div>
            <div className="rounded-xl border bg-gray-50 p-4">
              The next intelligence layer after this dashboard is trader role labeling.
            </div>
          </div>
        </section>
      </div>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Regimes v2</h2>
            <p className="mt-1 text-sm text-gray-500">
              Interpretable market states using the refined regime layer.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {regimes.length ? (
          <div className="mt-5 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Market</th>
                  <th className="py-3 pr-4 font-medium">Regime</th>
                  <th className="py-3 pr-4 font-medium">Reason</th>
                  <th className="py-3 pr-4 font-medium">Quality</th>
                  <th className="py-3 pr-4 font-medium">Liquidity</th>
                  <th className="py-3 pr-4 font-medium">Whale Share</th>
                  <th className="py-3 pr-4 font-medium">Trades</th>
                  <th className="py-3 font-medium">Traders</th>
                </tr>
              </thead>
              <tbody>
                {regimes.map((row) => (
                  <tr key={row.market_id} className="border-b align-top">
                    <td className="py-4 pr-4">
                      <MarketLinkCell
                        marketId={row.market_id}
                        title={row.title}
                        subtitle={row.market_id}
                      />
                    </td>
                    <td className="py-4 pr-4">
                      <span
                        className={`inline-flex rounded-full px-3 py-1 text-xs font-medium ${regimeBadgeClass(
                          row.regime,
                        )}`}
                      >
                        {row.regime ?? "unknown"}
                      </span>
                    </td>
                    <td className="py-4 pr-4 text-gray-600">
                      {row.regime_reason ?? "—"}
                    </td>
                    <td className="py-4 pr-4">{formatNumber(row.market_quality_score, 3)}</td>
                    <td className="py-4 pr-4">
                      {formatNumber(row.liquidity_health_score, 3)}
                    </td>
                    <td className="py-4 pr-4">
                      {formatPercent(row.whale_volume_share, 1)}
                    </td>
                    <td className="py-4 pr-4">{row.trades ?? "—"}</td>
                    <td className="py-4">{row.trader_count ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No regime rows returned for the selected day.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Risk Radar</h2>
            <p className="mt-1 text-sm text-gray-500">
              Structural risk scoring across prediction markets.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {radar.length ? (
          <div className="mt-5 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Market</th>
                  <th className="py-3 pr-4 font-medium">Risk</th>
                  <th className="py-3 pr-4 font-medium">Reason</th>
                  <th className="py-3 pr-4 font-medium">Dominant Role</th>
                  <th className="py-3 pr-4 font-medium">Trades</th>
                  <th className="py-3 pr-4 font-medium">Traders</th>
                  <th className="py-3 font-medium">Review</th>
                </tr>
              </thead>

              <tbody>
                {radar.map((row) => (
                  <tr key={row.market_id} className="border-b align-top">
                    <td className="py-4 pr-4">
                      <MarketLinkCell
                        marketId={row.market_id}
                        title={row.title}
                        subtitle={row.market_id}
                      />
                    </td>

                    <td className="py-4 pr-4">
                      <span
                        className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${riskTierBadge(
                          row.risk_tier,
                        )}`}
                      >
                        {row.risk_tier ?? "unknown"}
                      </span>
                    </td>

                    <td className="py-4 pr-4 text-gray-600">
                      {row.primary_risk_reason ?? "—"}
                    </td>

                    <td className="py-4 pr-4">{row.dominant_role ?? "—"}</td>

                    <td className="py-4 pr-4">{row.trades ?? "—"}</td>

                    <td className="py-4 pr-4">{row.unique_traders ?? "—"}</td>

                    <td className="py-4">
                      {row.needs_operator_review ? (
                        <span className="inline-flex rounded-full border border-red-200 bg-red-50 px-2.5 py-1 text-xs text-red-800">
                          review
                        </span>
                      ) : (
                        <span className="inline-flex rounded-full border border-gray-200 bg-gray-100 px-2.5 py-1 text-xs text-gray-600">
                          monitor
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No radar rows returned for the selected day.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Manipulation Signals</h2>
            <p className="mt-1 text-sm text-gray-500">
              Detection of suspicious market structure and coordinated price pushes.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {manipulation.length ? (
          <div className="mt-5 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Market</th>
                  <th className="py-3 pr-4 font-medium">Score</th>
                  <th className="py-3 pr-4 font-medium">Signal</th>
                  <th className="py-3 pr-4 font-medium">Largest Trader</th>
                  <th className="py-3 pr-4 font-medium">Trades</th>
                  <th className="py-3 pr-4 font-medium">Traders</th>
                  <th className="py-3 font-medium">Review</th>
                </tr>
              </thead>

              <tbody>
                {manipulation.map((row) => (
                  <tr key={row.market_id} className="border-b align-top">
                    <td className="py-4 pr-4">
                      <MarketLinkCell
                        marketId={row.market_id}
                        title={row.title}
                        subtitle={row.market_id}
                      />
                    </td>

                    <td className="py-4 pr-4">{formatNumber(row.manipulation_score, 2)}</td>

                    <td className="py-4 pr-4 text-gray-600">
                      {row.primary_signal ?? "—"}
                    </td>

                    <td className="py-4 pr-4">
                      {formatPercent(row.largest_trader_share, 1)}
                    </td>

                    <td className="py-4 pr-4">{row.trades ?? "—"}</td>

                    <td className="py-4 pr-4">{row.unique_traders ?? "—"}</td>

                    <td className="py-4">
                      {row.needs_operator_review ? (
                        <span className="inline-flex rounded-full border border-red-200 bg-red-50 px-2.5 py-1 text-xs text-red-800">
                          review
                        </span>
                      ) : (
                        <span className="inline-flex rounded-full border border-gray-200 bg-gray-100 px-2.5 py-1 text-xs text-gray-600">
                          monitor
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No manipulation signals detected for this day.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Cluster Map</h2>
            <p className="mt-1 text-sm text-gray-500">
              Repeating structural patterns across fragile, thin, and review worthy
              markets.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {clusters.length ? (
          <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {clusters.map((row) => (
              <div
                key={row.key}
                className={`rounded-2xl border p-4 shadow-sm ${clusterCellClass(row)}`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-gray-900">{row.regime}</div>
                    <div className="mt-1 text-xs text-gray-600">{row.category}</div>
                  </div>

                  <span
                    className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${integrityBandBadge(
                      row.band,
                    )}`}
                  >
                    {row.band}
                  </span>
                </div>

                <div className="mt-3">
                  <span
                    className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${signalBadgeClass(
                      row.signal,
                    )}`}
                  >
                    {row.signal}
                  </span>
                </div>

                <div className="mt-4 grid grid-cols-2 gap-3">
                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Markets</div>
                    <div className="mt-1 text-lg font-semibold text-gray-900">
                      {row.count}
                    </div>
                  </div>

                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Review</div>
                    <div className="mt-1 text-lg font-semibold text-gray-900">
                      {row.reviewCount}
                    </div>
                  </div>

                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Avg Integrity</div>
                    <div className="mt-1 text-sm font-semibold text-gray-900">
                      {row.avgIntegrity.toFixed(1)}
                    </div>
                  </div>

                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Avg Manipulation</div>
                    <div className="mt-1 text-sm font-semibold text-gray-900">
                      {row.avgManipulation.toFixed(2)}
                    </div>
                  </div>
                </div>

                <div className="mt-3 text-xs text-gray-600">
                  Avg radar risk: {row.avgRadarRisk.toFixed(2)}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No repeating structural clusters found for the selected day.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Integrity Heatmap</h2>
            <p className="mt-1 text-sm text-gray-500">
              Distribution of market structural health across regimes.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        <div className="mt-6 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="py-3 pr-4 font-medium">Regime</th>
                <th className="py-3 pr-4 font-medium">Strong</th>
                <th className="py-3 pr-4 font-medium">Stable</th>
                <th className="py-3 pr-4 font-medium">Fragile</th>
                <th className="py-3 pr-4 font-medium">Review</th>
                <th className="py-3 font-medium">Critical</th>
              </tr>
            </thead>

            <tbody>
              {Object.entries(heatmap).map(([regime, bands]) => (
                <tr key={regime} className="border-b">
                  <td className="py-4 pr-4 font-medium text-gray-900">{regime}</td>

                  {["strong", "stable", "fragile", "review", "critical"].map((b) => {
                    const cell = bands[b];
                    const avgScore = cell.count ? cell.score / cell.count : null;

                    return (
                      <td key={b} className="py-4 pr-4">
                        <div
                          className={`rounded-lg border px-3 py-2 text-center ${heatColor(
                            avgScore,
                          )}`}
                        >
                          <div className="text-sm font-semibold">{cell.count}</div>
                          <div className="text-xs text-gray-600">
                            {avgScore !== null ? avgScore.toFixed(0) : "—"}
                          </div>
                        </div>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Market Integrity Score</h2>
            <p className="mt-1 text-sm text-gray-500">
              Composite structural health score across regime, radar, manipulation,
              and trader composition.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {integrity.length ? (
          <div className="mt-5 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Market</th>
                  <th className="py-3 pr-4 font-medium">Trend</th>
                  <th className="py-3 pr-4 font-medium">Band</th>
                  <th className="py-3 pr-4 font-medium">Regime</th>
                  <th className="py-3 pr-4 font-medium">Reason</th>
                  <th className="py-3 pr-4 font-medium">Organic</th>
                  <th className="py-3 font-medium">Review</th>
                </tr>
              </thead>

              <tbody>
                {integrityRows.map((row) => {
                  const history = integrityHistoryMap.get(row.market_id);

                  return (
                    <tr key={row.market_id} className="border-b align-top">
                      <td className="py-4 pr-4">
                        <MarketLinkCell
                          marketId={row.market_id}
                          title={row.title}
                          subtitle={row.market_id}
                        />
                      </td>

                      <td className="py-4 pr-4">
                        <IntegrityTrendCell points={history?.points ?? []} />
                      </td>

                      <td className="py-4 pr-4">
                        <span
                          className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${integrityBandBadge(
                            row.integrity_band,
                          )}`}
                        >
                          {row.integrity_band ?? "unknown"}
                        </span>
                      </td>

                      <td className="py-4 pr-4 text-gray-600">{row.regime ?? "—"}</td>

                      <td className="py-4 pr-4 text-gray-600">
                        {row.primary_reason ?? "—"}
                      </td>

                      <td className="py-4 pr-4">
                        {formatPercent(row.neutral_role_share, 1)}
                      </td>

                      <td className="py-4">
                        {row.needs_operator_review ? (
                          <span className="inline-flex rounded-full border border-red-200 bg-red-50 px-2.5 py-1 text-xs text-red-800">
                            review
                          </span>
                        ) : (
                          <span className="inline-flex rounded-full border border-gray-200 bg-gray-100 px-2.5 py-1 text-xs text-gray-600">
                            monitor
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No integrity rows returned for the selected day.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Operator Review Queue</h2>
            <p className="mt-1 text-sm text-gray-500">
              Markets automatically flagged by structural integrity and manipulation
              models.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {reviewQueue.length ? (
          <div className="mt-5 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Market</th>
                  <th className="py-3 pr-4 font-medium">Trend</th>
                  <th className="py-3 pr-4 font-medium">Manipulation</th>
                  <th className="py-3 pr-4 font-medium">Radar Risk</th>
                  <th className="py-3 pr-4 font-medium">Regime</th>
                  <th className="py-3 font-medium">Reason</th>
                </tr>
              </thead>

              <tbody>
                {reviewQueue.map((row) => {
                  const history = integrityHistoryMap.get(row.market_id);

                  return (
                    <tr key={row.market_id} className="border-b align-top">
                      <td className="py-4 pr-4">
                        <MarketLinkCell
                          marketId={row.market_id}
                          title={row.title}
                          subtitle={row.market_id}
                        />
                      </td>

                      <td className="py-4 pr-4">
                        <IntegrityTrendCell points={history?.points ?? []} />
                      </td>

                      <td className="py-4 pr-4">
                        {formatNumber(row.manipulation_score, 2)}
                      </td>

                      <td className="py-4 pr-4">
                        {formatNumber(row.radar_risk_score, 2)}
                      </td>

                      <td className="py-4 pr-4">
                        <span
                          className={`inline-flex rounded-full px-3 py-1 text-xs font-medium ${regimeBadgeClass(
                            row.regime,
                          )}`}
                        >
                          {row.regime ?? "unknown"}
                        </span>
                      </td>

                      <td className="py-4 text-gray-600">{row.primary_reason ?? "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No markets currently require manual operator review.
          </div>
        )}
      </section>

      <section className="mt-8 rounded-2xl border bg-white p-6 shadow-sm">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold">Top Traders</h2>
            <p className="mt-1 text-sm text-gray-500">
              Largest observed participants in the current daily window.
            </p>
          </div>

          <span className="rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-600">
            Day {effectiveDay ?? "—"}
          </span>
        </div>

        {traders.length ? (
          <div className="mt-5 grid gap-4 md:grid-cols-2">
            {traders.map((row) => (
              <div
                key={`${row.market_id}-${row.trader_id}`}
                className="rounded-2xl border bg-gray-50 p-5"
              >
                <div className="text-sm font-medium text-gray-900">
                  <Link
                    href={`/ops/${encodeURIComponent(row.market_id)}`}
                    className="hover:text-gray-700 hover:underline"
                  >
                    {row.title ?? row.market_id}
                  </Link>
                </div>
                <div className="mt-1 break-all text-xs text-gray-500">{row.trader_id}</div>

                <div className="mt-4 grid grid-cols-3 gap-3">
                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Volume</div>
                    <div className="mt-1 text-sm font-medium text-gray-900">
                      {formatNumber(row.volume, 2)}
                    </div>
                  </div>

                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Market Share</div>
                    <div className="mt-1 text-sm font-medium text-gray-900">
                      {formatPercent(row.market_volume_share, 1)}
                    </div>
                  </div>

                  <div className="rounded-xl border bg-white p-3">
                    <div className="text-xs text-gray-500">Buy Ratio</div>
                    <div className="mt-1 text-sm font-medium text-gray-900">
                      {formatNumber(row.buy_ratio, 2)}
                    </div>
                  </div>
                </div>

                <div className="mt-4 flex flex-wrap gap-2">
                  {row.is_large_participant ? (
                    <span className="inline-flex rounded-full bg-slate-900 px-2.5 py-1 text-xs text-white">
                      large
                    </span>
                  ) : null}
                  {row.is_one_sided ? (
                    <span className="inline-flex rounded-full bg-slate-900 px-2.5 py-1 text-xs text-white">
                      one-sided
                    </span>
                  ) : null}
                  {row.is_high_frequency ? (
                    <span className="inline-flex rounded-full bg-slate-900 px-2.5 py-1 text-xs text-white">
                      high-frequency
                    </span>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-5 rounded-xl border bg-gray-50 p-4 text-sm text-gray-500">
            No trader rows returned for the selected day.
          </div>
        )}
      </section>
    </main>
  );
}