import Link from "next/link";
import SectionCard from "@/components/SectionCard";
import KeyValueTable from "@/components/KeyValueTable";
import { getMarketSnapshot } from "@/lib/api";
import {
  formatPercent01,
  formatMetricValue,
} from "@/lib/format";
import {
  buildDecisionSummary,
  buildRecommendedAction,
  deriveDisplayAlignmentState,
  deriveDisplayStructuralState,
  deriveParticipantFlags,
  formatDisplaySignals,
  getDemoSocialSignal,
  mapFlagsForDemo,
} from "@/lib/marketNarrative";

type PageProps = {
  params: Promise<{
    marketId: string;
  }>;
};

type CohortItem = {
  cohort?: string;
  traders?: number;
  unique_traders?: number;
  trader_count?: number;
  trades?: number;
  trade_count?: number;
  notional_total?: number;
  notional?: number;
  avg_trade_size?: number;
  average_trade_size?: number;
};

function getStatusPillClass(
  value?: string | null,
  type?: "structural" | "social" | "alignment"
) {
  if (type === "social") {
    switch (value) {
      case "high":
        return "bg-violet-100 text-violet-700";
      case "forming":
      case "developing":
        return "bg-purple-100 text-purple-700";
      case "low":
        return "bg-fuchsia-100 text-fuchsia-700";
      default:
        return "bg-violet-100 text-violet-700";
    }
  }

  if (type === "alignment") {
    switch (value) {
      case "strong":
        return "bg-emerald-100 text-emerald-700";
      case "divergent":
        return "bg-amber-100 text-amber-700";
      case "weak":
        return "bg-zinc-100 text-zinc-700";
      default:
        return "bg-zinc-100 text-zinc-700";
    }
  }

  switch (value) {
    case "strong":
      return "bg-emerald-100 text-emerald-700";
    case "mixed":
      return "bg-amber-100 text-amber-700";
    case "weak":
      return "bg-zinc-100 text-zinc-700";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function StatusPill({
  label,
  value,
  type = "structural",
  emphasis = "secondary",
}: {
  label: string;
  value?: string | null;
  type?: "structural" | "social" | "alignment";
  emphasis?: "primary" | "secondary";
}) {
  if (!value) return null;

  const sizeClass =
    emphasis === "primary"
      ? "h-10 rounded-xl px-4 text-sm font-semibold"
      : "h-8 rounded-lg px-3 text-xs font-medium";

  return (
    <span
      className={`inline-flex items-center whitespace-nowrap ${sizeClass} ${getStatusPillClass(
        value,
        type
      )}`}
    >
      {label}: {value}
    </span>
  );
}

function asCohortArray(value: unknown): CohortItem[] {
  if (!Array.isArray(value)) return [];

  return value.filter(
    (item): item is CohortItem =>
      typeof item === "object" && item !== null
  );
}

function normalizeCohortName(value: string): string {
  const v = value.toLowerCase();
  if (v === "speculator") return "speculative";
  return v;
}

function mergeCohorts(
  sameDayValue: unknown,
  rollingWindowValue: unknown
): Map<string, { traders: number; trades: number; notional: number; avgTradeSize: number }> {
  const combined = [
    ...asCohortArray(sameDayValue),
    ...asCohortArray(rollingWindowValue),
  ];

  const merged = new Map<
    string,
    { traders: number; trades: number; notional: number; avgTradeSize: number }
  >();

  for (const item of combined) {
    const cohortRaw = item.cohort || "unknown";
    const cohort = normalizeCohortName(String(cohortRaw));

    const traders =
      Number(item.traders ?? item.unique_traders ?? item.trader_count ?? 0) || 0;

    const trades =
      Number(item.trades ?? item.trade_count ?? 0) || 0;

    const notional =
      Number(item.notional_total ?? item.notional ?? 0) || 0;

    const avgTradeSize =
      Number(item.avg_trade_size ?? item.average_trade_size ?? 0) || 0;

    const current = merged.get(cohort) || {
      traders: 0,
      trades: 0,
      notional: 0,
      avgTradeSize: 0,
    };

    current.traders = Math.max(current.traders, traders);
    current.trades = Math.max(current.trades, trades);
    current.notional = Math.max(current.notional, notional);
    current.avgTradeSize = Math.max(current.avgTradeSize, avgTradeSize);

    merged.set(cohort, current);
  }

  return merged;
}

function formatCohortSummary(
  merged: Map<string, { traders: number; trades: number; notional: number; avgTradeSize: number }>
): string {
  if (merged.size === 0) return "—";

  const ordered = Array.from(merged.entries()).sort((a, b) => {
    const sizeDiff = b[1].avgTradeSize - a[1].avgTradeSize;
    if (sizeDiff !== 0) return sizeDiff;
    return b[1].traders - a[1].traders;
  });

  return ordered
    .map(([cohort, stats]) => {
      const traders = stats.traders.toLocaleString();
      const avgSize = stats.avgTradeSize.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      });

      return `${cohort}: ${traders} traders, avg size ${avgSize}`;
    })
    .join(" • ");
}

function cohortShare(
  merged: Map<string, { traders: number; trades: number; notional: number; avgTradeSize: number }>,
  cohortName: string
): number | null {
  const totalTraders = Array.from(merged.values()).reduce(
    (sum, item) => sum + item.traders,
    0
  );

  if (totalTraders <= 0) return null;

  const cohort = merged.get(cohortName);
  if (!cohort) return 0;

  return cohort.traders / totalTraders;
}

export default async function MarketDetailPage({ params }: PageProps) {
  const { marketId } = await params;

  let snapshot;
  let error: string | null = null;

  try {
    snapshot = await getMarketSnapshot(marketId);
  } catch (err) {
    error = err instanceof Error ? err.message : "Failed to load market snapshot";
  }

  if (error || !snapshot) {
    return (
      <div className="mx-auto max-w-5xl space-y-4">
        <Link href="/" className="text-sm font-medium text-blue-600 hover:underline">
          ← Back to overview
        </Link>

        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 text-red-700">
          {error || "Unable to load market snapshot"}
        </div>
      </div>
    );
  }

  const market = snapshot.market || {};
  const launch = snapshot.launch_intelligence || {};
  const social = snapshot.social_intelligence || {};
  const sameDay = snapshot.traders?.same_day || {};
  const rollingWindow = snapshot.traders?.rolling_window || {};

  const participationQuality =
    launch.participation_quality_score !== null &&
    launch.participation_quality_score !== undefined &&
    !Number.isNaN(Number(launch.participation_quality_score))
      ? Number(launch.participation_quality_score)
      : null;

  const liquidityDurability =
    launch.liquidity_durability_score !== null &&
    launch.liquidity_durability_score !== undefined &&
    !Number.isNaN(Number(launch.liquidity_durability_score))
      ? Number(launch.liquidity_durability_score)
      : null;

  const concentrationHHI =
    market.concentration_hhi !== null &&
    market.concentration_hhi !== undefined &&
    !Number.isNaN(Number(market.concentration_hhi))
      ? Number(market.concentration_hhi)
      : null;

  const structuralState = deriveDisplayStructuralState({
    structuralScore:
      launch.launch_readiness_score !== null &&
      launch.launch_readiness_score !== undefined &&
      !Number.isNaN(Number(launch.launch_readiness_score))
        ? Number(launch.launch_readiness_score)
        : null,
    participationQuality,
    liquidityDurability,
    concentrationHHI,
    fallbackRecommendation: launch.recommendation ?? null,
  });

  const socialState = getDemoSocialSignal(marketId);

  const alignmentState = deriveDisplayAlignmentState(
    structuralState,
    socialState
  );

  const mergedCohorts = mergeCohorts(
    sameDay.cohorts_summary,
    rollingWindow.cohorts_summary
  );
  const combinedCohorts = formatCohortSummary(mergedCohorts);

  const neutralShare = cohortShare(mergedCohorts, "neutral");
  const whaleShare = cohortShare(mergedCohorts, "whale");
  const speculativeShare = cohortShare(mergedCohorts, "speculative");

  const totalTraders = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.traders,
    0
  );

  const totalTrades = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.trades,
    0
  );

  const totalVolume = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.notional,
    0
  );

  const formattedTotalTraders =
    totalTraders > 0 ? totalTraders.toLocaleString() : "Not available";

  const formattedTotalTrades =
    totalTrades > 0 ? totalTrades.toLocaleString() : "Not available";

  const formattedTotalVolume =
    totalVolume > 0
      ? totalVolume.toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: 2,
        })
      : "Not available";

  const decisionSummaryText = buildDecisionSummary({
    structuralState,
    socialState,
    alignmentState,
    whaleShare,
    speculativeShare,
    participationQuality,
    liquidityDurability,
    concentrationHHI,
    isSocialDemo: true,
  });

  const recommendedAction = buildRecommendedAction({
    structuralState,
    socialState,
    alignmentState,
    whaleShare,
    speculativeShare,
    participationQuality,
    liquidityDurability,
    concentrationHHI,
    isSocialDemo: true,
  });

  const participantFlags = deriveParticipantFlags({
    neutralShare,
    whaleShare,
    speculativeShare,
    participationQuality,
  });

  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <div>
        <Link href="/" className="text-sm font-medium text-blue-600 hover:underline">
          ← Back to overview
        </Link>
      </div>

      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Market Detail TEST 123</p>

        <div className="mt-1 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-zinc-900">
              {market.title || marketId}
            </h1>

            <p className="mt-2 text-sm text-zinc-600">
              {market.protocol || "—"} · {market.chain || "—"}
            </p>

            {market.url ? (
              <div className="mt-3">
                <a
                  href={market.url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-sm font-medium text-blue-600 hover:underline"
                >
                  Open external market
                </a>
              </div>
            ) : null}
          </div>

          <div className="flex flex-col items-end gap-2">
            <div className="group relative">
              <StatusPill
                label="alignment"
                value={alignmentState}
                type="alignment"
                emphasis="primary"
              />

              <div className="absolute right-0 top-12 z-20 hidden w-72 rounded-xl border border-zinc-200 bg-white p-3 text-xs text-zinc-600 shadow-lg group-hover:block">
                <p className="font-semibold text-zinc-900">Alignment</p>
                <p className="mt-1 text-zinc-600">
                  How well structure and demand agree.
                </p>
                <div className="mt-2 space-y-1">
                  <p>
                    <span className="font-medium text-zinc-900">Strong</span> → good structure + strong demand
                  </p>
                  <p>
                    <span className="font-medium text-zinc-900">Divergent</span> → structure and demand disagree
                  </p>
                  <p>
                    <span className="font-medium text-zinc-900">Weak</span> → both are weak or misleading
                  </p>
                </div>
              </div>
            </div>

            <div className="flex flex-wrap justify-end gap-2">
              <StatusPill
                label="structural"
                value={structuralState}
                type="structural"
              />
              <StatusPill
                label="social demo"
                value={socialState}
                type="social"
              />
            </div>
          </div>
        </div>

        <p className="mt-4 max-w-3xl text-sm text-zinc-600">
          This page summarizes structural quality, participant mix, and demo demand
          signals to show whether a live market looks healthy, fragile, or worth
          monitoring more closely.
        </p>

        <p className="mt-2 max-w-3xl text-sm italic text-zinc-500">
          Alignment compares structural quality and demand. Strong alignment suggests
          both agree, while divergence may signal fragility, manipulation, or mispricing.
        </p>
      </section>

      <SectionCard title="Decision Summary">
        <p className="mb-4 text-xs italic text-zinc-500">
          High-level interpretation of structural quality, demo demand signal, and overall alignment
        </p>

        <KeyValueTable
          rows={[
            { label: "Structural State", value: structuralState || "—" },
            { label: "Social Demo Signal", value: socialState || "—" },
            { label: "Alignment", value: alignmentState || "—" },
            { label: "Market View", value: structuralState || "—" },
            { label: "Summary", value: decisionSummaryText },
            { label: "Recommended Action", value: recommendedAction },
            {
              label: "Key Signals",
              value: formatDisplaySignals({
                structuralState,
                socialState,
              }),
            },
          ]}
        />
      </SectionCard>

      <div className="grid gap-8 xl:grid-cols-2">
        <SectionCard title="Structural Health">
          <p className="mb-4 text-xs italic text-zinc-500">
            Structural strength here reflects liquidity quality, participation quality, concentration, and durability
          </p>

          <KeyValueTable
            rows={[
              { label: "Integrity Band", value: market.integrity_band || "—" },
              { label: "Review Priority", value: market.review_priority || "—" },
              { label: "Spread Median", value: formatMetricValue(market.spread_median) },
              { label: "Depth 2pct Median", value: formatMetricValue(market.depth_2pct_median) },
              { label: "Concentration HHI", value: formatMetricValue(market.concentration_hhi) },
              { label: "Structural Score", value: formatMetricValue(launch.launch_readiness_score) },
              { label: "Structural Risk", value: formatMetricValue(launch.launch_risk_score) },
              { label: "Liquidity Durability", value: formatMetricValue(launch.liquidity_durability_score) },
              { label: "Participation Quality", value: formatMetricValue(launch.participation_quality_score) },
              { label: "Flags", value: mapFlagsForDemo(launch.flags).join(", ") || "—" },
            ]}
          />
        </SectionCard>

        <SectionCard title="Demand Signals (Demo)">
          <p className="mb-4 text-xs italic text-violet-600">
            Experimental proxy for demo only, not live social ingestion
          </p>

          <KeyValueTable
            rows={[
              { label: "Social Demo Signal", value: socialState || "—" },
              { label: "Summary", value: social.summary || "—" },
              { label: "Attention Score", value: formatMetricValue(social.attention_score) },
              { label: "Demand Proxy Score (Demo)", value: formatMetricValue(social.demand_score) },
              { label: "Sentiment Score", value: formatMetricValue(social.sentiment_score) },
              { label: "Trend Velocity", value: formatMetricValue(social.trend_velocity) },
              { label: "Demo Confidence", value: formatMetricValue(social.confidence_score) },
              { label: "Flags", value: mapFlagsForDemo(social.flags).join(", ") || "—" },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="Participant Signals">
        <p className="mb-4 text-xs italic text-zinc-500">
          Participant mix derived from cohort composition across the current observation window
        </p>

        <KeyValueTable
          rows={[
            {
              label: "Participation Quality",
              value: formatMetricValue(launch.participation_quality_score),
            },
            {
              label: "Total Traders",
              value: formattedTotalTraders,
            },
            {
              label: "Total Trades",
              value: formattedTotalTrades,
            },
            {
              label: "Total Volume",
              value: formattedTotalVolume,
            },
            {
              label: "Neutral Share",
              value: neutralShare !== null ? formatPercent01(neutralShare) : "Not available",
            },
            {
              label: "Whale Share",
              value: whaleShare !== null ? formatPercent01(whaleShare) : "Not available",
            },
            {
              label: "Speculative Share",
              value:
                speculativeShare !== null
                  ? formatPercent01(speculativeShare)
                  : "Not available",
            },
            {
              label: "Participant Flags",
              value: participantFlags.length > 0 ? participantFlags.join(", ") : "—",
            },
            {
              label: "Cohort Summary",
              value: combinedCohorts || "—",
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}