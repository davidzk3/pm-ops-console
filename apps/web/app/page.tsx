import CandidateCard from "@/components/CandidateCard";
import { getLaunchCandidates, getSocialCandidates } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import {
  buildCardCommentary,
  deriveDisplayAlignmentState,
  deriveDisplayStructuralState,
  deriveParticipantFlags,
  getDemoSocialSignal,
} from "@/lib/marketNarrative";
import { LaunchCandidate, SocialCandidate } from "@/lib/types";

type MixedMarket = {
  market_id: string;
  title?: string | null;
  category?: string | null;
  url?: string | null;
  structural_recommendation?: string | null;
  structural_score?: number | null;
  summary?: string | null;
  flags?: string[] | null;
  participation_quality_score?: number | null;
  liquidity_durability_score?: number | null;
  concentration_hhi?: number | null;
  neutral_share?: number | null;
  whale_share?: number | null;
  speculative_share?: number | null;
};

function mergeMarkets(
  launchCandidates: LaunchCandidate[],
  socialCandidates: SocialCandidate[],
  limit = 100
): MixedMarket[] {
  const merged = new Map<string, MixedMarket>();

  for (const item of launchCandidates) {
    merged.set(item.market_id, {
      market_id: item.market_id,
      title: item.title,
      category: item.category,
      url: item.url,
      structural_recommendation: item.recommendation,
      structural_score: item.launch_readiness_score,
      flags: item.flags,
      participation_quality_score:
        (item as any).participation_quality_score ?? null,
      liquidity_durability_score:
        (item as any).liquidity_durability_score ?? null,
      concentration_hhi: (item as any).concentration_hhi ?? null,
      neutral_share: (item as any).neutral_share ?? null,
      whale_share: (item as any).whale_share ?? null,
      speculative_share: (item as any).speculative_share ?? null,
    });
  }

  for (const item of socialCandidates) {
    const existing = merged.get(item.market_id);

    if (existing) {
      merged.set(item.market_id, {
        ...existing,
        title: existing.title || item.title,
        category: existing.category || item.category,
        url: existing.url || item.url,
        flags: existing.flags || item.flags,
      });
    } else {
      merged.set(item.market_id, {
        market_id: item.market_id,
        title: item.title,
        category: item.category,
        url: item.url,
        structural_recommendation: "observe",
        structural_score: null,
        flags: item.flags,
        participation_quality_score: null,
        liquidity_durability_score: null,
        concentration_hhi: null,
        neutral_share: null,
        whale_share: null,
        speculative_share: null,
      });
    }
  }

  return Array.from(merged.values()).slice(0, limit);
}

export default async function HomePage() {
  let launchCandidates: LaunchCandidate[] = [];
  let socialCandidates: SocialCandidate[] = [];
  let pageError: string | null = null;

  try {
    launchCandidates = await getLaunchCandidates(100);
  } catch (err) {
    pageError =
      err instanceof Error ? err.message : "Failed to load structural markets";
  }

  try {
    socialCandidates = await getSocialCandidates(100);
  } catch {
    // social is simulated for demo purposes
  }

  const mergedMarkets = mergeMarkets(launchCandidates, socialCandidates, 100);

  const markets = mergedMarkets
    .filter((m) => m.url && m.url.trim() !== "")
    .filter((m) => m.structural_recommendation !== "not_ready")
    .slice(0, 30);

  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">
          Prediction Market Intelligence
        </p>

        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          Markets across structural quality and demo demand signals
        </h1>

        <p className="mt-2 max-w-3xl text-sm text-zinc-600">
          This demo evaluates live markets along two separate dimensions.
          Structural strength reflects liquidity quality, participation quality,
          concentration, and durability, not volume alone. Social signal used is
          simulated for demo purposes only and does not represent live social
          ingestion.
        </p>
      </section>

      {pageError ? (
        <section className="rounded-2xl border border-red-200 bg-red-50 p-5 text-red-700 shadow-sm">
          {pageError}
        </section>
      ) : markets.length === 0 ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 text-zinc-600 shadow-sm">
          No markets found.
        </section>
      ) : (
        <section className="space-y-4">
          <div className="flex items-end justify-between gap-4">
            <div>
              <h2 className="text-xl font-semibold text-zinc-900">
                Market Explorer
              </h2>
              <p className="mt-1 text-sm italic text-zinc-500">
                Mixed live markets with real structural scoring and simulated social demo signals
              </p>
            </div>
            <p className="text-sm text-zinc-500">{markets.length} markets</p>
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            {markets.map((item) => {
              const structuralState = deriveDisplayStructuralState({
                structuralScore: item.structural_score ?? null,
                participationQuality: item.participation_quality_score ?? null,
                liquidityDurability: item.liquidity_durability_score ?? null,
                concentrationHHI: item.concentration_hhi ?? null,
                fallbackRecommendation: item.structural_recommendation ?? null,
              });

              const socialSignal = getDemoSocialSignal(item.market_id);

              const alignmentState = deriveDisplayAlignmentState(
                structuralState,
                socialSignal
              );

              return (
                <CandidateCard
                  key={item.market_id}
                  marketId={item.market_id}
                  title={item.title}
                  category={item.category}
                  structuralState={structuralState}
                  socialSignal={socialSignal}
                  scoreLabel="Structural quality score"
                  scoreValue={
                    item.structural_score !== null &&
                    item.structural_score !== undefined
                      ? formatNumber(item.structural_score)
                      : "—"
                  }
                  summary={buildCardCommentary({
                    structuralState,
                    socialState: socialSignal,
                    alignmentState,
                    participationQuality:
                      item.participation_quality_score ?? null,
                    liquidityDurability:
                      item.liquidity_durability_score ?? null,
                    concentrationHHI: item.concentration_hhi ?? null,
                    whaleShare: item.whale_share ?? null,
                    speculativeShare: item.speculative_share ?? null,
                    isSocialDemo: true,
                  })}
                  flags={deriveParticipantFlags({
                    neutralShare: item.neutral_share ?? null,
                    whaleShare: item.whale_share ?? null,
                    speculativeShare: item.speculative_share ?? null,
                    participationQuality:
                      item.participation_quality_score ?? null,
                  })}
                  url={item.url}
                />
              );
            })}
          </div>
        </section>
      )}
    </div>
  );
}