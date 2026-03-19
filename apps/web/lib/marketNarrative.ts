export type NarrativeInputs = {
  structuralState?: string | null;
  socialState?: string | null;
  alignmentState?: string | null;
  participationQuality?: number | null;
  liquidityDurability?: number | null;
  concentrationHHI?: number | null;
  whaleShare?: number | null;
  speculativeShare?: number | null;
  flags?: string[] | null;
  isSocialDemo?: boolean;
};

export function mapStructuralLabel(value?: string | null): string | null {
  switch (value) {
    case "launch_ready":
      return "strong";
    case "monitor_then_launch":
      return "mixed";
    case "watch":
      return "weak";
    case "observe":
      return "developing";
    case "not_ready":
      return "not_ready";
    default:
      return value || null;
  }
}

export function mapSocialLabel(value?: string | null): string | null {
  switch (value) {
    case "rising":
    case "high":
      return "high";
    case "forming":
    case "moderate":
      return "forming";
    case "watch":
    case "low":
      return "low";
    default:
      return value || null;
  }
}

export function mapAlignmentLabel(value?: string | null): string | null {
  switch (value) {
    case "strong":
      return "strong";
    case "mixed":
      return "divergent";
    case "weak":
      return "weak";
    default:
      return value || null;
  }
}

function mapSignalLabel(value: string): string {
  switch (value) {
    case "launch_ready":
      return "structural strength";
    case "monitor_before_launch":
    case "monitor_then_launch":
      return "mixed structure";
    case "social_demand_rising":
      return "high demo demand";
    case "social_demand_watch":
      return "watch demo demand";
    default:
      return value.replace(/_/g, " ");
  }
}

export function formatSignalsForDemo(value: unknown): string {
  if (!Array.isArray(value) || value.length === 0) return "—";

  return value
    .map((item) => String(item))
    .map(mapSignalLabel)
    .join(", ");
}

export function mapFlagLabel(flag: string): string {
  switch (flag) {
    case "STRONG_NEUTRAL_BASE":
      return "healthy neutral base";
    case "WHALE_DEPENDENCY_PRESENT":
      return "whale dependency";
    case "HIGH_MANIPULATION_RISK":
      return "manipulation risk";
    case "DEMAND_PROXY_RISING":
      return "rising demo demand";
    case "STRONG_PARTICIPATION_BASE":
      return "strong participation base";
    default:
      return flag.toLowerCase().replace(/_/g, " ");
  }
}

export function mapFlagsForDemo(flags?: string[] | null): string[] {
  if (!flags || flags.length === 0) return [];
  return flags.map(mapFlagLabel);
}

export function deriveParticipantFlags(params: {
  neutralShare?: number | null;
  whaleShare?: number | null;
  speculativeShare?: number | null;
  participationQuality?: number | null;
}): string[] {
  const {
    neutralShare = null,
    whaleShare = null,
    speculativeShare = null,
    participationQuality = null,
  } = params;

  const isHealthyNeutralBase =
    neutralShare !== null &&
    whaleShare !== null &&
    speculativeShare !== null &&
    neutralShare >= 0.7 &&
    whaleShare <= 0.2 &&
    speculativeShare <= 0.15;

  const isWhaleDependency =
    whaleShare !== null &&
    participationQuality !== null &&
    (whaleShare >= 0.35 || (whaleShare >= 0.25 && participationQuality < 0.6));

  const isSpeculativeDominance =
    speculativeShare !== null && speculativeShare >= 0.3;

  const isWeakParticipation =
    participationQuality !== null && participationQuality < 0.5;

  const flags: string[] = [];

  if (isWhaleDependency) {
    flags.push("whale dependency");
  } else if (isSpeculativeDominance) {
    flags.push("speculative dominance");
  } else if (isHealthyNeutralBase) {
    flags.push("healthy neutral base");
  }

  if (isWeakParticipation) {
    flags.push("weak participation");
  }

  return flags;
}

export function buildDecisionSummary(params: NarrativeInputs): string {
  const {
    structuralState,
    socialState,
    alignmentState,
    whaleShare,
    speculativeShare,
    participationQuality,
    liquidityDurability,
    concentrationHHI,
  } = params;

  const parts: string[] = [];

  if (structuralState === "strong" && socialState === "high") {
    parts.push("the market looks structurally strong and demand is confirming that structure");
  } else if (structuralState === "strong" && socialState === "forming") {
    parts.push("the market looks structurally strong and demand appears to be forming");
  } else if (structuralState === "strong" && socialState === "low") {
    parts.push("the market looks structurally strong, but demand remains limited relative to that structure");
  } else if (structuralState === "mixed" && socialState === "high") {
    parts.push("demand is elevated, but structure remains uneven");
  } else if (structuralState === "mixed" && socialState === "forming") {
    parts.push("demand is forming, but structure remains inconsistent");
  } else if (structuralState === "mixed" && socialState === "low") {
    parts.push("the market shows uneven structure without meaningful demand support");
  } else if (structuralState === "weak" && socialState === "high") {
    parts.push("demand is elevated, but the market remains structurally fragile");
  } else if (structuralState === "weak" && socialState === "forming") {
    parts.push("demand is emerging, but the market still lacks structural support");
  } else if (structuralState === "weak" && socialState === "low") {
    parts.push("both structure and demand remain weak");
  }

  if (alignmentState === "strong") {
    parts.push("structure and demand are aligned");
  } else if (alignmentState === "divergent") {
    parts.push("structure and demand are diverging");
  } else if (alignmentState === "weak") {
    parts.push("neither side provides strong confirmation");
  }

  if (whaleShare !== null && whaleShare >= 0.4) {
    parts.push("participation is whale-heavy");
  }

  if (speculativeShare !== null && speculativeShare >= 0.2) {
    parts.push("speculative participation is elevated");
  }

  if (liquidityDurability !== null && liquidityDurability < 0.45) {
    parts.push("liquidity durability remains fragile");
  }

  if (concentrationHHI !== null && concentrationHHI >= 0.2) {
    parts.push("concentration risk is meaningful");
  }

  if (participationQuality !== null && participationQuality >= 0.75) {
    parts.push("participation quality is solid");
  }

  if (parts.length === 0) {
    return "This market shows mixed structure and demand conditions.";
  }

  const sentence = parts.join(", ");
  return sentence.charAt(0).toUpperCase() + sentence.slice(1) + ".";
}

export function buildCardCommentary(params: NarrativeInputs): string {
  const {
    structuralState,
    socialState,
    alignmentState,
  } = params;

  if (structuralState === "strong" && socialState === "high") {
    return "Healthy structure with demand confirmation.";
  }

  if (structuralState === "strong" && socialState === "forming") {
    return "Healthy structure with demand beginning to form.";
  }

  if (structuralState === "strong" && socialState === "low") {
    return "Structurally healthy but current demand remains limited.";
  }

  if (structuralState === "mixed" && socialState === "high") {
    return "Demand is outpacing structure.";
  }

  if (structuralState === "mixed" && socialState === "forming") {
    return "Demand is forming, but structure remains uneven.";
  }

  if (structuralState === "mixed" && socialState === "low") {
    return "Uneven structure with limited demand support.";
  }

  if (structuralState === "weak" && socialState === "high") {
    return "High demand on weak structure.";
  }

  if (structuralState === "weak" && socialState === "forming") {
    return "Demand is emerging without structural support.";
  }

  if (structuralState === "weak" && socialState === "low") {
    return "Weak structure with limited demand support.";
  }

  if (alignmentState === "divergent") {
    return "Structure and demand are diverging.";
  }

  if (alignmentState === "weak") {
    return "Both structure and demand remain weak.";
  }

  return "Mixed signals across structure and demand.";
}

export function buildRecommendedAction(params: NarrativeInputs): string {
  const {
    structuralState,
    socialState,
    alignmentState,
    whaleShare,
    speculativeShare,
    participationQuality,
    liquidityDurability,
    concentrationHHI,
    isSocialDemo = false,
  } = params;

  const demandLabel = isSocialDemo
    ? "demand-side discovery"
    : "demand-side incentives";

  if (structuralState === "strong" && socialState === "low") {
    return `Prioritize ${demandLabel}. Structure is healthy, but demand is not yet matching market quality.`;
  }

  if (structuralState === "strong" && socialState === "forming") {
    return `Light ${demandLabel} may help convert early interest into stronger participation. Avoid adding heavy liquidity support where structure is already healthy.`;
  }

  if (structuralState === "strong" && socialState === "high") {
    return "No major intervention needed. Structure and demand are reinforcing each other.";
  }

  if (structuralState === "mixed" && socialState === "high") {
    return "Stabilize market structure before amplifying demand further. Improve liquidity depth, reduce concentration, and support more balanced participation.";
  }

  if (structuralState === "mixed" && socialState === "forming") {
    return "Improve structure first, then test light demand-side support. Focus on liquidity consistency and participation quality before scaling attention.";
  }

  if (structuralState === "mixed" && socialState === "low") {
    return "Do not prioritize incentives yet. First determine whether the market deserves structural improvement or should remain lower priority.";
  }

  if (structuralState === "weak" && socialState === "high") {
    return "Direct support toward liquidity quality and price formation. Demand is present, but the market remains too fragile for additional promotion.";
  }

  if (structuralState === "weak" && socialState === "forming") {
    return "Avoid scaling demand into weak structure. Improve liquidity support and participation quality before encouraging more activity.";
  }

  if (structuralState === "weak" && socialState === "low") {
    return "Low priority for incentives. Consider redesign, relaunch, or deprioritization rather than capital deployment.";
  }

  if (alignmentState === "divergent") {
    if (whaleShare !== null && whaleShare >= 0.35) {
      return "Reduce whale dependency before expanding incentives. Market quality is being shaped by concentrated participation.";
    }

    if (speculativeShare !== null && speculativeShare >= 0.3) {
      return "Avoid rewarding raw activity. First reduce speculative dominance and improve participation quality.";
    }

    if (liquidityDurability !== null && liquidityDurability < 0.45) {
      return "Support liquidity durability first. The market is not stable enough for broader demand expansion.";
    }

    return "Treat this as a divergence case. Diagnose whether the bottleneck is liquidity quality, concentration, or lack of genuine demand before allocating incentives.";
  }

  if (concentrationHHI !== null && concentrationHHI >= 0.2) {
    return "Target broader participation rather than headline activity. Concentration risk is too high for untargeted incentives.";
  }

  if (participationQuality !== null && participationQuality < 0.5) {
    return "Focus on improving participation quality before scaling incentives. Current activity is not reliable enough.";
  }

  return "No clear action recommendation yet. Review structure, demand, and participant mix together before deploying incentives.";
}

export type DemoSocialState = "high" | "forming" | "low";
export type DisplayStructuralState = "strong" | "mixed" | "weak" | null;
export type DisplayAlignmentState = "strong" | "divergent" | "weak";

export function hashString(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

export function getDemoSocialSignal(marketId: string): DemoSocialState {
  const hash = hashString(marketId) % 100;

  if (hash < 34) return "high";
  if (hash < 68) return "forming";
  return "low";
}

export function deriveDisplayStructuralState(params: {
  structuralScore?: number | null;
  participationQuality?: number | null;
  liquidityDurability?: number | null;
  concentrationHHI?: number | null;
  fallbackRecommendation?: string | null;
}): DisplayStructuralState {
  const {
    structuralScore = null,
    participationQuality = null,
    liquidityDurability = null,
    concentrationHHI = null,
    fallbackRecommendation = null,
  } = params;

  if (
    structuralScore !== null &&
    structuralScore >= 0.55 &&
    (participationQuality === null || participationQuality >= 0.65) &&
    (liquidityDurability === null || liquidityDurability >= 0.55) &&
    (concentrationHHI === null || concentrationHHI < 0.15)
  ) {
    return "strong";
  }

  if (structuralScore !== null && structuralScore < 0.45) {
    return "weak";
  }

  if (structuralScore !== null) {
    return "mixed";
  }

  const mapped = mapStructuralLabel(fallbackRecommendation);
  if (mapped === "strong" || mapped === "mixed" || mapped === "weak") {
    return mapped;
  }

  return null;
}

export function deriveDisplayAlignmentState(
  structuralState?: string | null,
  socialState?: DemoSocialState
): DisplayAlignmentState {
  if (structuralState === "strong" && socialState === "high") return "strong";
  if (structuralState === "weak" && socialState === "low") return "weak";
  return "divergent";
}

export function formatDisplaySignals(params: {
  structuralState?: string | null;
  socialState?: DemoSocialState;
}): string {
  const { structuralState, socialState } = params;

  const structuralLabel =
    structuralState === "strong"
      ? "strong structure"
      : structuralState === "mixed"
      ? "mixed structure"
      : structuralState === "weak"
      ? "weak structure"
      : "unknown structure";

  const socialLabel =
    socialState === "high"
      ? "high demo demand"
      : socialState === "forming"
      ? "forming demo demand"
      : "low demo demand";

  return `${structuralLabel}, ${socialLabel}`;
}