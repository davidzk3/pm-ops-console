export type SnapshotResponse = {
  market: {
    market_id: string
    protocol: string
    chain: string
    title: string
    category: string | null
    url: string
    day: string
    volume: number | null
    trades: number | null
    unique_traders: number | null
    spread_median: number | null
    depth_2pct_median: number | null
    concentration_hhi: number | null
    health_score: number | null
    risk_score: number | null
    has_manual_override: boolean
    flags: string[]
    regime: string | null
    regime_reason: string | null
    market_quality_score: number | null
    liquidity_health_score: number | null
    concentration_risk_score: number | null
    whale_volume_share: number | null
    radar_risk_score: number | null
    manipulation_score: number | null
    manipulation_signal: string | null
    whale_role_share: number | null
    speculator_role_share: number | null
    neutral_role_share: number | null
    possible_farmer_count: number | null
    integrity_score: number | null
    integrity_band: string | null
    review_priority: string | null
    primary_reason: string | null
    needs_operator_review: boolean
    has_regime_data: boolean
    has_radar_data: boolean
    has_manipulation_data: boolean
    data_completeness_score: number | null
    is_partial_coverage: boolean
  }

  timeline: Array<{
    day: string
    volume: number | null
    trades: number | null
    unique_traders: number | null
    spread_median: number | null
    depth_2pct_median: number | null
    concentration_hhi: number | null
    health_score: number | null
    risk_score: number | null
  }>

  incidents: Array<{
    id: number
    market_id: string
    day: string
    status: string
    note: string | null
    created_by: string | null
    created_at: string
  }>

  incident_events: Array<{
    id: number
    incident_id: number
    market_id: string
    day: string
    event_type: string
    from_status: string | null
    to_status: string | null
    note: string | null
    created_by: string | null
    created_at: string
  }>

  incident_effectiveness: Array<{
    incident_id: number
    market_id: string
    day: string
    status: string
    note: string | null
    created_by: string | null
    created_at: string
    before_day: string | null
    after_day: string | null
    before: Record<string, number | null>
    after: Record<string, number | null>
    delta: Record<string, number | null>
    delta_score: number | null
  }>

  interventions: Array<{
    market_id: string
    day: string
    action_code: string
    action_count: number
    first_created_at: string
    last_created_at: string
    id: number
    incident_id: number | null
    title: string
    status: string
    params: Record<string, unknown> | null
    created_by: string | null
    created_at: string
    applied_at: string | null
  }>

  interventions_effectiveness: Array<{
    id: number
    market_id: string
    incident_id: number | null
    day: string
    action_code: string
    title: string
    status: string
    params: Record<string, unknown> | null
    created_by: string | null
    created_at: string
    applied_at: string | null
    applied_day: string | null
    action_count: number
    first_created_at: string
    last_created_at: string
    before_day: string | null
    after_day: string | null
    before: Record<string, number | null>
    after: Record<string, number | null>
    delta: Record<string, number | null>
    delta_score: number | null
    roi_score: number | null
  }>

  interventions_effectiveness_ui?: {
    heat?: {
      good_up?: string[]
      good_down?: string[]
      steps?: Record<string, number>
      precision?: Record<string, number>
    }
  }

  intervention_cumulative?: {
    days: number
    count_total: number
    count_effective: number
    risk_score: number | null
    health_score: number | null
    spread_median: number | null
    depth_2pct_median: number | null
  }

  overrides: unknown[]

  traders: {
    summary: unknown[]
    cohorts_summary: unknown[]
    intelligence: unknown[]
  }

  impact?: {
    window_days: number
    recent_window: { start: string; end: string }
    prior_window: { start: string; end: string }
    market_quality_delta: {
      spread_median_delta: number | null
      depth_2pct_delta: number | null
      concentration_hhi_delta: number | null
      unique_traders_delta: number | null
      health_score_delta: number | null
    }
    recent_cohort_share: unknown[]
    prior_cohort_share: unknown[]
    cohort_share_delta: unknown[]
    diagnosis: string | null
    market_regime: string | null
    cohort_risk_flags: string[]
  }

  errors: string[]

  coverage_summary: {
    has_timeline: boolean
    has_integrity_history: boolean
    has_impact: boolean
    has_trader_summary: boolean
    has_cohort_summary: boolean
    has_trader_intelligence: boolean
    has_incidents: boolean
    has_interventions: boolean
    has_overrides: boolean
    coverage_level: string
    coverage_reason: string
    downstream_coverage_count: number
  }
}

export type DemoViewModel = {
  header: {
    title: string
    subtitle: string
    marketTitle: string
    marketId: string
    protocol: string
    chain: string
    reviewWindowStatus: string
    structuralOnlyNote: string
  }

  verification: {
    ruleSummary: string
    primarySources: string[]
    resolutionPosture: string
    ambiguityNotes: string[]
    note: string
  }

  reviewContext: {
    cautionLabel: string
    cautionLevel: string
    contextConfidence: string
    recommendedAction: string
    rationale: string[]
  }

  structure: {
    spreadMedian: string
    healthScore: string
    riskScore: string
    regime: string
    regimeReason: string
    integrityBand: string
    reviewPriority: string
    primaryReason: string
    integrityScore: string
    liquidityHealthScore: string
    marketQualityScore: string
    note: string | null
  }

  timeline: Array<{
    label: string
    spread: number | null
    health: number | null
    risk: number | null
  }>
}

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Sparse coverage"
  }
  return value.toFixed(digits)
}

function formatDayLabel(day?: string): string {
  if (!day) return "Unknown"
  const d = new Date(day)
  if (Number.isNaN(d.getTime())) return day
  return d.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  })
}

function computeContextConfidence(snapshot: SnapshotResponse): string {
  const market = snapshot.market
  const coverage = snapshot.coverage_summary

  const keyMetricsAvailable =
    market.spread_median !== null &&
    market.health_score !== null &&
    market.risk_score !== null

  if (keyMetricsAvailable && coverage.has_timeline) {
    return "Useful structural context"
  }

  if (keyMetricsAvailable) {
    return "Partial structural context"
  }

  return "Sparse structural context"
}

function computeReviewContext(snapshot: SnapshotResponse) {
  const market = snapshot.market
  const coverage = snapshot.coverage_summary
  const rationale: string[] = []

  let severity = 0

  if (market.needs_operator_review) {
    severity += 2
    rationale.push("operator review required")
  }

  if (market.health_score !== null && market.health_score < 0.3) {
    severity += 2
    rationale.push("weak market health")
  }

  if (market.risk_score !== null && market.risk_score >= 80) {
    severity += 2
    rationale.push("high structural risk")
  }

  if (
    market.spread_median !== null &&
    market.spread_median !== undefined &&
    market.spread_median >= 0.01
  ) {
    severity += 1
    rationale.push("wider spread")
  }

  if (market.regime === "thin_market") {
    severity += 2
    rationale.push("thin market structure")
  }

  if (market.regime === "whale_dominated") {
    severity += 1
    rationale.push("concentrated participation")
  }

  if (
    market.manipulation_signal &&
    market.manipulation_signal !== "none" &&
    market.manipulation_signal !== "inactive_market"
  ) {
    severity += 2
    rationale.push(`manipulation signal: ${market.manipulation_signal}`)
  }

  if (market.is_partial_coverage || coverage.coverage_level === "partial") {
    severity += 1
    rationale.push("partial downstream coverage")
  }

  if (market.primary_reason) {
    rationale.push(market.primary_reason)
  }

  const uniqueRationale = [...new Set(rationale)]

  if (severity >= 5) {
    return {
      cautionLabel: "Manual Review Recommended",
      cautionLevel: "High",
      contextConfidence: computeContextConfidence(snapshot),
      recommendedAction: "Escalate for manual scrutiny",
      rationale: uniqueRationale,
    }
  }

  if (severity >= 3) {
    return {
      cautionLabel: "Review With Caution",
      cautionLevel: "Elevated",
      contextConfidence: computeContextConfidence(snapshot),
      recommendedAction: "Review proposal with caution",
      rationale: uniqueRationale,
    }
  }

  return {
    cautionLabel: "Standard Review",
    cautionLevel: "Moderate",
    contextConfidence: computeContextConfidence(snapshot),
    recommendedAction: "Proceed with standard verification checks",
    rationale: uniqueRationale.length ? uniqueRationale : ["no major structural alert"],
  }
}

function deriveRuleSummary(snapshot: SnapshotResponse): string {
  const title = snapshot.market.title?.toLowerCase() ?? ""

  if (title.includes("jon stewart") && title.includes("nomination")) {
    return "Market resolves based on whether Jon Stewart becomes the official Democratic presidential nominee in 2028."
  }

  if (title.includes("arsenal") && title.includes("premier league")) {
    return "Market resolves based on whether Arsenal are the official winners of the 2025–26 English Premier League."
  }

  if (title.includes("minnesota timberwolves") && title.includes("western conference finals")) {
    return "Market resolves based on whether the Minnesota Timberwolves are the official winners of the NBA Western Conference Finals."
  }

  if (title.startsWith("will ") && title.includes(" win ")) {
    return "Market resolves based on whether the named subject is the official winner of the referenced event."
  }

  return "Derived rule summary unavailable. Reviewer should consult the original market rules before making a final decision."
}

function derivePrimarySources(snapshot: SnapshotResponse): string[] {
  const title = snapshot.market.title?.toLowerCase() ?? ""

  if (title.includes("premier league") || title.includes("arsenal")) {
    return [
      "Premier League official final standings",
      "Official competition records",
      "Reputable sports data providers for secondary confirmation",
    ]
  }

  if (title.includes("western conference finals") || title.includes("minnesota timberwolves")) {
    return [
      "NBA official playoff results",
      "Official conference finals records",
      "Reputable sports data providers for secondary confirmation",
    ]
  }

  if (title.includes("nomination") || title.includes("election")) {
    return [
      "Official election or party authority source",
      "Protocol market rules and resolution source",
      "Reputable secondary reporting only as confirmation",
    ]
  }

  return [
    "Official primary source for the event outcome",
    "Protocol market rules and resolution source",
    "Reputable secondary reporting only as confirmation",
  ]
}

function deriveResolutionPosture(snapshot: SnapshotResponse): string {
  const title = snapshot.market.title?.toLowerCase() ?? ""

  if (
    title.includes("2028") ||
    title.includes("2026") ||
    title.includes("2025–26") ||
    title.includes("2025-26")
  ) {
    return "This event appears unresolved in the current sample. Any proposal should be checked against official final results once the event has concluded."
  }

  return "Reviewer should verify whether the event has concluded and whether the proposal matches the official outcome."
}

function deriveAmbiguityNotes(snapshot: SnapshotResponse): string[] {
  const title = snapshot.market.title?.toLowerCase() ?? ""
  const notes: string[] = []

  if (title.includes("win")) {
    notes.push("Confirm what counts as an official win under the market rules.")
  }

  if (title.includes("premier league")) {
    notes.push("Check whether edge cases such as points deductions, voided matches, or competition cancellation are addressed in the original rules.")
  }

  if (title.includes("nomination")) {
    notes.push("Confirm whether nomination means formal party nomination, presumptive nominee status, or another protocol-defined standard.")
  }

  if (title.includes("western conference finals")) {
    notes.push("Confirm whether the market refers specifically to the official conference finals winner rather than broader playoff advancement.")
  }

  if (notes.length === 0) {
    notes.push("Reviewer should inspect the original market wording for undefined edge cases.")
  }

  return notes
}

export function mapSnapshotToDemoViewModel(
  snapshot: SnapshotResponse
): DemoViewModel {
  const market = snapshot.market
  const coverage = snapshot.coverage_summary
  const reviewContext = computeReviewContext(snapshot)

  let structureNote: string | null = null
  if (
    market.spread_median === null ||
    market.health_score === null ||
    market.risk_score === null
  ) {
    structureNote =
      "Some structural fields are not populated for this market, so reviewer judgment should rely more heavily on rules and official evidence than on market structure."
  } else if (coverage.coverage_level === "partial" || market.is_partial_coverage) {
    structureNote =
      "Structural signals are available, but overall downstream coverage remains partial."
  }

  return {
    header: {
      title: "UMA Resolution Support Demo",
      subtitle:
        "Structural context for reviewing flagged prediction market proposals during the optimistic window",
      marketTitle: market.title ?? "Unknown market",
      marketId: market.market_id ?? "Unknown market ID",
      protocol: market.protocol ?? "Unknown protocol",
      chain: market.chain ?? "Unknown chain",
      reviewWindowStatus: market.needs_operator_review
        ? "Flagged for reviewer inspection"
        : "No active review flag",
      structuralOnlyNote: "Structural context only. Not an outcome resolver.",
    },

    verification: {
      ruleSummary: deriveRuleSummary(snapshot),
      primarySources: derivePrimarySources(snapshot),
      resolutionPosture: deriveResolutionPosture(snapshot),
      ambiguityNotes: deriveAmbiguityNotes(snapshot),
      note: "Derived reviewer guidance from market title. Final resolution still depends on official market rules and evidence.",
    },

    reviewContext,

    structure: {
      spreadMedian: formatNumber(market.spread_median, 3),
      healthScore: formatNumber(market.health_score, 3),
      riskScore: formatNumber(market.risk_score, 1),
      regime: market.regime ?? "Sparse coverage",
      regimeReason: market.regime_reason ?? "Sparse coverage",
      integrityBand: market.integrity_band ?? "Sparse coverage",
      reviewPriority: market.review_priority ?? "Sparse coverage",
      primaryReason: market.primary_reason ?? "Sparse coverage",
      integrityScore: formatNumber(market.integrity_score, 1),
      liquidityHealthScore: formatNumber(market.liquidity_health_score, 3),
      marketQualityScore: formatNumber(market.market_quality_score, 3),
      note: structureNote,
    },

    timeline: snapshot.timeline.map((point) => ({
      label: formatDayLabel(point.day),
      spread: point.spread_median ?? null,
      health: point.health_score ?? null,
      risk: point.risk_score ?? null,
    })),
  }
}