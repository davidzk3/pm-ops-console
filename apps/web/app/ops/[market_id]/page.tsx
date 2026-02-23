import Link from "next/link";
import OperatorConsolePanel from "./OperatorConsolePanel";
import OverridePanel from "./OverridePanel";

/* -----------------------------
   Core types
------------------------------ */
type Flag = {
  flag_code: string;
  severity: number;
  details: Record<string, any>;
};

type Market = {
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
  flags: Flag[];
  has_manual_override?: boolean;
};

type EffectBlock = {
  risk_score: number | null;
  health_score: number | null;
  spread_median: number | null;
  depth_2pct_median: number | null;
  concentration_hhi: number | null;
  unique_traders: number | null;
  volume: number | null;
  trades?: number | null;
};

type IncidentEffect = {
  id: number;
  market_id: string;
  day: string;
  status: string;
  note: string;
  created_by: string;
  created_at: string;
  after_day: string | null;
  before: EffectBlock | null;
  after: EffectBlock | null;
  delta: EffectBlock | null;
};

/**
 * Backend returns more fields than this (delta_score, roi_score, etc.)
 * We keep it permissive to avoid TS blocking the UI.
 */
type InterventionEffect = {
  id: number;
  market_id: string;
  incident_id?: number | null;
  day: string;
  status: string;
  action_code: string;
  title: string;
  created_by: string;
  created_at: string;
  applied_at: string | null;
  applied_day?: string | null;
  after_day: string | null;
  before_day?: string | null;
  action_count?: number | null;
  params?: any;

  before: EffectBlock | null;
  after: EffectBlock | null;
  delta: EffectBlock | null;

  // optional extras from API (safe)
  delta_score?: number | null;
  roi_score?: number | null;
};

/** cumulative intervention attribution */
type InterventionCumulative = {
  days: number;
  count_total: number;
  count_effective: number;
  risk_score: number | null;
  health_score: number | null;
  spread_median: number | null;
  depth_2pct_median: number | null;
};

type IncidentRow = {
  id: number;
  market_id: string;
  day: string;
  status: string;
  note: string;
  created_by: string;
  created_at: string;
};

type InterventionRow = {
  id: number;
  market_id: string;
  incident_id: number | null;
  day: string;
  action_code: string;
  title: string;
  status: string;
  params: any;
  created_by: string;
  created_at: string;
  applied_at: string | null;
};

type ManualOverride = {
  market_id: string;
  day: string;
  risk_score_override: number | null;
  health_score_override: number | null;
  note: string | null;
  created_by: string;
  created_at: string;
};

type TimelineRow = {
  day: string;
  volume?: number;
  trades?: number;
  unique_traders?: number;
  spread_median?: number;
  depth_2pct_median?: number;
  concentration_hhi?: number;
  health_score?: number;
  risk_score?: number;
};

/* ---------------------------------------
   MVP STEP: Market audit trail feed
   UPDATE: use incident_events (append-only)
---------------------------------------- */
type IncidentEventRow = {
  id: number;
  incident_id: number;
  market_id: string;
  day: string;
  event_type: string; // CREATED | STATUS_CHANGE (you can add more later)
  from_status: string | null;
  to_status: string | null;
  note: string | null;
  created_by: string;
  created_at: string;
};

type ActivityEvent = {
  ts: number;
  whenLabel: string;
  kind: "INCIDENT" | "INTERVENTION" | "OVERRIDE";
  title: string;
  subtitle?: string | null;
  status?: string | null;
  createdBy?: string | null;
  day?: string | null;
  refId?: number | null;
};

/* -----------------------------
   Market impact response
------------------------------ */
type CohortShareRow = {
  cohort: string;
  notional_share: number;
  trade_share: number;
};

type CohortShareDeltaRow = {
  cohort: string;
  notional_share_delta: number;
  trade_share_delta: number;
};

type MarketImpactResponse = {
  window_days: number;
  recent_window: { start: string; end: string };
  prior_window: { start: string; end: string };
  market_quality_delta: Record<string, number>;
  recent_cohort_share: CohortShareRow[];
  prior_cohort_share: CohortShareRow[];
  cohort_share_delta: CohortShareDeltaRow[];
  diagnosis: string;
  market_regime: string;
  cohort_risk_flags: string[];
};

type MarketImpactHistoryPoint = {
  anchor_day: string; // YYYY-MM-DD
  window_days: number;
  recent_window: { start: string; end: string };
  diagnosis: string;
  market_regime: string;
};

function toTs(isoLike: string | null | undefined, dayFallback?: string | null) {
  const try1 = isoLike ? Date.parse(isoLike) : NaN;
  if (Number.isFinite(try1)) return try1;

  const d = (dayFallback ?? "").trim();
  if (d) {
    const t = Date.parse(`${d}T00:00:00Z`);
    if (Number.isFinite(t)) return t;
  }
  return 0;
}

/** Consistent timestamp formatter for server components */
function fmtWhen(isoLike: string | null | undefined, dayFallback?: string | null) {
  if (isoLike) {
    const d = new Date(isoLike);
    if (!Number.isNaN(d.getTime())) {
      // stable regardless of server locale
      return d.toISOString().replace("T", " ").replace("Z", " UTC");
    }
  }
  return dayFallback ? `${dayFallback}` : "-";
}

function buildActivityFeed(args: { incidentEvents: IncidentEventRow[]; interventions: InterventionRow[]; overrides: ManualOverride[] }) {
  const out: ActivityEvent[] = [];

  // Incident events (append-only)
  for (const ev of args.incidentEvents ?? []) {
    const et = (ev.event_type ?? "").toUpperCase();
    const toStatus = (ev.to_status ?? "").toUpperCase() || null;
    const fromStatus = (ev.from_status ?? "").toUpperCase() || null;

    let title = "Incident event";
    let subtitle: string | null = `incident #${ev.incident_id} · ${ev.day}`;

    if (et === "CREATED") {
      title = `Incident opened`;
      if (ev.note) title += `: ${ev.note}`;
      subtitle = `#${ev.incident_id} · ${ev.day}`;
    } else if (et === "STATUS_CHANGE") {
      title = `Incident status changed`;
      if (fromStatus && toStatus) title = `Incident moved ${fromStatus} → ${toStatus}`;
      if (ev.note) subtitle = `${subtitle} · note: ${ev.note}`;
    } else {
      // fallback for future event types
      title = `Incident ${et || "EVENT"}`;
      if (ev.note) subtitle = `${subtitle} · ${ev.note}`;
    }

    out.push({
      ts: toTs(ev.created_at, ev.day),
      whenLabel: fmtWhen(ev.created_at, ev.day),
      kind: "INCIDENT",
      title,
      subtitle,
      status: toStatus || (et === "CREATED" ? "OPEN" : null),
      createdBy: ev.created_by ?? null,
      day: ev.day ?? null,
      refId: ev.incident_id,
    });
  }

  // Interventions (planned + applied)
  for (const itv of args.interventions ?? []) {
    out.push({
      ts: toTs(itv.created_at, itv.day),
      whenLabel: fmtWhen(itv.created_at, itv.day),
      kind: "INTERVENTION",
      title: `Intervention planned: ${itv.title || itv.action_code}`,
      subtitle: `#${itv.id} · ${itv.action_code} · ${itv.day}`,
      status: (itv.status ?? "").toUpperCase(),
      createdBy: itv.created_by ?? null,
      day: itv.day ?? null,
      refId: itv.id,
    });

    if (itv.applied_at) {
      out.push({
        ts: toTs(itv.applied_at, itv.day),
        whenLabel: fmtWhen(itv.applied_at, itv.day),
        kind: "INTERVENTION",
        title: `Intervention applied: ${itv.title || itv.action_code}`,
        subtitle: `#${itv.id} · ${itv.action_code}`,
        status: "APPLIED",
        createdBy: itv.created_by ?? null,
        day: itv.day ?? null,
        refId: itv.id,
      });
    }
  }

  // Overrides
  for (const o of args.overrides ?? []) {
    const risk = o.risk_score_override === null || o.risk_score_override === undefined ? "-" : String(o.risk_score_override);
    const health = o.health_score_override === null || o.health_score_override === undefined ? "-" : String(o.health_score_override);

    out.push({
      ts: toTs(o.created_at, o.day),
      whenLabel: fmtWhen(o.created_at, o.day),
      kind: "OVERRIDE",
      title: `Manual override set (risk ${risk}, health ${health})`,
      subtitle: o.note ? `Note: ${o.note}` : null,
      status: "ACTIVE",
      createdBy: o.created_by ?? null,
      day: o.day ?? null,
      refId: null,
    });
  }

  out.sort((a, b) => b.ts - a.ts);
  return out;
}

function kindBadge(kind: ActivityEvent["kind"]) {
  if (kind === "INCIDENT") return "bg-gray-50 text-gray-700 border-gray-200";
  if (kind === "INTERVENTION") return "bg-purple-50 text-purple-800 border-purple-200";
  return "bg-blue-50 text-blue-800 border-blue-200";
}

function statusBadge(status?: string | null) {
  const s = (status ?? "").toUpperCase();
  if (!s) return "bg-gray-50 text-gray-600 border-gray-200";
  if (s === "OPEN") return "bg-red-50 text-red-800 border-red-200";
  if (s === "MONITOR") return "bg-yellow-50 text-yellow-800 border-yellow-200";
  if (s === "RESOLVED") return "bg-green-50 text-green-800 border-green-200";
  if (s === "APPLIED") return "bg-green-50 text-green-800 border-green-200";
  if (s === "REVERTED") return "bg-gray-50 text-gray-700 border-gray-200";
  if (s === "CANCELLED") return "bg-gray-50 text-gray-700 border-gray-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

/* ------------------------------------- */

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

// Effectiveness windows (UI contract)
// days = attribution window
// afterDays = how far after the incident day we compare (capped to latest available day)
const INCIDENT_EFFECT_DAYS = 30;
const INCIDENT_AFTER_DAYS = 3;

const INTERVENTION_EFFECT_DAYS = 30;
// keep 0 for now unless you also add after_days for interventions endpoint
const INTERVENTION_AFTER_DAYS = 0;

/** NEW: single-call snapshot contract (shipability) */
type TraderSummaryRow = {
  trader_id: string;
  days_active: number;
  trades: number;
  notional_total: number;
  notional_buy: number;
  notional_sell: number;
  avg_trade_size: number;
  first_ts: string;
  last_ts: string;
};

type CohortSummaryRow = {
  cohort: string;
  traders: number;
  trades: number;
  notional_total: number;
  avg_trade_size: number;
  days_covered: number;
};

type TraderIntelligenceRow = {
  trader_id: string;
  days_active: number;
  trades: number;
  notional_total: number;
  avg_trade_size: number;
  buy_ratio: number;
  cohort: string;
  role_tag: string;
  flags?: Record<string, any>;
};

type TradersBundle = {
  summary: TraderSummaryRow[];
  cohorts_summary: CohortSummaryRow[];
  intelligence: TraderIntelligenceRow[];
};

type InterventionsEffectivenessUi = {
  heat: {
    good_up: string[];
    good_down: string[];
    steps: Record<string, number>;
    precision: Record<string, number>;
  };
};

type MarketSnapshotResponse = {
  market: Market | null;
  timeline: TimelineRow[];
  incidents: IncidentRow[];
  incident_events: IncidentEventRow[];
  incident_effectiveness: IncidentEffect[];
  interventions: InterventionRow[];

  // ✅ plural keys from backend snapshot
  interventions_effectiveness: InterventionEffect[];
  interventions_effectiveness_ui: InterventionsEffectivenessUi | null;

  intervention_cumulative: InterventionCumulative | null;

  overrides: ManualOverride[];
  traders: TradersBundle;
  impact: MarketImpactResponse | null;
  errors: { key?: string; message?: string; status?: number }[];
};

async function fetchInbox(): Promise<Market[]> {
  const res = await fetch(`${API_BASE}/ops/inbox`, { cache: "no-store" });
  if (!res.ok) return [];
  return res.json();
}

/**
 * Snapshot fetch:
 * - One call for the whole page
 * - Hardens UI: if server returns partials, we still render using defaults.
 */
async function fetchMarketSnapshot(marketId: string): Promise<MarketSnapshotResponse | null> {
  const res = await fetch(`${API_BASE}/ops/markets/${encodeURIComponent(marketId)}/snapshot`, { cache: "no-store" });
  if (!res.ok) return null;

  const raw: any = await res.json();

  // Normalize to always have arrays (never null)
  const out: MarketSnapshotResponse = {
    market: raw?.market ?? null,
    timeline: Array.isArray(raw?.timeline) ? raw.timeline : [],
    incidents: Array.isArray(raw?.incidents) ? raw.incidents : [],
    incident_events: Array.isArray(raw?.incident_events) ? raw.incident_events : [],
    incident_effectiveness: Array.isArray(raw?.incident_effectiveness) ? raw.incident_effectiveness : [],
    interventions: Array.isArray(raw?.interventions) ? raw.interventions : [],

    // ✅ new snapshot keys
    interventions_effectiveness: Array.isArray(raw?.interventions_effectiveness) ? raw.interventions_effectiveness : [],
    interventions_effectiveness_ui: raw?.interventions_effectiveness_ui ?? null,

    intervention_cumulative: raw?.intervention_cumulative ?? null,

    overrides: Array.isArray(raw?.overrides) ? raw.overrides : [],
    traders: {
      summary: Array.isArray(raw?.traders?.summary) ? raw.traders.summary : [],
      cohorts_summary: Array.isArray(raw?.traders?.cohorts_summary) ? raw.traders.cohorts_summary : [],
      intelligence: Array.isArray(raw?.traders?.intelligence) ? raw.traders.intelligence : [],
    },
    impact: raw?.impact ?? null,
    errors: Array.isArray(raw?.errors) ? raw.errors : [],
  };

  // accept correct field + common misspelling (in case)
  if (out.impact) {
    const data: any = out.impact as any;
    const rawRegime = data?.market_regime ?? data?.market_regiem ?? null;
    if (!rawRegime) {
      const dx = String(data?.diagnosis ?? "").toUpperCase();
      (out.impact as any).market_regime = dx === "LIQUIDITY_IMPROVING" ? "LIQUIDITY_EXPANSION" : "STABLE";
    } else {
      (out.impact as any).market_regime = String(rawRegime);
    }
  }

  return out;
}

/* ----------------------------------------
   Date helpers for impact history
----------------------------------------- */
function parseIsoDay(day: string): Date | null {
  const s = String(day ?? "").trim();
  if (!s) return null;
  const d = new Date(`${s}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function toIsoDayUtc(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function addDaysIso(day: string, deltaDays: number): string | null {
  const d = parseIsoDay(day);
  if (!d) return null;
  d.setUTCDate(d.getUTCDate() + deltaDays);
  return toIsoDayUtc(d);
}

/** market impact history from snapshot-based impact endpoint (still multiple calls, but small + safe) */
async function fetchMarketImpact(marketId: string, opts?: { days?: number; anchor_day?: string | null }): Promise<MarketImpactResponse | null> {
  const days = typeof opts?.days === "number" ? opts!.days : 14;
  const anchor = (opts?.anchor_day ?? null) ? `&anchor_day=${encodeURIComponent(String(opts!.anchor_day))}` : "";

  const res = await fetch(`${API_BASE}/ops/markets/${encodeURIComponent(marketId)}/traders/impact?days=${days}${anchor}`, {
    cache: "no-store",
  });
  if (!res.ok) return null;

  const data: any = await res.json();

  // accept correct field + common misspelling (in case)
  const rawRegime = data?.market_regime ?? data?.market_regiem ?? null;

  if (!rawRegime) {
    const dx = String(data?.diagnosis ?? "").toUpperCase();
    data.market_regime = dx === "LIQUIDITY_IMPROVING" ? "LIQUIDITY_EXPANSION" : "STABLE";
  } else {
    data.market_regime = String(rawRegime);
  }

  return data as MarketImpactResponse;
}

async function fetchMarketImpactHistory(marketId: string, latestDay: string, windowDays = 14): Promise<MarketImpactHistoryPoint[]> {
  const anchors = [0, -7, -14, -21]
    .map((d) => addDaysIso(latestDay, d))
    .filter((x): x is string => Boolean(x));

  const rows = await Promise.all(
    anchors.map(async (anchor_day) => {
      const imp = await fetchMarketImpact(marketId, { days: windowDays, anchor_day });
      if (!imp) return null;

      return {
        anchor_day,
        window_days: imp.window_days,
        recent_window: imp.recent_window,
        diagnosis: imp.diagnosis,
        market_regime: imp.market_regime,
      } as MarketImpactHistoryPoint;
    })
  );

  return rows
    .filter((x): x is MarketImpactHistoryPoint => Boolean(x))
    .sort((a, b) => String(b.anchor_day).localeCompare(String(a.anchor_day)));
}

function flagStyle(sev: number) {
  if (sev >= 4) return "bg-red-100 text-red-800 border-red-300";
  if (sev === 3) return "bg-yellow-100 text-yellow-800 border-yellow-300";
  return "bg-gray-100 text-gray-700 border-gray-300";
}

function Stat({ label, value }: { label: string; value: any }) {
  return (
    <div className="border rounded-xl p-4">
      <div className="text-xs text-gray-500">{label}</div>
      <div className="text-lg font-medium mt-1">{value ?? "-"}</div>
    </div>
  );
}

function trendLabel(delta: number, goodWhenDown: boolean) {
  if (!Number.isFinite(delta)) return "flat";
  if (Math.abs(delta) < 1e-9) return "flat";
  const improving = goodWhenDown ? delta < 0 : delta > 0;
  return improving ? "improving" : "worsening";
}

function safeNumber(x: any): number {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : NaN;
}

function actionPlanForFlag(flag: Flag) {
  switch (flag.flag_code) {
    case "SPREAD_BLOWOUT":
      return {
        title: "Liquidity degradation detected",
        actions: ["Confirm LP quoting status", "Temporarily increase maker rewards", "Monitor spread and depth every few hours"],
      };
    case "WHALE_DOMINANCE":
      return {
        title: "Excess trader concentration",
        actions: ["Reduce per wallet reward caps", "Add diminishing rewards on size", "Watch HHI daily"],
      };
    default:
      return {
        title: "Operational review required",
        actions: ["Investigate drivers, add an incident note"],
      };
  }
}

function fmtNumber(x: any) {
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toLocaleString();
  return String(x);
}

function fmtFloat(x: any, digits = 4) {
  if (x === null || x === undefined) return "-";
  if (typeof x !== "number") return String(x);
  return x.toFixed(digits);
}

function deltaBadge(metric: string, d: any) {
  if (d === null || d === undefined || typeof d !== "number" || !Number.isFinite(d)) {
    return <span className="text-xs text-gray-400">n/a</span>;
  }

  const goodWhenDown = new Set(["spread_median", "concentration_hhi", "risk_score"]);
  const isGood = goodWhenDown.has(metric) ? d < 0 : d > 0;

  const cls = isGood ? "bg-green-100 text-green-800 border-green-300" : "bg-red-100 text-red-800 border-red-300";
  const sign = d > 0 ? "+" : "";
  const val = metric.includes("spread") || metric.includes("hhi") ? fmtFloat(d, 4) : fmtNumber(d);

  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border ${cls}`}>
      {sign}
      {val}
    </span>
  );
}

function EmptyState({ text }: { text: string }) {
  return <p className="text-sm text-gray-500">{text}</p>;
}

/* -----------------------------
   Impact UI helpers
------------------------------ */
function flagChipClass(flag: string) {
  const f = (flag ?? "").toUpperCase();
  if (f.includes("DOMINANCE") || f.includes("RISK")) return "bg-red-50 text-red-800 border-red-200";
  if (f.includes("EROSION") || f.includes("THIN") || f.includes("LOW_CONVICTION")) return "bg-yellow-50 text-yellow-900 border-yellow-200";
  if (f.includes("STABLE")) return "bg-green-50 text-green-800 border-green-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

// market regime badge helper
function regimeBadgeClass(regime: string) {
  const r = (regime ?? "").toUpperCase();
  if (r === "MICROSTRUCTURE_STRESS" || r === "EXECUTION_DEGRADING") return "bg-red-50 text-red-800 border-red-200";
  if (r === "PARTICIPATION_DECAY" || r === "LIQUIDITY_THINNING") return "bg-yellow-50 text-yellow-900 border-yellow-200";
  if (r === "LIQUIDITY_EXPANSION" || r === "PARTICIPATION_GROWTH") return "bg-blue-50 text-blue-800 border-blue-200";
  if (r === "STRUCTURALLY_HEALTHY") return "bg-green-50 text-green-800 border-green-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

function arrowForDelta(isGood: boolean, d: number) {
  if (!Number.isFinite(d) || Math.abs(d) < 1e-12) return "→";
  return isGood ? "↑" : "↓";
}

/**
 * Market-quality badge:
 * - Adds arrow (↑ good, ↓ bad, → flat)
 * - Keeps your existing goodWhenDown logic
 */
function metricBadgeFromDelta(metric: string, d: any) {
  if (d === null || d === undefined || typeof d !== "number" || !Number.isFinite(d)) {
    return <span className="text-xs text-gray-400">n/a</span>;
  }

  // These are "good when down" for impact deltas (note: *_delta keys)
  const goodWhenDown = new Set(["spread_median_delta", "concentration_hhi_delta"]);
  const isGood = goodWhenDown.has(metric) ? d < 0 : d > 0;

  const cls = isGood ? "bg-green-100 text-green-800 border-green-300" : "bg-red-100 text-red-800 border-red-300";
  const sign = d > 0 ? "+" : "";
  const val = metric.includes("spread") || metric.includes("hhi") ? fmtFloat(d, 4) : fmtNumber(d);

  const arrow = arrowForDelta(isGood, d);

  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border inline-flex items-center gap-1 ${cls}`}>
      <span className="leading-none">{arrow}</span>
      <span className="leading-none">
        {sign}
        {val}
      </span>
    </span>
  );
}

function sharePct(x: any) {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(1)}%`;
}

function shareDeltaPct(x: any) {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "-";
  const sign = n > 0 ? "+" : "";
  return `${sign}${(n * 100).toFixed(1)}pp`;
}

function deltaChipClass(d: number) {
  const n = typeof d === "number" ? d : Number(d);
  if (!Number.isFinite(n) || Math.abs(n) < 1e-12) return "bg-gray-50 text-gray-700 border-gray-200";
  return n > 0 ? "bg-green-50 text-green-800 border-green-200" : "bg-red-50 text-red-800 border-red-200";
}

function arrowForPp(d: number) {
  const n = typeof d === "number" ? d : Number(d);
  if (!Number.isFinite(n) || Math.abs(n) < 1e-12) return "→";
  return n > 0 ? "↑" : "↓";
}

function cohortKey(x: string) {
  return String(x ?? "").trim().toUpperCase();
}

function cohortShareMap(rows: CohortShareRow[] | null | undefined) {
  const m: Record<string, CohortShareRow> = {};
  for (const r of rows ?? []) {
    m[cohortKey(r.cohort)] = r;
  }
  return m;
}

function activeShareIndex(impact: MarketImpactResponse) {
  const r = cohortShareMap(impact.recent_cohort_share);
  const p = cohortShareMap(impact.prior_cohort_share);

  const rActive = typeof r["ACTIVE"]?.notional_share === "number" ? r["ACTIVE"].notional_share : NaN;
  const pActive = typeof p["ACTIVE"]?.notional_share === "number" ? p["ACTIVE"].notional_share : NaN;

  if (!Number.isFinite(rActive) || !Number.isFinite(pActive) || pActive <= 1e-12) return null;
  return rActive / pActive;
}

function fmtIndex(x: number | null) {
  if (x === null) return "-";
  if (!Number.isFinite(x)) return "-";
  return `${x.toFixed(2)}x`;
}

function indexChipClass(x: number | null) {
  if (x === null) return "bg-gray-50 text-gray-700 border-gray-200";
  // 1.00 = flat. below 0.90 is deterioration worth highlighting
  if (x < 0.9) return "bg-yellow-50 text-yellow-900 border-yellow-200";
  if (x > 1.1) return "bg-green-50 text-green-800 border-green-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

function institutionalHeadline(impact: MarketImpactResponse) {
  const flags = (impact.cohort_risk_flags ?? []).map((x) => String(x ?? "").toUpperCase());
  const q = impact.market_quality_delta ?? {};

  const spread = typeof q["spread_median_delta"] === "number" ? q["spread_median_delta"] : NaN;
  const hhi = typeof q["concentration_hhi_delta"] === "number" ? q["concentration_hhi_delta"] : NaN;

  const microBad = (Number.isFinite(spread) && spread > 0) || (Number.isFinite(hhi) && hhi > 0);
  const middleErosion = flags.includes("MIDDLE_LAYER_EROSION");
  const thinMiddle = flags.includes("THIN_MIDDLE_LAYER");

  // Priority order: what you want an ops analyst to react to immediately
  if (microBad) {
    return { text: "Execution quality deteriorating (spread or concentration rising)", cls: "bg-red-50 text-red-800 border-red-200" };
  }
  if (middleErosion) {
    return { text: "Active trader base shrinking (middle layer erosion)", cls: "bg-yellow-50 text-yellow-900 border-yellow-200" };
  }
  if (thinMiddle) {
    return { text: "Thin middle layer (market depends on casual flow and whales)", cls: "bg-yellow-50 text-yellow-900 border-yellow-200" };
  }
  return { text: "No dominant institutional risk detected in this window", cls: "bg-green-50 text-green-800 border-green-200" };
}

function execSummary(impact: MarketImpactResponse) {
  const q = impact.market_quality_delta ?? {};

  const spread = typeof q["spread_median_delta"] === "number" ? q["spread_median_delta"] : NaN;
  const hhi = typeof q["concentration_hhi_delta"] === "number" ? q["concentration_hhi_delta"] : NaN;
  const depth = typeof q["depth_2pct_delta"] === "number" ? q["depth_2pct_delta"] : NaN;
  const traders = typeof q["unique_traders_delta"] === "number" ? q["unique_traders_delta"] : NaN;
  const health = typeof q["health_score_delta"] === "number" ? q["health_score_delta"] : NaN;

  const microBad = (Number.isFinite(spread) && spread > 0) || (Number.isFinite(hhi) && hhi > 0);
  const participationBad = (Number.isFinite(traders) && traders < 0) || (Number.isFinite(health) && health < 0);
  const liquidityGood = Number.isFinite(depth) && depth > 0 && (Number.isFinite(spread) ? spread <= 0 : true);

  const flags = (impact.cohort_risk_flags ?? []).map((x) => (x ?? "").toUpperCase());
  const middleThin = flags.some((f) => f.includes("THIN_MIDDLE") || f.includes("MIDDLE_LAYER"));

  // One-line, operator-friendly
  if (microBad && participationBad) {
    return `Execution quality deteriorated (spread or HHI up) alongside weaker participation (traders or health down).`;
  }
  if (microBad && !participationBad) {
    return `Microstructure worsened (spread or HHI up) despite stable participation.`;
  }
  if (!microBad && liquidityGood && !participationBad) {
    return `Liquidity conditions improved (depth up, spread stable or down) with stable participation.`;
  }
  if (middleThin && !microBad) {
    return `Cohort mix is shifting with a thinner middle layer; monitor durability of liquidity.`;
  }
  return `Mixed signals across microstructure and cohort composition; monitor next window.`;
}

function MarketImpactPanel({ impact, history }: { impact: MarketImpactResponse | null; history: MarketImpactHistoryPoint[] }) {
  if (!impact) return <EmptyState text="No impact data available yet." />;

  const q = impact.market_quality_delta ?? {};
  const diagnosis = (impact.diagnosis ?? "STABLE").toUpperCase();
  const regime = (impact.market_regime ?? "STABLE").toUpperCase();

  const diagCls =
    diagnosis === "LIQUIDITY_IMPROVING"
      ? "bg-green-50 text-green-800 border-green-200"
      : diagnosis === "CONCENTRATION_RISK"
      ? "bg-red-50 text-red-800 border-red-200"
      : diagnosis === "PARTICIPATION_EXPANDING"
      ? "bg-blue-50 text-blue-800 border-blue-200"
      : "bg-gray-50 text-gray-700 border-gray-200";

  const head = institutionalHeadline(impact);
  const activeIdx = activeShareIndex(impact);

  return (
    <div className="border rounded-2xl p-4 bg-white">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <div className="font-medium">Institutional impact (window {impact.window_days}d)</div>
            <span className={`text-xs px-2 py-0.5 rounded-full border ${diagCls}`}>{diagnosis}</span>
            <span className={`text-xs px-2 py-0.5 rounded-full border ${regimeBadgeClass(regime)}`}>{regime}</span>
          </div>

          <div className="mt-2">
            <span className={`text-xs px-2 py-0.5 rounded-full border inline-flex items-center ${head.cls}`}>{head.text}</span>
          </div>

          <div className="text-sm text-gray-700 mt-2">{execSummary(impact)}</div>

          <div className="mt-2 flex flex-wrap gap-2">
            <span className={`text-xs px-2 py-0.5 rounded-full border inline-flex items-center gap-2 ${indexChipClass(activeIdx)}`}>
              <span className="text-gray-700">Active notional index</span>
              <span className="font-medium">{fmtIndex(activeIdx)}</span>
            </span>
            <span className="text-xs px-2 py-0.5 rounded-full border bg-gray-50 text-gray-700 border-gray-200 inline-flex items-center gap-2">
              <span>Recent</span>
              <span className="font-medium">
                {impact.recent_window?.start} → {impact.recent_window?.end}
              </span>
            </span>
          </div>

          <div className="text-xs text-gray-500 mt-2">
            Prior: <span className="font-medium">{impact.prior_window?.start}</span> → <span className="font-medium">{impact.prior_window?.end}</span>
          </div>

          {history?.length ? (
            <div className="mt-3">
              <div className="text-[11px] text-gray-500 mb-1">Regime history (4 weekly anchors)</div>
              <div className="flex flex-wrap gap-2">
                {history.map((h) => {
                  const reg = String(h.market_regime ?? "STABLE").toUpperCase();
                  const diag = String(h.diagnosis ?? "STABLE").toUpperCase();
                  return (
                    <div key={h.anchor_day} className="px-2 py-1 rounded-lg border bg-white">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-[11px] text-gray-500">{h.anchor_day}</span>
                        <span className={`text-[11px] px-2 py-0.5 rounded-full border ${regimeBadgeClass(reg)}`}>{reg}</span>
                        <span className="text-[11px] px-2 py-0.5 rounded-full border bg-gray-50 border-gray-200 text-gray-700">{diag}</span>
                      </div>
                      <div className="text-[11px] text-gray-500 mt-1">
                        {h.recent_window?.start} → {h.recent_window?.end}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-2">
          {(impact.cohort_risk_flags ?? []).slice(0, 6).map((f, idx) => (
            <span key={`${f}-${idx}`} className={`text-xs px-2 py-0.5 rounded-full border ${flagChipClass(f)}`}>
              {String(f ?? "").toUpperCase()}
            </span>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
        <div className="border rounded-xl p-3 bg-gray-50">
          <div className="text-sm font-medium mb-2">Market quality delta</div>
          <div className="flex flex-wrap gap-2">
            <span className="text-xs border rounded-full px-2 py-0.5 bg-white inline-flex items-center gap-2">
              <span>spread</span>
              {metricBadgeFromDelta("spread_median_delta", q["spread_median_delta"])}
            </span>
            <span className="text-xs border rounded-full px-2 py-0.5 bg-white inline-flex items-center gap-2">
              <span>depth</span>
              {metricBadgeFromDelta("depth_2pct_delta", q["depth_2pct_delta"])}
            </span>
            <span className="text-xs border rounded-full px-2 py-0.5 bg-white inline-flex items-center gap-2">
              <span>hhi</span>
              {metricBadgeFromDelta("concentration_hhi_delta", q["concentration_hhi_delta"])}
            </span>
            <span className="text-xs border rounded-full px-2 py-0.5 bg-white inline-flex items-center gap-2">
              <span>traders</span>
              {metricBadgeFromDelta("unique_traders_delta", q["unique_traders_delta"])}
            </span>
            <span className="text-xs border rounded-full px-2 py-0.5 bg-white inline-flex items-center gap-2">
              <span>health</span>
              {metricBadgeFromDelta("health_score_delta", q["health_score_delta"])}
            </span>
          </div>
          <div className="text-[11px] text-gray-500 mt-2">For spread and HHI: down is good. For depth, traders, health: up is good.</div>
        </div>

        <div className="border rounded-xl p-3 bg-gray-50">
          <div className="text-sm font-medium mb-2">Cohort share delta</div>

          {impact.cohort_share_delta?.length ? (
            <div className="overflow-x-auto border rounded-lg bg-white">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-xs text-gray-600">
                  <tr>
                    <th className="text-left font-medium px-3 py-2">Cohort</th>
                    <th className="text-right font-medium px-3 py-2">Notional Δ</th>
                    <th className="text-right font-medium px-3 py-2">Trade Δ</th>
                  </tr>
                </thead>
                <tbody>
                  {impact.cohort_share_delta.map((r, idx) => (
                    <tr key={`${r.cohort}-${idx}`} className="border-t">
                      <td className="px-3 py-2 font-medium">{String(r.cohort ?? "").toUpperCase()}</td>

                      <td className="px-3 py-2 text-right">
                        <span className={`text-xs px-2 py-0.5 rounded-full border inline-flex items-center gap-1 ${deltaChipClass(r.notional_share_delta)}`}>
                          <span className="leading-none">{arrowForPp(r.notional_share_delta)}</span>
                          <span className="leading-none">{shareDeltaPct(r.notional_share_delta)}</span>
                        </span>
                      </td>

                      <td className="px-3 py-2 text-right">
                        <span className={`text-xs px-2 py-0.5 rounded-full border inline-flex items-center gap-1 ${deltaChipClass(r.trade_share_delta)}`}>
                          <span className="leading-none">{arrowForPp(r.trade_share_delta)}</span>
                          <span className="leading-none">{shareDeltaPct(r.trade_share_delta)}</span>
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-sm text-gray-500">No cohort delta available.</div>
          )}

          <div className="text-[11px] text-gray-500 mt-2">Δ shown in percentage points (pp) between recent and prior window.</div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
        <div className="border rounded-xl p-3">
          <div className="text-sm font-medium mb-2">Recent cohort shares</div>
          {impact.recent_cohort_share?.length ? (
            <div className="overflow-x-auto border rounded-lg">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-xs text-gray-600">
                  <tr>
                    <th className="text-left font-medium px-3 py-2">Cohort</th>
                    <th className="text-right font-medium px-3 py-2">Notional</th>
                    <th className="text-right font-medium px-3 py-2">Trades</th>
                  </tr>
                </thead>
                <tbody>
                  {impact.recent_cohort_share.map((r, idx) => (
                    <tr key={`${r.cohort}-${idx}`} className="border-t">
                      <td className="px-3 py-2 font-medium">{String(r.cohort ?? "").toUpperCase()}</td>
                      <td className="px-3 py-2 text-right">{sharePct(r.notional_share)}</td>
                      <td className="px-3 py-2 text-right">{sharePct(r.trade_share)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-sm text-gray-500">No cohort data.</div>
          )}
        </div>

        <div className="border rounded-xl p-3">
          <div className="text-sm font-medium mb-2">Prior cohort shares</div>
          {impact.prior_cohort_share?.length ? (
            <div className="overflow-x-auto border rounded-lg">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-xs text-gray-600">
                  <tr>
                    <th className="text-left font-medium px-3 py-2">Cohort</th>
                    <th className="text-right font-medium px-3 py-2">Notional</th>
                    <th className="text-right font-medium px-3 py-2">Trades</th>
                  </tr>
                </thead>
                <tbody>
                  {impact.prior_cohort_share.map((r, idx) => (
                    <tr key={`${r.cohort}-${idx}`} className="border-t">
                      <td className="px-3 py-2 font-medium">{String(r.cohort ?? "").toUpperCase()}</td>
                      <td className="px-3 py-2 text-right">{sharePct(r.notional_share)}</td>
                      <td className="px-3 py-2 text-right">{sharePct(r.trade_share)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-sm text-gray-500">No cohort data.</div>
          )}
        </div>
      </div>
    </div>
  );
}

/** Step 5 UI table */
function TraderSummaryTable({ rows }: { rows: TraderSummaryRow[] }) {
  if (!rows?.length) return <EmptyState text="No trader data yet. Seed trades first." />;

  return (
    <div className="mt-4 overflow-x-auto border rounded-xl bg-white">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 text-xs text-gray-600">
          <tr>
            <th className="text-left font-medium px-3 py-2">Trader</th>
            <th className="text-right font-medium px-3 py-2">Days active</th>
            <th className="text-right font-medium px-3 py-2">Trades</th>
            <th className="text-right font-medium px-3 py-2">Notional</th>
            <th className="text-right font-medium px-3 py-2">Buy</th>
            <th className="text-right font-medium px-3 py-2">Sell</th>
            <th className="text-right font-medium px-3 py-2">Avg size</th>
            <th className="text-left font-medium px-3 py-2">First</th>
            <th className="text-left font-medium px-3 py-2">Last</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((r) => (
            <tr key={r.trader_id} className="border-t">
              <td className="px-3 py-2 font-mono text-xs">{r.trader_id}</td>
              <td className="px-3 py-2 text-right">{fmtNumber(r.days_active)}</td>
              <td className="px-3 py-2 text-right">{fmtNumber(r.trades)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.notional_total, 2)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.notional_buy, 2)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.notional_sell, 2)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.avg_trade_size, 2)}</td>
              <td className="px-3 py-2 text-xs text-gray-600">{r.first_ts}</td>
              <td className="px-3 py-2 text-xs text-gray-600">{r.last_ts}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Step 6.1 UI: cohort summary table */
function CohortSummaryTable({ rows }: { rows: CohortSummaryRow[] }) {
  if (!rows?.length) return <EmptyState text="No cohort data yet. Seed trades first." />;

  return (
    <div className="mt-4 overflow-x-auto border rounded-xl bg-white">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 text-xs text-gray-600">
          <tr>
            <th className="text-left font-medium px-3 py-2">Cohort</th>
            <th className="text-right font-medium px-3 py-2">Traders</th>
            <th className="text-right font-medium px-3 py-2">Trades</th>
            <th className="text-right font-medium px-3 py-2">Notional</th>
            <th className="text-right font-medium px-3 py-2">Avg size</th>
            <th className="text-right font-medium px-3 py-2">Days</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((r, idx) => (
            <tr key={`${r.cohort}-${idx}`} className="border-t">
              <td className="px-3 py-2 font-medium">{String(r.cohort ?? "").toUpperCase()}</td>
              <td className="px-3 py-2 text-right">{fmtNumber(r.traders)}</td>
              <td className="px-3 py-2 text-right">{fmtNumber(r.trades)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.notional_total, 2)}</td>
              <td className="px-3 py-2 text-right">{fmtFloat(r.avg_trade_size, 2)}</td>
              <td className="px-3 py-2 text-right">{fmtNumber(r.days_covered)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function roleTagBadgeClass(tag: string) {
  const t = (tag ?? "").toUpperCase();
  if (t === "WHALE") return "bg-indigo-50 text-indigo-800 border-indigo-200";
  if (t === "MAKER_LIKE") return "bg-green-50 text-green-800 border-green-200";
  if (t === "INCENTIVE_FARMER") return "bg-yellow-50 text-yellow-900 border-yellow-200";
  if (t === "DIRECTIONAL") return "bg-red-50 text-red-800 border-red-200";
  if (t === "ACTIVE_RETAIL") return "bg-blue-50 text-blue-800 border-blue-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

/** Step 6.2 UI: trader intelligence table */
function TraderIntelligenceTable({ rows }: { rows: TraderIntelligenceRow[] }) {
  if (!rows?.length) return <EmptyState text="No intelligence data yet. Seed trades first." />;

  return (
    <div className="mt-4 overflow-x-auto border rounded-xl bg-white">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 text-xs text-gray-600">
          <tr>
            <th className="text-left font-medium px-3 py-2">Trader</th>
            <th className="text-left font-medium px-3 py-2">Cohort</th>
            <th className="text-left font-medium px-3 py-2">Role tag</th>
            <th className="text-right font-medium px-3 py-2">Days</th>
            <th className="text-right font-medium px-3 py-2">Trades</th>
            <th className="text-right font-medium px-3 py-2">Notional</th>
            <th className="text-right font-medium px-3 py-2">Avg size</th>
            <th className="text-right font-medium px-3 py-2">Buy ratio</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((r) => {
            const buyRatioPct = typeof r.buy_ratio === "number" && Number.isFinite(r.buy_ratio) ? `${(r.buy_ratio * 100).toFixed(1)}%` : "-";
            return (
              <tr key={r.trader_id} className="border-t">
                <td className="px-3 py-2 font-mono text-xs">{r.trader_id}</td>
                <td className="px-3 py-2">
                  <span className="text-xs px-2 py-0.5 rounded-full border bg-gray-50 border-gray-200">{(r.cohort ?? "UNKNOWN").toUpperCase()}</span>
                </td>
                <td className="px-3 py-2">
                  <span className={`text-xs px-2 py-0.5 rounded-full border ${roleTagBadgeClass(r.role_tag)}`}>{(r.role_tag ?? "RETAIL").toUpperCase()}</span>
                </td>
                <td className="px-3 py-2 text-right">{fmtNumber(r.days_active)}</td>
                <td className="px-3 py-2 text-right">{fmtNumber(r.trades)}</td>
                <td className="px-3 py-2 text-right">{fmtFloat(r.notional_total, 2)}</td>
                <td className="px-3 py-2 text-right">{fmtFloat(r.avg_trade_size, 2)}</td>
                <td className="px-3 py-2 text-right">{buyRatioPct}</td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <div className="px-3 py-2 text-xs text-gray-500 border-t bg-gray-50">Role tags are heuristic MVP labels. You can refine logic server-side later.</div>
    </div>
  );
}

export default async function MarketPage(props: any) {
  const params = await Promise.resolve(props?.params ?? {});
  const rawMarketId = params.market_id ?? params.marketId ?? params.id ?? props?.market_id ?? props?.marketId ?? "";
  const marketIdStr = String(rawMarketId ?? "").trim();

  // We now rely on snapshot for everything, including the market header.
  // If marketId is wrong, we still show the old "market not found" debug panel using inbox.
  const snap = marketIdStr ? await fetchMarketSnapshot(marketIdStr) : null;
  let market: Market | null = (snap?.market as any) ?? null;

  let inboxRows: Market[] = [];
  if (!market) {
    inboxRows = await fetchInbox();
    market = inboxRows.find((r) => String(r.market_id).trim() === marketIdStr) ?? null;
  }

  if (!market) {
    return (
      <main className="p-8 max-w-3xl">
        <Link href="/ops" className="text-sm text-gray-600 hover:underline">
          {"<- Back to Ops Inbox"}
        </Link>

        <h1 className="text-xl font-semibold mt-4">Market not found</h1>

        <p className="text-sm text-gray-500 mt-2">
          Requested market_id: <code className="px-1 py-0.5 border rounded">{marketIdStr || "(empty)"}</code>
        </p>

        <p className="text-sm text-gray-500 mt-2">
          Available market_ids:{" "}
          <code className="px-1 py-0.5 border rounded">{inboxRows.length ? inboxRows.map((r) => r.market_id).join(", ") : "(inbox empty)"}</code>
        </p>

        <p className="text-sm text-gray-500 mt-4">Debug params:</p>
        <pre className="text-xs text-gray-600 mt-2 border rounded p-3 overflow-x-auto">{JSON.stringify({ params, propsKeys: Object.keys(props ?? {}) }, null, 2)}</pre>

        <p className="text-sm text-gray-500 mt-4">
          Verify API:
          <br />
          <code className="px-1 py-0.5 border rounded">{`${API_BASE}/ops/markets/m2/snapshot`}</code>
        </p>
      </main>
    );
  }

  const resolvedId = String(market.market_id).trim();

  // Snapshot provides everything used below (one round trip).
  // If snapshot is missing (should not happen if market exists), fall back to empty defaults.
  const timeline = snap?.timeline ?? [];
  const incidents = snap?.incidents ?? [];
  const interventions = snap?.interventions ?? [];
  const overrides = snap?.overrides ?? [];
  const incidentEvents = snap?.incident_events ?? [];
  const incidentEffects = snap?.incident_effectiveness ?? [];

  // ✅ now correctly wired
  const interventionEffects = snap?.interventions_effectiveness ?? [];
  const interventionCumulative = snap?.intervention_cumulative ?? null;

  const traderSummary = snap?.traders?.summary ?? [];
  const cohortSummary = snap?.traders?.cohorts_summary ?? [];
  const traderIntelligence = snap?.traders?.intelligence ?? [];
  const impact = snap?.impact ?? null;

  // safer: prefer override matching current market day, then fallback to newest
  const activeOverride = market.has_manual_override ? overrides.find((o) => o.day === market.day) ?? overrides?.[0] ?? null : null;

  const timelineSorted = [...(timeline ?? [])].sort((a, b) => String(a.day).localeCompare(String(b.day)));
  const latest = timelineSorted.length ? timelineSorted[timelineSorted.length - 1] : null;
  const prior = timelineSorted.length >= 8 ? timelineSorted[timelineSorted.length - 8] : timelineSorted[0] ?? null;

  // impact history anchors off latest available day (timeline preferred)
  const latestDayForImpact = String(latest?.day ?? market.day ?? "").trim();
  const impactHistory = latestDayForImpact ? await fetchMarketImpactHistory(resolvedId, latestDayForImpact, 14) : [];

  const dRisk = safeNumber(latest?.risk_score) - safeNumber(prior?.risk_score);
  const dHealth = safeNumber(latest?.health_score) - safeNumber(prior?.health_score);
  const dSpread = safeNumber(latest?.spread_median) - safeNumber(prior?.spread_median);
  const dDepth = safeNumber(latest?.depth_2pct_median) - safeNumber(prior?.depth_2pct_median);
  const dHHI = safeNumber(latest?.concentration_hhi) - safeNumber(prior?.concentration_hhi);
  const dTraders = safeNumber(latest?.unique_traders) - safeNumber(prior?.unique_traders);

  const activity = buildActivityFeed({
    incidentEvents,
    interventions,
    overrides,
  });

  return (
    <main className="p-8 max-w-5xl">
      <Link href="/ops" className="text-sm text-gray-600 hover:underline">
        {"<- Back to Ops Inbox"}
      </Link>

      <div className="flex items-start justify-between gap-4 mt-2">
        <div>
          <h1 className="text-2xl font-semibold">{market.title}</h1>
          <p className="text-sm text-gray-500 mt-1">
            {market.market_id} {" - "} {market.protocol} {" - "} {market.chain} {" - "} {market.category ?? "uncategorized"} {" - "} {market.day}
          </p>
        </div>

        {market.has_manual_override ? (
          <div className="text-xs px-3 py-1 rounded-full border bg-blue-50 text-blue-800 border-blue-200">Manual override active</div>
        ) : null}
      </div>

      {/* Snapshot warning strip if server reported partial errors */}
      {snap?.errors?.length ? (
        <div className="mt-4 border rounded-xl p-3 bg-yellow-50 border-yellow-200 text-yellow-900">
          <div className="font-medium text-sm">Snapshot partial errors</div>
          <div className="text-xs mt-1">Some sub-sections failed server-side, but the page is still rendering.</div>
          <ul className="text-xs mt-2 list-disc ml-5">
            {snap.errors.slice(0, 6).map((e, i) => (
              <li key={i}>
                {e.key ? `${e.key}: ` : ""}
                {e.message ?? "error"}
                {typeof e.status === "number" ? ` (status ${e.status})` : ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6">
        <Stat label="Risk score" value={market.risk_score} />
        <Stat label="Health score" value={market.health_score} />
        <Stat label="Volume" value={Number(market.volume).toLocaleString()} />
        <Stat label="Unique traders" value={market.unique_traders} />
        <Stat label="Trades" value={market.trades} />
        <Stat label="Spread (median)" value={market.spread_median} />
        <Stat label="Depth @2%" value={market.depth_2pct_median} />
        <Stat label="Concentration (HHI)" value={market.concentration_hhi} />
      </div>

      {/* Institutional impact panel + regime history */}
      <section className="mt-8">
        <div className="flex items-end justify-between gap-4 mb-3 flex-wrap">
          <h2 className="font-medium">Institutional impact</h2>
          <div className="text-xs text-gray-500">
            Snapshot includes <code className="px-1 py-0.5 border rounded bg-white">{`impact`}</code>
          </div>
        </div>

        <MarketImpactPanel impact={impact} history={impactHistory} />
      </section>

      {/* Operator quick actions strip (above the fold) */}
      <section className="mt-6 border rounded-2xl p-4 bg-gray-50">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="font-medium">Operator actions</div>
          <div className="text-xs text-gray-500">Jump to the main workflow sections</div>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <a href="#operator-console" className="text-sm px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-100">
            Incidents and interventions
          </a>
          <a href="#overrides" className="text-sm px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-100">
            Set override
          </a>
          <a href="#timeline" className="text-sm px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-100">
            Timeline
          </a>
          <a href="#activity" className="text-sm px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-100">
            Activity feed
          </a>
          <a href="#traders" className="text-sm px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-100">
            Trader intelligence
          </a>
        </div>
      </section>

      {/* overrides */}
      <section id="overrides" className="mt-10">
        <div className="flex items-end justify-between gap-4 mb-3">
          <h2 className="font-medium">
            Manual overrides <span className="text-xs text-gray-400">(operator adjustments)</span>
          </h2>
          <div className="text-xs text-gray-500">Snapshot includes overrides</div>
        </div>

        {activeOverride ? (
          <div className="border rounded-2xl p-4 bg-blue-50 border-blue-200">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="font-medium">
                Active on {activeOverride.day}
                <span className="ml-2 text-xs px-2 py-0.5 rounded-full border bg-white border-blue-200 text-blue-800">active</span>
              </div>
              <div className="text-xs text-gray-600">
                by {activeOverride.created_by} {" - "} {fmtWhen(activeOverride.created_at, activeOverride.day)}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3 text-sm">
              <div className="border rounded-xl p-3 bg-white">
                <div className="text-xs text-gray-500">Risk override</div>
                <div className="text-lg font-medium mt-1">{fmtNumber(activeOverride.risk_score_override)}</div>
              </div>
              <div className="border rounded-xl p-3 bg-white">
                <div className="text-xs text-gray-500">Health override</div>
                <div className="text-lg font-medium mt-1">{fmtNumber(activeOverride.health_score_override)}</div>
              </div>
              <div className="border rounded-xl p-3 bg-white">
                <div className="text-xs text-gray-500">Note</div>
                <div className="mt-1">{activeOverride.note ?? <span className="text-gray-400">-</span>}</div>
              </div>
            </div>

            <div className="text-xs text-gray-600 mt-3">Displayed risk and health are overridden for the current market day.</div>
          </div>
        ) : null}

        {overrides.length ? (
          <div className="mt-3 space-y-2">
            {overrides.map((o, idx) => (
              <div key={`${o.day}-${idx}`} className="border rounded-xl p-3 bg-white flex flex-col md:flex-row md:items-center md:justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium">{o.day}</span>
                  {idx === 0 && market.has_manual_override ? (
                    <span className="text-xs px-2 py-0.5 rounded-full border bg-blue-50 text-blue-800 border-blue-200">active</span>
                  ) : null}
                </div>

                <div className="flex flex-wrap gap-2 text-xs">
                  <span className="px-2 py-0.5 rounded-full border bg-gray-50">risk {fmtNumber(o.risk_score_override)}</span>
                  <span className="px-2 py-0.5 rounded-full border bg-gray-50">health {fmtNumber(o.health_score_override)}</span>
                  <span className="px-2 py-0.5 rounded-full border bg-gray-50">by {o.created_by}</span>
                </div>

                <div className="text-xs text-gray-500">{fmtWhen(o.created_at, o.day)}</div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState text="No overrides recorded." />
        )}
      </section>

      <OverridePanel marketId={resolvedId} overrides={overrides} />

      <section className="mt-10">
        <h2 className="font-medium mb-3">Active flags</h2>

        {market.flags.length ? (
          <div className="space-y-3">
            {market.flags.map((f, i) => (
              <div key={i} className="border rounded-xl p-4">
                <div className="flex justify-between items-center">
                  <div className="font-medium">{f.flag_code}</div>
                  <span className={`px-2 py-1 rounded-full border text-xs ${flagStyle(f.severity)}`}>severity {f.severity}</span>
                </div>

                <pre className="text-xs text-gray-600 mt-3 overflow-x-auto">{JSON.stringify(f.details, null, 2)}</pre>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState text="No active flags." />
        )}
      </section>

      <section className="mt-12">
        <h2 className="font-medium mb-4">Action plan</h2>

        {market.flags.length ? (
          <div className="space-y-4">
            {market.flags.map((f, i) => {
              const plan = actionPlanForFlag(f);
              return (
                <div key={i} className="border rounded-xl p-4 bg-gray-50">
                  <div className="font-medium mb-2">{plan.title}</div>
                  <div className="text-sm">
                    <strong>Suggested actions:</strong>
                    <ul className="list-disc ml-5 mt-1">
                      {plan.actions.map((a, j) => (
                        <li key={j}>{a}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <EmptyState text="No action required at this time." />
        )}
      </section>

      {/* audit trail feed (incident_events + interventions + overrides) */}
      <section id="activity" className="mt-12">
        <div className="flex items-end justify-between gap-4 mb-3">
          <h2 className="font-medium">Ops activity feed</h2>
          <div className="text-xs text-gray-500">Snapshot includes incident_events + interventions + overrides</div>
        </div>

        {activity.length ? (
          <div className="space-y-2">
            {activity.map((e, idx) => (
              <div key={`${e.kind}-${e.ts}-${idx}`} className="border rounded-xl p-3 bg-white">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className={`text-[11px] px-2 py-0.5 rounded-full border ${kindBadge(e.kind)}`}>{e.kind}</span>

                      {e.status ? <span className={`text-[11px] px-2 py-0.5 rounded-full border ${statusBadge(e.status)}`}>{e.status}</span> : null}

                      <div className="font-medium truncate">{e.title}</div>
                    </div>

                    {e.subtitle ? <div className="text-xs text-gray-500 mt-1 truncate">{e.subtitle}</div> : null}

                    <div className="text-xs text-gray-400 mt-1">
                      {e.whenLabel}
                      {e.createdBy ? ` · by ${e.createdBy}` : ""}
                      {e.day ? ` · day ${e.day}` : ""}
                    </div>
                  </div>

                  <div className="shrink-0 text-xs text-gray-400">
                    {e.kind === "INCIDENT" ? "incident" : e.kind === "INTERVENTION" ? "itv" : "override"}
                    {typeof e.refId === "number" ? ` #${e.refId}` : ""}
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState text="No activity recorded yet." />
        )}
      </section>

      {/* Step 5 UI: Top traders */}
      <section id="traders" className="mt-12">
        <div className="flex items-end justify-between gap-4 mb-3 flex-wrap">
          <h2 className="font-medium">Top traders (30d)</h2>
          <div className="text-xs text-gray-500">Snapshot includes traders.summary</div>
        </div>

        <TraderSummaryTable rows={traderSummary} />
      </section>

      {/* Step 6.1 UI: Cohorts summary */}
      <section className="mt-12">
        <div className="flex items-end justify-between gap-4 mb-3 flex-wrap">
          <h2 className="font-medium">Cohorts summary (30d)</h2>
          <div className="text-xs text-gray-500">Snapshot includes traders.cohorts_summary</div>
        </div>

        <CohortSummaryTable rows={cohortSummary} />
      </section>

      {/* Step 6.2 UI: Trader intelligence */}
      <section className="mt-12">
        <div className="flex items-end justify-between gap-4 mb-3 flex-wrap">
          <h2 className="font-medium">Trader intelligence (30d)</h2>
          <div className="text-xs text-gray-500">Snapshot includes traders.intelligence</div>
        </div>

        <TraderIntelligenceTable rows={traderIntelligence} />
      </section>

      <section id="operator-console" className="mt-12">
        <OperatorConsolePanel
          marketId={resolvedId}
          incidents={incidents}
          interventions={interventions}
          interventionEffects={interventionEffects}
          cumulative={interventionCumulative}
        />
      </section>

      <section id="timeline" className="mt-10">
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <div>
            <h2 className="font-medium">30-day timeline</h2>
            <p className="text-xs text-gray-500 mt-1">Shows daily drift so ops can see whether a market is improving or deteriorating.</p>
          </div>

          {latest && prior ? (
            <div className="text-xs text-gray-600">
              Comparing <span className="font-medium">{prior.day}</span> to <span className="font-medium">{latest.day}</span>
            </div>
          ) : null}
        </div>

        {latest && prior ? (
          <div className="mt-3 flex flex-wrap gap-2 text-xs">
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              Risk {Number.isFinite(dRisk) ? fmtFloat(dRisk, 0) : "n/a"} ({trendLabel(dRisk, true)})
            </span>
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              Health {Number.isFinite(dHealth) ? fmtFloat(dHealth, 0) : "n/a"} ({trendLabel(dHealth, false)})
            </span>
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              Spread {Number.isFinite(dSpread) ? fmtFloat(dSpread, 4) : "n/a"} ({trendLabel(dSpread, true)})
            </span>
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              Depth {Number.isFinite(dDepth) ? fmtFloat(dDepth, 0) : "n/a"} ({trendLabel(dDepth, false)})
            </span>
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              HHI {Number.isFinite(dHHI) ? fmtFloat(dHHI, 4) : "n/a"} ({trendLabel(dHHI, true)})
            </span>
            <span className="px-2 py-1 rounded-full border bg-gray-50">
              Traders {Number.isFinite(dTraders) ? fmtFloat(dTraders, 0) : "n/a"} ({trendLabel(dTraders, false)})
            </span>
          </div>
        ) : (
          <div className="mt-3 text-sm text-gray-500">No timeline data available.</div>
        )}

        {timelineSorted.length ? (
          <div className="mt-4 overflow-x-auto border rounded-xl">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-xs text-gray-600">
                <tr>
                  <th className="text-left font-medium px-3 py-2">Day</th>
                  <th className="text-right font-medium px-3 py-2">Risk</th>
                  <th className="text-right font-medium px-3 py-2">Health</th>
                  <th className="text-right font-medium px-3 py-2">Spread</th>
                  <th className="text-right font-medium px-3 py-2">Depth @2%</th>
                  <th className="text-right font-medium px-3 py-2">HHI</th>
                  <th className="text-right font-medium px-3 py-2">Traders</th>
                  <th className="text-right font-medium px-3 py-2">Volume</th>
                </tr>
              </thead>
              <tbody>
                {timelineSorted.map((r) => (
                  <tr key={r.day} className="border-t">
                    <td className="px-3 py-2">{r.day}</td>
                    <td className="px-3 py-2 text-right">{fmtFloat(r.risk_score, 0)}</td>
                    <td className="px-3 py-2 text-right">{fmtFloat(r.health_score, 0)}</td>
                    <td className="px-3 py-2 text-right">{fmtFloat(r.spread_median, 4)}</td>
                    <td className="px-3 py-2 text-right">{fmtNumber(r.depth_2pct_median)}</td>
                    <td className="px-3 py-2 text-right">{fmtFloat(r.concentration_hhi, 4)}</td>
                    <td className="px-3 py-2 text-right">{fmtNumber(r.unique_traders)}</td>
                    <td className="px-3 py-2 text-right">{fmtNumber(r.volume)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>

      <section className="mt-12">
        <h2 className="font-medium mb-3">
          Incident annotations <span className="text-xs text-gray-400">(with effectiveness)</span>
        </h2>

        {incidentEffects.length ? (
          <div className="space-y-3">
            {incidentEffects.map((i) => (
              <div key={i.id} className="border rounded-xl p-4">
                <div className="flex justify-between gap-4">
                  <div className="font-medium">{i.note}</div>
                  <span className="text-xs border px-2 py-0.5 rounded">{i.status}</span>
                </div>

                <div className="text-xs text-gray-500 mt-1">
                  {i.day} {" - "} by {i.created_by}
                  {i.after_day ? ` (compared to ${i.after_day})` : ""}
                </div>

                <div className="mt-4 border rounded-xl p-3 bg-gray-50">
                  <div className="text-sm font-medium mb-2">Effectiveness</div>

                  {!i.before || !i.after || !i.delta ? (
                    <div className="text-sm text-gray-500">Missing metrics for before or after.</div>
                  ) : (
                    <>
                      <div className="flex flex-wrap gap-2">
                        {deltaBadge("risk_score", i.delta.risk_score)}
                        {deltaBadge("spread_median", i.delta.spread_median)}
                        {deltaBadge("depth_2pct_median", i.delta.depth_2pct_median)}
                        {deltaBadge("concentration_hhi", i.delta.concentration_hhi)}
                        {deltaBadge("unique_traders", i.delta.unique_traders)}
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3 text-sm">
                        <div className="border rounded-lg p-3 bg-white">
                          <div className="text-xs text-gray-500 mb-2">Before</div>
                          <div className="flex flex-wrap gap-2">
                            <span className="text-xs border rounded px-2 py-0.5">risk {fmtNumber(i.before.risk_score)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">spread {fmtFloat(i.before.spread_median, 4)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">depth {fmtNumber(i.before.depth_2pct_median)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">hhi {fmtFloat(i.before.concentration_hhi, 4)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">traders {fmtNumber(i.before.unique_traders)}</span>
                          </div>
                        </div>

                        <div className="border rounded-lg p-3 bg-white">
                          <div className="text-xs text-gray-500 mb-2">After</div>
                          <div className="flex flex-wrap gap-2">
                            <span className="text-xs border rounded px-2 py-0.5">risk {fmtNumber(i.after.risk_score)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">spread {fmtFloat(i.after.spread_median, 4)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">depth {fmtNumber(i.after.depth_2pct_median)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">hhi {fmtFloat(i.after.concentration_hhi, 4)}</span>
                            <span className="text-xs border rounded px-2 py-0.5">traders {fmtNumber(i.after.unique_traders)}</span>
                          </div>
                        </div>
                      </div>

                      <div className="text-xs text-gray-500 mt-2">For risk, spread, and HHI: down is good. For depth and traders: up is good.</div>
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState text="No incidents recorded." />
        )}
      </section>
    </main>
  );
}