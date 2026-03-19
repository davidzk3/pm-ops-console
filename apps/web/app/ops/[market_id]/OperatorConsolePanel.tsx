"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { fetchJSON } from "../../lib (rename back to lib)/api";

type IncidentStatus = "OPEN" | "MONITOR" | "RESOLVED";
type InterventionStatus = "PLANNED" | "APPLIED" | "REVERTED" | "CANCELLED";

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

type EffectBlock = {
  risk_score?: number | null;
  health_score?: number | null;
  spread_median?: number | null;
  depth_2pct_median?: number | null;
  concentration_hhi?: number | null;
  unique_traders?: number | null;
  volume?: number | null;
  trades?: number | null;
};

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
  after_day?: string | null;
  before_day?: string | null;
  action_count?: number | null;
  params?: any;
  before?: EffectBlock | null;
  after?: EffectBlock | null;
  delta?: EffectBlock | null;
  delta_score?: number | null;
  roi_score?: number | null;
};

type CumulativeImpact = {
  days: number;
  count_total?: number | null;
  count_effective?: number | null;
  risk_score?: number | null;
  health_score?: number | null;
  spread_median?: number | null;
  depth_2pct_median?: number | null;
};

function todayISO() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function fmtWhen(isoLike: string | null | undefined, dayFallback?: string | null) {
  if (isoLike) {
    const d = new Date(isoLike);
    if (!Number.isNaN(d.getTime())) {
      return d.toISOString().replace("T", " ").replace("Z", " UTC");
    }
  }
  return dayFallback ? `${dayFallback}` : "-";
}

function safeParseJSON(raw: string) {
  const s = (raw ?? "").trim();
  if (!s) return {};
  try {
    const v = JSON.parse(s);
    if (v && typeof v === "object" && !Array.isArray(v)) return v;
    throw new Error("JSON must be an object");
  } catch (e: any) {
    throw new Error(
      e?.message ? `Params JSON error: ${e.message}` : "Params must be valid JSON",
    );
  }
}

function fmtNumber(x: any) {
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toLocaleString();
  const n = Number(x);
  return Number.isFinite(n) ? n.toLocaleString() : String(x);
}

function fmtFloat(x: any, digits = 4) {
  if (x === null || x === undefined) return "-";
  if (typeof x === "number") return x.toFixed(digits);
  const n = Number(x);
  return Number.isFinite(n) ? n.toFixed(digits) : String(x);
}

function isGoodDelta(metric: string, d: number) {
  const goodWhenDown = new Set(["spread_median", "concentration_hhi", "risk_score"]);
  return goodWhenDown.has(metric) ? d < 0 : d > 0;
}

function hasMeaningfulDelta(delta?: EffectBlock | null) {
  if (!delta) return false;
  return [
    delta.risk_score,
    delta.health_score,
    delta.spread_median,
    delta.depth_2pct_median,
    delta.concentration_hhi,
    delta.unique_traders,
    delta.volume,
    delta.trades,
  ].some((v) => typeof v === "number" && Number.isFinite(v));
}

function DeltaPill(props: {
  label: string;
  metric: string;
  value: any;
  digits?: number;
}) {
  const { label, metric, value, digits } = props;

  if (
    value === null ||
    value === undefined ||
    typeof value !== "number" ||
    Number.isNaN(value)
  ) {
    return (
      <span className="text-xs px-2 py-0.5 rounded-full border bg-gray-50 text-gray-500 border-gray-200">
        {label}: n/a
      </span>
    );
  }

  const good = isGoodDelta(metric, value);
  const cls = good
    ? "bg-green-50 text-green-800 border-green-200"
    : "bg-red-50 text-red-800 border-red-200";

  const sign = value > 0 ? "+" : "";
  const pretty =
    metric.includes("spread") || metric.includes("hhi")
      ? `${sign}${fmtFloat(value, digits ?? 4)}`
      : `${sign}${fmtNumber(value)}`;

  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border ${cls}`}>
      {label}: {pretty}
    </span>
  );
}

function normalizeIncidentStatus(s: any): IncidentStatus {
  const x = String(s ?? "").trim().toUpperCase();
  if (x === "MONITOR") return "MONITOR";
  if (x === "RESOLVED") return "RESOLVED";
  return "OPEN";
}

function incidentStatusPill(status: string) {
  const s = String(status ?? "").toUpperCase();
  if (s === "RESOLVED") return "bg-green-50 text-green-800 border-green-200";
  if (s === "MONITOR") return "bg-yellow-50 text-yellow-800 border-yellow-200";
  if (s === "OPEN") return "bg-red-50 text-red-800 border-red-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

function normalizeInterventionStatus(s: any): InterventionStatus {
  const x = String(s ?? "").trim().toUpperCase();
  if (x === "APPLIED") return "APPLIED";
  if (x === "REVERTED") return "REVERTED";
  if (x === "CANCELLED") return "CANCELLED";
  return "PLANNED";
}

function interventionStatusPill(status: string) {
  const s = String(status ?? "").toUpperCase();
  if (s === "APPLIED") return "bg-green-50 text-green-800 border-green-200";
  if (s === "REVERTED") return "bg-gray-50 text-gray-700 border-gray-200";
  if (s === "CANCELLED") return "bg-gray-50 text-gray-700 border-gray-200";
  if (s === "PLANNED") return "bg-blue-50 text-blue-800 border-blue-200";
  return "bg-gray-50 text-gray-700 border-gray-200";
}

function EmptyBox({ text }: { text: string }) {
  return (
    <div className="rounded-xl border bg-gray-50 px-3 py-3 text-sm text-gray-500">
      {text}
    </div>
  );
}

export default function OperatorConsolePanel(props: {
  marketId: string;
  incidents: IncidentRow[];
  interventions: InterventionRow[];
  interventionEffects: InterventionEffect[];
  cumulative?: CumulativeImpact | null;
}) {
  const router = useRouter();
  const today = useMemo(() => todayISO(), []);

  const incidentsArr = props.incidents ?? [];
  const interventionsArr = props.interventions ?? [];
  const interventionEffectsArr = props.interventionEffects ?? [];

  const effectById = useMemo(() => {
    const m = new Map<number, InterventionEffect>();
    for (const e of interventionEffectsArr) {
      if (typeof e?.id === "number") m.set(e.id, e);
    }
    return m;
  }, [interventionEffectsArr]);

  const analyticsByAction = useMemo(() => {
    const map = new Map<
      string,
      {
        count: number;
        risk: number[];
        health: number[];
        spread: number[];
        depth: number[];
      }
    >();

    for (const e of interventionEffectsArr) {
      if (!hasMeaningfulDelta(e?.delta)) continue;

      const key = String(e.action_code ?? "UNKNOWN").trim() || "UNKNOWN";
      if (!map.has(key)) {
        map.set(key, { count: 0, risk: [], health: [], spread: [], depth: [] });
      }

      const bucket = map.get(key)!;
      bucket.count += 1;

      if (typeof e.delta?.risk_score === "number") bucket.risk.push(e.delta.risk_score);
      if (typeof e.delta?.health_score === "number") bucket.health.push(e.delta.health_score);
      if (typeof e.delta?.spread_median === "number") bucket.spread.push(e.delta.spread_median);
      if (typeof e.delta?.depth_2pct_median === "number") {
        bucket.depth.push(e.delta.depth_2pct_median);
      }
    }

    function avg(arr: number[]) {
      if (!arr.length) return null;
      return arr.reduce((a, b) => a + b, 0) / arr.length;
    }

    return Array.from(map.entries())
      .map(([action, data]) => ({
        action,
        count: data.count,
        avgRisk: avg(data.risk),
        avgHealth: avg(data.health),
        avgSpread: avg(data.spread),
        avgDepth: avg(data.depth),
      }))
      .sort((a, b) => b.count - a.count);
  }, [interventionEffectsArr]);

  const incidentsSorted = useMemo(() => {
    const arr = [...incidentsArr];
    arr.sort((a, b) => {
      const d = String(b.day ?? "").localeCompare(String(a.day ?? ""));
      if (d !== 0) return d;
      const ca = Date.parse(a.created_at ?? "");
      const cb = Date.parse(b.created_at ?? "");
      if (Number.isFinite(ca) && Number.isFinite(cb)) return cb - ca;
      return String(b.created_at ?? "").localeCompare(String(a.created_at ?? ""));
    });
    return arr;
  }, [incidentsArr]);

  const interventionsSorted = useMemo(() => {
    const arr = [...interventionsArr];
    arr.sort((a, b) => {
      const d = String(b.day ?? "").localeCompare(String(a.day ?? ""));
      if (d !== 0) return d;
      const ca = Date.parse(a.created_at ?? "");
      const cb = Date.parse(b.created_at ?? "");
      if (Number.isFinite(ca) && Number.isFinite(cb)) return cb - ca;
      return String(b.created_at ?? "").localeCompare(String(a.created_at ?? ""));
    });
    return arr;
  }, [interventionsArr]);

  const [incidentDay, setIncidentDay] = useState(today);
  const [incidentStatus, setIncidentStatus] = useState<IncidentStatus>("OPEN");
  const [incidentNote, setIncidentNote] = useState("");
  const [incidentCreatedBy, setIncidentCreatedBy] = useState("operator");

  const [interventionDay, setInterventionDay] = useState(today);
  const [interventionIncidentId, setInterventionIncidentId] = useState<string>("");
  const [actionCode, setActionCode] = useState("LIQUIDITY_BOOST");
  const [title, setTitle] = useState("");
  const [interventionStatus, setInterventionStatus] =
    useState<InterventionStatus>("PLANNED");
  const [paramsJson, setParamsJson] = useState<string>(
    JSON.stringify(
      { spread_bps: 10, depth_delta: 500, health_delta: 3, risk_delta: -2 },
      null,
      2,
    ),
  );
  const [interventionCreatedBy, setInterventionCreatedBy] = useState("operator");

  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  async function createIncident() {
    setErr(null);
    setBusy("incident");
    try {
      if (!incidentNote.trim()) throw new Error("Incident note is required");

      await fetchJSON(`/ops/markets/${encodeURIComponent(props.marketId)}/incidents`, {
        method: "POST",
        body: JSON.stringify({
          day: incidentDay,
          status: incidentStatus,
          note: incidentNote.trim(),
          created_by: incidentCreatedBy.trim() || "operator",
        }),
        write: true,
      });

      setIncidentNote("");
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to create incident");
    } finally {
      setBusy(null);
    }
  }

  async function setIncidentStatusById(incidentId: number, status: IncidentStatus) {
    setErr(null);
    setBusy(`incidentStatus:${incidentId}:${status}`);
    try {
      await fetchJSON(`/ops/incidents/${incidentId}/status`, {
        method: "POST",
        body: JSON.stringify({ status }),
        write: true,
      });
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to update incident status");
    } finally {
      setBusy(null);
    }
  }

  async function createIntervention() {
    setErr(null);
    setBusy("intervention");
    try {
      if (!actionCode.trim()) throw new Error("Action code is required");

      const parsedParams = safeParseJSON(paramsJson);

      await fetchJSON(`/ops/markets/${encodeURIComponent(props.marketId)}/interventions`, {
        method: "POST",
        body: JSON.stringify({
          day: interventionDay,
          incident_id: interventionIncidentId ? Number(interventionIncidentId) : null,
          action_code: actionCode.trim(),
          title: (title || actionCode).trim(),
          status: interventionStatus,
          params: parsedParams,
          created_by: interventionCreatedBy.trim() || "operator",
        }),
        write: true,
      });

      setTitle("");
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to create intervention");
    } finally {
      setBusy(null);
    }
  }

  async function applyIntervention(interventionId: number) {
    setErr(null);
    setBusy(`apply:${interventionId}`);
    try {
      await fetchJSON(`/ops/interventions/${interventionId}/apply`, {
        method: "POST",
        body: JSON.stringify({}),
        write: true,
      });
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to apply intervention");
    } finally {
      setBusy(null);
    }
  }

  async function revertIntervention(interventionId: number) {
    setErr(null);
    setBusy(`revert:${interventionId}`);
    try {
      await fetchJSON(`/ops/interventions/${interventionId}/revert`, {
        method: "POST",
        body: JSON.stringify({}),
        write: true,
      });
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to revert intervention");
    } finally {
      setBusy(null);
    }
  }

  async function cancelIntervention(interventionId: number) {
    setErr(null);
    setBusy(`cancel:${interventionId}`);
    try {
      await fetchJSON(`/ops/interventions/${interventionId}/cancel`, {
        method: "POST",
        body: JSON.stringify({}),
        write: true,
      });
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to cancel intervention");
    } finally {
      setBusy(null);
    }
  }

  const cum = props.cumulative ?? null;

  const hasCumulative =
    !!cum &&
    (typeof cum.count_total === "number" ||
      typeof cum.count_effective === "number" ||
      typeof cum.risk_score === "number" ||
      typeof cum.health_score === "number" ||
      typeof cum.spread_median === "number" ||
      typeof cum.depth_2pct_median === "number");

  const hasAnyOperatorHistory =
    incidentsSorted.length > 0 ||
    interventionsSorted.length > 0 ||
    interventionEffectsArr.length > 0 ||
    hasCumulative;

  return (
    <section className="mt-12">
      <div className="flex items-end justify-between gap-4 mb-3 flex-wrap">
        <h2 className="font-medium">Operator workflow</h2>
        <div className="text-xs text-gray-500">
          Create incidents, plan actions, then apply when needed
        </div>
      </div>

      {!hasAnyOperatorHistory ? (
        <div className="mb-6 rounded-2xl border bg-white p-4">
          <div className="font-medium text-sm text-gray-900">
            No operator history recorded yet
          </div>
          <div className="mt-1 text-sm text-gray-500">
            This is normal for newly live ingested markets. You can open the first incident or
            create the first intervention from here.
          </div>
        </div>
      ) : null}

      {hasCumulative ? (
        <div className="border rounded-2xl p-4 bg-white mb-6">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div className="font-medium">
                Last {cum?.days ?? 30} days intervention impact
              </div>
              <div className="text-xs text-gray-500 mt-1">
                Total: {fmtNumber(cum?.count_total ?? "-")} · Effective:{" "}
                {fmtNumber(cum?.count_effective ?? "-")}
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <DeltaPill
                label="Δ Risk"
                metric="risk_score"
                value={cum?.risk_score ?? null}
                digits={0}
              />
              <DeltaPill
                label="Δ Health"
                metric="health_score"
                value={cum?.health_score ?? null}
                digits={0}
              />
              <DeltaPill
                label="Δ Spread"
                metric="spread_median"
                value={cum?.spread_median ?? null}
                digits={4}
              />
              <DeltaPill
                label="Δ Depth"
                metric="depth_2pct_median"
                value={cum?.depth_2pct_median ?? null}
                digits={0}
              />
            </div>
          </div>
        </div>
      ) : null}

      {err ? (
        <div className="border rounded-xl p-3 mb-4 bg-red-50 border-red-200 text-sm text-red-800">
          {err}
        </div>
      ) : null}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="border rounded-2xl p-4 bg-white">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="font-medium">Create incident</div>
              <div className="text-xs text-gray-500">
                POST /ops/markets/{props.marketId}/incidents
              </div>
            </div>
            <button
              onClick={createIncident}
              disabled={busy !== null || incidentNote.trim().length === 0}
              className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
            >
              {busy === "incident" ? "Creating..." : "Create"}
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <label className="text-sm">
              Day
              <input
                type="date"
                value={incidentDay}
                onChange={(e) => setIncidentDay(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              />
            </label>

            <label className="text-sm">
              Status
              <select
                value={incidentStatus}
                onChange={(e) => setIncidentStatus(e.target.value as IncidentStatus)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              >
                <option value="OPEN">OPEN</option>
                <option value="MONITOR">MONITOR</option>
                <option value="RESOLVED">RESOLVED</option>
              </select>
            </label>

            <label className="text-sm md:col-span-2">
              Note
              <textarea
                value={incidentNote}
                onChange={(e) => setIncidentNote(e.target.value)}
                rows={4}
                className="mt-1 w-full border rounded-xl px-3 py-2"
                placeholder="What happened and what should ops watch"
              />
            </label>

            <label className="text-sm md:col-span-2">
              Created by
              <input
                value={incidentCreatedBy}
                onChange={(e) => setIncidentCreatedBy(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              />
            </label>
          </div>

          <div className="mt-5">
            <div className="text-sm font-medium">Recent incidents</div>

            {incidentsSorted.length ? (
              <div className="mt-2 space-y-2">
                {incidentsSorted.map((i) => {
                  const st = normalizeIncidentStatus(i.status);
                  const isResolved = st === "RESOLVED";
                  const isMonitor = st === "MONITOR";
                  const isOpen = st === "OPEN";

                  const isBusyMonitor = busy === `incidentStatus:${i.id}:MONITOR`;
                  const isBusyResolve = busy === `incidentStatus:${i.id}:RESOLVED`;
                  const isBusyReopen = busy === `incidentStatus:${i.id}:OPEN`;

                  return (
                    <div key={i.id} className="border rounded-xl p-3 text-sm">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <div className="font-medium break-words">
                              #{i.id} · {i.note}
                            </div>
                            <span
                              className={`text-xs border rounded-full px-2 py-0.5 ${incidentStatusPill(
                                st,
                              )}`}
                            >
                              {st}
                            </span>
                          </div>
                          <div className="text-xs text-gray-500 mt-1">
                            {i.day} · by {i.created_by} · {fmtWhen(i.created_at, i.day)}
                          </div>
                        </div>

                        <div className="flex items-center gap-2 shrink-0">
                          <button
                            onClick={() => setIncidentStatusById(i.id, "MONITOR")}
                            disabled={busy !== null || isMonitor}
                            className="text-xs px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 disabled:opacity-50"
                          >
                            {isBusyMonitor ? "Updating..." : "Monitor"}
                          </button>

                          <button
                            onClick={() => setIncidentStatusById(i.id, "RESOLVED")}
                            disabled={busy !== null || isResolved}
                            className="text-xs px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 disabled:opacity-50"
                          >
                            {isBusyResolve ? "Updating..." : "Resolve"}
                          </button>

                          <button
                            onClick={() => setIncidentStatusById(i.id, "OPEN")}
                            disabled={busy !== null || isOpen}
                            className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
                          >
                            {isBusyReopen ? "Updating..." : "Reopen"}
                          </button>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <EmptyBox text="No operator incidents recorded yet for this live market." />
            )}
          </div>
        </div>

        <div className="border rounded-2xl p-4 bg-white">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="font-medium">Create intervention</div>
              <div className="text-xs text-gray-500">
                POST /ops/markets/{props.marketId}/interventions
              </div>
            </div>
            <button
              onClick={createIntervention}
              disabled={busy !== null || actionCode.trim().length === 0}
              className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
            >
              {busy === "intervention" ? "Creating..." : "Create"}
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-4">
            <label className="text-sm">
              Day
              <input
                type="date"
                value={interventionDay}
                onChange={(e) => setInterventionDay(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              />
            </label>

            <label className="text-sm">
              Status
              <select
                value={interventionStatus}
                onChange={(e) =>
                  setInterventionStatus(e.target.value as InterventionStatus)
                }
                className="mt-1 w-full border rounded-xl px-3 py-2"
              >
                <option value="PLANNED">PLANNED</option>
                <option value="APPLIED">APPLIED</option>
                <option value="REVERTED">REVERTED</option>
                <option value="CANCELLED">CANCELLED</option>
              </select>
            </label>

            <label className="text-sm">
              Link to incident (optional)
              <select
                value={interventionIncidentId}
                onChange={(e) => setInterventionIncidentId(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              >
                <option value="">None</option>
                {incidentsSorted.map((i) => (
                  <option key={i.id} value={String(i.id)}>
                    #{i.id} {i.day} {String(i.status ?? "").toUpperCase()}
                  </option>
                ))}
              </select>
            </label>

            <label className="text-sm">
              Action code
              <input
                value={actionCode}
                onChange={(e) => setActionCode(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              />
            </label>

            <label className="text-sm md:col-span-2">
              Title
              <input
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
                placeholder="Short human label"
              />
            </label>

            <label className="text-sm md:col-span-2">
              Params (JSON)
              <textarea
                value={paramsJson}
                onChange={(e) => setParamsJson(e.target.value)}
                rows={6}
                className="mt-1 w-full border rounded-xl px-3 py-2 font-mono text-xs"
                placeholder='{"spread_bps": 10, "depth_delta": 500, "health_delta": 3, "risk_delta": -2}'
              />
            </label>

            <label className="text-sm md:col-span-2">
              Created by
              <input
                value={interventionCreatedBy}
                onChange={(e) => setInterventionCreatedBy(e.target.value)}
                className="mt-1 w-full border rounded-xl px-3 py-2"
              />
            </label>
          </div>

          {analyticsByAction.length ? (
            <div className="mt-5 border rounded-2xl p-4 bg-gray-50">
              <div className="flex items-end justify-between gap-4">
                <div>
                  <div className="font-medium">
                    Intervention performance (last {cum?.days ?? 30} days)
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    Grouped by action_code. Averages are computed from populated deltas only.
                  </div>
                </div>
              </div>

              <div className="mt-3 space-y-3">
                {analyticsByAction.map((a) => (
                  <div key={a.action} className="border rounded-xl p-3 bg-white text-sm">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-medium truncate">{a.action}</div>
                      <div className="text-xs text-gray-500 shrink-0">
                        Count: {a.count}
                      </div>
                    </div>

                    <div className="mt-2 flex flex-wrap gap-2">
                      <DeltaPill
                        label="Avg Δ Risk"
                        metric="risk_score"
                        value={a.avgRisk}
                        digits={0}
                      />
                      <DeltaPill
                        label="Avg Δ Health"
                        metric="health_score"
                        value={a.avgHealth}
                        digits={0}
                      />
                      <DeltaPill
                        label="Avg Δ Spread"
                        metric="spread_median"
                        value={a.avgSpread}
                        digits={4}
                      />
                      <DeltaPill
                        label="Avg Δ Depth"
                        metric="depth_2pct_median"
                        value={a.avgDepth}
                        digits={0}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="mt-5">
            <div className="text-sm font-medium">Planned and recent</div>

            {interventionsSorted.length ? (
              <div className="mt-2 space-y-2">
                {interventionsSorted.map((itv) => {
                  const st = normalizeInterventionStatus(itv.status);
                  const isApplied = st === "APPLIED";
                  const isReverted = st === "REVERTED";
                  const isCancelled = st === "CANCELLED";

                  const canApply = !isApplied && !isReverted && !isCancelled;
                  const canRevert = isApplied;
                  const canCancel = !isApplied && !isReverted && !isCancelled;

                  const eff = effectById.get(itv.id);
                  const delta = eff?.delta ?? null;
                  const hasDelta = hasMeaningfulDelta(delta);

                  let impactText: string | null = null;
                  if (!hasDelta) {
                    if (isApplied) {
                      impactText =
                        "Applied. Effectiveness will appear once usable daily comparison metrics are available.";
                    } else {
                      impactText =
                        "No effectiveness data yet. This is normal until the action is applied and downstream metrics populate.";
                    }
                  }

                  return (
                    <div key={itv.id} className="border rounded-xl p-3 text-sm">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <div className="font-medium break-words">
                              #{itv.id} · {itv.action_code}
                            </div>
                            <span
                              className={`text-xs border rounded-full px-2 py-0.5 ${interventionStatusPill(
                                st,
                              )}`}
                            >
                              {st}
                            </span>
                            {typeof itv.incident_id === "number" ? (
                              <span className="text-xs border rounded-full px-2 py-0.5 bg-gray-50 text-gray-700 border-gray-200">
                                incident #{itv.incident_id}
                              </span>
                            ) : null}
                          </div>

                          <div className="text-xs text-gray-500 truncate mt-1">
                            {itv.title}
                          </div>

                          <div className="text-xs text-gray-400 mt-1">
                            {itv.day} · by {itv.created_by} · {fmtWhen(itv.created_at, itv.day)}
                            {itv.applied_at
                              ? ` · applied ${fmtWhen(itv.applied_at, itv.day)}`
                              : ""}
                            {eff?.after_day ? ` · compared to ${eff.after_day}` : ""}
                          </div>

                          <div className="mt-2 flex flex-wrap gap-2">
                            {!hasDelta ? (
                              <span className="text-xs text-gray-400">{impactText}</span>
                            ) : (
                              <>
                                <DeltaPill
                                  label="Δ Risk"
                                  metric="risk_score"
                                  value={delta?.risk_score as any}
                                  digits={0}
                                />
                                <DeltaPill
                                  label="Δ Health"
                                  metric="health_score"
                                  value={delta?.health_score as any}
                                  digits={0}
                                />
                                <DeltaPill
                                  label="Δ Spread"
                                  metric="spread_median"
                                  value={delta?.spread_median as any}
                                  digits={4}
                                />
                                <DeltaPill
                                  label="Δ Depth"
                                  metric="depth_2pct_median"
                                  value={delta?.depth_2pct_median as any}
                                  digits={0}
                                />
                              </>
                            )}
                          </div>

                          {eff?.before || eff?.after ? (
                            <details className="mt-3">
                              <summary className="text-xs text-gray-600 cursor-pointer select-none">
                                View effectiveness detail
                              </summary>

                              <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-3">
                                <div className="border rounded-xl p-3 bg-gray-50">
                                  <div className="text-xs text-gray-500 mb-2">Before</div>
                                  <div className="flex flex-wrap gap-2">
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      risk {fmtNumber(eff.before?.risk_score)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      health {fmtNumber(eff.before?.health_score)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      spread {fmtFloat(eff.before?.spread_median, 4)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      depth {fmtNumber(eff.before?.depth_2pct_median)}
                                    </span>
                                  </div>
                                </div>

                                <div className="border rounded-xl p-3 bg-gray-50">
                                  <div className="text-xs text-gray-500 mb-2">After</div>
                                  <div className="flex flex-wrap gap-2">
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      risk {fmtNumber(eff.after?.risk_score)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      health {fmtNumber(eff.after?.health_score)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      spread {fmtFloat(eff.after?.spread_median, 4)}
                                    </span>
                                    <span className="text-xs border rounded px-2 py-0.5 bg-white">
                                      depth {fmtNumber(eff.after?.depth_2pct_median)}
                                    </span>
                                  </div>
                                </div>
                              </div>
                            </details>
                          ) : null}
                        </div>

                        <div className="flex items-center gap-2 shrink-0">
                          <button
                            onClick={() => applyIntervention(itv.id)}
                            disabled={busy !== null || !canApply}
                            className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
                          >
                            {busy === `apply:${itv.id}`
                              ? "Applying..."
                              : isApplied
                                ? "Applied"
                                : "Apply"}
                          </button>

                          <button
                            onClick={() => revertIntervention(itv.id)}
                            disabled={busy !== null || !canRevert}
                            className="text-xs px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 disabled:opacity-50"
                          >
                            {busy === `revert:${itv.id}` ? "Reverting..." : "Revert"}
                          </button>

                          <button
                            onClick={() => cancelIntervention(itv.id)}
                            disabled={busy !== null || !canCancel}
                            className="text-xs px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 disabled:opacity-50"
                          >
                            {busy === `cancel:${itv.id}` ? "Cancelling..." : "Cancel"}
                          </button>
                        </div>
                      </div>

                      <details className="mt-3">
                        <summary className="text-xs text-gray-600 cursor-pointer select-none">
                          View params
                        </summary>
                        <pre className="mt-2 text-xs bg-gray-50 border rounded-xl p-3 overflow-x-auto">
                          {JSON.stringify(itv.params ?? {}, null, 2)}
                        </pre>
                      </details>
                    </div>
                  );
                })}
              </div>
            ) : (
              <EmptyBox text="No interventions recorded yet for this live market." />
            )}
          </div>
        </div>
      </div>
    </section>
  );
}