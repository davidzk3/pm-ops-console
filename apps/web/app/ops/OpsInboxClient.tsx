"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { MarketRow } from "./page";

type SortKey = "risk_desc" | "health_asc" | "flags_desc" | "volume_desc" | "trades_desc";

function badgeStyle(sev: number) {
  if (sev >= 4) return "bg-red-100 text-red-800 border-red-300";
  if (sev === 3) return "bg-yellow-100 text-yellow-800 border-yellow-300";
  return "bg-gray-100 text-gray-700 border-gray-300";
}

function getStatus(m: MarketRow) {
  const flags = Array.isArray(m.flags) ? m.flags : [];
  const maxSeverity = Math.max(...(flags.map((f) => f.severity) ?? [0]));

  if (maxSeverity >= 4) {
    return { label: "Critical", cls: "bg-red-600 text-white border-red-700" };
  }

  if ((m.risk_score ?? 0) >= 60) {
    return { label: "Escalated", cls: "bg-red-100 text-red-800 border-red-300" };
  }

  if (m.has_manual_override) {
    return { label: "Stabilized", cls: "bg-blue-100 text-blue-800 border-blue-300" };
  }

  if ((m.risk_score ?? 0) >= 30) {
    return { label: "Monitor", cls: "bg-yellow-100 text-yellow-800 border-yellow-300" };
  }

  return { label: "Healthy", cls: "bg-green-100 text-green-800 border-green-300" };
}

function riskPill(risk: number | null | undefined) {
  const r = typeof risk === "number" ? risk : null;

  if (r === null) return { label: "RISK -", cls: "bg-gray-100 text-gray-700 border-gray-300" };
  if (r >= 85) return { label: `RISK ${r}`, cls: "bg-red-100 text-red-900 border-red-300" };
  if (r >= 70) return { label: `RISK ${r}`, cls: "bg-orange-100 text-orange-900 border-orange-300" };
  if (r >= 50) return { label: `RISK ${r}`, cls: "bg-yellow-100 text-yellow-900 border-yellow-300" };
  return { label: `RISK ${r}`, cls: "bg-gray-100 text-gray-700 border-gray-300" };
}

function riskBand(risk: number | null | undefined) {
  const r = typeof risk === "number" ? risk : -1;
  if (r >= 80) return { label: "CRITICAL", cls: "bg-red-600 text-white border-red-700" };
  if (r >= 60) return { label: "HIGH", cls: "bg-red-100 text-red-800 border-red-300" };
  if (r >= 40) return { label: "MEDIUM", cls: "bg-yellow-100 text-yellow-800 border-yellow-300" };
  if (r >= 0) return { label: "LOW", cls: "bg-green-100 text-green-800 border-green-300" };
  return { label: "n/a", cls: "bg-gray-50 text-gray-600 border-gray-200" };
}

function fmtNumber(x: any) {
  if (x === null || x === undefined) return "-";
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n.toLocaleString() : "-";
}

function fmtFloat(x: any, digits = 4) {
  if (x === null || x === undefined) return "-";
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n.toFixed(digits) : "-";
}

/* -----------------------------
   Microstructure regime
------------------------------ */
type Regime = {
  code: "EXECUTION_STRESS" | "THIN_LIQUIDITY" | "CONCENTRATION_RISK" | "PARTICIPATION_DECAY" | "STABLE";
  label: string;
  cls: string;
  reason?: string;
};

function microstructureRegime(m: MarketRow): Regime {
  const spread = typeof m.spread_median === "number" ? m.spread_median : NaN;
  const depth = typeof m.depth_2pct_median === "number" ? m.depth_2pct_median : NaN;
  const hhi = typeof m.concentration_hhi === "number" ? m.concentration_hhi : NaN;
  const traders = typeof m.unique_traders === "number" ? m.unique_traders : NaN;

  // Heuristic thresholds (tune later)
  const spreadStress = Number.isFinite(spread) && spread >= 0.020; // 2.0% median spread
  const thinDepth = Number.isFinite(depth) && depth <= 300; // very low depth at 2%
  const concRisk = Number.isFinite(hhi) && hhi >= 0.35; // concentrated flow
  const participationLow = Number.isFinite(traders) && traders <= 25;

  // Priority order: what should pop first in an ops queue
  if (spreadStress) {
    return {
      code: "EXECUTION_STRESS",
      label: "EXECUTION STRESS",
      cls: "bg-red-50 text-red-800 border-red-200",
      reason: "Spread elevated",
    };
  }
  if (thinDepth) {
    return {
      code: "THIN LIQUIDITY",
      label: "THIN LIQUIDITY",
      cls: "bg-yellow-50 text-yellow-900 border-yellow-200",
      reason: "Depth thin",
    };
  }
  if (concRisk) {
    return {
      code: "CONCENTRATION_RISK",
      label: "CONCENTRATION RISK",
      cls: "bg-orange-50 text-orange-900 border-orange-200",
      reason: "HHI high",
    };
  }
  if (participationLow) {
    return {
      code: "PARTICIPATION_DECAY",
      label: "PARTICIPATION DECAY",
      cls: "bg-blue-50 text-blue-800 border-blue-200",
      reason: "Few active traders",
    };
  }

  return {
    code: "STABLE",
    label: "STABLE",
    cls: "bg-green-50 text-green-800 border-green-200",
  };
}

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

type QuickActionCode = "LIQUIDITY_BOOST" | "TIGHTEN_SPREAD" | "DIVERSIFY_FLOW" | "KICKSTART_PARTICIPATION";

function actionLabel(code: QuickActionCode) {
  if (code === "LIQUIDITY_BOOST") return "Liquidity boost";
  if (code === "TIGHTEN_SPREAD") return "Tighten spread";
  if (code === "DIVERSIFY_FLOW") return "Diversify flow";
  return "Kickstart participation";
}

function actionParams(code: QuickActionCode) {
  // These are placeholder knobs. We can tune after live data.
  if (code === "LIQUIDITY_BOOST") {
    return { spread_bps: 10, depth_delta: 500, health_delta: 3, risk_delta: -2 };
  }
  if (code === "TIGHTEN_SPREAD") {
    return { spread_bps: 8, depth_delta: 250, health_delta: 2, risk_delta: -2 };
  }
  if (code === "DIVERSIFY_FLOW") {
    return { maker_bonus_bps: 15, max_trader_share: 0.20, health_delta: 2, risk_delta: -3 };
  }
  return { referral_bonus: 25, min_new_traders: 20, health_delta: 2, risk_delta: -1 };
}

export default function OpsInboxClient({ rows }: { rows: MarketRow[] }) {
  const [protocolFilter, setProtocolFilter] = useState<string>("ALL");
  const [chainFilter, setChainFilter] = useState<string>("ALL");
  const [flagFilter, setFlagFilter] = useState<string>("ALL");
  const [statusFilter, setStatusFilter] = useState<string>("ALL");
  const [overrideOnly, setOverrideOnly] = useState<boolean>(false);
  const [sortKey, setSortKey] = useState<SortKey>("risk_desc");

  // inline override UI state
  const [overrideMarket, setOverrideMarket] = useState<string | null>(null);
  const [overrideRisk, setOverrideRisk] = useState<number>(50);
  const [overrideHealth, setOverrideHealth] = useState<number>(50);
  const [overrideNote, setOverrideNote] = useState<string>("Inbox override");
  const [busyId, setBusyId] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  const protocols = useMemo(() => {
    const set = new Set(rows.map((r) => String(r.protocol ?? "")).filter(Boolean));
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [rows]);

  const chains = useMemo(() => {
    const set = new Set(rows.map((r) => String(r.chain ?? "")).filter(Boolean));
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [rows]);

  const flagCodes = useMemo(() => {
    const set = new Set<string>();
    for (const r of rows) {
      const flags = Array.isArray(r.flags) ? r.flags : [];
      for (const f of flags) {
        const code = String(f.flag_code ?? "").trim();
        if (code) set.add(code);
      }
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [rows]);

  const sorted = useMemo(() => {
    return [...rows].sort((a, b) => {
      const aFlags = Array.isArray(a.flags) ? a.flags : [];
      const bFlags = Array.isArray(b.flags) ? b.flags : [];

      const ar = typeof a.risk_score === "number" ? a.risk_score : -1;
      const br = typeof b.risk_score === "number" ? b.risk_score : -1;

      const ah = typeof a.health_score === "number" ? a.health_score : 1e9;
      const bh = typeof b.health_score === "number" ? b.health_score : 1e9;

      const af = aFlags.length;
      const bf = bFlags.length;

      const av = typeof a.volume === "number" ? a.volume : 0;
      const bv = typeof b.volume === "number" ? b.volume : 0;

      const at = typeof a.trades === "number" ? a.trades : 0;
      const bt = typeof b.trades === "number" ? b.trades : 0;

      let primary = 0;
      if (sortKey === "risk_desc") primary = br - ar;
      else if (sortKey === "health_asc") primary = ah - bh;
      else if (sortKey === "flags_desc") primary = bf - af;
      else if (sortKey === "volume_desc") primary = bv - av;
      else if (sortKey === "trades_desc") primary = bt - at;

      if (primary !== 0) return primary;

      const aMax = Math.max(...(aFlags.map((f) => f.severity) ?? [0]));
      const bMax = Math.max(...(bFlags.map((f) => f.severity) ?? [0]));
      return bMax - aMax;
    });
  }, [rows, sortKey]);

  const filtered = useMemo(() => {
    return sorted.filter((m) => {
      const status = getStatus(m).label;
      const flags = Array.isArray(m.flags) ? m.flags : [];

      if (protocolFilter !== "ALL" && String(m.protocol ?? "") !== protocolFilter) return false;
      if (chainFilter !== "ALL" && String(m.chain ?? "") !== chainFilter) return false;
      if (statusFilter !== "ALL" && status !== statusFilter) return false;
      if (overrideOnly && !m.has_manual_override) return false;

      if (flagFilter !== "ALL") {
        const hasFlag = flags.some((f) => String(f.flag_code ?? "") === flagFilter);
        if (!hasFlag) return false;
      }

      return true;
    });
  }, [sorted, protocolFilter, chainFilter, flagFilter, statusFilter, overrideOnly]);

  async function postOverride(marketId: string) {
    setErr(null);
    setOk(null);
    setBusyId(marketId);

    try {
      const res = await fetch(`${API_BASE}/ops/markets/${encodeURIComponent(marketId)}/overrides`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          risk_score_override: overrideRisk,
          health_score_override: overrideHealth,
          note: overrideNote,
          created_by: "operator",
        }),
      });

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(txt || `Override failed (${res.status})`);
      }

      window.location.reload();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to save override");
    } finally {
      setBusyId(null);
    }
  }

  async function runQuickAction(m: MarketRow, code: QuickActionCode) {
    setErr(null);
    setOk(null);
    setBusyId(`${m.market_id}:${code}`);

    try {
      // 1) Create intervention
      const createRes = await fetch(`${API_BASE}/ops/markets/${encodeURIComponent(m.market_id)}/interventions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          day: m.day, // important: apply against the same day you are viewing in inbox
          incident_id: null,
          action_code: code,
          title: `Quick ${actionLabel(code).toLowerCase()}`,
          status: "PLANNED",
          params: actionParams(code),
        }),
      });

      if (!createRes.ok) {
        const txt = await createRes.text().catch(() => "");
        throw new Error(txt || `Create intervention failed (${createRes.status})`);
      }

      const created = await createRes.json().catch(() => null);
      const interventionId = typeof created?.id === "number" ? created.id : Number(created?.id);

      if (!Number.isFinite(interventionId)) {
        throw new Error("Intervention created but no id returned from API");
      }

      // 2) Apply intervention
      const applyRes = await fetch(`${API_BASE}/ops/interventions/${interventionId}/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });

      if (!applyRes.ok) {
        const txt = await applyRes.text().catch(() => "");
        throw new Error(txt || `Apply intervention failed (${applyRes.status})`);
      }

      setOk(`${actionLabel(code)} applied`);
      window.location.reload();
    } catch (e: any) {
      setErr(e?.message ?? "Quick action failed");
    } finally {
      setBusyId(null);
    }
  }

  function resetFilters() {
    setProtocolFilter("ALL");
    setChainFilter("ALL");
    setFlagFilter("ALL");
    setStatusFilter("ALL");
    setOverrideOnly(false);
    setSortKey("risk_desc");
  }

  function actionsForRegime(r: Regime["code"]): QuickActionCode[] {
    if (r === "THIN_LIQUIDITY") return ["LIQUIDITY_BOOST"];
    if (r === "EXECUTION_STRESS") return ["TIGHTEN_SPREAD"];
    if (r === "CONCENTRATION_RISK") return ["DIVERSIFY_FLOW"];
    if (r === "PARTICIPATION_DECAY") return ["KICKSTART_PARTICIPATION"];
    return [];
  }

  return (
    <>
      <div className="mt-6 flex flex-wrap items-center gap-3">
        <select value={protocolFilter} onChange={(e) => setProtocolFilter(e.target.value)} className="border rounded px-2 py-1 text-sm bg-white">
          <option value="ALL">All protocols</option>
          {protocols.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>

        <select value={chainFilter} onChange={(e) => setChainFilter(e.target.value)} className="border rounded px-2 py-1 text-sm bg-white">
          <option value="ALL">All chains</option>
          {chains.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select value={flagFilter} onChange={(e) => setFlagFilter(e.target.value)} className="border rounded px-2 py-1 text-sm bg-white">
          <option value="ALL">All flags</option>
          {flagCodes.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>

        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} className="border rounded px-2 py-1 text-sm bg-white">
          <option value="ALL">All status</option>
          <option value="Critical">Critical</option>
          <option value="Escalated">Escalated</option>
          <option value="Monitor">Monitor</option>
          <option value="Stabilized">Stabilized</option>
          <option value="Healthy">Healthy</option>
        </select>

        <label className="text-sm flex items-center gap-2 select-none">
          <input type="checkbox" checked={overrideOnly} onChange={(e) => setOverrideOnly(e.target.checked)} />
          Manual override only
        </label>

        <label className="text-sm flex items-center gap-2 select-none">
          <span className="text-xs text-gray-500">Sort</span>
          <select value={sortKey} onChange={(e) => setSortKey(e.target.value as SortKey)} className="border rounded px-2 py-1 text-sm bg-white">
            <option value="risk_desc">Risk high to low</option>
            <option value="health_asc">Health low to high</option>
            <option value="flags_desc">Flags high to low</option>
            <option value="volume_desc">Volume high to low</option>
            <option value="trades_desc">Trades high to low</option>
          </select>
        </label>

        <button onClick={resetFilters} className="text-xs px-2 py-1 rounded border bg-gray-50 hover:bg-gray-100">
          Reset
        </button>

        <div className="text-xs text-gray-500 ml-auto">
          Showing <span className="font-medium text-gray-700">{filtered.length}</span> of{" "}
          <span className="font-medium text-gray-700">{rows.length}</span>
        </div>
      </div>

      {err ? <div className="mt-4 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-800">{err}</div> : null}
      {ok ? <div className="mt-4 rounded-lg border border-green-200 bg-green-50 p-3 text-sm text-green-800">{ok}</div> : null}

      {filtered.length === 0 ? (
        <p className="text-sm text-gray-500 mt-6">No markets match your filters.</p>
      ) : (
        <div className="mt-4 space-y-3">
          {filtered.map((m) => {
            const status = getStatus(m);
            const isEscalate = status.label === "Critical" || status.label === "Escalated";

            const flags = Array.isArray(m.flags) ? m.flags : [];
            const maxSeverity = Math.max(...(flags.map((f) => f.severity) ?? [0]));

            const borderCls =
              status.label === "Critical"
                ? "border-red-400 shadow-sm"
                : isEscalate
                ? "border-red-300"
                : status.label === "Monitor"
                ? "border-yellow-300"
                : "border-gray-200";

            const rp = riskPill(m.risk_score);
            const regime = microstructureRegime(m);
            const quickActions = actionsForRegime(regime.code);

            return (
              <div key={m.market_id} className={`rounded-2xl p-4 bg-white border ${borderCls}`}>
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <Link href={`/ops/${encodeURIComponent(m.market_id)}`} className="text-lg font-medium hover:underline">
                      {m.title}
                    </Link>

                    <div className="text-xs text-gray-500 mt-1 truncate">
                      {m.market_id} {" - "} {m.protocol} {" - "} {m.chain} {" - "} {m.category ?? "uncategorized"} {" - "} {m.day}
                    </div>

                    {/* microstructure snapshot row */}
                    <div className="flex flex-wrap gap-2 mt-2 text-xs">
                      <span className="px-2 py-0.5 rounded-full border bg-gray-50">spread {fmtFloat(m.spread_median, 4)}</span>
                      <span className="px-2 py-0.5 rounded-full border bg-gray-50">depth {fmtNumber(m.depth_2pct_median)}</span>
                      <span className="px-2 py-0.5 rounded-full border bg-gray-50">HHI {fmtFloat(m.concentration_hhi, 4)}</span>
                      <span className="px-2 py-0.5 rounded-full border bg-gray-50">traders {fmtNumber(m.unique_traders)}</span>
                    </div>

                    {/* triage badges */}
                    <div className="mt-2 flex flex-wrap gap-2">
                      <span className={`text-xs px-2 py-1 rounded-full border ${status.cls}`}>{status.label}</span>

                      <span className={`text-xs px-3 py-1 rounded-full border font-medium ${rp.cls}`}>{rp.label}</span>

                      <span
                        title={regime.reason ? `Why: ${regime.reason}` : "Microstructure regime"}
                        className={`text-xs px-2 py-1 rounded-full border ${regime.cls}`}
                      >
                        {regime.label}
                      </span>

                      {(() => {
                        const band = riskBand(m.risk_score);
                        return (
                          <span className={`text-xs px-2 py-1 rounded-full border ${band.cls}`}>
                            band {band.label}
                          </span>
                        );
                      })()}

                      <span className="text-xs px-2 py-1 rounded-full border bg-gray-50">health {m.health_score ?? "-"}</span>
                      <span className="text-xs px-2 py-1 rounded-full border bg-gray-50">flags {flags.length}</span>

                      {m.has_manual_override ? (
                        <span className="text-xs px-2 py-1 rounded-full border bg-blue-50 text-blue-800 border-blue-200">override</span>
                      ) : (
                        <span className="text-xs px-2 py-1 rounded-full border bg-gray-50 text-gray-500 border-gray-200">no override</span>
                      )}

                      <button
                        onClick={() => {
                          setErr(null);
                          setOk(null);
                          if (overrideMarket === m.market_id) {
                            setOverrideMarket(null);
                            return;
                          }
                          setOverrideMarket(m.market_id);
                          setOverrideRisk(m.risk_score ?? 50);
                          setOverrideHealth(m.health_score ?? 50);
                          setOverrideNote("Inbox override");
                        }}
                        className="text-xs px-2 py-1 rounded-full border bg-blue-50 text-blue-800 border-blue-200 hover:bg-blue-100"
                      >
                        Manual override
                      </button>
                    </div>

                    {/* NEW: regime quick actions */}
                    {quickActions.length ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {quickActions.map((code) => {
                          const bid = `${m.market_id}:${code}`;
                          return (
                            <button
                              key={code}
                              onClick={() => runQuickAction(m, code)}
                              disabled={busyId !== null}
                              className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
                              title={`Create + apply ${actionLabel(code)} intervention`}
                            >
                              {busyId === bid ? "Applying..." : actionLabel(code)}
                            </button>
                          );
                        })}
                      </div>
                    ) : null}
                  </div>

                  <div className="text-xs text-gray-600 shrink-0 text-right">
                    <div>vol {fmtNumber(m.volume)}</div>
                    <div>trades {fmtNumber(m.trades)}</div>
                    <div>traders {fmtNumber(m.unique_traders)}</div>
                  </div>
                </div>

                {flags.length ? (
                  <div className="mt-3 flex flex-wrap gap-2">
                    {flags.map((f, i) => (
                      <span key={`${m.market_id}-${f.flag_code}-${i}`} className={`text-xs px-2 py-1 rounded-full border ${badgeStyle(f.severity)}`}>
                        {f.flag_code} (sev {f.severity})
                      </span>
                    ))}
                  </div>
                ) : (
                  <div className="mt-3 text-xs text-gray-400">No flags</div>
                )}

                {/* existing quick actions (links) */}
                <div className="mt-4 flex flex-wrap gap-2">
                  <Link href={`/ops/${encodeURIComponent(m.market_id)}#incident`} className="text-xs px-3 py-1 rounded border bg-gray-50 hover:bg-gray-100">
                    Open incident
                  </Link>

                  <Link
                    href={`/ops/${encodeURIComponent(m.market_id)}#intervention`}
                    className="text-xs px-3 py-1 rounded border bg-gray-50 hover:bg-gray-100"
                  >
                    Apply intervention
                  </Link>

                  {maxSeverity >= 4 ? <span className="text-xs px-3 py-1 rounded border bg-red-50 text-red-800 border-red-200">needs attention</span> : null}
                </div>

                {/* inline override panel */}
                {overrideMarket === m.market_id ? (
                  <div className="mt-4 border rounded-lg p-3 bg-blue-50 space-y-2">
                    <div className="text-xs text-blue-900">
                      POST <code className="px-1 py-0.5 border rounded bg-white">{`/ops/markets/${m.market_id}/overrides`}</code>
                    </div>

                    <div className="flex flex-wrap gap-2 items-end">
                      <label className="text-xs">
                        Risk override
                        <input
                          type="number"
                          value={overrideRisk}
                          onChange={(e) => setOverrideRisk(Number(e.target.value))}
                          className="mt-1 border rounded px-2 py-1 text-sm w-28 bg-white"
                        />
                      </label>

                      <label className="text-xs">
                        Health override
                        <input
                          type="number"
                          value={overrideHealth}
                          onChange={(e) => setOverrideHealth(Number(e.target.value))}
                          className="mt-1 border rounded px-2 py-1 text-sm w-28 bg-white"
                        />
                      </label>

                      <label className="text-xs flex-1 min-w-[220px]">
                        Note
                        <input
                          value={overrideNote}
                          onChange={(e) => setOverrideNote(e.target.value)}
                          className="mt-1 border rounded px-2 py-1 text-sm w-full bg-white"
                          placeholder="Why are you overriding?"
                        />
                      </label>

                      <button
                        onClick={() => postOverride(m.market_id)}
                        disabled={busyId === m.market_id}
                        className="text-xs px-3 py-2 rounded border bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
                      >
                        {busyId === m.market_id ? "Saving..." : "Save override"}
                      </button>

                      <button
                        onClick={() => setOverrideMarket(null)}
                        disabled={busyId === m.market_id}
                        className="text-xs px-3 py-2 rounded border bg-white hover:bg-gray-50 disabled:opacity-50"
                      >
                        Cancel
                      </button>
                    </div>

                    <div className="text-[11px] text-blue-900/80">
                      This writes today’s override for the market. Refresh reloads the inbox to show updated scores and the manual override badge.
                    </div>
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </>
  );
}