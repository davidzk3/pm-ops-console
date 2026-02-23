"use client";

type EffectRow = {
  id: number;
  market_id: string;
  incident_id: number | null;
  day: string;
  applied_day?: string;
  action_code: string;
  title: string;
  status: string;
  params: Record<string, any>;
  created_by: string;
  created_at: string;
  applied_at: string | null;

  before_day: string;
  after_day: string;

  before: Record<string, number | null>;
  after: Record<string, number | null>;
  delta: Record<string, number | null>;

  delta_score?: number | null;
  roi_score?: number | null;
};

type UiMeta = {
  heat: {
    good_up: string[];
    good_down: string[];
    steps: Record<string, number>;
    precision: Record<string, number>;
  };
};

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function isNumber(x: any): x is number {
  return typeof x === "number" && Number.isFinite(x);
}

function formatNumber(v: any, decimals: number) {
  if (!isNumber(v)) return "–";
  if (decimals <= 0) return String(Math.round(v));
  return v.toFixed(decimals);
}

/**
Heat logic

We convert raw delta into a "goodness signed value":
- if good_up metric: positive is good
- if good_down metric: negative is good, so we flip sign

Then we bucket intensity by steps:
0 none
1 mild
2 medium
3 strong
4 max
*/
function getHeat(meta: UiMeta | null, metric: string, rawDelta: number | null | undefined) {
  if (!meta || !isNumber(rawDelta)) {
    return { level: 0, isGood: null as boolean | null };
  }

  const { good_up, good_down, steps } = meta.heat;

  let adjusted = rawDelta;

  if (good_down.includes(metric)) {
    adjusted = -rawDelta;
  }

  const step = isNumber(steps[metric]) && steps[metric] > 0 ? steps[metric] : 1;
  const level = clamp(Math.round(Math.abs(adjusted) / step), 0, 4);

  if (level === 0) return { level: 0, isGood: null as boolean | null };
  return { level, isGood: adjusted > 0 };
}

/**
Tailwind classes for the heat background.
You can tune these later without touching business logic.
*/
function heatClass(level: number, isGood: boolean | null) {
  if (level === 0 || isGood === null) return "bg-transparent";

  if (isGood) {
    if (level === 1) return "bg-emerald-50";
    if (level === 2) return "bg-emerald-100";
    if (level === 3) return "bg-emerald-200";
    return "bg-emerald-300";
  } else {
    if (level === 1) return "bg-rose-50";
    if (level === 2) return "bg-rose-100";
    if (level === 3) return "bg-rose-200";
    return "bg-rose-300";
  }
}

function signed(v: number | null | undefined, decimals: number) {
  if (!isNumber(v)) return "–";
  const s = v > 0 ? "+" : "";
  return s + formatNumber(v, decimals);
}

export default function InterventionsEffectivenessTable(props: {
  rows: EffectRow[];
  ui?: UiMeta | null;
}) {
  const rows = props.rows || [];
  const ui = props.ui || null;

  const metrics: Array<{ key: string; label: string }> = [
    { key: "risk_score", label: "Risk" },
    { key: "health_score", label: "Health" },
    { key: "spread_median", label: "Spread" },
    { key: "depth_2pct_median", label: "Depth 2pct" },
    { key: "unique_traders", label: "Traders" },
    { key: "concentration_hhi", label: "HHI" },
  ];

  const precision = (k: string) => {
    const p = ui?.heat?.precision?.[k];
    return typeof p === "number" ? p : 2;
  };

  return (
    <div className="rounded-2xl border p-4">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <div className="text-lg font-semibold">Intervention effectiveness</div>
          <div className="text-sm text-slate-500">Before day vs applied day deltas</div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-[980px] w-full border-collapse text-sm">
          <thead>
            <tr className="text-left text-slate-600">
              <th className="py-2 pr-3">Applied</th>
              <th className="py-2 pr-3">Action</th>
              <th className="py-2 pr-3">Title</th>
              <th className="py-2 pr-3">Delta score</th>
              <th className="py-2 pr-3">ROI</th>

              {metrics.map((m) => (
                <th key={m.key} className="py-2 pr-3">
                  {m.label}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td className="py-3 text-slate-500" colSpan={5 + metrics.length}>
                  No interventions yet
                </td>
              </tr>
            ) : (
              rows.map((r) => {
                const applied = r.applied_day || r.day;
                const deltaScore = r.delta_score ?? null;
                const roiScore = r.roi_score ?? null;

                return (
                  <tr key={r.id} className="border-t">
                    <td className="py-2 pr-3 whitespace-nowrap">{applied}</td>
                    <td className="py-2 pr-3 whitespace-nowrap font-mono">{r.action_code}</td>
                    <td className="py-2 pr-3 max-w-[320px] truncate" title={r.title}>
                      {r.title}
                    </td>

                    {/* delta_score heat */}
                    {(() => {
                      const metric = "delta_score";
                      const { level, isGood } = getHeat(ui, metric, isNumber(deltaScore) ? deltaScore : null);
                      return (
                        <td className={"py-2 pr-3 whitespace-nowrap " + heatClass(level, isGood)}>
                          {signed(deltaScore, precision(metric))}
                        </td>
                      );
                    })()}

                    {/* roi_score heat */}
                    {(() => {
                      const metric = "roi_score";
                      const { level, isGood } = getHeat(ui, metric, isNumber(roiScore) ? roiScore : null);
                      return (
                        <td className={"py-2 pr-3 whitespace-nowrap " + heatClass(level, isGood)}>
                          {signed(roiScore, precision(metric))}
                        </td>
                      );
                    })()}

                    {metrics.map((m) => {
                      const v = r.delta?.[m.key] ?? null;
                      const { level, isGood } = getHeat(ui, m.key, isNumber(v) ? v : null);
                      return (
                        <td
                          key={m.key}
                          className={"py-2 pr-3 whitespace-nowrap " + heatClass(level, isGood)}
                          title={`before ${formatNumber(r.before?.[m.key], precision(m.key))} → after ${formatNumber(
                            r.after?.[m.key],
                            precision(m.key)
                          )}`}
                        >
                          {signed(isNumber(v) ? v : null, precision(m.key))}
                        </td>
                      );
                    })}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {ui ? (
        <div className="mt-3 text-xs text-slate-500">
          Heat is normalized using backend steps. Green means improvement. Red means deterioration.
        </div>
      ) : null}
    </div>
  );
}