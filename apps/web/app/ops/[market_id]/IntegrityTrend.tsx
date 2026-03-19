"use client";

import { useEffect, useMemo, useState } from "react";
import { fetchIntegrityHistory } from "../../lib (rename back to lib)/api";

type Point = {
  day: string;
  integrity_score: number | null;
  radar_risk_score: number | null;
  manipulation_score: number | null;
  regime: string | null;
};

type IntegrityHistoryResponse = {
  market_id: string;
  points: Point[];
  count: number;
};

function fmtNum(value: number | null | undefined, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "—";
  }
  return Number(value).toFixed(digits);
}

function directionMeta(current: number | null | undefined, previous: number | null | undefined) {
  if (
    current === null ||
    current === undefined ||
    previous === null ||
    previous === undefined ||
    !Number.isFinite(current) ||
    !Number.isFinite(previous)
  ) {
    return {
      label: "flat",
      delta: null as number | null,
      cls: "border-gray-200 bg-gray-50 text-gray-700",
    };
  }

  const delta = Number(current) - Number(previous);

  if (Math.abs(delta) < 1e-9) {
    return {
      label: "flat",
      delta,
      cls: "border-gray-200 bg-gray-50 text-gray-700",
    };
  }

  if (delta > 0) {
    return {
      label: "improving",
      delta,
      cls: "border-green-200 bg-green-50 text-green-800",
    };
  }

  return {
    label: "deteriorating",
    delta,
    cls: "border-red-200 bg-red-50 text-red-800",
  };
}

function regimeBadgeClass(regime: string | null | undefined) {
  const r = (regime ?? "").toLowerCase();

  if (r === "organic_market") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (r === "whale_dominated") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  if (r === "farming_dominated") {
    return "border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700";
  }
  if (r === "thin_market") {
    return "border-yellow-200 bg-yellow-50 text-yellow-900";
  }
  if (r === "inactive") {
    return "border-gray-200 bg-gray-100 text-gray-700";
  }
  return "border-blue-200 bg-blue-50 text-blue-700";
}

function summaryAlert(point: Point | null) {
  if (!point) {
    return {
      text: "No integrity snapshot available yet.",
      cls: "border-gray-200 bg-gray-50 text-gray-700",
    };
  }

  const integrity = typeof point.integrity_score === "number" ? point.integrity_score : NaN;
  const radar = typeof point.radar_risk_score === "number" ? point.radar_risk_score : NaN;
  const manipulation =
    typeof point.manipulation_score === "number" ? point.manipulation_score : NaN;
  const regime = String(point.regime ?? "").toLowerCase();

  if ((Number.isFinite(manipulation) && manipulation >= 0.7) || (Number.isFinite(radar) && radar >= 70)) {
    return {
      text: "High operator attention required. Structural or manipulation risk is elevated.",
      cls: "border-red-200 bg-red-50 text-red-800",
    };
  }

  if (
    regime === "thin_market" ||
    regime === "whale_dominated" ||
    (Number.isFinite(integrity) && integrity < 50)
  ) {
    return {
      text: "Market structure looks fragile. Monitor depth, concentration, and participant quality.",
      cls: "border-yellow-200 bg-yellow-50 text-yellow-900",
    };
  }

  return {
    text: "Structure appears stable in the latest snapshot.",
    cls: "border-green-200 bg-green-50 text-green-800",
  };
}

export default function IntegrityTrend({
  marketId,
}: {
  marketId: string;
}) {
  const [points, setPoints] = useState<Point[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const res =
          (await fetchIntegrityHistory(marketId)) as IntegrityHistoryResponse;

        if (!cancelled) {
          const rows = Array.isArray(res.points) ? [...res.points] : [];
          rows.sort((a, b) => String(a.day).localeCompare(String(b.day)));
          setPoints(rows);
        }
      } catch (err) {
        console.error("Failed to load integrity history", err);
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, [marketId]);

  const latest = useMemo(() => {
    return points.length ? points[points.length - 1] : null;
  }, [points]);

  const previous = useMemo(() => {
    return points.length >= 2 ? points[points.length - 2] : null;
  }, [points]);

  const direction = directionMeta(
    latest?.integrity_score ?? null,
    previous?.integrity_score ?? null,
  );

  const alert = summaryAlert(latest);

  if (loading) {
    return (
      <div className="border rounded-xl p-4 bg-white text-sm text-gray-500">
        Loading integrity history…
      </div>
    );
  }

  if (!points.length) {
    return (
      <div className="border rounded-xl p-4 bg-white text-sm text-gray-500">
        No historical integrity data yet.
      </div>
    );
  }

  return (
    <div className="border rounded-xl p-4 bg-white">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-sm font-medium">Integrity trend</div>
          <div className="mt-1 text-xs text-gray-500">
            Historical structural quality for this market.
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className="inline-flex rounded-full border bg-gray-50 px-2.5 py-1 text-xs text-gray-700">
            points {points.length}
          </span>

          <span
            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${direction.cls}`}
          >
            {direction.label}
            {direction.delta !== null
              ? ` (${direction.delta > 0 ? "+" : ""}${direction.delta.toFixed(1)})`
              : ""}
          </span>

          <span
            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${regimeBadgeClass(
              latest?.regime,
            )}`}
          >
            {latest?.regime ?? "unknown"}
          </span>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
        <div className="rounded-xl border bg-gray-50 p-3">
          <div className="text-[11px] uppercase tracking-wide text-gray-500">
            Latest integrity
          </div>
          <div className="mt-1 text-xl font-semibold text-gray-900">
            {fmtNum(latest?.integrity_score, 1)}
          </div>
        </div>

        <div className="rounded-xl border bg-gray-50 p-3">
          <div className="text-[11px] uppercase tracking-wide text-gray-500">
            Radar risk
          </div>
          <div className="mt-1 text-xl font-semibold text-gray-900">
            {fmtNum(latest?.radar_risk_score, 2)}
          </div>
        </div>

        <div className="rounded-xl border bg-gray-50 p-3">
          <div className="text-[11px] uppercase tracking-wide text-gray-500">
            Manipulation
          </div>
          <div className="mt-1 text-xl font-semibold text-gray-900">
            {fmtNum(latest?.manipulation_score, 2)}
          </div>
        </div>

        <div className="rounded-xl border bg-gray-50 p-3">
          <div className="text-[11px] uppercase tracking-wide text-gray-500">
            Latest day
          </div>
          <div className="mt-1 text-sm font-semibold text-gray-900">
            {latest?.day ?? "—"}
          </div>
        </div>
      </div>

      <div className={`mt-4 rounded-xl border px-3 py-2 text-sm ${alert.cls}`}>
        {alert.text}
      </div>

      <table className="mt-4 w-full text-sm">
        <thead className="text-gray-500">
          <tr>
            <th className="text-left py-1">Day</th>
            <th className="text-left py-1">Integrity</th>
            <th className="text-left py-1">Radar</th>
            <th className="text-left py-1">Manipulation</th>
            <th className="text-left py-1">Regime</th>
          </tr>
        </thead>

        <tbody>
          {points
            .slice()
            .reverse()
            .map((p) => (
              <tr key={p.day} className="border-t">
                <td className="py-1">{p.day}</td>

                <td className="py-1">
                  {p.integrity_score != null
                    ? p.integrity_score.toFixed(1)
                    : "—"}
                </td>

                <td className="py-1">
                  {p.radar_risk_score != null
                    ? p.radar_risk_score.toFixed(2)
                    : "—"}
                </td>

                <td className="py-1">
                  {p.manipulation_score != null
                    ? p.manipulation_score.toFixed(2)
                    : "—"}
                </td>

                <td className="py-1">{p.regime ?? "—"}</td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
}