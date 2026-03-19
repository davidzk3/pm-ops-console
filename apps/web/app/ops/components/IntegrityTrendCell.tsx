import Sparkline from "./Sparkline";

type IntegrityHistoryPoint = {
  day: string;
  integrity_score: number | null;
};

type IntegrityTrendCellProps = {
  points: IntegrityHistoryPoint[];
};

export default function IntegrityTrendCell({
  points,
}: IntegrityTrendCellProps) {
  const latest =
    points.length > 0 ? points[points.length - 1]?.integrity_score ?? null : null;

  const prev =
    points.length > 1 ? points[points.length - 2]?.integrity_score ?? null : null;

  const delta =
    latest !== null && prev !== null ? Number((latest - prev).toFixed(1)) : null;

  const deltaClass =
    delta === null
      ? "text-gray-400"
      : delta > 0
        ? "text-emerald-600"
        : delta < 0
          ? "text-red-600"
          : "text-gray-500";

  return (
    <div className="flex items-center gap-3">
      <Sparkline
        points={points.map((p) => ({
          day: p.day,
          value: p.integrity_score,
        }))}
      />
      <div className="min-w-[52px] text-right">
        <div className="text-sm font-medium text-gray-900">
          {latest !== null ? latest.toFixed(1) : "—"}
        </div>
        <div className={`text-xs ${deltaClass}`}>
          {delta === null ? "—" : `${delta > 0 ? "+" : ""}${delta}`}
        </div>
      </div>
    </div>
  );
}