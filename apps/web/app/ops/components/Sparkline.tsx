type SparklinePoint = {
  day: string;
  value: number | null;
};

type SparklineProps = {
  points: SparklinePoint[];
  width?: number;
  height?: number;
  strokeClassName?: string;
};

export default function Sparkline({
  points,
  width = 120,
  height = 32,
  strokeClassName = "stroke-cyan-500",
}: SparklineProps) {
  const clean = points.filter((p) => typeof p.value === "number") as Array<{
    day: string;
    value: number;
  }>;

  if (clean.length === 0) {
    return <div className="text-xs text-gray-400">No history</div>;
  }

  if (clean.length === 1) {
    return (
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className="overflow-visible">
        <circle
          cx={width / 2}
          cy={height / 2}
          r="3"
          className={strokeClassName.replace("stroke-", "fill-")}
        />
      </svg>
    );
  }

  const values = clean.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const xStep = width / (clean.length - 1);

  const coords = clean.map((p, i) => {
    const x = i * xStep;
    const y = height - ((p.value - min) / range) * (height - 4) - 2;
    return { x, y, value: p.value };
  });

  const d = coords
    .map((c, i) => `${i === 0 ? "M" : "L"} ${c.x.toFixed(2)} ${c.y.toFixed(2)}`)
    .join(" ");

  const last = coords[coords.length - 1];

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className="overflow-visible">
      <path
        d={d}
        fill="none"
        strokeWidth="2"
        className={strokeClassName}
        vectorEffect="non-scaling-stroke"
      />
      <circle
        cx={last.x}
        cy={last.y}
        r="2.5"
        className={strokeClassName.replace("stroke-", "fill-")}
      />
    </svg>
  );
}