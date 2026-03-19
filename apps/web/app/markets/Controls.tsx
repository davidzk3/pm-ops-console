"use client";

import { useMemo } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

type Props = {
  initialDay: string;
  initialLimit: number;
  initialSort: string;
  initialOrder: "asc" | "desc";
};

type Preset = {
  key: string;
  label: string;
  sort: string;
  order: "asc" | "desc";
};

const PRESETS: Preset[] = [
  { key: "durable", label: "Top Durable Markets", sort: "structural_score", order: "desc" },
  { key: "active", label: "Most Active Markets", sort: "trades", order: "desc" },
  { key: "volume", label: "Highest Volume Markets", sort: "volume", order: "desc" },
  { key: "tight", label: "Tightest Spreads", sort: "avg_spread", order: "asc" },
  { key: "bbo", label: "Most BBO Ticks", sort: "bbo_ticks", order: "desc" },
  { key: "burst", label: "Burstiest Markets", sort: "burst_score", order: "desc" },
];

function normalizeDay(day: string) {
  // allow empty, user may want latest day behavior
  return day.trim();
}

export default function Controls({ initialDay, initialLimit, initialSort, initialOrder }: Props) {
  const router = useRouter();
  const pathname = usePathname();
  const sp = useSearchParams();

  const current = useMemo(() => {
    const day = sp.get("day") ?? initialDay;
    const limit = Number(sp.get("limit") ?? initialLimit);
    const sort = sp.get("sort") ?? initialSort;
    const order = (sp.get("order") as "asc" | "desc") ?? initialOrder;
    return { day, limit, sort, order };
  }, [sp, initialDay, initialLimit, initialSort, initialOrder]);

  function push(next: Partial<typeof current>) {
    const q = new URLSearchParams(sp.toString());

    const day = next.day !== undefined ? normalizeDay(next.day) : current.day;
    const limit = next.limit !== undefined ? next.limit : current.limit;
    const sort = next.sort !== undefined ? next.sort : current.sort;
    const order = next.order !== undefined ? next.order : current.order;

    if (day) q.set("day", day);
    else q.delete("day"); // empty day means "let API choose latest day"

    q.set("limit", String(limit));
    q.set("sort", sort);
    q.set("order", order);

    router.replace(`${pathname}?${q.toString()}`);
  }

  function isActivePreset(p: Preset) {
    return current.sort === p.sort && current.order === p.order;
  }

  return (
    <div style={{ marginTop: 10 }}>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {PRESETS.map((p) => (
          <button
            key={p.key}
            type="button"
            onClick={() => push({ sort: p.sort, order: p.order })}
            style={{
              padding: "6px 10px",
              borderRadius: 999,
              border: "1px solid #111",
              background: isActivePreset(p) ? "#111" : "white",
              color: isActivePreset(p) ? "white" : "#111",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            {p.label}
          </button>
        ))}
      </div>

      <div style={{ marginTop: 10, display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>day</span>
          <input
            value={current.day}
            onChange={(e) => push({ day: e.target.value })}
            placeholder="YYYY-MM-DD"
            style={{ padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          />
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>limit</span>
          <input
            value={String(current.limit)}
            onChange={(e) => {
              const n = Number(e.target.value);
              push({ limit: Number.isFinite(n) && n > 0 ? n : current.limit });
            }}
            style={{ width: 90, padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          />
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>sort</span>
          <select
            value={current.sort}
            onChange={(e) => push({ sort: e.target.value })}
            style={{ padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          >
            <option value="structural_score">structural_score</option>
            <option value="trades">trades</option>
            <option value="volume">volume</option>
            <option value="avg_spread">avg_spread</option>
            <option value="price_volatility">price_volatility</option>
            <option value="bbo_ticks">bbo_ticks</option>
            <option value="burst_score">burst_score</option>
            <option value="identity_coverage">identity_coverage</option>
            <option value="hhi">hhi</option>
            <option value="unique_traders">unique_traders</option>
          </select>
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>order</span>
          <select
            value={current.order}
            onChange={(e) => push({ order: e.target.value as "asc" | "desc" })}
            style={{ padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          >
            <option value="desc">desc</option>
            <option value="asc">asc</option>
          </select>
        </label>

        <button
          type="button"
          onClick={() => router.refresh()}
          style={{
            padding: "6px 12px",
            border: "1px solid #111",
            borderRadius: 10,
            background: "#111",
            color: "white",
            cursor: "pointer",
          }}
        >
          refresh
        </button>
      </div>

      <div style={{ marginTop: 8, color: "#666", fontSize: 12 }}>
        Tip: compute writes daily rows. If day is empty, API auto selects latest day with data.
      </div>
    </div>
  );
}