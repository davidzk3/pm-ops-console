// app/markets/page.tsx
import Link from "next/link";

type MicroRow = {
  market_id: string;
  day: string;
  window_hours: number;

  volume: number;
  trades: number;
  unique_traders: number;

  identity_coverage: number | null;
  identity_blind: boolean;

  top1_trader_share: number | null;
  top5_trader_share: number | null;
  hhi: number | null;

  price_volatility: number | null;

  bbo_ticks: number;
  avg_spread: number | null;

  suspicious_burst_flag: boolean;
  burst_score: number | null;

  structural_score: number | null;

  structural_percentile?: number | null;
  structural_rank?: number | null;

  title: string | null;
  url: string | null;
};

type LatestDayResponse = {
  day: string | null;
  rows: number;
};

function fmt(n: any, d = 4) {
  if (n === null || n === undefined) return "";
  if (typeof n === "number") return Number.isFinite(n) ? n.toFixed(d) : "";
  return String(n);
}

function fmtInt(n: any) {
  if (n === null || n === undefined) return "";
  if (typeof n === "number") return Number.isFinite(n) ? Math.trunc(n).toString() : "";
  return String(n);
}

function safeUrl(u: string | null) {
  if (!u) return null;
  try {
    new URL(u);
    return u;
  } catch {
    return null;
  }
}

function isIsoDay(s: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function todayIsoDayUtc() {
  return new Date().toISOString().slice(0, 10);
}

async function fetchLatestDay(): Promise<LatestDayResponse> {
  const base = process.env.API_BASE_URL || "http://127.0.0.1:8000";
  const url = `${base}/ops/microstructure/latest_day`;

  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`latest_day failed ${res.status} ${txt}`);
  }
  return (await res.json()) as LatestDayResponse;
}

async function fetchTop(params: { day: string; limit: number; sort: string; order: string }) {
  const base = process.env.API_BASE_URL || "http://127.0.0.1:8000";

  const q = new URLSearchParams();
  q.set("day", params.day);
  q.set("limit", String(params.limit));
  q.set("sort", params.sort);
  q.set("order", params.order);

  const url = `${base}/ops/microstructure/top?${q.toString()}`;

  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`fetch failed ${res.status} ${txt}`);
  }
  return (await res.json()) as MicroRow[];
}

function buildHref(baseParams: URLSearchParams, patch: Record<string, string>) {
  const p = new URLSearchParams(baseParams.toString());
  for (const [k, v] of Object.entries(patch)) p.set(k, v);
  return `/markets?${p.toString()}`;
}

export default async function MarketsPage(props: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = (await props.searchParams) ?? {};

  const dayParam = typeof sp.day === "string" ? sp.day : undefined;
  const limitParam = typeof sp.limit === "string" ? Number(sp.limit) : 20;
  const sortParam = typeof sp.sort === "string" ? sp.sort : "structural_score";
  const orderParam = typeof sp.order === "string" ? sp.order : "desc";

  let day = dayParam && isIsoDay(dayParam) ? dayParam : undefined;
  let latestInfo: LatestDayResponse | null = null;

  if (!day) {
    try {
      latestInfo = await fetchLatestDay();
      day = latestInfo?.day && isIsoDay(latestInfo.day) ? latestInfo.day : todayIsoDayUtc();
    } catch {
      day = todayIsoDayUtc();
    }
  }

  let rows: MicroRow[] = [];
  let fetchError: string | null = null;

  try {
    rows = await fetchTop({
      day,
      limit: Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 20,
      sort: sortParam,
      order: orderParam === "asc" ? "asc" : "desc",
    });
  } catch (e: any) {
    fetchError = e?.message ? String(e.message) : "unknown error";
  }

  const baseSp = new URLSearchParams();
  baseSp.set("day", day);
  baseSp.set("limit", String(Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 20));
  baseSp.set("sort", sortParam);
  baseSp.set("order", orderParam === "asc" ? "asc" : "desc");

  const presets: Array<{ label: string; patch: Record<string, string> }> = [
    { label: "Top Durable Markets", patch: { sort: "structural_score", order: "desc" } },
    { label: "Most Active Markets", patch: { sort: "trades", order: "desc" } },
    { label: "Highest Volume Markets", patch: { sort: "volume", order: "desc" } },
    { label: "Tightest Spreads", patch: { sort: "avg_spread", order: "asc" } },
    { label: "Most BBO Ticks", patch: { sort: "bbo_ticks", order: "desc" } },
    { label: "Burstiest Markets", patch: { sort: "burst_score", order: "desc" } },
  ];

  const queryForDisplay = new URLSearchParams(baseSp.toString());

  return (
    <div style={{ padding: 16, fontFamily: "ui-sans-serif, system-ui" }}>
      <div style={{ display: "flex", gap: 12, alignItems: "baseline", flexWrap: "wrap" }}>
        <h1 style={{ fontSize: 24, margin: 0 }}>Markets ranked by structural quality</h1>
        <div style={{ color: "#666" }}>source: market_microstructure_daily</div>
      </div>

      <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
        {presets.map((p) => {
          const active = sortParam === p.patch.sort && orderParam === p.patch.order;
          return (
            <Link
              key={p.label}
              href={buildHref(baseSp, p.patch)}
              style={{
                padding: "6px 10px",
                borderRadius: 999,
                border: active ? "1px solid #111" : "1px solid #ddd",
                textDecoration: "none",
                background: active ? "#111" : "white",
                color: active ? "white" : "#111",
                fontSize: 12,
                fontWeight: 600,
              }}
            >
              {p.label}
            </Link>
          );
        })}
      </div>

      <form method="get" style={{ marginTop: 12, display: "flex", gap: 12, flexWrap: "wrap" }}>
        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>day</span>
          <input
            name="day"
            defaultValue={day}
            placeholder="YYYY-MM-DD"
            style={{ padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          />
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>limit</span>
          <input
            name="limit"
            defaultValue={String(limitParam)}
            style={{ width: 90, padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          />
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>sort</span>
          <select
            name="sort"
            defaultValue={sortParam}
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
          </select>
        </label>

        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span>order</span>
          <select
            name="order"
            defaultValue={orderParam}
            style={{ padding: "6px 8px", border: "1px solid #ddd", borderRadius: 8 }}
          >
            <option value="desc">desc</option>
            <option value="asc">asc</option>
          </select>
        </label>

        <button
          type="submit"
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
      </form>

      <div style={{ marginTop: 10, color: "#666", fontSize: 12 }}>
        Tip: run compute for a day, then load that day here. Example: <code>2026-02-27</code>.
        {latestInfo?.day ? (
          <>
            {" "}
            Latest day with data: <code>{latestInfo.day}</code>.
          </>
        ) : null}
      </div>

      {fetchError ? (
        <div
          style={{
            marginTop: 12,
            padding: 12,
            borderRadius: 10,
            border: "1px solid #f0caca",
            background: "#fff7f7",
            color: "#7a1f1f",
            fontSize: 12,
          }}
        >
          API error: {fetchError}
        </div>
      ) : null}

      {!fetchError && rows.length === 0 ? (
        <div
          style={{
            marginTop: 12,
            padding: 12,
            borderRadius: 10,
            border: "1px solid #eee",
            background: "#fafafa",
            color: "#444",
            fontSize: 12,
          }}
        >
          No rows returned for <code>{day}</code>. Try another day you computed.
        </div>
      ) : null}

      <div style={{ marginTop: 14, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #eee" }}>
              <th style={{ padding: 10 }}>rank</th>
              <th style={{ padding: 10 }}>market</th>
              <th style={{ padding: 10 }}>score</th>
              <th style={{ padding: 10 }}>pct</th>
              <th style={{ padding: 10 }}>trades</th>
              <th style={{ padding: 10 }}>volume</th>
              <th style={{ padding: 10 }}>spread</th>
              <th style={{ padding: 10 }}>bbo</th>
              <th style={{ padding: 10 }}>flags</th>
              <th style={{ padding: 10 }}>identity</th>
            </tr>
          </thead>

          <tbody>
            {rows.map((r, i) => {
              const url = safeUrl(r.url);
              const title = r.title || r.market_id;

              const pct =
                r.structural_percentile === null || r.structural_percentile === undefined
                  ? ""
                  : `${Math.round(r.structural_percentile * 100)}%`;

              const rank = r.structural_rank ?? i + 1;

              const flags = [r.suspicious_burst_flag ? "burst" : null, r.identity_blind ? "blind" : null].filter(
                Boolean
              ) as string[];

              return (
                <tr key={`${r.market_id}-${i}`} style={{ borderBottom: "1px solid #f2f2f2" }}>
                  <td style={{ padding: 10, whiteSpace: "nowrap" }}>{rank}</td>

                  <td style={{ padding: 10, minWidth: 360 }}>
                    <div style={{ fontWeight: 600 }}>{title}</div>
                    <div style={{ color: "#666", fontSize: 12 }}>
                      {r.market_id}
                      {url ? (
                        <>
                          {" "}
                          <a href={url} target="_blank" rel="noreferrer">
                            open
                          </a>
                        </>
                      ) : null}
                    </div>
                  </td>

                  <td style={{ padding: 10 }}>{fmt(r.structural_score, 6)}</td>
                  <td style={{ padding: 10 }}>{pct}</td>
                  <td style={{ padding: 10 }}>{fmtInt(r.trades)}</td>
                  <td style={{ padding: 10 }}>{fmt(r.volume, 2)}</td>
                  <td style={{ padding: 10 }}>{fmt(r.avg_spread, 6)}</td>
                  <td style={{ padding: 10 }}>{fmtInt(r.bbo_ticks)}</td>
                  <td style={{ padding: 10 }}>{flags.length ? flags.join(", ") : ""}</td>
                  <td style={{ padding: 10 }}>{r.identity_blind ? "blind" : fmt(r.identity_coverage, 3)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 14, color: "#666", fontSize: 12 }}>
        Query:{" "}
        <code style={{ background: "#f7f7f7", padding: "2px 6px", borderRadius: 6 }}>
          {`/ops/microstructure/top?${queryForDisplay.toString()}`}
        </code>
      </div>
    </div>
  );
}