"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { fetchJSON } from "../../lib/api";

type ManualOverride = {
  market_id: string;
  day: string;
  risk_score_override: number | null;
  health_score_override: number | null;
  note: string | null;
  created_by: string;
  created_at: string;
};

export default function OverridePanel(props: {
  marketId: string;
  overrides: ManualOverride[];
}) {
  const router = useRouter();

  const todayISO = useMemo(() => {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }, []);

  const [day, setDay] = useState(todayISO);
  const [risk, setRisk] = useState<string>("");
  const [health, setHealth] = useState<string>("");
  const [note, setNote] = useState<string>("");
  const [createdBy, setCreatedBy] = useState<string>("operator");

  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function upsertOverride() {
    setErr(null);
    setBusy(true);
    try {
      const riskVal = risk.trim() === "" ? null : Number(risk);
      const healthVal = health.trim() === "" ? null : Number(health);

      if (riskVal !== null && !Number.isFinite(riskVal)) throw new Error("Risk override must be a number or empty");
      if (healthVal !== null && !Number.isFinite(healthVal)) throw new Error("Health override must be a number or empty");

      await fetchJSON(`/ops/markets/${encodeURIComponent(props.marketId)}/overrides`, {
        method: "POST",
        body: JSON.stringify({
          day,
          risk_score_override: riskVal,
          health_score_override: healthVal,
          note: note.trim() ? note.trim() : null,
          created_by: createdBy,
        }),
        write: true,
      });

      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to upsert override");
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="mt-12">
      <div className="flex items-end justify-between gap-4 mb-3">
        <h2 className="font-medium">Manual override</h2>
        <div className="text-xs text-gray-500">POST /ops/markets/{props.marketId}/overrides</div>
      </div>

      {err ? (
        <div className="border rounded-xl p-3 mb-4 bg-red-50 border-red-200 text-sm text-red-800">{err}</div>
      ) : null}

      <div className="border rounded-2xl p-4 bg-white">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="text-sm">
            Day
            <input
              type="date"
              value={day}
              onChange={(e) => setDay(e.target.value)}
              className="mt-1 w-full border rounded-xl px-3 py-2"
            />
          </label>

          <label className="text-sm">
            Created by
            <input
              value={createdBy}
              onChange={(e) => setCreatedBy(e.target.value)}
              className="mt-1 w-full border rounded-xl px-3 py-2"
            />
          </label>

          <label className="text-sm">
            Risk override (optional)
            <input
              value={risk}
              onChange={(e) => setRisk(e.target.value)}
              className="mt-1 w-full border rounded-xl px-3 py-2"
              placeholder="eg 45"
            />
          </label>

          <label className="text-sm">
            Health override (optional)
            <input
              value={health}
              onChange={(e) => setHealth(e.target.value)}
              className="mt-1 w-full border rounded-xl px-3 py-2"
              placeholder="eg 70"
            />
          </label>

          <label className="text-sm md:col-span-2">
            Note
            <textarea
              value={note}
              onChange={(e) => setNote(e.target.value)}
              rows={3}
              className="mt-1 w-full border rounded-xl px-3 py-2"
              placeholder="Why are we overriding"
            />
          </label>
        </div>

        <div className="mt-4 flex items-center justify-end">
          <button
            onClick={upsertOverride}
            disabled={busy}
            className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
          >
            {busy ? "Saving..." : "Save override"}
          </button>
        </div>
      </div>

      <div className="mt-5">
        <div className="text-sm font-medium">Recent overrides</div>
        {props.overrides.length ? (
          <div className="mt-2 space-y-2">
            {props.overrides.map((o, idx) => (
              <div key={`${o.day}-${idx}`} className="border rounded-xl p-3 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium">{o.day}</div>
                  <div className="text-xs text-gray-500">{o.created_by}</div>
                </div>
                <div className="text-xs text-gray-600 mt-1">
                  risk: {o.risk_score_override ?? "-"} | health: {o.health_score_override ?? "-"}
                </div>
                {o.note ? <div className="text-xs text-gray-600 mt-1">{o.note}</div> : null}
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-gray-500 mt-2">No overrides yet</div>
        )}
      </div>
    </section>
  );
}