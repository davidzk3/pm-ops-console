"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { fetchJSON } from "../lib (rename back to lib)/api";

type Props = {
  marketId: string;
};

function todayISO() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export default function InboxActions({ marketId }: Props) {
  const router = useRouter();
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  const day = useMemo(() => todayISO(), []);

  async function quickIncident() {
    setErr(null);
    setOk(null);
    setBusy("incident");
    try {
      await fetchJSON(`/ops/markets/${encodeURIComponent(marketId)}/incidents`, {
        method: "POST",
        body: JSON.stringify({
          day,
          status: "OPEN",
          note: "Quick incident created from inbox",
        }),
        write: true,
      });

      setOk("Incident created");
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to create incident");
    } finally {
      setBusy(null);
    }
  }

  async function quickLiquidityBoost() {
    setErr(null);
    setOk(null);
    setBusy("boost");
    try {
      // 1) create intervention
      const created = await fetchJSON(`/ops/markets/${encodeURIComponent(marketId)}/interventions`, {
        method: "POST",
        body: JSON.stringify({
          day,
          incident_id: null,
          action_code: "LIQUIDITY_BOOST",
          title: "Quick liquidity boost",
          status: "PLANNED",
          // these match the backend apply_intervention defaults/overrides
          params: {
            spread_bps: 10,
            depth_delta: 500,
            health_delta: 3,
            risk_delta: -2,
          },
        }),
        write: true,
      });

      const interventionId = typeof created?.id === "number" ? created.id : Number(created?.id);
      if (!Number.isFinite(interventionId)) {
        throw new Error("Intervention created but no id returned from API");
      }

      // 2) apply intervention (this is what actually changes metrics)
      await fetchJSON(`/ops/interventions/${interventionId}/apply`, {
        method: "POST",
        body: JSON.stringify({}),
        write: true,
      });

      setOk("Liquidity boost applied");
      router.refresh();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to apply liquidity boost");
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <Link
          href={`/ops/${encodeURIComponent(marketId)}`}
          className="text-xs px-3 py-2 rounded-xl border bg-white hover:bg-gray-50"
        >
          Open console
        </Link>

        <button
          onClick={quickIncident}
          disabled={busy !== null}
          className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
        >
          {busy === "incident" ? "Creating..." : "Create incident"}
        </button>

        <button
          onClick={quickLiquidityBoost}
          disabled={busy !== null}
          className="text-xs px-3 py-2 rounded-xl border bg-gray-50 hover:bg-gray-100 disabled:opacity-50"
        >
          {busy === "boost" ? "Applying..." : "Liquidity boost"}
        </button>
      </div>

      {err ? (
        <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-xl px-3 py-2">
          {err}
        </div>
      ) : null}

      {ok ? (
        <div className="text-xs text-green-800 bg-green-50 border border-green-200 rounded-xl px-3 py-2">
          {ok}
        </div>
      ) : null}
    </div>
  );
}
