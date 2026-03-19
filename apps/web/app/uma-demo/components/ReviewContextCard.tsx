type Props = {
  data: {
    resolutionSupportScore: number
    cautionLabel: string
    recommendedAction: string
    rationale: string[]
  }
}

function getTone(score: number) {
  if (score >= 80) return "text-red-300 border-red-500/30 bg-red-500/10"
  if (score >= 60) return "text-amber-300 border-amber-500/30 bg-amber-500/10"
  if (score >= 40) return "text-yellow-300 border-yellow-500/30 bg-yellow-500/10"
  return "text-emerald-300 border-emerald-500/30 bg-emerald-500/10"
}

export function ReviewContextCard({ data }: Props) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-xl">
      <div className="mb-5 flex items-start justify-between">
        <div>
          <h2 className="text-xl font-semibold">Proposal Review Context</h2>
          <p className="mt-1 text-sm text-slate-400">
            Reviewer oriented structural triage for the optimistic window.
          </p>
        </div>
        <div className={`rounded-xl border px-4 py-2 text-sm ${getTone(data.resolutionSupportScore)}`}>
          {data.cautionLabel}
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
          <p className="text-sm text-slate-400">Resolution Support Score</p>
          <p className="mt-2 text-4xl font-semibold">{data.resolutionSupportScore}</p>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
          <p className="text-sm text-slate-400">Caution Label</p>
          <p className="mt-2 text-lg font-semibold">{data.cautionLabel}</p>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
          <p className="text-sm text-slate-400">Recommended Action</p>
          <p className="mt-2 text-lg font-semibold">{data.recommendedAction}</p>
        </div>
      </div>

      <div className="mt-5">
        <p className="mb-2 text-sm text-slate-400">Main structural drivers</p>
        <div className="flex flex-wrap gap-2">
          {data.rationale.map((item) => (
            <span
              key={item}
              className="rounded-full border border-slate-700 bg-slate-950 px-3 py-1 text-xs text-slate-300"
            >
              {item}
            </span>
          ))}
        </div>
      </div>
    </section>
  )
}