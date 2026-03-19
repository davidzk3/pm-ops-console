type Props = {
  data: {
    title: string
    subtitle: string
    marketTitle: string
    marketId: string
    protocol: string
    reviewWindowStatus: string
    structuralOnlyNote: string
  }
}

export function DemoHeader({ data }: Props) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-xl">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-2">
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-cyan-400">
            {data.title}
          </p>
          <h1 className="text-3xl font-semibold tracking-tight">{data.marketTitle}</h1>
          <p className="max-w-3xl text-sm text-slate-300">{data.subtitle}</p>
          <p className="inline-flex rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs text-amber-300">
            {data.structuralOnlyNote}
          </p>
        </div>

        <div className="grid grid-cols-1 gap-3 rounded-xl border border-slate-800 bg-slate-950/70 p-4 text-sm lg:min-w-[280px]">
          <div className="flex items-center justify-between">
            <span className="text-slate-400">Market ID</span>
            <span className="font-medium">{data.marketId}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-slate-400">Protocol</span>
            <span className="font-medium">{data.protocol}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-slate-400">Review status</span>
            <span className="font-medium text-cyan-300">{data.reviewWindowStatus}</span>
          </div>
        </div>
      </div>
    </section>
  )
}