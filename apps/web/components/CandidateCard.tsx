import Link from "next/link";

type CandidateCardProps = {
  marketId: string;
  title?: string | null;
  category?: string | null;
  structuralState?: string | null;
  socialSignal?: string | null;
  scoreLabel: string;
  scoreValue: string;
  summary?: string | null;
  flags?: string[] | null;
  url?: string | null;
};

function getStructuralBadgeClass(value?: string | null) {
  switch (value) {
    case "strong":
      return "bg-emerald-100 text-emerald-700";
    case "mixed":
      return "bg-amber-100 text-amber-700";
    case "weak":
      return "bg-zinc-100 text-zinc-700";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function getSocialBadgeClass(value?: string | null) {
  switch (value) {
    case "high":
      return "bg-violet-100 text-violet-700";
    case "forming":
      return "bg-purple-100 text-purple-700";
    case "low":
      return "bg-fuchsia-100 text-fuchsia-700";
    default:
      return "bg-violet-100 text-violet-700";
  }
}

function Badge({
  children,
  className,
}: {
  children: React.ReactNode;
  className: string;
}) {
  return (
    <span
      className={`inline-flex h-11 items-center rounded-2xl px-5 text-sm font-semibold whitespace-nowrap ${className}`}
    >
      {children}
    </span>
  );
}

export default function CandidateCard({
  marketId,
  title,
  category,
  structuralState,
  socialSignal,
  scoreLabel,
  scoreValue,
  summary,
  flags,
  url,
}: CandidateCardProps) {
  const detailHref = {
    pathname: `/markets/${marketId}`,
    query: {
      title: title || "",
      url: url || "",
    },
  };

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0 flex-1">
          <h3 className="text-xl font-semibold text-zinc-900">
            {title || marketId}
          </h3>

          {category ? (
            <p className="mt-2 text-sm text-zinc-500">{category}</p>
          ) : null}
        </div>

        <div className="flex flex-col items-end gap-2">
          {structuralState ? (
            <Badge className={getStructuralBadgeClass(structuralState)}>
              structural: {structuralState}
            </Badge>
          ) : null}

          {socialSignal ? (
            <Badge className={getSocialBadgeClass(socialSignal)}>
              social demo: {socialSignal}
            </Badge>
          ) : null}
        </div>
      </div>

      <div className="mt-6 space-y-3">
        <p className="text-sm font-semibold text-zinc-800">
          {scoreLabel}: <span className="font-normal">{scoreValue}</span>
        </p>

        {summary ? (
          <p className="text-sm text-zinc-600">{summary}</p>
        ) : null}

        {flags && flags.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {flags.map((flag) => (
              <span
                key={flag}
                className="rounded-full border border-zinc-200 px-4 py-2 text-sm text-zinc-600"
              >
                {flag}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="mt-6 flex items-center gap-4 text-sm font-medium">
        <Link href={detailHref} className="text-blue-600 hover:underline">
          View detail
        </Link>

        {url ? (
          <a
            href={url}
            target="_blank"
            rel="noreferrer"
            className="text-zinc-800 hover:underline"
          >
            Open market
          </a>
        ) : null}
      </div>
    </div>
  );
}