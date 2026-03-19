import Link from "next/link";

type Props = {
  marketId: string;
  title?: string | null;
  category?: string | null;
  structuralState?: string | null;
  socialSignal?: string | null;
  scoreLabel?: string;
  scoreValue?: string;
  summary?: string | null;
  flags?: string[] | null;
  url?: string | null;
};

function mapStructuralLabel(value?: string | null): string | null {
  switch (value) {
    case "launch_ready":
      return "strong";
    case "monitor_then_launch":
      return "mixed";
    case "watch":
      return "weak";
    case "observe":
      return "developing";
    default:
      return value || null;
  }
}

function getStructuralBadgeClass(value?: string | null) {
  switch (value) {
    case "launch_ready":
    case "strong":
      return "bg-emerald-100 text-emerald-700";
    case "monitor_then_launch":
    case "mixed":
      return "bg-amber-100 text-amber-700";
    case "watch":
    case "weak":
      return "bg-red-100 text-red-700";
    case "observe":
    case "developing":
      return "bg-zinc-100 text-zinc-700";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function mapSocialLabel(value?: string | null): string | null {
  switch (value) {
    case "high":
      return "high";
    case "forming":
      return "forming";
    case "low":
      return "low";
    default:
      return value || null;
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
  text,
  className,
}: {
  text: string;
  className: string;
}) {
  return (
    <span
      className={`inline-flex h-9 items-center rounded-xl px-4 text-sm font-medium whitespace-nowrap ${className}`}
    >
      {text}
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
}: Props) {
  const showCategory =
    category && category.trim() !== "" && category !== "Uncategorized";

  const structuralLabel = mapStructuralLabel(structuralState);
  const socialLabel = mapSocialLabel(socialSignal);

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <h3 className="text-base font-semibold leading-7 text-zinc-900">
            {title || marketId}
          </h3>

          {showCategory ? (
            <p className="mt-1 text-sm text-zinc-500">{category}</p>
          ) : null}
        </div>

        <div className="flex shrink-0 flex-col items-end gap-2">
          {structuralLabel ? (
            <Badge
              text={`structural: ${structuralLabel}`}
              className={getStructuralBadgeClass(structuralState)}
            />
          ) : null}

          {socialLabel ? (
            <Badge
              text={`social demo: ${socialLabel}`}
              className={getSocialBadgeClass(socialSignal)}
            />
          ) : null}
        </div>
      </div>

      {scoreLabel && scoreValue ? (
        <p className="mb-2 text-sm text-zinc-700">
          <span className="font-medium">{scoreLabel}:</span> {scoreValue}
        </p>
      ) : null}

      {summary ? <p className="mb-3 text-sm text-zinc-600">{summary}</p> : null}

      {flags && flags.length > 0 ? (
        <div className="mb-4 flex flex-wrap gap-2">
          {flags.slice(0, 4).map((flag) => (
            <span
              key={flag}
              className="rounded-full border border-zinc-200 px-3 py-1 text-xs text-zinc-600"
            >
              {flag}
            </span>
          ))}
        </div>
      ) : null}

      <div className="flex flex-wrap gap-3 text-sm">
        <Link
          href={`/markets/${marketId}`}
          className="font-medium text-blue-600 hover:underline"
        >
          View detail
        </Link>

        {url ? (
          <a
            href={url}
            target="_blank"
            rel="noreferrer"
            className="font-medium text-zinc-700 hover:underline"
          >
            Open market
          </a>
        ) : (
          <span className="text-zinc-400">Link unavailable</span>
        )}
      </div>
    </div>
  );
}