export function formatNumber(value: unknown, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toFixed(digits);
}

export function formatInteger(value: unknown): string {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString();
}

export function formatPercent01(value: unknown, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

export function prettyLabel(input: string): string {
  return input
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

export function isPlainObject(value: unknown): value is Record<string, any> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatCohortSummaryArray(value: Record<string, any>[]): string {
  const parts = value
    .map((item) => {
      const cohort =
        item.cohort ||
        item.role ||
        item.label ||
        item.name ||
        "unknown";

      const traders =
        item.traders ??
        item.unique_traders ??
        item.trader_count ??
        null;

      if (traders !== null && traders !== undefined && !Number.isNaN(Number(traders))) {
        return `${String(cohort).toLowerCase()}: ${Number(traders).toLocaleString()} traders`;
      }

      return String(cohort).toLowerCase();
    })
    .filter(Boolean);

  return parts.length > 0 ? parts.join(" • ") : "—";
}

export function formatMetricValue(
  value: unknown,
  emptyLabel = "Not available"
): string {
  if (value === null || value === undefined || value === "") return emptyLabel;
  if (typeof value === "number" && Number.isNaN(value)) return emptyLabel;
  return typeof value === "number" ? Number(value).toFixed(2) : String(value);
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";

  if (typeof value === "number") {
    return Number(value).toFixed(2);
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return "—";

    if (value.every((item) => typeof item === "string" || typeof item === "number")) {
      return value.join(", ");
    }

    if (value.every((item) => isPlainObject(item))) {
      const first = value[0] as Record<string, any>;
      const looksLikeCohorts =
        "cohort" in first ||
        "traders" in first ||
        "unique_traders" in first ||
        "trader_count" in first;

      if (looksLikeCohorts) {
        return formatCohortSummaryArray(value as Record<string, any>[]);
      }

      return value
        .map((item) =>
          Object.entries(item)
            .filter(([, v]) => v !== null && v !== undefined && v !== "")
            .slice(0, 3)
            .map(([k, v]) => `${prettyLabel(k)}: ${String(v)}`)
            .join(" • ")
        )
        .join(" | ");
    }

    return JSON.stringify(value);
  }

  if (isPlainObject(value)) {
    return Object.entries(value)
      .filter(([, v]) => v !== null && v !== undefined && v !== "")
      .slice(0, 6)
      .map(([k, v]) => `${prettyLabel(k)}: ${String(v)}`)
      .join(" • ");
  }

  return String(value);
}