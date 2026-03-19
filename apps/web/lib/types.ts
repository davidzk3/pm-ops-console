export type LaunchCandidate = {
  market_id: string;
  title?: string | null;
  category?: string | null;
  url?: string | null;
  recommendation?: string | null;
  recommendation_reason?: string | null;
  launch_readiness_score?: number | null;
  launch_risk_score?: number | null;
  participation_quality_score?: number | null;
  liquidity_durability_score?: number | null;
  flags?: string[] | null;
  engine_version?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type SocialCandidate = {
  market_id: string;
  title?: string | null;
  category?: string | null;
  url?: string | null;
  recommendation?: string | null;
  summary?: string | null;
  attention_score?: number | null;
  sentiment_score?: number | null;
  demand_score?: number | null;
  trend_velocity?: number | null;
  mention_count?: number | null;
  source_count?: number | null;
  confidence_score?: number | null;
  flags?: string[] | null;
  engine_version?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type SnapshotResponse = {
  market?: Record<string, any>;
  launch_intelligence?: Record<string, any> | null;
  social_intelligence?: Record<string, any> | null;
  opportunity_summary?: Record<string, any> | null;
  timeline?: any[] | null;
  incidents?: any[] | null;
  incident_events?: any[] | null;
  incident_effectiveness?: any[] | null;
  interventions?: any[] | null;
  interventions_effectiveness?: any[] | null;
  interventions_effectiveness_ui?: any[] | null;
  intervention_cumulative?: Record<string, any> | null;
  overrides?: any[] | null;
  traders?: {
    same_day?: Record<string, any> | null;
    rolling_window?: Record<string, any> | null;
  } | null;
  impact?: Record<string, any> | null;
  errors?: any[] | null;
  snapshot_meta?: Record<string, any> | null;
  coverage_summary?: Record<string, any> | null;
};