export type IncidentSeverity = 'low' | 'medium' | 'high' | 'critical';

export type IncidentStatus = 'open' | 'acknowledged' | 'resolved';

export interface Incident {
  id: string;
  source: string;
  title: string;
  description: string | null;
  severity: IncidentSeverity;
  status: IncidentStatus;
  acknowledged_at: string | null;
  acknowledged_by_user_id: string | null;
  resolved_at: string | null;
  resolved_by_user_id: string | null;
  occurred_at: string;
  created_at: string;
  updated_at: string;
}

export interface IncidentListResponse {
  items: Incident[];
  limit: number;
  offset: number;
  total: number;
}

export type TriagePriority = 'P1' | 'P2' | 'P3' | 'P4';

export interface IncidentTriage {
  incident_id: string;
  event_id: string;
  source: string;
  original_severity: IncidentSeverity;
  priority: TriagePriority;
  requires_human_review: boolean;
  created_at: string;
}

export type EnrichmentCategory =
  | 'availability'
  | 'performance'
  | 'security'
  | 'capacity'
  | 'dependency'
  | 'deployment'
  | 'data'
  | 'unknown';

export interface IncidentEnrichment {
  request_event_id: string;
  incident_id: string;
  trigger_event_id: string;
  provider: string;
  model: string;
  summary: string;
  category: EnrichmentCategory;
  suspected_component: string | null;
  investigation_steps: string[];
  created_at: string;
}

export interface EnrichmentListResponse {
  items: IncidentEnrichment[];
  total: number;
  limit: number;
  offset: number;
}
