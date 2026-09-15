export type RemediationActionKind = 'restart_service';
export type RemediationProposalStatus = 'pending_approval' | 'approved' | 'rejected';
export type RemediationExecutionStatus =
  | 'requested' | 'in_progress' | 'succeeded' | 'failed' | 'outcome_unknown';

export interface RemediationProposalCreateRequest {
  incident_id: string;
  action_kind: RemediationActionKind;
  target: string;
  reason: string;
}

export interface RemediationProposalRejectRequest {
  rejection_reason: string;
}

export interface RemediationProposal {
  id: string;
  incident_id: string;
  action_kind: RemediationActionKind;
  target: string;
  reason: string;
  status: RemediationProposalStatus;
  proposed_by_user_id: string;
  approved_by_user_id: string | null;
  approved_at: string | null;
  rejected_by_user_id: string | null;
  rejected_at: string | null;
  rejection_reason: string | null;
  created_at: string;
  updated_at: string;
}

export interface RemediationProposalListResponse {
  items: RemediationProposal[];
  limit: number;
  offset: number;
  total: number;
}

export interface RemediationExecution {
  id: string;
  proposal_id: string;
  action_kind: RemediationActionKind;
  target: string;
  status: RemediationExecutionStatus;
  requested_by_user_id: string;
  requested_at: string;
  completed_at: string | null;
  updated_at: string;
}
