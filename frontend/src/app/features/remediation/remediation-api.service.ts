import { HttpClient, HttpParams } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RemediationListQuery } from './remediation-list-query';
import { RemediationExecution, RemediationProposal, RemediationProposalCreateRequest,
  RemediationProposalListResponse, RemediationProposalRejectRequest } from './remediation.models';

@Injectable({ providedIn: 'root' })
export class RemediationApiService {
  private readonly http = inject(HttpClient);

  listProposals(query: RemediationListQuery): Observable<RemediationProposalListResponse> {
    let params = new HttpParams().set('limit', query.limit).set('offset', query.offset);
    if (query.incidentId) params = params.set('incident_id', query.incidentId);
    if (query.status) params = params.set('status', query.status);
    if (query.actionKind) params = params.set('action_kind', query.actionKind);
    if (query.target) params = params.set('target', query.target);
    if (query.proposedByUserId) params = params.set('proposed_by_user_id', query.proposedByUserId);
    if (query.createdFrom) params = params.set('created_from', query.createdFrom);
    if (query.createdTo) params = params.set('created_to', query.createdTo);
    return this.http.get<RemediationProposalListResponse>('/api/v1/remediation-proposals', { params });
  }

  getProposal(proposalId: string): Observable<RemediationProposal> {
    return this.http.get<RemediationProposal>(`/api/v1/remediation-proposals/${proposalId}`);
  }

  createProposal(request: RemediationProposalCreateRequest): Observable<RemediationProposal> {
    return this.http.post<RemediationProposal>('/api/v1/remediation-proposals', request);
  }

  approveProposal(proposalId: string): Observable<RemediationProposal> {
    return this.http.post<RemediationProposal>(`/api/v1/remediation-proposals/${proposalId}/approve`, null);
  }

  rejectProposal(proposalId: string, request: RemediationProposalRejectRequest): Observable<RemediationProposal> {
    return this.http.post<RemediationProposal>(`/api/v1/remediation-proposals/${proposalId}/reject`, request);
  }

  getExecution(proposalId: string): Observable<RemediationExecution> {
    return this.http.get<RemediationExecution>(`/api/v1/remediation-proposals/${proposalId}/execution`);
  }

  requestExecution(proposalId: string): Observable<RemediationExecution> {
    return this.http.post<RemediationExecution>(`/api/v1/remediation-proposals/${proposalId}/execute`, null);
  }
}
