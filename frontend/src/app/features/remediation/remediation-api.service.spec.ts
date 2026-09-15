import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { firstValueFrom } from 'rxjs';
import { RemediationApiService } from './remediation-api.service';
import { DEFAULT_REMEDIATION_LIST_QUERY, RemediationListQuery } from './remediation-list-query';
import { RemediationExecution, RemediationProposal, RemediationProposalCreateRequest, RemediationProposalListResponse } from './remediation.models';

const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';
const proposal: RemediationProposal = {
  id, incident_id: id, action_kind: 'restart_service', target: 'payments-api', reason: 'Human reason',
  status: 'pending_approval', proposed_by_user_id: id, approved_by_user_id: null, approved_at: null,
  rejected_by_user_id: null, rejected_at: null, rejection_reason: null,
  created_at: '2026-09-15T18:00:00Z', updated_at: '2026-09-15T18:00:00Z',
};
const execution: RemediationExecution = {
  id, proposal_id: id, action_kind: 'restart_service', target: 'payments-api',
  status: 'requested', requested_by_user_id: id, requested_at: '2026-09-15T18:00:00Z',
  completed_at: null, updated_at: '2026-09-15T18:00:00Z',
};

describe('RemediationApiService', () => {
  let api: RemediationApiService;
  let http: HttpTestingController;
  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [provideHttpClient(), provideHttpClientTesting()] });
    api = TestBed.inject(RemediationApiService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  it('lists typed proposals with backend defaults and omits optional filters', async () => {
    const response: RemediationProposalListResponse = { items: [], limit: 20, offset: 0, total: 0 };
    const result = firstValueFrom(api.listProposals(DEFAULT_REMEDIATION_LIST_QUERY));
    const request = http.expectOne('/api/v1/remediation-proposals?limit=20&offset=0');
    expect(request.request.method).toBe('GET');
    expect(request.request.headers.has('Authorization')).toBe(false);
    expect(request.request.params.keys().sort()).toEqual(['limit', 'offset']);
    request.flush(response);
    await expect(result).resolves.toEqual(response);
  });

  it('serializes every supported filter and pagination field', async () => {
    const query: RemediationListQuery = {
      incidentId: id, status: 'approved', actionKind: 'restart_service', target: 'payments-api',
      proposedByUserId: id, createdFrom: '2026-09-15T10:00:00.000Z',
      createdTo: '2026-09-15T12:00:00.000Z', limit: 50, offset: 100,
    };
    const result = firstValueFrom(api.listProposals(query));
    const request = http.expectOne((candidate) => candidate.url === '/api/v1/remediation-proposals');
    expect(request.request.method).toBe('GET');
    expect(Object.fromEntries(request.request.params.keys().map((key) =>
      [key, request.request.params.get(key)]))).toEqual({
      limit: '50', offset: '100', incident_id: id, status: 'approved',
      action_kind: 'restart_service', target: 'payments-api', proposed_by_user_id: id,
      created_from: query.createdFrom, created_to: query.createdTo,
    });
    request.flush({ items: [proposal], limit: 50, offset: 100, total: 101 });
    await expect(result).resolves.toMatchObject({ items: [proposal], total: 101 });
  });

  it('creates a typed proposal with the exact request body', async () => {
    const body: RemediationProposalCreateRequest = {
      incident_id: id, action_kind: 'restart_service', target: 'payments-api', reason: 'Human reason',
    };
    const result = firstValueFrom(api.createProposal(body));
    const request = http.expectOne('/api/v1/remediation-proposals');
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual(body);
    request.flush(proposal);
    await expect(result).resolves.toEqual(proposal);
  });

  it('approves without domain payload and rejects with an exact reason body', async () => {
    const approved = { ...proposal, status: 'approved' as const };
    const approval = firstValueFrom(api.approveProposal(id));
    const approveRequest = http.expectOne(`/api/v1/remediation-proposals/${id}/approve`);
    expect(approveRequest.request.method).toBe('POST');
    expect(approveRequest.request.body).toBeNull();
    approveRequest.flush(approved);
    await expect(approval).resolves.toEqual(approved);

    const rejected = { ...proposal, status: 'rejected' as const };
    const rejection = firstValueFrom(api.rejectProposal(id, { rejection_reason: 'unsafe target' }));
    const rejectRequest = http.expectOne(`/api/v1/remediation-proposals/${id}/reject`);
    expect(rejectRequest.request.method).toBe('POST');
    expect(rejectRequest.request.body).toEqual({ rejection_reason: 'unsafe target' });
    rejectRequest.flush(rejected);
    await expect(rejection).resolves.toEqual(rejected);
  });

  it('reads typed proposal and execution detail from relative GET endpoints', async () => {
    const proposalResult = firstValueFrom(api.getProposal(id));
    const proposalRequest = http.expectOne(`/api/v1/remediation-proposals/${id}`);
    expect(proposalRequest.request.method).toBe('GET');
    proposalRequest.flush(proposal);
    await expect(proposalResult).resolves.toEqual(proposal);

    const executionResult = firstValueFrom(api.getExecution(id));
    const executionRequest = http.expectOne(`/api/v1/remediation-proposals/${id}/execution`);
    expect(executionRequest.request.method).toBe('GET');
    executionRequest.flush(execution);
    await expect(executionResult).resolves.toEqual(execution);
  });
});
