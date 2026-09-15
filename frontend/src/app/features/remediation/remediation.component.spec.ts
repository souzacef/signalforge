import { Component, signal } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { NavigationEnd, provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { filter, firstValueFrom, Observable, of, Subject, take, throwError } from 'rxjs';
import { vi } from 'vitest';
import { AuthService } from '../../core/auth/auth.service';
import { User, UserRole } from '../../core/models/user';
import { RemediationApiService } from './remediation-api.service';
import { RemediationListQuery } from './remediation-list-query';
import { RemediationProposal, RemediationProposalCreateRequest, RemediationProposalListResponse } from './remediation.models';
import { RemediationComponent } from './remediation.component';

const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';
const proposal: RemediationProposal = {
  id, incident_id: id, action_kind: 'restart_service', target: 'payments-api', reason: 'Human reason',
  status: 'pending_approval', proposed_by_user_id: id, approved_by_user_id: null, approved_at: null,
  rejected_by_user_id: null, rejected_at: null, rejection_reason: null,
  created_at: '2026-09-15T18:00:00Z', updated_at: '2026-09-15T18:00:00Z',
};
const response = (overrides: Partial<RemediationProposalListResponse> = {}): RemediationProposalListResponse =>
  ({ items: [proposal], limit: 20, offset: 0, total: 1, ...overrides });

@Component({ template: '' })
class DetailStub {}

describe('RemediationComponent', () => {
  const currentUser = signal<User | null>(null);
  const createProposal = vi.fn<(request: RemediationProposalCreateRequest) => Observable<RemediationProposal>>();
  const listProposals = vi.fn<(query: RemediationListQuery) => Observable<RemediationProposalListResponse>>();
  beforeEach(() => {
    listProposals.mockReset(); createProposal.mockReset(); currentUser.set(null);
    listProposals.mockReturnValue(of(response({ total: 201 })));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'remediation', component: RemediationComponent },
          { path: 'remediation/:proposalId', component: DetailStub }]),
        { provide: RemediationApiService, useValue: { listProposals, createProposal } },
        { provide: AuthService, useValue: { currentUser } },
      ],
    });
  });
  afterEach(() => vi.restoreAllMocks());
  async function open(url = '/remediation') {
    const harness = await RouterTestingHarness.create(url);
    const component = harness.routeDebugElement?.componentInstance as RemediationComponent;
    return { harness, component, router: TestBed.inject(Router) };
  }
  async function navigateWith(action: () => void, router: Router) {
    const completed = firstValueFrom(router.events.pipe(
      filter((event): event is NavigationEnd => event instanceof NavigationEnd), take(1),
    ));
    action();
    await completed;
  }

  function asRole(role: UserRole, userId = id): void {
    currentUser.set({ id: userId, email: 'user@example.test', role, is_active: true,
      created_at: proposal.created_at, updated_at: proposal.updated_at });
  }
  const httpError = (status: number, detail?: string) => new HttpErrorResponse({
    status, error: detail === undefined ? {} : { detail },
  });

  it.each(['viewer', 'operator', 'admin'] as const)('shows creation only to allowed role %s', async (role) => {
    asRole(role);
    const { harness } = await open();
    const button = [...(harness.routeNativeElement?.querySelectorAll('button') ?? [])]
      .find((item) => item.textContent?.includes('New remediation proposal'));
    expect(!!button).toBe(role !== 'viewer');
  });

  it('validates create fields, trims input, fixes action, and prevents duplicate POST', async () => {
    asRole('operator');
    const pending = new Subject<RemediationProposal>();
    createProposal.mockReturnValue(pending);
    const { harness, component } = await open();
    component.openCreate();
    component.createForm.patchValue({ incidentId: 'bad', target: 'Payments API', reason: '  ' });
    component.submitCreate();
    expect(component.createIncidentError()).toContain('UUID');
    expect(component.createTargetError()).toContain('lowercase');
    expect(component.createReasonError()).toContain('reason');
    expect(createProposal).not.toHaveBeenCalled();
    for (const target of ['', 'bad/path', 'bad?query', '.bad', 'ok\n', 'x'.repeat(101)]) {
      component.createForm.patchValue({ incidentId: id, target, reason: 'valid' });
      component.submitCreate();
      expect(createProposal).not.toHaveBeenCalled();
    }
    component.createForm.patchValue({ target: '  billing.worker  ', reason: '  restart needed  ' });
    component.submitCreate(); component.submitCreate();
    expect(createProposal).toHaveBeenCalledTimes(1);
    expect(createProposal).toHaveBeenCalledWith({ incident_id: id, action_kind: 'restart_service',
      target: 'billing.worker', reason: 'restart needed' });
    expect(component.createState()).toBe('creating');
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('Creating proposal');
    expect(component.response()?.items).toEqual([proposal]);
  });

  it('blocks a reason over 1000 trimmed characters and retains the form', async () => {
    asRole('admin');
    const { component } = await open(); component.openCreate();
    component.createForm.setValue({ incidentId: id, target: 'orders_v2', reason: 'x'.repeat(1001) });
    component.submitCreate();
    expect(component.createReasonError()).toContain('1000');
    expect(component.createForm.controls.reason.value).toHaveLength(1001);
    expect(createProposal).not.toHaveBeenCalled();
  });

  it('cancels a clean create form without HTTP', async () => {
    asRole('admin');
    const { component } = await open();
    component.openCreate(); component.createForm.patchValue({ incidentId: 'bad' });
    component.submitCreate(); component.cancelCreate();
    expect(component.createOpen()).toBe(false);
    expect(component.createForm.getRawValue()).toEqual({ incidentId: '', target: '', reason: '' });
    expect(component.createIncidentError()).toBeNull();
    expect(createProposal).not.toHaveBeenCalled();
  });

  it('navigates to the returned ID with queue parameters after creation', async () => {
    asRole('admin');
    createProposal.mockReturnValue(of({ ...proposal, id: '79caa2b3-59e2-4187-953c-e590a68aab2a' }));
    const { component, router } = await open('/remediation?status=approved&limit=50');
    component.openCreate(); component.createForm.setValue({ incidentId: id, target: 'payments-api', reason: 'Restart' });
    await navigateWith(() => component.submitCreate(), router);
    expect(router.url).toBe('/remediation/79caa2b3-59e2-4187-953c-e590a68aab2a?status=approved&limit=50');
  });

  it.each([
    [404, 'Incident not found', 'Incident not found.'],
    [409, 'Incident is not eligible for remediation', 'This incident is not eligible for remediation.'],
    [409, 'A matching pending remediation proposal already exists', 'A matching pending remediation proposal already exists.'],
    [409, 'private detail', 'Proposal could not be created because the current server state does not allow it.'],
    [403, 'private detail', 'You do not have permission to create remediation proposals.'],
    [500, 'private detail', 'Remediation proposal could not be created. Try again.'],
  ] as const)('maps create %s safely', async (status, detail, feedback) => {
    asRole('operator');
    createProposal.mockReturnValue(throwError(() => httpError(status, detail)));
    const { harness, component, router } = await open();
    component.openCreate(); component.createForm.setValue({ incidentId: id, target: 'payments-api', reason: 'Restart' });
    component.submitCreate(); harness.detectChanges();
    expect(component.createFeedback()).toBe(feedback);
    expect(component.createForm.controls.reason.value).toBe('Restart');
    expect(router.url).toBe('/remediation');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
  });

  it('restores URL filters and links proposal detail with current query parameters', async () => {
    const { harness, component } = await open('/remediation?status=pending_approval&limit=50&offset=100');
    expect(component.filterForm.getRawValue().status).toBe('pending_approval');
    expect(component.query()).toMatchObject({ limit: 50, offset: 100 });
    expect(harness.routeNativeElement?.querySelector<HTMLAnchorElement>('.proposal-link')?.getAttribute('href'))
      .toBe(`/remediation/${id}?status=pending_approval&limit=50&offset=100`);
  });

  it('applies trimmed filters through URL, resets offset, and preserves size', async () => {
    const { component, router } = await open('/remediation?limit=50&offset=100');
    component.filterForm.patchValue({ status: 'approved', target: '  payments-api  ' });
    await navigateWith(() => component.applyFilters(), router);
    expect(router.url).toBe('/remediation?status=approved&target=payments-api&limit=50');
    expect(listProposals).toHaveBeenLastCalledWith(expect.objectContaining({
      status: 'approved', target: 'payments-api', offset: 0,
    }));
  });

  it('blocks invalid UUID and date range without navigation or new request', async () => {
    const { component, router } = await open();
    const calls = listProposals.mock.calls.length;
    component.filterForm.patchValue({
      incidentId: 'invalid', proposedByUserId: 'also invalid',
      createdFrom: '2026-09-16T12:00', createdTo: '2026-09-15T12:00',
    });
    component.applyFilters();
    expect(component.incidentIdError()).toContain('UUID');
    expect(component.proposedByUserIdError()).toContain('UUID');
    expect(component.dateRangeError()).toContain('Created from');
    expect(router.url).toBe('/remediation');
    expect(listProposals).toHaveBeenCalledTimes(calls);
  });

  it('clears filters and preserves limit, and page changes preserve filters', async () => {
    const { component, router } = await open('/remediation?status=approved&limit=50&offset=50');
    await navigateWith(() => component.changePage({
      pageIndex: 2, pageSize: 50, length: 201, previousPageIndex: 1,
    }), router);
    expect(router.url).toBe('/remediation?status=approved&limit=50&offset=100');
    await navigateWith(() => component.clearFilters(), router);
    expect(router.url).toBe('/remediation?limit=50');
  });

  it('shows loading, table status and backend total range', async () => {
    const pending = new Subject<RemediationProposalListResponse>();
    listProposals.mockReturnValue(pending);
    const { harness } = await open();
    expect(harness.routeNativeElement?.textContent).toContain('Loading remediation proposals');
    pending.next(response({ total: 57, items: Array(20).fill(proposal) }));
    pending.complete();
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('Pending approval');
    expect(harness.routeNativeElement?.textContent).toContain('Restart service');
    expect(harness.routeNativeElement?.textContent).toContain('1–20 of 57 proposals');
  });

  it('distinguishes unfiltered and filtered empty queues', async () => {
    listProposals.mockReturnValue(of(response({ items: [], total: 0 })));
    const { harness, router } = await open();
    expect(harness.routeNativeElement?.textContent).toContain('No remediation proposals are currently available.');
    await router.navigateByUrl('/remediation?status=rejected');
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('No remediation proposals match the selected filters.');
  });

  it('shows a safe error and retries the current query', async () => {
    listProposals.mockReturnValueOnce(throwError(() => new Error('private detail')))
      .mockReturnValueOnce(of(response()));
    const { harness, component } = await open('/remediation?status=approved');
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposals could not be loaded.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    component.retry();
    harness.detectChanges();
    expect(listProposals).toHaveBeenCalledTimes(2);
    expect(harness.routeNativeElement?.textContent).toContain('payments-api');
  });

  it('corrects an out-of-range page and cancels obsolete requests', async () => {
    listProposals.mockReturnValueOnce(of(response({ items: [], total: 21, offset: 40 })))
      .mockReturnValueOnce(of(response({ total: 21, offset: 20 })));
    const { router } = await open('/remediation?offset=40');
    expect(router.url).toBe('/remediation?offset=20');
    expect(listProposals).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 20 }));
    const first = new Subject<RemediationProposalListResponse>();
    const second = new Subject<RemediationProposalListResponse>();
    listProposals.mockReturnValueOnce(first).mockReturnValueOnce(second);
    await router.navigateByUrl('/remediation?status=approved');
    await router.navigateByUrl('/remediation?status=rejected');
    expect(first.observed).toBe(false);
    first.next(response());
    second.next(response({ items: [], total: 0 }));
    expect(listProposals).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'rejected' }));
  });

  it('canonicalizes malformed URL before requesting', async () => {
    const { router } = await open('/remediation?status=bad&offset=-2&surprise=x');
    expect(router.url).toBe('/remediation');
    expect(listProposals).toHaveBeenCalledTimes(1);
    expect(listProposals).toHaveBeenCalledWith(expect.objectContaining({ status: null, offset: 0 }));
  });
});
