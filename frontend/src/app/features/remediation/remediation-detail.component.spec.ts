import { HttpErrorResponse } from '@angular/common/http';
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { Observable, of, Subject, throwError } from 'rxjs';
import { vi } from 'vitest';
import { AuthService } from '../../core/auth/auth.service';
import { User, UserRole } from '../../core/models/user';
import { RemediationApiService } from './remediation-api.service';
import { RemediationDetailComponent } from './remediation-detail.component';
import { RemediationExecution, RemediationExecutionStatus, RemediationProposal, RemediationProposalStatus } from './remediation.models';

const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';
const otherId = '79caa2b3-59e2-4187-953c-e590a68aab2a';
const date = '2026-09-15T18:00:00Z';
function proposal(status: RemediationProposalStatus, overrides: Partial<RemediationProposal> = {}): RemediationProposal {
  return {
    id, incident_id: otherId, action_kind: 'restart_service', target: 'payments-api',
    reason: '<script>alert("x")</script>', status, proposed_by_user_id: otherId,
    approved_by_user_id: status === 'approved' ? otherId : null,
    approved_at: status === 'approved' ? date : null,
    rejected_by_user_id: status === 'rejected' ? otherId : null,
    rejected_at: status === 'rejected' ? date : null,
    rejection_reason: status === 'rejected' ? '<b>human reason</b>' : null,
    created_at: date, updated_at: date, ...overrides,
  };
}
function execution(status: RemediationExecutionStatus): RemediationExecution {
  return {
    id: otherId, proposal_id: id, action_kind: 'restart_service', target: 'payments-api',
    status, requested_by_user_id: otherId, requested_at: date,
    completed_at: ['succeeded', 'failed', 'outcome_unknown'].includes(status) ? date : null,
    updated_at: date,
  };
}
const notFound = () => new HttpErrorResponse({ status: 404, statusText: 'Not found' });

describe('RemediationDetailComponent', () => {
  const currentUser = signal<User | null>(null);
  const approveProposal = vi.fn<(id: string) => Observable<RemediationProposal>>();
  const rejectProposal = vi.fn<(id: string, body: { rejection_reason: string }) => Observable<RemediationProposal>>();
  const getProposal = vi.fn<(id: string) => Observable<RemediationProposal>>();
  const getExecution = vi.fn<(id: string) => Observable<RemediationExecution>>();
  const requestExecution = vi.fn<(id: string) => Observable<RemediationExecution>>();
  beforeEach(() => {
    getProposal.mockReset(); getExecution.mockReset(); approveProposal.mockReset();
    rejectProposal.mockReset(); requestExecution.mockReset(); currentUser.set(null);
    getProposal.mockReturnValue(of(proposal('pending_approval')));
    getExecution.mockReturnValue(of(execution('requested')));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'remediation/:proposalId', component: RemediationDetailComponent }]),
        { provide: RemediationApiService, useValue: { getProposal, getExecution, approveProposal, rejectProposal, requestExecution } },
        { provide: AuthService, useValue: { currentUser } },
      ],
    });
  });
  afterEach(() => vi.restoreAllMocks());
  async function open(url = `/remediation/${id}`) {
    const harness = await RouterTestingHarness.create(url);
    const component = harness.routeDebugElement?.componentInstance as RemediationDetailComponent;
    return { harness, component, router: TestBed.inject(Router) };
  }

  function asRole(role: UserRole, userId = id): void {
    currentUser.set({ id: userId, email: 'user@example.test', role, is_active: true,
      created_at: date, updated_at: date });
  }
  const httpError = (status: number, detail?: string) => new HttpErrorResponse({
    status, error: detail === undefined ? {} : { detail },
  });

  it.each([
    ['viewer', id, false, false],
    ['operator', otherId, false, true],
    ['operator', id, false, false],
    ['admin', otherId, false, true],
    ['admin', id, true, true],
  ] as const)('applies pending decision roles: %s %s', async (role, userId, approve, reject) => {
    asRole(role, userId);
    const { harness } = await open();
    const buttons = [...(harness.routeNativeElement?.querySelectorAll('button') ?? [])]
      .map((button) => button.textContent?.trim());
    expect(buttons.includes('Approve')).toBe(approve);
    expect(buttons.includes('Reject')).toBe(reject);
    const text = harness.routeNativeElement?.textContent ?? '';
    if (role === 'admin' && userId === otherId) expect(text).toContain('A proposer cannot approve their own');
    if (role === 'operator' && userId === id) expect(text).toContain('Only an administrator can decide');
  });

  it.each([
    ['approved', 'viewer'], ['approved', 'operator'], ['approved', 'admin'],
    ['rejected', 'viewer'], ['rejected', 'operator'], ['rejected', 'admin'],
  ] as const)('shows no decision controls for %s to %s', async (status, role) => {
    asRole(role); getProposal.mockReturnValue(of(proposal(status)));
    const { harness } = await open();
    const buttons = [...(harness.routeNativeElement?.querySelectorAll('button') ?? [])]
      .map((button) => button.textContent?.trim());
    expect(buttons).not.toContain('Approve');
    expect(buttons).not.toContain('Reject');
  });

  it('confirms approval once, retains pending until response, and reads execution after success', async () => {
    asRole('admin');
    const pending = new Subject<RemediationProposal>();
    approveProposal.mockReturnValue(pending);
    getExecution.mockReturnValue(throwError(() => httpError(404)));
    const { harness, component } = await open();
    component.openApproval(); harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('Approval allows an authorized execution request');
    component.cancelDecision(); expect(approveProposal).not.toHaveBeenCalled();
    component.openApproval(); component.confirmApproval(); component.confirmApproval();
    expect(approveProposal).toHaveBeenCalledTimes(1);
    expect(component.mutationState()).toBe('approving');
    expect(component.proposal()?.status).toBe('pending_approval');
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('Approving proposal');
    expect(harness.routeNativeElement?.querySelector('nav button')?.hasAttribute('disabled')).toBe(true);
    const approved = proposal('approved', { approved_by_user_id: id });
    pending.next(approved); pending.complete(); harness.detectChanges();
    expect(component.proposal()).toEqual(approved);
    expect(harness.routeNativeElement?.textContent).toContain('Approved by user ID');
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposal approved.');
    expect(harness.routeNativeElement?.textContent).toContain('No execution has been requested');
    expect(getExecution).toHaveBeenCalledTimes(1);
    expect(approveProposal).toHaveBeenCalledTimes(1);
    expect(harness.routeNativeElement?.textContent).toContain('Request execution');
    expect(requestExecution).not.toHaveBeenCalled();
  });

  it('validates rejection, trims reason, keeps it after error and renders backend metadata', async () => {
    asRole('operator', otherId);
    const pending = new Subject<RemediationProposal>(); rejectProposal.mockReturnValue(pending);
    const { harness, component } = await open();
    component.openRejection(); component.confirmRejection();
    expect(component.rejectionError()).toContain('reason');
    component.rejectionForm.controls.reason.setValue('x'.repeat(1001)); component.confirmRejection();
    expect(component.rejectionError()).toContain('1000');
    component.cancelDecision(); expect(rejectProposal).not.toHaveBeenCalled();
    component.openRejection(); component.rejectionForm.controls.reason.setValue('  unsafe target  ');
    component.confirmRejection(); component.confirmRejection();
    expect(rejectProposal).toHaveBeenCalledTimes(1);
    expect(rejectProposal).toHaveBeenCalledWith(id, { rejection_reason: 'unsafe target' });
    expect(component.proposal()?.status).toBe('pending_approval');
    expect(component.mutationState()).toBe('rejecting');
    harness.detectChanges(); expect(harness.routeNativeElement?.textContent).toContain('Rejecting proposal');
    pending.error(new Error('private detail')); harness.detectChanges();
    expect(component.rejectionForm.controls.reason.value).toBe('  unsafe target  ');
    expect(component.decisionFeedback()).toContain('could not be rejected');
    rejectProposal.mockReturnValue(of(proposal('rejected'))); component.confirmRejection(); harness.detectChanges();
    expect(component.proposal()?.status).toBe('rejected');
    expect(component.decisionFeedback()).toBe('Remediation proposal rejected.');
    expect(harness.routeNativeElement?.textContent).toContain('Rejected by user ID');
    expect(harness.routeNativeElement?.querySelector('b')).toBeNull();
    expect(getExecution).not.toHaveBeenCalled();
  });

  it.each(['approving', 'rejecting'] as const)('reloads authoritative state after %s transition conflict', async (kind) => {
    asRole('admin');
    getProposal.mockReturnValueOnce(of(proposal('pending_approval')))
      .mockReturnValueOnce(of(proposal('approved')));
    getExecution.mockReturnValue(throwError(() => httpError(404)));
    if (kind === 'approving') approveProposal.mockReturnValue(throwError(() =>
      httpError(409, 'Invalid remediation proposal state transition')));
    else rejectProposal.mockReturnValue(throwError(() =>
      httpError(409, 'Invalid remediation proposal state transition')));
    const { harness, component } = await open();
    if (kind === 'approving') { component.openApproval(); component.confirmApproval(); }
    else { component.openRejection(); component.rejectionForm.controls.reason.setValue('No'); component.confirmRejection(); }
    harness.detectChanges();
    expect(component.proposal()?.status).toBe('approved');
    expect(component.decisionFeedback()).toContain('Proposal state changed before this decision completed');
    expect(getProposal).toHaveBeenCalledTimes(2);
    expect(getExecution).toHaveBeenCalledTimes(1);
    expect(kind === 'approving' ? approveProposal : rejectProposal).toHaveBeenCalledTimes(1);
  });

  it('maps self-approval, permission, missing and generic decision errors safely', async () => {
    asRole('admin');
    approveProposal.mockReturnValueOnce(throwError(() =>
      httpError(409, 'A proposer cannot approve their own remediation proposal')))
      .mockReturnValueOnce(throwError(() => httpError(403, 'private detail')))
      .mockReturnValueOnce(throwError(() => new Error('private detail')))
      .mockReturnValueOnce(throwError(() => httpError(404)));
    const { harness, component } = await open();
    component.openApproval(); component.confirmApproval();
    expect(component.decisionFeedback()).toContain('You cannot approve');
    expect(component.proposal()?.status).toBe('pending_approval');
    component.confirmApproval(); expect(component.decisionFeedback()).toContain('do not have permission');
    component.confirmApproval(); expect(component.decisionFeedback()).toContain('could not be approved');
    component.confirmApproval(); harness.detectChanges();
    expect(component.proposalState()).toBe('not-found');
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposal not found.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    expect(approveProposal).toHaveBeenCalledTimes(4);
  });

  it('keeps the user session and pending proposal after rejection 403, then handles 404', async () => {
    asRole('operator', otherId);
    rejectProposal.mockReturnValueOnce(throwError(() => httpError(403, 'private detail')))
      .mockReturnValueOnce(throwError(() => httpError(404, 'private detail')));
    const { harness, component } = await open();
    component.openRejection(); component.rejectionForm.controls.reason.setValue('  reason  ');
    component.confirmRejection(); harness.detectChanges();
    expect(component.decisionFeedback()).toContain('do not have permission');
    expect(component.proposal()?.status).toBe('pending_approval');
    expect(currentUser()?.role).toBe('operator');
    expect(component.rejectionForm.controls.reason.value).toBe('  reason  ');
    component.confirmRejection(); harness.detectChanges();
    expect(component.proposalState()).toBe('not-found');
    expect(rejectProposal).toHaveBeenCalledTimes(2);
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
  });

  it('clears confirmation on Refresh and reloads current proposal', async () => {
    asRole('admin');
    const { component } = await open();
    component.openApproval(); expect(component.decisionMode()).toBe('confirm-approval');
    component.refresh();
    expect(component.decisionMode()).toBe('idle');
    expect(getProposal).toHaveBeenCalledTimes(2);
    expect(approveProposal).not.toHaveBeenCalled();
    component.openRejection(); component.rejectionForm.controls.reason.setValue('No');
    component.refresh();
    expect(component.decisionMode()).toBe('idle');
    expect(component.rejectionForm.controls.reason.value).toBe('');
  });

  it('discards stale post-approval execution read after route navigation', async () => {
    asRole('admin');
    const executionRead = new Subject<RemediationExecution>();
    getExecution.mockReturnValue(executionRead);
    approveProposal.mockReturnValue(of(proposal('approved', { approved_by_user_id: id })));
    getProposal.mockImplementation((proposalId) => of(proposal('pending_approval', {
      id: proposalId, target: proposalId === id ? 'payments-api' : 'new-target',
    })));
    const { component, router } = await open();
    component.openApproval(); component.confirmApproval();
    expect(component.executionState()).toBe('loading');
    await router.navigateByUrl(`/remediation/${otherId}`);
    executionRead.next(execution('requested'));
    expect(component.proposal()?.id).toBe(otherId);
    expect(component.execution()).toBeNull();
    expect(component.executionState()).toBe('not-applicable');
  });

  it('discards late A mutation after A to B and A to B to A route changes', async () => {
    asRole('admin');
    const first = new Subject<RemediationProposal>();
    approveProposal.mockReturnValue(first);
    getProposal.mockImplementation((proposalId) => of(proposal('pending_approval', {
      id: proposalId, target: proposalId === id ? 'payments-api' : 'new-target',
    })));
    const { component, router } = await open();
    component.openApproval(); component.confirmApproval();
    await router.navigateByUrl(`/remediation/${otherId}`);
    first.next(proposal('approved')); first.complete();
    expect(component.proposal()?.id).toBe(otherId);
    expect(component.proposal()?.status).toBe('pending_approval');
    await router.navigateByUrl(`/remediation/${id}`);
    const second = new Subject<RemediationProposal>(); approveProposal.mockReturnValue(second);
    component.openApproval(); component.confirmApproval();
    await router.navigateByUrl(`/remediation/${otherId}`);
    await router.navigateByUrl(`/remediation/${id}`);
    second.next(proposal('approved')); second.complete();
    expect(component.proposal()?.status).toBe('pending_approval');
    expect(getExecution).not.toHaveBeenCalled();
  });

  it('rejects malformed proposal UUID before any HTTP call and provides Back', async () => {
    const { harness } = await open('/remediation/not-an-id');
    expect(getProposal).not.toHaveBeenCalled();
    expect(getExecution).not.toHaveBeenCalled();
    expect(harness.routeNativeElement?.textContent).toContain('Invalid remediation proposal link.');
    expect(harness.routeNativeElement?.querySelector('a')?.getAttribute('href')).toBe('/remediation');
  });

  it('loads pending proposal, shows proposer and decision boundary, and skips execution', async () => {
    const { harness } = await open();
    const text = harness.routeNativeElement?.textContent ?? '';
    expect(text).toContain('A human decision has not yet been recorded.');
    expect(text).toContain(otherId);
    expect(text).toContain('Execution is unavailable until this proposal is approved.');
    expect(text).not.toContain('Approved by user ID');
    expect(text).not.toContain('Rejected by user ID');
    expect(getExecution).not.toHaveBeenCalled();
    expect(harness.routeNativeElement?.querySelectorAll('h1')).toHaveLength(1);
    expect(harness.routeNativeElement?.querySelector('script')).toBeNull();
    expect(text).toContain('<script>alert("x")</script>');
    expect(text).not.toMatch(/Create proposal|Approve proposal|Reject proposal|Execute proposal/);
  });

  it('shows not-found and generic proposal errors, and retries safely', async () => {
    getProposal.mockReturnValueOnce(throwError(notFound)).mockReturnValueOnce(
      throwError(() => new Error('private detail'))).mockReturnValueOnce(of(proposal('pending_approval')));
    const { harness, component } = await open();
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposal not found.');
    component.refresh();
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposal details could not be loaded.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    component.refresh();
    harness.detectChanges();
    expect(getProposal).toHaveBeenCalledTimes(3);
    expect(harness.routeNativeElement?.textContent).toContain('payments-api');
  });

  it('preserves queue parameters on Back and uses plain queue path for direct links', async () => {
    const fromQueue = await open(`/remediation/${id}?status=approved&limit=50&offset=100`);
    expect(fromQueue.harness.routeNativeElement?.querySelector('a')?.getAttribute('href'))
      .toBe('/remediation?status=approved&limit=50&offset=100');
  });

  it('presents approved attribution and treats execution 404 as normal absence', async () => {
    getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValue(throwError(notFound));
    const { harness } = await open();
    const text = harness.routeNativeElement?.textContent ?? '';
    expect(text).toContain('Approved by user ID');
    expect(text).toContain('Approval permits an authorized execution request');
    expect(text).toContain('No execution has been requested for this approved proposal.');
    expect(text).not.toContain('Execution status could not be loaded.');
    expect(getExecution).toHaveBeenCalledWith(id);
  });

  it('presents rejected attribution and plain-text reason, without execution request', async () => {
    getProposal.mockReturnValue(of(proposal('rejected')));
    const { harness } = await open();
    const text = harness.routeNativeElement?.textContent ?? '';
    expect(text).toContain('Rejected by user ID');
    expect(text).toContain('Rejected proposals cannot proceed to execution.');
    expect(text).toContain('This rejected proposal cannot be executed.');
    expect(text).toContain('<b>human reason</b>');
    expect(harness.routeNativeElement?.querySelector('b')).toBeNull();
    expect(getExecution).not.toHaveBeenCalled();
  });

  it.each(['requested', 'in_progress', 'succeeded', 'failed', 'outcome_unknown'] as const)(
    'presents approved execution status %s with only exposed durable metadata', async (status) => {
      getProposal.mockReturnValue(of(proposal('approved')));
      getExecution.mockReturnValue(of(execution(status)));
      const { harness } = await open();
      const text = harness.routeNativeElement?.textContent ?? '';
      const label = {
        requested: 'Requested', in_progress: 'In progress', succeeded: 'Succeeded',
        failed: 'Failed', outcome_unknown: 'Outcome unknown',
      }[status];
      expect(text).toContain(`Status: ${label}`);
      expect(text).toContain('Requested by user ID');
      expect(text).toContain(otherId);
      expect(text).toContain('Requested at');
      expect(text).toContain('Updated');
      expect(text.includes('Completed at')).toBe(['succeeded', 'failed', 'outcome_unknown'].includes(status));
      if (status === 'outcome_unknown') {
        expect(text).toContain('final actuator outcome could not be determined');
        expect(text).not.toContain('recorded as failed');
      }
      if (status === 'failed') {
        expect(text).toContain('durable execution is recorded as failed');
        expect(text).not.toMatch(/timeout|transport|HTTP error|actuator response/i);
      }
    },
  );

  it('keeps a generic execution error inside its section and Refresh reloads both reads', async () => {
    getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValueOnce(throwError(() => new Error('private detail')))
      .mockReturnValueOnce(of(execution('in_progress')));
    const { harness, component } = await open();
    expect(harness.routeNativeElement?.textContent).toContain('Execution status could not be loaded.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    expect(harness.routeNativeElement?.textContent).toContain('Remediation proposal');
    component.refresh(); harness.detectChanges();
    expect(getProposal).toHaveBeenCalledTimes(2);
    expect(getExecution).toHaveBeenCalledTimes(2);
    expect(harness.routeNativeElement?.textContent).toContain('Status: In progress');
  });

  it('loads proposal before execution and cancels stale route responses', async () => {
    const first = new Subject<RemediationProposal>();
    getProposal.mockReturnValueOnce(first).mockReturnValueOnce(of(proposal('pending_approval', {
      id: otherId, target: 'new-target',
    })));
    const { harness, router } = await open();
    expect(getExecution).not.toHaveBeenCalled();
    await router.navigateByUrl(`/remediation/${otherId}`);
    expect(first.observed).toBe(false);
    first.next(proposal('approved'));
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain('new-target');
    expect(harness.routeNativeElement?.textContent).not.toContain('payments-api');
    expect(getExecution).not.toHaveBeenCalled();
  });
  it.each(['viewer', 'operator', 'admin'] as const)('allows only admin to request approved missing execution: %s', async (role) => {
    asRole(role); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValue(throwError(notFound));
    const { harness, component } = await open();
    harness.detectChanges();
    expect(component.canRequestExecution(component.proposal()!)).toBe(role === 'admin');
    expect(harness.routeNativeElement?.textContent).toContain('No execution has been requested');
    expect(harness.routeNativeElement?.textContent?.includes('Request execution')).toBe(role === 'admin');
  });

  it('requires confirmation, displays action and target, and sends one request without optimistic state', async () => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValue(throwError(notFound));
    const pending = new Subject<RemediationExecution>(); requestExecution.mockReturnValue(pending);
    const { harness, component } = await open();
    component.openExecutionRequest(); harness.detectChanges();
    const text = harness.routeNativeElement?.textContent ?? '';
    expect(text).toContain('Request remediation execution?');
    expect(text).toContain('Restart service');
    expect(text).toContain('payments-api');
    component.cancelExecutionRequest(); expect(requestExecution).not.toHaveBeenCalled();
    component.openExecutionRequest(); component.confirmExecutionRequest(); component.confirmExecutionRequest();
    harness.detectChanges();
    expect(requestExecution).toHaveBeenCalledTimes(1);
    expect(component.execution()).toBeNull();
    expect(component.executionState()).toBe('missing');
    expect(component.mutationState()).toBe('requesting-execution');
    expect(harness.routeNativeElement?.textContent).toContain('Requesting execution');
    expect(harness.routeNativeElement?.textContent).toContain('No execution has been requested');
    expect(harness.routeNativeElement?.querySelector('nav button')?.hasAttribute('disabled')).toBe(true);
    component.refresh(); expect(getProposal).toHaveBeenCalledTimes(1);
    pending.next(execution('requested')); pending.complete(); harness.detectChanges();
    expect(component.execution()).toEqual(execution('requested'));
    expect(component.mutationState()).toBe('idle');
    expect(component.executionRequestMode()).toBe('idle');
    expect(harness.routeNativeElement?.textContent).toContain('Execution request accepted.');
    expect(harness.routeNativeElement?.textContent).toContain('awaiting asynchronous processing');
    expect(harness.routeNativeElement?.textContent).not.toContain('Confirm execution request');
    expect(getExecution).toHaveBeenCalledTimes(1);
  });

  it.each(['pending_approval', 'rejected'] as const)('never requests execution for %s', async (status) => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal(status)));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(requestExecution).not.toHaveBeenCalled();
    expect(harness.routeNativeElement?.textContent).not.toContain('Request execution');
  });

  it.each(['loading', 'ready', 'error'] as const)('does not offer request when execution is %s', async (state) => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    if (state === 'loading') getExecution.mockReturnValue(new Subject<RemediationExecution>());
    else if (state === 'ready') getExecution.mockReturnValue(of(execution('requested')));
    else getExecution.mockReturnValue(throwError(() => new Error('private detail')));
    const { harness, component } = await open(); harness.detectChanges();
    expect(component.executionState()).toBe(state);
    expect(harness.routeNativeElement?.textContent).not.toContain('Request execution');
  });

  it.each(['requested', 'in_progress', 'succeeded', 'failed', 'outcome_unknown'] as const)(
    'keeps durable status %s read-only with precise language', async (status) => {
      asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
      getExecution.mockReturnValue(of(execution(status)));
      const { harness, component } = await open(); harness.detectChanges();
      expect(component.canRequestExecution(component.proposal()!)).toBe(false);
      expect(harness.routeNativeElement?.textContent).not.toMatch(/Request execution|Retry execution|Run again/);
      const expected = {
        requested: 'durably recorded', in_progress: 'recorded this execution as in progress',
        succeeded: 'recorded as succeeded', failed: 'recorded as failed',
        outcome_unknown: 'outcome could not be determined',
      }[status];
      expect(harness.routeNativeElement?.textContent).toContain(expected);
    });

  it('reconciles duplicate 409 with one GET and no second POST', async () => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValueOnce(throwError(notFound)).mockReturnValueOnce(of(execution('in_progress')));
    requestExecution.mockReturnValue(throwError(() => httpError(409,
      'Execution has already been requested for this remediation proposal')));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(requestExecution).toHaveBeenCalledTimes(1);
    expect(getExecution).toHaveBeenCalledTimes(2);
    expect(component.execution()?.status).toBe('in_progress');
    expect(harness.routeNativeElement?.textContent).toContain('Execution was already requested');
    expect(harness.routeNativeElement?.textContent).not.toContain('Request execution');
  });

  it.each(['missing', 'failure'] as const)('keeps duplicate reconciliation %s unconfirmed until Refresh', async (result) => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValueOnce(throwError(notFound)).mockReturnValueOnce(
      throwError(() => result === 'missing' ? httpError(404) : new Error('private detail')))
      .mockReturnValueOnce(throwError(notFound));
    requestExecution.mockReturnValue(throwError(() => httpError(409,
      'Execution has already been requested for this remediation proposal')));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(component.executionState()).toBe('unconfirmed');
    expect(harness.routeNativeElement?.textContent).not.toContain('Request execution');
    expect(harness.routeNativeElement?.textContent).toContain('Refresh before');
    component.refresh(); harness.detectChanges();
    expect(component.executionState()).toBe('missing');
    expect(harness.routeNativeElement?.textContent).toContain('Request execution');
    expect(requestExecution).toHaveBeenCalledTimes(1);
  });

  it.each(['exists', 'missing', 'failure'] as const)('reconciles ambiguous POST failure when GET %s', async (result) => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValueOnce(throwError(notFound)).mockReturnValueOnce(
      result === 'exists' ? of(execution('requested')) :
        throwError(() => result === 'missing' ? httpError(404) : new Error('private detail')));
    requestExecution.mockReturnValue(throwError(() => new Error('transport private detail')));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(requestExecution).toHaveBeenCalledTimes(1);
    expect(getExecution).toHaveBeenCalledTimes(2);
    expect(component.executionState()).toBe(result === 'exists' ? 'ready' : result === 'missing' ? 'missing' : 'unconfirmed');
    expect(harness.routeNativeElement?.textContent).toContain(result === 'exists'
      ? 'An execution record exists' : result === 'missing'
        ? 'No durable execution record was found' : 'Refresh before trying again');
    expect(harness.routeNativeElement?.textContent?.includes('Request execution')).toBe(result === 'missing');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    if (result === 'failure') {
      getExecution.mockReturnValueOnce(throwError(notFound));
      component.refresh(); harness.detectChanges();
      expect(component.executionState()).toBe('missing');
      expect(harness.routeNativeElement?.textContent).toContain('Request execution');
    }
  });

  it('reloads proposal after exact not-approved 409 and applies returned state', async () => {
    asRole('admin'); getProposal.mockReturnValueOnce(of(proposal('approved')))
      .mockReturnValueOnce(of(proposal('rejected')));
    getExecution.mockReturnValue(throwError(notFound));
    requestExecution.mockReturnValue(throwError(() => httpError(409,
      'Remediation proposal is not approved for execution')));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(requestExecution).toHaveBeenCalledTimes(1);
    expect(getProposal).toHaveBeenCalledTimes(2);
    expect(component.proposal()?.status).toBe('rejected');
    expect(harness.routeNativeElement?.textContent).toContain('Proposal state no longer allows');
    expect(harness.routeNativeElement?.textContent).not.toContain('Remediation proposal is not approved for execution');
  });

  it.each([403, 404] as const)('handles execute %s without exposing raw detail or clearing session', async (status) => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValue(throwError(notFound));
    requestExecution.mockReturnValue(throwError(() => httpError(status, 'private detail')));
    const { harness, component } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest(); harness.detectChanges();
    expect(requestExecution).toHaveBeenCalledTimes(1);
    expect(currentUser()?.role).toBe('admin');
    expect(component.proposalState()).toBe(status === 404 ? 'not-found' : 'ready');
    expect(harness.routeNativeElement?.textContent).toContain(status === 404
      ? 'Remediation proposal not found.' : 'You do not have permission to request remediation execution.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
  });

  it('discards late execute POST across A to B and A to B to A', async () => {
    asRole('admin'); getProposal.mockImplementation((proposalId) => of(proposal('approved', { id: proposalId })));
    getExecution.mockReturnValue(throwError(notFound));
    const first = new Subject<RemediationExecution>(); requestExecution.mockReturnValue(first);
    const { component, router } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest();
    await router.navigateByUrl(`/remediation/${otherId}`);
    first.next(execution('requested'));
    expect(component.proposal()?.id).toBe(otherId);
    expect(component.execution()).toBeNull();
    await router.navigateByUrl(`/remediation/${id}`);
    const second = new Subject<RemediationExecution>(); requestExecution.mockReturnValue(second);
    component.openExecutionRequest(); component.confirmExecutionRequest();
    await router.navigateByUrl(`/remediation/${otherId}`);
    await router.navigateByUrl(`/remediation/${id}`);
    second.next(execution('requested'));
    expect(component.proposal()?.id).toBe(id);
    expect(component.execution()).toBeNull();
    expect(component.executionState()).toBe('missing');
  });

  it('discards late reconciliation GET from obsolete generation', async () => {
    asRole('admin'); getProposal.mockImplementation((proposalId) => of(proposal('approved', { id: proposalId })));
    const lateRead = new Subject<RemediationExecution>();
    getExecution.mockReturnValueOnce(throwError(notFound)).mockReturnValueOnce(lateRead)
      .mockReturnValueOnce(throwError(notFound));
    requestExecution.mockReturnValue(throwError(() => new Error('transport')));
    const { component, router } = await open();
    component.openExecutionRequest(); component.confirmExecutionRequest();
    expect(component.executionState()).toBe('reconciling');
    await router.navigateByUrl(`/remediation/${otherId}`);
    lateRead.next(execution('succeeded'));
    expect(component.proposal()?.id).toBe(otherId);
    expect(component.execution()).toBeNull();
    expect(component.executionState()).toBe('missing');
  });

  it('uses manual Refresh to monitor status and external requests', async () => {
    asRole('admin'); getProposal.mockReturnValue(of(proposal('approved')));
    getExecution.mockReturnValueOnce(throwError(notFound))
      .mockReturnValueOnce(of(execution('requested')))
      .mockReturnValueOnce(of(execution('in_progress')))
      .mockReturnValueOnce(of(execution('succeeded')));
    const { harness, component } = await open();
    expect(component.executionState()).toBe('missing');
    component.refresh(); harness.detectChanges(); expect(component.execution()?.status).toBe('requested');
    component.refresh(); harness.detectChanges(); expect(component.execution()?.status).toBe('in_progress');
    component.refresh(); harness.detectChanges(); expect(component.execution()?.status).toBe('succeeded');
    expect(getProposal).toHaveBeenCalledTimes(4);
    expect(getExecution).toHaveBeenCalledTimes(4);
    expect(requestExecution).not.toHaveBeenCalled();
    expect(harness.routeNativeElement?.textContent).not.toContain('Request execution');
  });

});
