import { HttpErrorResponse } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { Observable, of, Subject, throwError } from 'rxjs';
import { vi } from 'vitest';
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
  const getProposal = vi.fn<(id: string) => Observable<RemediationProposal>>();
  const getExecution = vi.fn<(id: string) => Observable<RemediationExecution>>();
  beforeEach(() => {
    getProposal.mockReset(); getExecution.mockReset();
    getProposal.mockReturnValue(of(proposal('pending_approval')));
    getExecution.mockReturnValue(of(execution('requested')));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'remediation/:proposalId', component: RemediationDetailComponent }]),
        { provide: RemediationApiService, useValue: { getProposal, getExecution } },
      ],
    });
  });
  afterEach(() => vi.restoreAllMocks());
  async function open(url = `/remediation/${id}`) {
    const harness = await RouterTestingHarness.create(url);
    const component = harness.routeDebugElement?.componentInstance as RemediationDetailComponent;
    return { harness, component, router: TestBed.inject(Router) };
  }

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
});
