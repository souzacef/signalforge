import { TestBed } from '@angular/core/testing';
import { NavigationEnd, provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { filter, firstValueFrom, Observable, of, Subject, take, throwError } from 'rxjs';
import { vi } from 'vitest';
import { RemediationApiService } from './remediation-api.service';
import { RemediationListQuery } from './remediation-list-query';
import { RemediationProposal, RemediationProposalListResponse } from './remediation.models';
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

describe('RemediationComponent', () => {
  const listProposals = vi.fn<(query: RemediationListQuery) => Observable<RemediationProposalListResponse>>();
  beforeEach(() => {
    listProposals.mockReset();
    listProposals.mockReturnValue(of(response({ total: 201 })));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'remediation', component: RemediationComponent }]),
        { provide: RemediationApiService, useValue: { listProposals } },
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
