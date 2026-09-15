import { TestBed } from '@angular/core/testing';
import { NavigationEnd, provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { filter, firstValueFrom, Observable, of, Subject, take, throwError } from 'rxjs';
import { vi } from 'vitest';
import { IncidentListQuery } from './incident-list-query';
import { Incident, IncidentListResponse } from './incident.models';
import { IncidentsApiService } from './incidents-api.service';
import { IncidentsComponent } from './incidents.component';

const incident: Incident = {
  id: '57ed68ac-bc67-493e-a621-f63da52d6b12',
  source: 'prometheus',
  title: 'Checkout error rate elevated',
  description: null,
  severity: 'critical',
  status: 'acknowledged',
  acknowledged_at: '2026-09-15T18:00:00Z',
  acknowledged_by_user_id: '79caa2b3-59e2-4187-953c-e590a68aab2a',
  resolved_at: null,
  resolved_by_user_id: null,
  occurred_at: '2026-09-15T17:45:00Z',
  created_at: '2026-09-15T17:46:00Z',
  updated_at: '2026-09-15T18:00:00Z',
};

function response(overrides: Partial<IncidentListResponse> = {}): IncidentListResponse {
  return { items: [incident], limit: 20, offset: 0, total: 1, ...overrides };
}

describe('IncidentsComponent', () => {
  const listIncidents = vi.fn<(query: IncidentListQuery) => Observable<IncidentListResponse>>();

  beforeEach(() => {
    listIncidents.mockReset();
    listIncidents.mockReturnValue(of(response({ total: 201 })));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'incidents', component: IncidentsComponent }]),
        { provide: IncidentsApiService, useValue: { listIncidents } },
      ],
    });
  });

  afterEach(() => vi.restoreAllMocks());

  async function open(url = '/incidents') {
    const harness = await RouterTestingHarness.create(url);
    const component = harness.routeDebugElement?.componentInstance as IncidentsComponent;
    return { harness, component, router: TestBed.inject(Router) };
  }

  async function navigateWith(action: () => void, router: Router): Promise<void> {
    const completed = firstValueFrom(
      router.events.pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        take(1),
      ),
    );
    action();
    await completed;
  }

  it('populates filter editing state from the initial URL query', async () => {
    const { component } = await open(
      '/incidents?status=open&severity=high&source=prometheus&limit=50&offset=100',
    );
    expect(component.filterForm.getRawValue()).toMatchObject({
      status: 'open',
      severity: 'high',
      source: 'prometheus',
    });
    expect(component.query()).toMatchObject({ limit: 50, offset: 100 });
    expect(listIncidents).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'open', severity: 'high', source: 'prometheus' }),
    );
  });

  it('applies filters through the route, resets offset, and preserves page size', async () => {
    const { component, router } = await open('/incidents?limit=50&offset=100');
    component.filterForm.patchValue({ status: 'open', source: '  Prometheus EU  ' });
    await navigateWith(() => component.applyFilters(), router);
    expect(router.url).toBe('/incidents?status=open&source=Prometheus%20EU&limit=50');
  });

  it('clears filters, resets offset, and preserves page size', async () => {
    const { component, router } = await open(
      '/incidents?status=open&source=prometheus&limit=50&offset=50',
    );
    await navigateWith(() => component.clearFilters(), router);
    expect(router.url).toBe('/incidents?limit=50');
  });

  it('changes server pagination while preserving committed filters', async () => {
    const { component, router } = await open('/incidents?status=open');
    await navigateWith(
      () => component.changePage({ pageIndex: 2, pageSize: 50, length: 200, previousPageIndex: 0 }),
      router,
    );
    expect(router.url).toBe('/incidents?status=open&limit=50&offset=100');
    expect(listIncidents).toHaveBeenLastCalledWith(
      expect.objectContaining({ status: 'open', limit: 50, offset: 100 }),
    );
  });

  it('does not navigate or request for an invalid date range', async () => {
    const { component, router } = await open();
    const calls = listIncidents.mock.calls.length;
    component.filterForm.patchValue({
      occurredFrom: '2026-09-16T12:00',
      occurredTo: '2026-09-15T12:00',
    });
    component.applyFilters();
    expect(component.dateRangeError()).toContain('Occurred from');
    expect(router.url).toBe('/incidents');
    expect(listIncidents).toHaveBeenCalledTimes(calls);
  });

  it('shows loading, then a successful table with text labels and total range', async () => {
    const pending = new Subject<IncidentListResponse>();
    listIncidents.mockReturnValue(pending);
    const { harness } = await open();
    expect(harness.routeNativeElement?.textContent).toContain('Loading incidents');

    pending.next(response({ total: 137, items: Array(20).fill(incident) }));
    pending.complete();
    harness.detectChanges();
    const text = harness.routeNativeElement?.textContent;
    expect(text).toContain('Checkout error rate elevated');
    expect(text).toContain('Critical');
    expect(text).toContain('Acknowledged');
    expect(text).toContain('1–20 of 137 incidents');
  });

  it('distinguishes unfiltered empty data from filtered no-match data', async () => {
    listIncidents.mockReturnValue(of(response({ items: [], total: 0 })));
    const unfiltered = await open();
    expect(unfiltered.harness.routeNativeElement?.textContent).toContain(
      'No incident records are currently available.',
    );

    await unfiltered.router.navigateByUrl('/incidents?status=resolved');
    unfiltered.harness.detectChanges();
    expect(unfiltered.harness.routeNativeElement?.textContent).toContain(
      'No incidents match the selected filters.',
    );
  });

  it('shows a safe error and retries the current URL query', async () => {
    listIncidents
      .mockReturnValueOnce(throwError(() => new Error('private detail')))
      .mockReturnValueOnce(of(response()));
    const { harness, component } = await open('/incidents?status=open');
    expect(harness.routeNativeElement?.textContent).toContain('Incidents could not be loaded.');
    expect(harness.routeNativeElement?.textContent).not.toContain('private detail');
    component.retry();
    harness.detectChanges();
    expect(listIncidents).toHaveBeenCalledTimes(2);
    expect(harness.routeNativeElement?.textContent).toContain(incident.title);
  });

  it('corrects a page beyond the last result and reloads it with replace semantics', async () => {
    listIncidents
      .mockReturnValueOnce(of(response({ items: [], total: 21, offset: 40 })))
      .mockReturnValueOnce(of(response({ total: 21, offset: 20 })));
    const { router } = await open('/incidents?offset=40');
    expect(router.url).toBe('/incidents?offset=20');
    expect(listIncidents).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 20 }));
  });

  it('returns an empty result at a later offset to offset zero', async () => {
    listIncidents
      .mockReturnValueOnce(of(response({ items: [], total: 0, offset: 40 })))
      .mockReturnValueOnce(of(response({ items: [], total: 0, offset: 0 })));
    const { router } = await open('/incidents?offset=40');
    expect(router.url).toBe('/incidents');
    expect(listIncidents).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 0 }));
  });

  it('cancels an obsolete request so it cannot overwrite the newer view', async () => {
    const first = new Subject<IncidentListResponse>();
    const second = new Subject<IncidentListResponse>();
    listIncidents.mockReturnValueOnce(first).mockReturnValueOnce(second);
    const { harness, router } = await open('/incidents?status=open');
    await router.navigateByUrl('/incidents?status=resolved');
    expect(first.observed).toBe(false);
    first.next(response());
    second.next(response({ items: [], total: 0 }));
    second.complete();
    harness.detectChanges();
    expect(harness.routeNativeElement?.textContent).toContain(
      'No incidents match the selected filters.',
    );
    expect(harness.routeNativeElement?.textContent).not.toContain(incident.title);
  });
});
