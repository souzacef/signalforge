import { HttpErrorResponse } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { Observable, of, Subject, throwError } from 'rxjs';
import { vi } from 'vitest';
import { EnrichmentListResponse, Incident, IncidentTriage } from './incident.models';
import { IncidentsApiService } from './incidents-api.service';
import { IncidentDetailComponent } from './incident-detail.component';
import { isIncidentId } from './incident-id';

const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';
const otherId = '79caa2b3-59e2-4187-953c-e590a68aab2a';
const incident: Incident = {
  id,
  source: 'prometheus',
  title: 'Checkout error rate elevated',
  description: 'Elevated errors on checkout',
  severity: 'critical',
  status: 'acknowledged',
  acknowledged_at: '2026-09-15T18:00:00Z',
  acknowledged_by_user_id: otherId,
  resolved_at: null,
  resolved_by_user_id: null,
  occurred_at: '2026-09-15T17:45:00Z',
  created_at: '2026-09-15T17:46:00Z',
  updated_at: '2026-09-15T18:00:00Z',
};
const triage: IncidentTriage = {
  incident_id: id,
  event_id: otherId,
  source: 'prometheus',
  original_severity: 'critical',
  priority: 'P1',
  requires_human_review: true,
  created_at: '2026-09-15T17:47:00Z',
};
const enrichment: EnrichmentListResponse = {
  total: 2,
  limit: 1,
  offset: 0,
  items: [{
    request_event_id: otherId,
    incident_id: id,
    trigger_event_id: otherId,
    provider: 'provider-name',
    model: 'model-name',
    summary: 'Review checkout request errors',
    category: 'availability',
    suspected_component: 'checkout-api',
    investigation_steps: ['Inspect errors', 'Compare recent deployments'],
    created_at: '2026-09-15T17:48:00Z',
  }],
};
const notFound = new HttpErrorResponse({ status: 404, statusText: 'Not Found' });
const serverError = new HttpErrorResponse({ status: 503, statusText: 'Unavailable' });

function visible(harness: RouterTestingHarness): string {
  harness.detectChanges();
  return harness.routeNativeElement?.textContent ?? '';
}

describe('Incident ID validation', () => {
  it('accepts UUID shape and rejects malformed route values', () => {
    expect(isIncidentId(id)).toBe(true);
    expect(isIncidentId('not-an-id')).toBe(false);
    expect(isIncidentId(null)).toBe(false);
  });
});

describe('IncidentDetailComponent', () => {
  const getIncident = vi.fn<(value: string) => Observable<Incident>>();
  const getIncidentTriage = vi.fn<(value: string) => Observable<IncidentTriage>>();
  const getLatestIncidentEnrichment = vi.fn<(value: string) => Observable<EnrichmentListResponse>>();

  beforeEach(() => {
    getIncident.mockReset().mockReturnValue(of(incident));
    getIncidentTriage.mockReset().mockReturnValue(of(triage));
    getLatestIncidentEnrichment.mockReset().mockReturnValue(of(enrichment));
    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'incidents/:incidentId', component: IncidentDetailComponent }]),
        { provide: IncidentsApiService, useValue: { getIncident, getIncidentTriage, getLatestIncidentEnrichment } },
      ],
    });
  });

  afterEach(() => vi.restoreAllMocks());

  async function open(url = `/incidents/${id}`) {
    const harness = await RouterTestingHarness.create(url);
    const component = harness.routeDebugElement?.componentInstance as IncidentDetailComponent;
    return { harness, component, router: TestBed.inject(Router) };
  }

  it('loads a valid UUID, core Incident facts, and contextual lifecycle metadata', async () => {
    const { harness } = await open();
    expect(getIncident).toHaveBeenCalledWith(id);
    expect(getIncidentTriage).toHaveBeenCalledWith(id);
    expect(getLatestIncidentEnrichment).toHaveBeenCalledWith(id);
    const text = visible(harness);
    expect(text).toContain(incident.title);
    expect(text).toContain('Critical severity');
    expect(text).toContain('Acknowledged');
    expect(text).toContain(incident.description);
    expect(text).toContain('Incident ID');
    expect(text).not.toContain('Resolved by');
    expect(harness.routeNativeElement?.querySelector('time')?.getAttribute('datetime')).toBe(incident.occurred_at);
  });

  it('makes no API calls for a malformed UUID and offers a clean back link', async () => {
    const { harness } = await open('/incidents/not-a-uuid');
    expect(visible(harness)).toContain('Invalid incident link.');
    expect(getIncident).not.toHaveBeenCalled();
    expect(getIncidentTriage).not.toHaveBeenCalled();
    expect(getLatestIncidentEnrichment).not.toHaveBeenCalled();
    expect(harness.routeNativeElement?.querySelector<HTMLAnchorElement>('a')?.getAttribute('href')).toBe('/incidents');
  });

  it('preserves list query parameters in the back link', async () => {
    const { harness } = await open(`/incidents/${id}?status=open&limit=50&offset=100`);
    expect(harness.routeNativeElement?.querySelector<HTMLAnchorElement>('a')?.getAttribute('href')).toBe('/incidents?status=open&limit=50&offset=100');
  });

  it('shows Incident not found without secondary reads', async () => {
    getIncident.mockReturnValue(throwError(() => notFound));
    const { harness } = await open();
    expect(visible(harness)).toContain('Incident not found.');
    expect(getIncidentTriage).not.toHaveBeenCalled();
    expect(getLatestIncidentEnrichment).not.toHaveBeenCalled();
  });

  it('shows a safe Incident error and Retry reloads the current ID', async () => {
    getIncident.mockReturnValueOnce(throwError(() => serverError)).mockReturnValueOnce(of(incident));
    const { harness, component } = await open();
    expect(visible(harness)).toContain('Incident details could not be loaded.');
    expect(visible(harness)).not.toContain('Unavailable');
    component.refresh();
    expect(visible(harness)).toContain(incident.title);
    expect(getIncident).toHaveBeenCalledTimes(2);
  });

  it('Refresh re-fetches Incident, triage, and latest advisory without changing the URL', async () => {
    const { component, router } = await open(`/incidents/${id}?status=open`);
    component.refresh();
    expect(getIncident).toHaveBeenCalledTimes(2);
    expect(getIncidentTriage).toHaveBeenCalledTimes(2);
    expect(getLatestIncidentEnrichment).toHaveBeenCalledTimes(2);
    expect(router.url).toBe(`/incidents/${id}?status=open`);
  });

  it('cancels an obsolete route request and clears previous detail', async () => {
    const first = new Subject<Incident>();
    const second = new Subject<Incident>();
    getIncident.mockReturnValueOnce(first).mockReturnValueOnce(second);
    const { harness, router } = await open();
    await router.navigateByUrl(`/incidents/${otherId}`);
    expect(first.observed).toBe(false);
    first.next(incident);
    expect(visible(harness)).not.toContain(incident.title);
    second.next({ ...incident, id: otherId, title: 'New incident' });
    expect(visible(harness)).toContain('New incident');
    expect(visible(harness)).not.toContain(incident.title);
  });

  it.each([
    ['P1', 'P1 · Immediate'], ['P2', 'P2 · High'], ['P3', 'P3 · Moderate'], ['P4', 'P4 · Low'],
  ] as const)('labels %s priority as %s', async (priority, label) => {
    getIncidentTriage.mockReturnValue(of({ ...triage, priority }));
    const { harness } = await open();
    expect(visible(harness)).toContain(label);
    expect(visible(harness)).toContain('authoritative priority baseline');
  });

  it('renders human review true and false as explicit text', async () => {
    const first = await open();
    expect(visible(first.harness)).toContain('Human review required');
    getIncidentTriage.mockReturnValue(of({ ...triage, requires_human_review: false }));
    first.component.refresh();
    expect(visible(first.harness)).toContain('Human review not required');
  });

  it('treats triage 404 after Incident success as unavailable yet', async () => {
    getIncidentTriage.mockReturnValue(throwError(() => notFound));
    const { harness } = await open();
    expect(visible(harness)).toContain('Deterministic triage is not available yet.');
    expect(visible(harness)).not.toContain('Incident not found.');
  });

  it('shows a safe section-level triage failure', async () => {
    getIncidentTriage.mockReturnValue(throwError(() => serverError));
    const { harness } = await open();
    expect(visible(harness)).toContain('Deterministic triage could not be loaded.');
  });

  it('handles zero persisted enrichments as normal absence', async () => {
    getLatestIncidentEnrichment.mockReturnValue(of({ items: [], total: 0, limit: 1, offset: 0 }));
    const { harness } = await open();
    expect(visible(harness)).toContain('No persisted AI advisory is available');
    expect(visible(harness)).toContain('Deterministic triage remains authoritative');
  });

  it('renders latest advisory text, category, steps, provenance, timestamp and total count', async () => {
    const { harness } = await open();
    const text = visible(harness);
    expect(text).toContain('Advisory only. This does not change deterministic priority');
    expect(text).toContain('Review checkout request errors');
    expect(text).toContain('Availability');
    expect(text).toContain('checkout-api');
    expect(text).toContain('Inspect errors');
    expect(text).toContain('provider-name');
    expect(text).toContain('model-name');
    expect(text).toContain('Latest of 2 persisted advisory snapshots.');
    expect(harness.routeNativeElement?.querySelectorAll('.advisory-content ol li').length).toBe(2);
    expect(harness.routeNativeElement?.querySelector('.advisory-content time')?.getAttribute('datetime')).toBe(enrichment.items[0].created_at);
  });

  it('omits absent suspected component and renders AI text as plain text', async () => {
    getLatestIncidentEnrichment.mockReturnValue(of({
      ...enrichment,
      items: [{ ...enrichment.items[0], suspected_component: null, summary: '<script>alert(1)</script>' }],
    }));
    const { harness } = await open();
    expect(visible(harness)).toContain('<script>alert(1)</script>');
    expect(visible(harness)).not.toContain('Suspected component');
    expect(harness.routeNativeElement?.querySelector('script')).toBeNull();
  });

  it('shows a safe section-level advisory failure', async () => {
    getLatestIncidentEnrichment.mockReturnValue(throwError(() => serverError));
    const { harness } = await open();
    expect(visible(harness)).toContain('AI advisory could not be loaded.');
  });
});
