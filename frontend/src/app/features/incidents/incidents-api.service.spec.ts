import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { firstValueFrom } from 'rxjs';
import { DEFAULT_INCIDENT_LIST_QUERY, IncidentListQuery } from './incident-list-query';
import { IncidentListResponse } from './incident.models';
import { IncidentsApiService } from './incidents-api.service';

describe('IncidentsApiService', () => {
  let service: IncidentsApiService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(IncidentsApiService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('lists incidents at the typed endpoint with default pagination and no optional filters', async () => {
    const response: IncidentListResponse = { items: [], limit: 20, offset: 0, total: 0 };
    const result = firstValueFrom(service.listIncidents(DEFAULT_INCIDENT_LIST_QUERY));
    const request = http.expectOne(
      (candidate) =>
        candidate.url === '/api/v1/incidents' &&
        candidate.params.get('limit') === '20' &&
        candidate.params.get('offset') === '0',
    );
    expect(request.request.method).toBe('GET');
    expect(request.request.params.keys().sort()).toEqual(['limit', 'offset']);
    request.flush(response);
    await expect(result).resolves.toEqual(response);
  });

  it('serializes every supported filter and pagination field exactly', async () => {
    const query: IncidentListQuery = {
      status: 'open',
      severity: 'critical',
      source: 'Prometheus EU',
      occurredFrom: '2026-09-15T10:00:00.000Z',
      occurredTo: '2026-09-15T12:00:00.000Z',
      limit: 50,
      offset: 100,
    };
    const result = firstValueFrom(service.listIncidents(query));
    const request = http.expectOne(
      '/api/v1/incidents?limit=50&offset=100&status=open&severity=critical&source=Prometheus%20EU&occurred_from=2026-09-15T10:00:00.000Z&occurred_to=2026-09-15T12:00:00.000Z',
    );
    expect(request.request.params.get('status')).toBe('open');
    expect(request.request.params.get('severity')).toBe('critical');
    expect(request.request.params.get('source')).toBe('Prometheus EU');
    expect(request.request.params.get('occurred_from')).toBe(query.occurredFrom);
    expect(request.request.params.get('occurred_to')).toBe(query.occurredTo);
    request.flush({ items: [], limit: 50, offset: 100, total: 0 });
    await result;
  });
});

// Detail reads share the same focused Incident API layer as the list.
describe('IncidentsApiService detail reads', () => {
  let service: IncidentsApiService;
  let http: HttpTestingController;
  const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [provideHttpClient(), provideHttpClientTesting()] });
    service = TestBed.inject(IncidentsApiService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('gets the typed Incident by ID', async () => {
    const result = firstValueFrom(service.getIncident(id));
    const request = http.expectOne(`/api/v1/incidents/${id}`);
    expect(request.request.method).toBe('GET');
    request.flush({ id, source: 'prometheus', title: 'Incident' });
    await expect(result).resolves.toMatchObject({ id, title: 'Incident' });
  });

  it('gets the typed deterministic triage by ID', async () => {
    const result = firstValueFrom(service.getIncidentTriage(id));
    const request = http.expectOne(`/api/v1/incidents/${id}/triage`);
    expect(request.request.method).toBe('GET');
    request.flush({ incident_id: id, priority: 'P1', requires_human_review: true });
    await expect(result).resolves.toMatchObject({ incident_id: id, priority: 'P1' });
  });

  it('gets only the newest persisted enrichment snapshot', async () => {
    const result = firstValueFrom(service.getLatestIncidentEnrichment(id));
    const request = http.expectOne(`/api/v1/incidents/${id}/enrichments?limit=1&offset=0`);
    expect(request.request.method).toBe('GET');
    expect(request.request.params.keys().sort()).toEqual(['limit', 'offset']);
    request.flush({ items: [], total: 0, limit: 1, offset: 0 });
    await expect(result).resolves.toMatchObject({ items: [], limit: 1, offset: 0 });
  });
});
