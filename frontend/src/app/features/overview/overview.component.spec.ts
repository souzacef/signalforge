import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { Observable, of, Subject, throwError } from 'rxjs';
import { vi } from 'vitest';
import { IncidentListQuery } from '../incidents/incident-list-query';
import { Incident, IncidentListResponse } from '../incidents/incident.models';
import { IncidentsApiService } from '../incidents/incidents-api.service';
import { RemediationApiService } from '../remediation/remediation-api.service';
import { RemediationListQuery } from '../remediation/remediation-list-query';
import {
  RemediationProposal,
  RemediationProposalListResponse,
} from '../remediation/remediation.models';
import { OverviewComponent } from './overview.component';

const incidentId = (index: number) => `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`;
const proposerId = '11111111-2222-4333-8444-555555555555';

function incident(index: number, title = `Incident ${index}`): Incident {
  return {
    id: incidentId(index),
    source: `source-${index}`,
    title,
    description: null,
    severity: index % 2 === 0 ? 'critical' : 'high',
    status: 'open',
    acknowledged_at: null,
    acknowledged_by_user_id: null,
    resolved_at: null,
    resolved_by_user_id: null,
    occurred_at: `2026-09-${String(index).padStart(2, '0')}T12:00:00Z`,
    created_at: `2026-09-${String(index).padStart(2, '0')}T12:00:00Z`,
    updated_at: `2026-09-${String(index).padStart(2, '0')}T12:00:00Z`,
  };
}

function proposal(index: number): RemediationProposal {
  return {
    id: `aaaaaaaa-bbbb-4ccc-8ddd-${String(index).padStart(12, '0')}`,
    incident_id: incidentId(index),
    action_kind: 'restart_service',
    target: `service-${index}`,
    reason: 'Restore service',
    status: 'pending_approval',
    proposed_by_user_id: proposerId,
    approved_by_user_id: null,
    approved_at: null,
    rejected_by_user_id: null,
    rejected_at: null,
    rejection_reason: null,
    created_at: `2026-09-${String(index).padStart(2, '0')}T13:00:00Z`,
    updated_at: `2026-09-${String(index).padStart(2, '0')}T13:00:00Z`,
  };
}

function incidentResponse(items: Incident[] = [], total = items.length): IncidentListResponse {
  return { items, total, limit: 10, offset: 0 };
}

function proposalResponse(
  items: RemediationProposal[] = [],
  total = items.length,
): RemediationProposalListResponse {
  return { items, total, limit: 10, offset: 0 };
}

describe('OverviewComponent', () => {
  const listIncidents = vi.fn<(query: IncidentListQuery) => Observable<IncidentListResponse>>();
  const getIncident = vi.fn();
  const getIncidentTriage = vi.fn();
  const getLatestIncidentEnrichment = vi.fn();
  const listProposals =
    vi.fn<(query: RemediationListQuery) => Observable<RemediationProposalListResponse>>();
  const getProposal = vi.fn();
  const getExecution = vi.fn();

  async function configure(): Promise<void> {
    await TestBed.configureTestingModule({
      imports: [OverviewComponent],
      providers: [
        provideRouter([]),
        {
          provide: IncidentsApiService,
          useValue: {
            listIncidents,
            getIncident,
            getIncidentTriage,
            getLatestIncidentEnrichment,
          },
        },
        {
          provide: RemediationApiService,
          useValue: { listProposals, getProposal, getExecution },
        },
      ],
    }).compileComponents();
  }

  function create(): ComponentFixture<OverviewComponent> {
    const fixture = TestBed.createComponent(OverviewComponent);
    fixture.detectChanges();
    return fixture;
  }

  beforeEach(() => {
    listIncidents.mockReset();
    listProposals.mockReset();
    getIncident.mockReset();
    getIncidentTriage.mockReset();
    getLatestIncidentEnrichment.mockReset();
    getProposal.mockReset();
    getExecution.mockReset();

    listIncidents
      .mockReturnValueOnce(of(incidentResponse()))
      .mockReturnValueOnce(of(incidentResponse()))
      .mockReturnValueOnce(of(incidentResponse()));
    listProposals.mockReturnValue(of(proposalResponse()));
  });

  afterEach(() => {
    TestBed.resetTestingModule();
  });

  it('issues exactly the four typed list queries without detail or N+1 reads', async () => {
    await configure();
    create();

    expect(listIncidents.mock.calls).toEqual([
      [
        {
          status: 'open',
          severity: null,
          source: null,
          occurredFrom: null,
          occurredTo: null,
          limit: 10,
          offset: 0,
        },
      ],
      [
        {
          status: 'acknowledged',
          severity: null,
          source: null,
          occurredFrom: null,
          occurredTo: null,
          limit: 10,
          offset: 0,
        },
      ],
      [
        {
          status: 'open',
          severity: 'critical',
          source: null,
          occurredFrom: null,
          occurredTo: null,
          limit: 10,
          offset: 0,
        },
      ],
    ]);
    expect(listProposals).toHaveBeenCalledOnce();
    expect(listProposals).toHaveBeenCalledWith({
      incidentId: null,
      status: 'pending_approval',
      actionKind: null,
      target: null,
      proposedByUserId: null,
      createdFrom: null,
      createdTo: null,
      limit: 10,
      offset: 0,
    });
    expect(getIncident).not.toHaveBeenCalled();
    expect(getIncidentTriage).not.toHaveBeenCalled();
    expect(getLatestIncidentEnrichment).not.toHaveBeenCalled();
    expect(getProposal).not.toHaveBeenCalled();
    expect(getExecution).not.toHaveBeenCalled();
  });

  it('renders backend totals rather than current item counts', async () => {
    const items = [incident(6), incident(5), incident(4), incident(3), incident(2), incident(1)];
    const proposals = [
      proposal(6),
      proposal(5),
      proposal(4),
      proposal(3),
      proposal(2),
      proposal(1),
    ];
    listIncidents
      .mockReset()
      .mockReturnValueOnce(of(incidentResponse(items, 37)))
      .mockReturnValueOnce(of(incidentResponse([incident(7)], 18)))
      .mockReturnValueOnce(of(incidentResponse([incident(8)], 9)));
    listProposals.mockReturnValue(of(proposalResponse(proposals, 23)));
    await configure();
    const fixture = create();

    const cards = fixture.nativeElement.querySelectorAll('.metric-card') as NodeListOf<HTMLElement>;
    expect(
      [...cards].map((card) =>
        card.querySelector('.metric-label')?.textContent?.replace(/\s+/g, ' ').trim(),
      ),
    ).toEqual(['Open incidents', 'Acknowledged', 'Critical openCritical', 'Pending approval']);
    expect(
      [...cards].map((card) => card.querySelector('.metric-value')?.textContent?.trim()),
    ).toEqual(['37', '18', '9', '23']);
  });

  it('shows at most five incident previews in backend order with canonical links and scan fields', async () => {
    const items = [incident(6), incident(2), incident(5), incident(1), incident(4), incident(3)];
    listIncidents
      .mockReset()
      .mockReturnValueOnce(of(incidentResponse(items, 12)))
      .mockReturnValueOnce(of(incidentResponse()))
      .mockReturnValueOnce(of(incidentResponse()));
    await configure();
    const fixture = create();
    const rows = fixture.nativeElement.querySelectorAll(
      '.incident-list > li',
    ) as NodeListOf<HTMLElement>;

    expect(rows).toHaveLength(5);
    expect([...rows].map((row) => row.querySelector('.primary-link')?.textContent?.trim())).toEqual(
      items.slice(0, 5).map((item) => item.title),
    );
    expect(rows[0].textContent).toContain('Critical');
    expect(rows[0].textContent).toContain('source-6');
    expect(rows[0].textContent).toContain('Open');
    expect(rows[0].querySelector('time')?.getAttribute('datetime')).toBe(items[0].occurred_at);
    expect(rows[0].querySelector('.primary-link')?.getAttribute('href')).toBe(
      `/incidents/${items[0].id}?status=open`,
    );
    expect(
      fixture.nativeElement.querySelector('.preview-panel a[href="/incidents?status=open"]')
        ?.textContent,
    ).toContain('View all open incidents');
  });

  it('shows at most five pending proposals in backend order with compact proposers and links', async () => {
    const proposals = [
      proposal(6),
      proposal(2),
      proposal(5),
      proposal(1),
      proposal(4),
      proposal(3),
    ];
    listProposals.mockReturnValue(of(proposalResponse(proposals, 14)));
    await configure();
    const fixture = create();
    const rows = fixture.nativeElement.querySelectorAll(
      '.proposal-list > li',
    ) as NodeListOf<HTMLElement>;

    expect(rows).toHaveLength(5);
    expect([...rows].map((row) => row.querySelector('.target-value')?.textContent?.trim())).toEqual(
      proposals.slice(0, 5).map((item) => item.target),
    );
    expect(rows[0].textContent).toContain('Restart service');
    expect(rows[0].textContent).toContain('11111111…5555');
    expect(rows[0].querySelector('time')?.getAttribute('datetime')).toBe(proposals[0].created_at);
    expect(rows[0].querySelector('.primary-link')?.getAttribute('href')).toBe(
      `/remediation/${proposals[0].id}?status=pending_approval`,
    );
    expect(
      fixture.nativeElement.querySelector(
        '.preview-panel a[href="/remediation?status=pending_approval"]',
      )?.textContent,
    ).toContain('View all pending proposals');
    expect(fixture.nativeElement.textContent).not.toContain('Approve');
    expect(fixture.nativeElement.textContent).not.toContain('Reject');
    expect(fixture.nativeElement.textContent).not.toContain('Request execution');
  });

  it('renders both normal empty states and keeps filtered queue links available', async () => {
    await configure();
    const fixture = create();
    const text = fixture.nativeElement.textContent as string;

    expect(text).toContain('No open incidents.');
    expect(text).toContain('No remediation proposals are pending human review.');
    expect(fixture.nativeElement.querySelector('a[href="/incidents?status=open"]')).not.toBeNull();
    expect(
      fixture.nativeElement.querySelector('a[href="/remediation?status=pending_approval"]'),
    ).not.toBeNull();
  });

  it('announces initial loading without rendering fake totals', async () => {
    const open = new Subject<IncidentListResponse>();
    const acknowledged = new Subject<IncidentListResponse>();
    const critical = new Subject<IncidentListResponse>();
    const pending = new Subject<RemediationProposalListResponse>();
    listIncidents
      .mockReset()
      .mockReturnValueOnce(open)
      .mockReturnValueOnce(acknowledged)
      .mockReturnValueOnce(critical);
    listProposals.mockReturnValue(pending);
    await configure();
    const fixture = create();

    expect(fixture.componentInstance.loading()).toBe(true);
    expect(fixture.nativeElement.querySelector('.overview-page')?.getAttribute('aria-busy')).toBe(
      'true',
    );
    expect(fixture.nativeElement.querySelector('[role="status"]')?.textContent).toContain(
      'Loading operations overview…',
    );
    expect(fixture.nativeElement.querySelectorAll('.metric-value')).toHaveLength(0);

    open.next(incidentResponse());
    open.complete();
    acknowledged.next(incidentResponse());
    acknowledged.complete();
    critical.next(incidentResponse());
    critical.complete();
    pending.next(proposalResponse());
    pending.complete();
    fixture.detectChanges();

    expect(fixture.componentInstance.loading()).toBe(false);
    expect(fixture.nativeElement.querySelectorAll('.metric-value')).toHaveLength(4);
  });

  it('shows a safe aggregate error and retries all four reads', async () => {
    listIncidents.mockReset().mockReturnValueOnce(throwError(() => new Error('private detail')));
    await configure();
    const fixture = create();

    expect(fixture.nativeElement.querySelector('[role="alert"]')?.textContent).toContain(
      'Operations overview could not be loaded.',
    );
    expect(fixture.nativeElement.textContent).not.toContain('private detail');

    listIncidents
      .mockReturnValueOnce(of(incidentResponse()))
      .mockReturnValueOnce(of(incidentResponse()))
      .mockReturnValueOnce(of(incidentResponse()));
    listProposals.mockReturnValue(of(proposalResponse()));
    (
      fixture.nativeElement.querySelector('.error-state button') as HTMLButtonElement | null
    )?.click();
    fixture.detectChanges();

    expect(listIncidents).toHaveBeenCalledTimes(6);
    expect(listProposals).toHaveBeenCalledTimes(2);
    expect(fixture.nativeElement.querySelector('[role="alert"]')).toBeNull();
    expect(fixture.nativeElement.querySelectorAll('.metric-value')).toHaveLength(4);
  });

  it('preserves the snapshot during refresh and prevents duplicate refresh submissions', async () => {
    await configure();
    const fixture = create();
    const open = new Subject<IncidentListResponse>();
    const acknowledged = new Subject<IncidentListResponse>();
    const critical = new Subject<IncidentListResponse>();
    const pending = new Subject<RemediationProposalListResponse>();
    listIncidents
      .mockReturnValueOnce(open)
      .mockReturnValueOnce(acknowledged)
      .mockReturnValueOnce(critical);
    listProposals.mockReturnValueOnce(pending);

    fixture.componentInstance.refresh();
    fixture.detectChanges();
    fixture.componentInstance.refresh();
    fixture.detectChanges();

    expect(listIncidents).toHaveBeenCalledTimes(6);
    expect(listProposals).toHaveBeenCalledTimes(2);
    expect(fixture.nativeElement.querySelector('[role="status"]')?.textContent).toContain(
      'Refreshing operations overview…',
    );
    expect(fixture.nativeElement.querySelectorAll('.metric-value')).toHaveLength(4);
    expect(
      (fixture.nativeElement.querySelector('.page-heading button') as HTMLButtonElement | null)
        ?.disabled,
    ).toBe(true);

    open.next(incidentResponse([], 41));
    open.complete();
    acknowledged.next(incidentResponse([], 22));
    acknowledged.complete();
    critical.next(incidentResponse([], 7));
    critical.complete();
    pending.next(proposalResponse([], 13));
    pending.complete();
    fixture.detectChanges();

    expect(
      [...fixture.nativeElement.querySelectorAll('.metric-value')].map((value: Element) =>
        value.textContent?.trim(),
      ),
    ).toEqual(['41', '22', '7', '13']);
  });
});
