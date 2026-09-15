import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { catchError, EMPTY, merge, startWith, Subject, switchMap, tap } from 'rxjs';
import { isIncidentId } from './incident-id';
import {
  EnrichmentCategory,
  EnrichmentListResponse,
  Incident,
  IncidentSeverity,
  IncidentStatus,
  IncidentTriage,
  TriagePriority,
} from './incident.models';
import { IncidentsApiService } from './incidents-api.service';

type IncidentState = 'loading' | 'ready' | 'invalid' | 'not-found' | 'error';
type TriageState = 'loading' | 'ready' | 'missing' | 'error';
type EnrichmentState = 'loading' | 'ready' | 'empty' | 'error';

@Component({
  selector: 'app-incident-detail',
  imports: [DatePipe, MatButtonModule, MatProgressBarModule, RouterLink],
  templateUrl: './incident-detail.component.html',
  styleUrl: './incident-detail.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class IncidentDetailComponent {
  private readonly api = inject(IncidentsApiService);
  readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refreshRequest = new Subject<void>();

  readonly incident = signal<Incident | null>(null);
  readonly incidentState = signal<IncidentState>('loading');
  readonly triage = signal<IncidentTriage | null>(null);
  readonly triageState = signal<TriageState>('loading');
  readonly enrichment = signal<EnrichmentListResponse | null>(null);
  readonly enrichmentState = signal<EnrichmentState>('loading');

  readonly priorityLabels: Record<TriagePriority, string> = {
    P1: 'P1 · Immediate',
    P2: 'P2 · High',
    P3: 'P3 · Moderate',
    P4: 'P4 · Low',
  };
  readonly categoryLabels: Record<EnrichmentCategory, string> = {
    availability: 'Availability',
    performance: 'Performance',
    security: 'Security',
    capacity: 'Capacity',
    dependency: 'Dependency',
    deployment: 'Deployment',
    data: 'Data',
    unknown: 'Unknown',
  };
  readonly severityLabels: Record<IncidentSeverity, string> = {
    low: 'Low',
    medium: 'Medium',
    high: 'High',
    critical: 'Critical',
  };
  readonly statusLabels: Record<IncidentStatus, string> = {
    open: 'Open',
    acknowledged: 'Acknowledged',
    resolved: 'Resolved',
  };

  constructor() {
    this.route.paramMap
      .pipe(
        switchMap((params) =>
          this.refreshRequest.pipe(
            startWith(undefined),
            switchMap(() => this.load(params.get('incidentId'))),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe();
  }

  refresh(): void {
    this.refreshRequest.next();
  }

  private load(incidentId: string | null) {
    this.incident.set(null);
    this.triage.set(null);
    this.enrichment.set(null);
    this.triageState.set('loading');
    this.enrichmentState.set('loading');
    if (!isIncidentId(incidentId)) {
      this.incidentState.set('invalid');
      return EMPTY;
    }

    this.incidentState.set('loading');
    return this.api.getIncident(incidentId).pipe(
      tap((incident) => {
        this.incident.set(incident);
        this.incidentState.set('ready');
      }),
      switchMap(() =>
        merge(
          this.api.getIncidentTriage(incidentId).pipe(
            tap((triage) => {
              this.triage.set(triage);
              this.triageState.set('ready');
            }),
            catchError((error: unknown) => {
              this.triageState.set(this.isNotFound(error) ? 'missing' : 'error');
              return EMPTY;
            }),
          ),
          this.api.getLatestIncidentEnrichment(incidentId).pipe(
            tap((response) => {
              this.enrichment.set(response);
              this.enrichmentState.set(response.items.length ? 'ready' : 'empty');
            }),
            catchError(() => {
              this.enrichmentState.set('error');
              return EMPTY;
            }),
          ),
        ),
      ),
      catchError((error: unknown) => {
        this.incidentState.set(this.isNotFound(error) ? 'not-found' : 'error');
        return EMPTY;
      }),
    );
  }

  private isNotFound(error: unknown): boolean {
    return error instanceof HttpErrorResponse && error.status === 404;
  }
}
