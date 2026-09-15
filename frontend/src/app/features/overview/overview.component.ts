import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { RouterLink } from '@angular/router';
import { catchError, EMPTY, forkJoin, map, startWith, Subject, switchMap, tap } from 'rxjs';
import { IncidentListQuery } from '../incidents/incident-list-query';
import { Incident, IncidentSeverity, IncidentStatus } from '../incidents/incident.models';
import { IncidentsApiService } from '../incidents/incidents-api.service';
import { RemediationApiService } from '../remediation/remediation-api.service';
import { RemediationListQuery } from '../remediation/remediation-list-query';
import { RemediationActionKind, RemediationProposal } from '../remediation/remediation.models';

export interface OverviewSnapshot {
  openIncidentsTotal: number;
  acknowledgedIncidentsTotal: number;
  criticalOpenIncidentsTotal: number;
  pendingRemediationTotal: number;
  recentOpenIncidents: Incident[];
  pendingRemediationProposals: RemediationProposal[];
}

export const OPEN_INCIDENTS_QUERY: IncidentListQuery = {
  status: 'open',
  severity: null,
  source: null,
  occurredFrom: null,
  occurredTo: null,
  limit: 10,
  offset: 0,
};

export const ACKNOWLEDGED_INCIDENTS_QUERY: IncidentListQuery = {
  status: 'acknowledged',
  severity: null,
  source: null,
  occurredFrom: null,
  occurredTo: null,
  limit: 10,
  offset: 0,
};

export const CRITICAL_OPEN_INCIDENTS_QUERY: IncidentListQuery = {
  status: 'open',
  severity: 'critical',
  source: null,
  occurredFrom: null,
  occurredTo: null,
  limit: 10,
  offset: 0,
};

export const PENDING_REMEDIATION_QUERY: RemediationListQuery = {
  incidentId: null,
  status: 'pending_approval',
  actionKind: null,
  target: null,
  proposedByUserId: null,
  createdFrom: null,
  createdTo: null,
  limit: 10,
  offset: 0,
};

@Component({
  selector: 'app-overview',
  imports: [DatePipe, MatButtonModule, MatProgressBarModule, RouterLink],
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OverviewComponent {
  private readonly incidentsApi = inject(IncidentsApiService);
  private readonly remediationApi = inject(RemediationApiService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly loadRequest = new Subject<void>();

  readonly snapshot = signal<OverviewSnapshot | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal(false);

  constructor() {
    this.loadRequest
      .pipe(
        startWith(undefined),
        tap(() => {
          this.loading.set(true);
          this.loadError.set(false);
        }),
        switchMap(() =>
          forkJoin({
            open: this.incidentsApi.listIncidents(OPEN_INCIDENTS_QUERY),
            acknowledged: this.incidentsApi.listIncidents(ACKNOWLEDGED_INCIDENTS_QUERY),
            criticalOpen: this.incidentsApi.listIncidents(CRITICAL_OPEN_INCIDENTS_QUERY),
            pending: this.remediationApi.listProposals(PENDING_REMEDIATION_QUERY),
          }).pipe(
            map(({ open, acknowledged, criticalOpen, pending }): OverviewSnapshot => ({
              openIncidentsTotal: open.total,
              acknowledgedIncidentsTotal: acknowledged.total,
              criticalOpenIncidentsTotal: criticalOpen.total,
              pendingRemediationTotal: pending.total,
              recentOpenIncidents: open.items.slice(0, 5),
              pendingRemediationProposals: pending.items.slice(0, 5),
            })),
            tap({
              next: (snapshot) => {
                this.snapshot.set(snapshot);
                this.loading.set(false);
              },
              error: () => {
                this.snapshot.set(null);
                this.loading.set(false);
                this.loadError.set(true);
              },
            }),
            catchError(() => EMPTY),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe();
  }

  refresh(): void {
    if (!this.loading()) this.loadRequest.next();
  }

  severityLabel(value: IncidentSeverity): string {
    return { low: 'Low', medium: 'Medium', high: 'High', critical: 'Critical' }[value];
  }

  statusLabel(value: IncidentStatus): string {
    return { open: 'Open', acknowledged: 'Acknowledged', resolved: 'Resolved' }[value];
  }

  actionLabel(value: RemediationActionKind): string {
    return { restart_service: 'Restart service' }[value];
  }

  compactId(id: string): string {
    return `${id.slice(0, 8)}…${id.slice(-4)}`;
  }
}
