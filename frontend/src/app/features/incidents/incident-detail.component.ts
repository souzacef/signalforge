import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  computed,
  inject,
  signal,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { catchError, EMPTY, finalize, merge, startWith, Subject, switchMap, tap } from 'rxjs';
import { AuthService } from '../../core/auth/auth.service';
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
type MutationState = 'idle' | 'acknowledging' | 'resolving';
type RefreshReason = 'route' | 'manual' | 'conflict';
type LifecycleFeedback = {
  kind: 'success' | 'notice' | 'error';
  message: string;
};

@Component({
  selector: 'app-incident-detail',
  imports: [DatePipe, MatButtonModule, MatProgressBarModule, RouterLink],
  templateUrl: './incident-detail.component.html',
  styleUrl: './incident-detail.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class IncidentDetailComponent {
  private readonly api = inject(IncidentsApiService);
  private readonly auth = inject(AuthService);
  readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refreshRequest = new Subject<RefreshReason>();
  private activeIncidentId: string | null = null;
  private routeGeneration = 0;

  readonly incident = signal<Incident | null>(null);
  readonly incidentState = signal<IncidentState>('loading');
  readonly triage = signal<IncidentTriage | null>(null);
  readonly triageState = signal<TriageState>('loading');
  readonly enrichment = signal<EnrichmentListResponse | null>(null);
  readonly enrichmentState = signal<EnrichmentState>('loading');
  readonly mutationState = signal<MutationState>('idle');
  readonly resolveConfirmationOpen = signal(false);
  readonly lifecycleFeedback = signal<LifecycleFeedback | null>(null);
  readonly canManageLifecycle = computed(() => {
    const role = this.auth.currentUser()?.role;
    return role === 'operator' || role === 'admin';
  });

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
        tap((params) => this.resetLifecycleForRoute(params.get('incidentId'))),
        switchMap((params) =>
          this.refreshRequest.pipe(
            startWith<RefreshReason>('route'),
            switchMap((reason) => this.load(params.get('incidentId'), reason)),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe();
  }

  refresh(): void {
    if (this.mutationState() !== 'idle') return;
    this.resolveConfirmationOpen.set(false);
    this.lifecycleFeedback.set(null);
    this.refreshRequest.next('manual');
  }

  acknowledge(): void {
    const item = this.incident();
    if (
      !item ||
      item.id !== this.activeIncidentId ||
      item.status !== 'open' ||
      !this.canManageLifecycle() ||
      this.mutationState() !== 'idle'
    )
      return;

    this.runMutation('acknowledging', item.id);
  }

  showResolveConfirmation(): void {
    const item = this.incident();
    if (
      !item ||
      item.id !== this.activeIncidentId ||
      item.status !== 'acknowledged' ||
      !this.canManageLifecycle() ||
      this.mutationState() !== 'idle'
    )
      return;

    this.lifecycleFeedback.set(null);
    this.resolveConfirmationOpen.set(true);
  }

  cancelResolve(): void {
    if (this.mutationState() === 'idle') this.resolveConfirmationOpen.set(false);
  }

  confirmResolve(): void {
    const item = this.incident();
    if (
      !this.resolveConfirmationOpen() ||
      !item ||
      item.id !== this.activeIncidentId ||
      item.status !== 'acknowledged' ||
      !this.canManageLifecycle() ||
      this.mutationState() !== 'idle'
    )
      return;

    this.runMutation('resolving', item.id);
  }

  private load(incidentId: string | null, reason: RefreshReason) {
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
        if (this.activeIncidentId !== incidentId) return;
        this.incident.set(incident);
        this.incidentState.set('ready');
        this.resolveConfirmationOpen.set(false);
        if (reason === 'conflict') {
          this.lifecycleFeedback.set({
            kind: 'notice',
            message:
              'Incident state changed before this action completed. The latest state has been loaded.',
          });
        }
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
        if (this.activeIncidentId !== incidentId) return EMPTY;
        this.incidentState.set(this.isNotFound(error) ? 'not-found' : 'error');
        return EMPTY;
      }),
    );
  }

  private runMutation(state: Exclude<MutationState, 'idle'>, incidentId: string): void {
    const generation = this.routeGeneration;
    this.mutationState.set(state);
    this.lifecycleFeedback.set(null);
    const request =
      state === 'acknowledging'
        ? this.api.acknowledgeIncident(incidentId)
        : this.api.resolveIncident(incidentId);

    request
      .pipe(
        finalize(() => {
          if (
            this.activeIncidentId === incidentId &&
            this.routeGeneration === generation &&
            this.mutationState() === state
          ) {
            this.mutationState.set('idle');
          }
        }),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe({
        next: (incident) => {
          if (
            this.activeIncidentId !== incidentId ||
            this.routeGeneration !== generation ||
            incident.id !== incidentId
          )
            return;
          this.incident.set(incident);
          this.incidentState.set('ready');
          this.resolveConfirmationOpen.set(false);
          this.lifecycleFeedback.set({
            kind: 'success',
            message: state === 'acknowledging' ? 'Incident acknowledged.' : 'Incident resolved.',
          });
        },
        error: (error: unknown) => this.handleMutationError(error, state, incidentId, generation),
      });
  }

  private handleMutationError(
    error: unknown,
    state: Exclude<MutationState, 'idle'>,
    incidentId: string,
    generation: number,
  ): void {
    if (this.activeIncidentId !== incidentId || this.routeGeneration !== generation) return;

    this.resolveConfirmationOpen.set(false);
    if (error instanceof HttpErrorResponse && error.status === 409) {
      this.lifecycleFeedback.set({
        kind: 'notice',
        message: 'Incident state changed before this action completed. Loading the latest state.',
      });
      this.refreshRequest.next('conflict');
      return;
    }
    if (error instanceof HttpErrorResponse && error.status === 403) {
      this.lifecycleFeedback.set({
        kind: 'error',
        message: 'You do not have permission to change this incident.',
      });
      return;
    }
    if (error instanceof HttpErrorResponse && error.status === 404) {
      this.incident.set(null);
      this.incidentState.set('not-found');
      this.lifecycleFeedback.set(null);
      return;
    }

    this.lifecycleFeedback.set({
      kind: 'error',
      message:
        state === 'acknowledging'
          ? 'Incident could not be acknowledged. Try again.'
          : 'Incident could not be resolved. Try again.',
    });
  }

  private resetLifecycleForRoute(incidentId: string | null): void {
    this.routeGeneration += 1;
    this.activeIncidentId = isIncidentId(incidentId) ? incidentId : null;
    this.mutationState.set('idle');
    this.resolveConfirmationOpen.set(false);
    this.lifecycleFeedback.set(null);
  }

  private isNotFound(error: unknown): boolean {
    return error instanceof HttpErrorResponse && error.status === 404;
  }
}
