import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatPaginatorModule, PageEvent } from '@angular/material/paginator';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatSelectModule } from '@angular/material/select';
import { MatTableModule } from '@angular/material/table';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { catchError, EMPTY, startWith, Subject, switchMap, tap } from 'rxjs';
import { AuthService } from '../../core/auth/auth.service';
import { RemediationApiService } from './remediation-api.service';
import { isServiceTarget, knownRemediationDetail, TARGET_CONTROL_PATTERN } from './remediation-mutation';
import {
  DEFAULT_REMEDIATION_LIST_QUERY, isoToLocalDateTime, isRemediationUuid,
  localDateTimeToIso, parseRemediationListQuery, REMEDIATION_PAGE_SIZES,
  RemediationListQuery, RemediationPageSize, remediationQueryToParams,
} from './remediation-list-query';
import { RemediationActionKind, RemediationProposalCreateRequest, RemediationProposalListResponse, RemediationProposalStatus } from './remediation.models';

@Component({
  selector: 'app-remediation',
  imports: [DatePipe, MatButtonModule, MatFormFieldModule, MatInputModule, MatPaginatorModule,
    MatProgressBarModule, MatSelectModule, MatTableModule, ReactiveFormsModule, RouterLink],
  templateUrl: './remediation.component.html',
  styleUrl: './remediation.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RemediationComponent {
  private readonly api = inject(RemediationApiService);
  private readonly auth = inject(AuthService);
  readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly retryRequest = new Subject<void>();

  readonly statuses: ReadonlyArray<{ value: RemediationProposalStatus; label: string }> = [
    { value: 'pending_approval', label: 'Pending approval' },
    { value: 'approved', label: 'Approved' },
    { value: 'rejected', label: 'Rejected' },
  ];
  readonly actions: ReadonlyArray<{ value: RemediationActionKind; label: string }> = [
    { value: 'restart_service', label: 'Restart service' },
  ];
  readonly displayedColumns = ['status', 'action', 'target', 'incident', 'proposer', 'created'];
  readonly pageSizes = REMEDIATION_PAGE_SIZES;
  readonly query = signal<RemediationListQuery>(DEFAULT_REMEDIATION_LIST_QUERY);
  readonly response = signal<RemediationProposalListResponse | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal(false);
  readonly incidentIdError = signal<string | null>(null);
  readonly proposedByUserIdError = signal<string | null>(null);
  readonly targetError = signal<string | null>(null);
  readonly dateRangeError = signal<string | null>(null);
  readonly createOpen = signal(false);
  readonly createState = signal<'idle' | 'creating'>('idle');
  readonly createIncidentError = signal<string | null>(null);
  readonly createTargetError = signal<string | null>(null);
  readonly createReasonError = signal<string | null>(null);
  readonly createFeedback = signal<string | null>(null);
  readonly createForm = new FormGroup({
    incidentId: new FormControl('', { nonNullable: true }),
    target: new FormControl('', { nonNullable: true }),
    reason: new FormControl('', { nonNullable: true }),
  });

  readonly filterForm = new FormGroup({
    status: new FormControl<RemediationProposalStatus | null>(null),
    actionKind: new FormControl<RemediationActionKind | null>(null),
    target: new FormControl('', { nonNullable: true }),
    incidentId: new FormControl('', { nonNullable: true }),
    proposedByUserId: new FormControl('', { nonNullable: true }),
    createdFrom: new FormControl('', { nonNullable: true }),
    createdTo: new FormControl('', { nonNullable: true }),
  });

  constructor() {
    this.route.queryParamMap.pipe(
      switchMap((params) => {
        const parsed = parseRemediationListQuery(params);
        this.query.set(parsed.query);
        this.populateForm(parsed.query);
        this.clearErrors();
        if (!parsed.isCanonical) {
          this.loading.set(true);
          this.response.set(null);
          this.loadError.set(false);
          void this.router.navigate([], {
            relativeTo: this.route, queryParams: parsed.canonicalParams, replaceUrl: true,
          });
          return EMPTY;
        }
        return this.retryRequest.pipe(
          startWith(undefined),
          tap(() => {
            this.loading.set(true); this.response.set(null); this.loadError.set(false);
          }),
          switchMap(() => this.api.listProposals(parsed.query).pipe(
            tap({
              next: (response) => this.acceptResponse(parsed.query, response),
              error: () => { this.loading.set(false); this.loadError.set(true); },
            }),
            catchError(() => EMPTY),
          )),
        );
      }),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe();
  }

  canCreate(): boolean {
    const role = this.auth.currentUser()?.role;
    return role === 'operator' || role === 'admin';
  }

  openCreate(): void {
    if (this.canCreate() && this.createState() === 'idle') this.createOpen.set(true);
  }

  cancelCreate(): void {
    if (this.createState() !== 'idle') return;
    this.createOpen.set(false);
    this.createForm.reset({ incidentId: '', target: '', reason: '' });
    this.clearCreateFeedback();
  }

  submitCreate(): void {
    if (!this.canCreate() || !this.createOpen() || this.createState() !== 'idle') return;
    this.clearCreateFeedback();
    const raw = this.createForm.getRawValue();
    const incidentId = raw.incidentId.trim();
    const target = raw.target.trim();
    const reason = raw.reason.trim();
    if (!isRemediationUuid(incidentId)) this.createIncidentError.set('Enter a valid Incident ID (UUID).');
    if (!target) this.createTargetError.set('Enter a service target.');
    else if (target.length > 100) this.createTargetError.set('Target must be 100 characters or fewer.');
    else if (TARGET_CONTROL_PATTERN.test(raw.target) || !isServiceTarget(target)) this.createTargetError.set('Use lowercase letters, digits, dots, underscores or hyphens; start and end with a letter or digit.');
    if (!reason) this.createReasonError.set('Enter a reason.');
    else if (reason.length > 1000) this.createReasonError.set('Reason must be 1000 characters or fewer.');
    if (this.createIncidentError() || this.createTargetError() || this.createReasonError()) return;
    const request: RemediationProposalCreateRequest = {
      incident_id: incidentId, action_kind: 'restart_service', target, reason,
    };
    this.createState.set('creating');
    this.api.createProposal(request).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (proposal) => {
        this.createState.set('idle');
        this.createOpen.set(false);
        void this.router.navigate(['/remediation', proposal.id], {
          queryParams: this.route.snapshot.queryParams,
        });
      },
      error: (error: unknown) => {
        this.createState.set('idle');
        this.createFeedback.set(this.createErrorMessage(error));
      },
    });
  }

  private createErrorMessage(error: unknown): string {
    if (!(error instanceof HttpErrorResponse)) return 'Remediation proposal could not be created. Try again.';
    if (error.status === 404) return 'Incident not found.';
    if (error.status === 403) return 'You do not have permission to create remediation proposals.';
    if (error.status === 409) {
      const detail = knownRemediationDetail(error);
      if (detail === 'Incident is not eligible for remediation')
        return 'This incident is not eligible for remediation.';
      if (detail === 'A matching pending remediation proposal already exists')
        return 'A matching pending remediation proposal already exists.';
      return 'Proposal could not be created because the current server state does not allow it.';
    }
    return 'Remediation proposal could not be created. Try again.';
  }

  private clearCreateFeedback(): void {
    this.createIncidentError.set(null); this.createTargetError.set(null);
    this.createReasonError.set(null); this.createFeedback.set(null);
  }

  applyFilters(): void {
    const form = this.filterForm.getRawValue();
    this.clearErrors();
    const incidentId = form.incidentId.trim();
    const proposedByUserId = form.proposedByUserId.trim();
    const target = form.target.trim();
    if (incidentId && !isRemediationUuid(incidentId))
      this.incidentIdError.set('Enter a valid Incident ID (UUID).');
    if (proposedByUserId && !isRemediationUuid(proposedByUserId))
      this.proposedByUserIdError.set('Enter a valid proposer user ID (UUID).');
    if (target.length > 100) this.targetError.set('Target must be 100 characters or fewer.');
    const createdFrom = form.createdFrom ? localDateTimeToIso(form.createdFrom) : null;
    const createdTo = form.createdTo ? localDateTimeToIso(form.createdTo) : null;
    if ((form.createdFrom && !createdFrom) || (form.createdTo && !createdTo))
      this.dateRangeError.set('Enter valid created dates and times.');
    else if (createdFrom && createdTo && Date.parse(createdFrom) > Date.parse(createdTo))
      this.dateRangeError.set('Created from must be before or the same as created to.');
    if (this.incidentIdError() || this.proposedByUserIdError() || this.targetError() || this.dateRangeError()) return;
    this.navigate({
      incidentId: incidentId.toLowerCase() || null, status: form.status,
      actionKind: form.actionKind, target: target || null,
      proposedByUserId: proposedByUserId.toLowerCase() || null,
      createdFrom, createdTo, limit: this.query().limit, offset: 0,
    });
  }

  clearFilters(): void {
    this.clearErrors();
    this.filterForm.reset({ status: null, actionKind: null, target: '', incidentId: '',
      proposedByUserId: '', createdFrom: '', createdTo: '' });
    this.navigate({ ...DEFAULT_REMEDIATION_LIST_QUERY, limit: this.query().limit });
  }

  changePage(event: PageEvent): void {
    if (!REMEDIATION_PAGE_SIZES.includes(event.pageSize as RemediationPageSize)) return;
    this.navigate({ ...this.query(), limit: event.pageSize as RemediationPageSize,
      offset: event.pageIndex * event.pageSize });
  }

  retry(): void { this.retryRequest.next(); }

  hasActiveFilters(): boolean {
    const q = this.query();
    return !!(q.incidentId || q.status || q.actionKind || q.target || q.proposedByUserId ||
      q.createdFrom || q.createdTo);
  }

  rangeSummary(response: RemediationProposalListResponse): string {
    if (response.total === 0) return '0 proposals';
    return `${response.offset + 1}–${response.offset + response.items.length} of ${response.total} proposals`;
  }

  statusLabel(status: RemediationProposalStatus): string {
    return this.statuses.find((item) => item.value === status)?.label ?? status;
  }

  actionLabel(action: RemediationActionKind): string {
    return this.actions.find((item) => item.value === action)?.label ?? action;
  }

  compactId(id: string): string { return `${id.slice(0, 8)}…${id.slice(-4)}`; }

  private populateForm(q: RemediationListQuery): void {
    this.filterForm.setValue({
      status: q.status, actionKind: q.actionKind, target: q.target ?? '',
      incidentId: q.incidentId ?? '', proposedByUserId: q.proposedByUserId ?? '',
      createdFrom: isoToLocalDateTime(q.createdFrom), createdTo: isoToLocalDateTime(q.createdTo),
    }, { emitEvent: false });
  }

  private clearErrors(): void {
    this.incidentIdError.set(null); this.proposedByUserIdError.set(null);
    this.targetError.set(null); this.dateRangeError.set(null);
  }

  private acceptResponse(q: RemediationListQuery, response: RemediationProposalListResponse): void {
    if (q.offset > 0 && (response.total === 0 || q.offset >= response.total)) {
      const offset = response.total === 0 ? 0 : Math.floor((response.total - 1) / q.limit) * q.limit;
      void this.router.navigate([], { relativeTo: this.route,
        queryParams: remediationQueryToParams({ ...q, offset }), replaceUrl: true });
      return;
    }
    this.response.set(response); this.loading.set(false);
  }

  private navigate(q: RemediationListQuery): void {
    void this.router.navigate([], { relativeTo: this.route, queryParams: remediationQueryToParams(q) });
  }
}
