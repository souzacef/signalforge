import { DatePipe } from '@angular/common';
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
import { RemediationApiService } from './remediation-api.service';
import {
  DEFAULT_REMEDIATION_LIST_QUERY, isoToLocalDateTime, isRemediationUuid,
  localDateTimeToIso, parseRemediationListQuery, REMEDIATION_PAGE_SIZES,
  RemediationListQuery, RemediationPageSize, remediationQueryToParams,
} from './remediation-list-query';
import { RemediationActionKind, RemediationProposalListResponse, RemediationProposalStatus } from './remediation.models';

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
