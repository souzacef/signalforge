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
import { ActivatedRoute, Router } from '@angular/router';
import { catchError, EMPTY, startWith, Subject, switchMap, tap } from 'rxjs';
import {
  DEFAULT_INCIDENT_LIST_QUERY,
  INCIDENT_PAGE_SIZES,
  IncidentListQuery,
  IncidentPageSize,
  incidentQueryToParams,
  isoToLocalDateTime,
  localDateTimeToIso,
  parseIncidentListQuery,
} from './incident-list-query';
import { IncidentListResponse, IncidentSeverity, IncidentStatus } from './incident.models';
import { IncidentsApiService } from './incidents-api.service';

@Component({
  selector: 'app-incidents',
  imports: [
    DatePipe,
    MatButtonModule,
    MatFormFieldModule,
    MatInputModule,
    MatPaginatorModule,
    MatProgressBarModule,
    MatSelectModule,
    MatTableModule,
    ReactiveFormsModule,
  ],
  templateUrl: './incidents.component.html',
  styleUrl: './incidents.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class IncidentsComponent {
  private readonly api = inject(IncidentsApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly retryRequest = new Subject<void>();

  readonly statuses: ReadonlyArray<{ value: IncidentStatus; label: string }> = [
    { value: 'open', label: 'Open' },
    { value: 'acknowledged', label: 'Acknowledged' },
    { value: 'resolved', label: 'Resolved' },
  ];
  readonly severities: ReadonlyArray<{ value: IncidentSeverity; label: string }> = [
    { value: 'low', label: 'Low' },
    { value: 'medium', label: 'Medium' },
    { value: 'high', label: 'High' },
    { value: 'critical', label: 'Critical' },
  ];
  readonly displayedColumns = ['severity', 'incident', 'source', 'status', 'occurred'];
  readonly pageSizes = INCIDENT_PAGE_SIZES;
  readonly query = signal<IncidentListQuery>(DEFAULT_INCIDENT_LIST_QUERY);
  readonly response = signal<IncidentListResponse | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal(false);
  readonly dateRangeError = signal<string | null>(null);

  readonly filterForm = new FormGroup({
    status: new FormControl<IncidentStatus | null>(null),
    severity: new FormControl<IncidentSeverity | null>(null),
    source: new FormControl('', { nonNullable: true }),
    occurredFrom: new FormControl('', { nonNullable: true }),
    occurredTo: new FormControl('', { nonNullable: true }),
  });

  constructor() {
    this.route.queryParamMap
      .pipe(
        switchMap((params) => {
          const parsed = parseIncidentListQuery(params);
          this.query.set(parsed.query);
          this.populateFilterForm(parsed.query);
          this.dateRangeError.set(null);

          if (!parsed.isCanonical) {
            this.loading.set(true);
            this.response.set(null);
            this.loadError.set(false);
            void this.router.navigate([], {
              relativeTo: this.route,
              queryParams: parsed.canonicalParams,
              replaceUrl: true,
            });
            return EMPTY;
          }

          return this.retryRequest.pipe(
            startWith(undefined),
            tap(() => {
              this.loading.set(true);
              this.response.set(null);
              this.loadError.set(false);
            }),
            switchMap(() =>
              this.api.listIncidents(parsed.query).pipe(
                tap({
                  next: (response) => this.acceptResponse(parsed.query, response),
                  error: () => {
                    this.loading.set(false);
                    this.loadError.set(true);
                  },
                }),
                catchError(() => EMPTY),
              ),
            ),
          );
        }),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe();
  }

  applyFilters(): void {
    const form = this.filterForm.getRawValue();
    const occurredFrom = form.occurredFrom ? localDateTimeToIso(form.occurredFrom) : null;
    const occurredTo = form.occurredTo ? localDateTimeToIso(form.occurredTo) : null;

    if ((form.occurredFrom && occurredFrom === null) || (form.occurredTo && occurredTo === null)) {
      this.dateRangeError.set('Enter valid occurred dates and times.');
      return;
    }
    if (
      occurredFrom !== null &&
      occurredTo !== null &&
      Date.parse(occurredFrom) > Date.parse(occurredTo)
    ) {
      this.dateRangeError.set('Occurred from must be before or the same as occurred to.');
      return;
    }

    this.dateRangeError.set(null);
    this.navigate({
      status: form.status,
      severity: form.severity,
      source: form.source.trim() || null,
      occurredFrom,
      occurredTo,
      limit: this.query().limit,
      offset: 0,
    });
  }

  clearFilters(): void {
    this.dateRangeError.set(null);
    this.filterForm.reset({
      status: null,
      severity: null,
      source: '',
      occurredFrom: '',
      occurredTo: '',
    });
    this.navigate({ ...DEFAULT_INCIDENT_LIST_QUERY, limit: this.query().limit });
  }

  changePage(event: PageEvent): void {
    if (!INCIDENT_PAGE_SIZES.includes(event.pageSize as IncidentPageSize)) return;
    this.navigate({
      ...this.query(),
      limit: event.pageSize as IncidentPageSize,
      offset: event.pageIndex * event.pageSize,
    });
  }

  retry(): void {
    this.retryRequest.next();
  }

  hasActiveFilters(): boolean {
    const query = this.query();
    return (
      query.status !== null ||
      query.severity !== null ||
      query.source !== null ||
      query.occurredFrom !== null ||
      query.occurredTo !== null
    );
  }

  rangeSummary(response: IncidentListResponse): string {
    if (response.total === 0) return '0 incidents';
    return `${response.offset + 1}–${response.offset + response.items.length} of ${response.total} incidents`;
  }

  severityLabel(value: IncidentSeverity): string {
    return this.severities.find((severity) => severity.value === value)?.label ?? value;
  }

  statusLabel(value: IncidentStatus): string {
    return this.statuses.find((status) => status.value === value)?.label ?? value;
  }

  private populateFilterForm(query: IncidentListQuery): void {
    this.filterForm.setValue(
      {
        status: query.status,
        severity: query.severity,
        source: query.source ?? '',
        occurredFrom: isoToLocalDateTime(query.occurredFrom),
        occurredTo: isoToLocalDateTime(query.occurredTo),
      },
      { emitEvent: false },
    );
  }

  private acceptResponse(query: IncidentListQuery, response: IncidentListResponse): void {
    if (query.offset > 0 && (response.total === 0 || query.offset >= response.total)) {
      const lastOffset =
        response.total === 0 ? 0 : Math.floor((response.total - 1) / query.limit) * query.limit;
      void this.router.navigate([], {
        relativeTo: this.route,
        queryParams: incidentQueryToParams({ ...query, offset: lastOffset }),
        replaceUrl: true,
      });
      return;
    }
    this.response.set(response);
    this.loading.set(false);
  }

  private navigate(query: IncidentListQuery): void {
    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: incidentQueryToParams(query),
    });
  }
}
