import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { catchError, EMPTY, startWith, Subject, switchMap, tap } from 'rxjs';
import { RemediationApiService } from './remediation-api.service';
import { isRemediationUuid, parseRemediationListQuery } from './remediation-list-query';
import {
  RemediationActionKind, RemediationExecution, RemediationExecutionStatus,
  RemediationProposal, RemediationProposalStatus,
} from './remediation.models';

type ProposalState = 'loading' | 'ready' | 'invalid' | 'not-found' | 'error';
type ExecutionState = 'not-applicable' | 'loading' | 'ready' | 'missing' | 'error';

@Component({
  selector: 'app-remediation-detail',
  imports: [DatePipe, MatButtonModule, MatProgressBarModule, RouterLink],
  templateUrl: './remediation-detail.component.html',
  styleUrl: './remediation-detail.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RemediationDetailComponent {
  private readonly api = inject(RemediationApiService);
  readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refreshRequest = new Subject<void>();

  readonly proposal = signal<RemediationProposal | null>(null);
  readonly proposalState = signal<ProposalState>('loading');
  readonly execution = signal<RemediationExecution | null>(null);
  readonly executionState = signal<ExecutionState>('not-applicable');

  readonly statusLabels: Record<RemediationProposalStatus, string> = {
    pending_approval: 'Pending approval', approved: 'Approved', rejected: 'Rejected',
  };
  readonly executionLabels: Record<RemediationExecutionStatus, string> = {
    requested: 'Requested', in_progress: 'In progress', succeeded: 'Succeeded',
    failed: 'Failed', outcome_unknown: 'Outcome unknown',
  };
  readonly actionLabels: Record<RemediationActionKind, string> = {
    restart_service: 'Restart service',
  };

  constructor() {
    this.route.paramMap.pipe(
      switchMap((params) => this.refreshRequest.pipe(
        startWith(undefined),
        switchMap(() => this.load(params.get('proposalId'))),
      )),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe();
  }

  refresh(): void { this.refreshRequest.next(); }
  backQueryParams() { return parseRemediationListQuery(this.route.snapshot.queryParamMap).canonicalParams; }

  private load(id: string | null) {
    this.proposal.set(null);
    this.execution.set(null);
    this.executionState.set('not-applicable');
    if (!isRemediationUuid(id)) {
      this.proposalState.set('invalid');
      return EMPTY;
    }
    this.proposalState.set('loading');
    return this.api.getProposal(id).pipe(
      tap((proposal) => {
        this.proposal.set(proposal);
        this.proposalState.set('ready');
        this.executionState.set(proposal.status === 'approved' ? 'loading' : 'not-applicable');
      }),
      switchMap((proposal) => {
        if (proposal.status !== 'approved') return EMPTY;
        return this.api.getExecution(id).pipe(
          tap((execution) => {
            this.execution.set(execution);
            this.executionState.set('ready');
          }),
          catchError((error: unknown) => {
            this.executionState.set(error instanceof HttpErrorResponse && error.status === 404
              ? 'missing' : 'error');
            return EMPTY;
          }),
        );
      }),
      catchError((error: unknown) => {
        this.proposalState.set(error instanceof HttpErrorResponse && error.status === 404
          ? 'not-found' : 'error');
        return EMPTY;
      }),
    );
  }
}
