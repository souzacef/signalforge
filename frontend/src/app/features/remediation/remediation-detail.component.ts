import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { catchError, EMPTY, startWith, Subject, switchMap, tap } from 'rxjs';
import { AuthService } from '../../core/auth/auth.service';
import { RemediationApiService } from './remediation-api.service';
import { isRemediationUuid, parseRemediationListQuery } from './remediation-list-query';
import { knownRemediationDetail } from './remediation-mutation';
import {
  RemediationActionKind, RemediationExecution, RemediationExecutionStatus,
  RemediationProposal, RemediationProposalStatus,
} from './remediation.models';

type ProposalState = 'loading' | 'ready' | 'invalid' | 'not-found' | 'error';
type ExecutionState = 'not-applicable' | 'loading' | 'ready' | 'missing' | 'error' | 'reconciling' | 'unconfirmed';
type ExecutionRequestMode = 'idle' | 'confirm';
type DecisionMode = 'idle' | 'confirm-approval' | 'reject';
type MutationState = 'idle' | 'approving' | 'rejecting' | 'requesting-execution';

@Component({
  selector: 'app-remediation-detail',
  imports: [DatePipe, MatButtonModule, MatFormFieldModule, MatInputModule,
    MatProgressBarModule, ReactiveFormsModule, RouterLink],
  templateUrl: './remediation-detail.component.html',
  styleUrl: './remediation-detail.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RemediationDetailComponent {
  private readonly api = inject(RemediationApiService);
  private readonly auth = inject(AuthService);
  readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refreshRequest = new Subject<void>();
  private generation = 0;
  private routeIdentity: string | null = null;

  readonly proposal = signal<RemediationProposal | null>(null);
  readonly proposalState = signal<ProposalState>('loading');
  readonly execution = signal<RemediationExecution | null>(null);
  readonly executionState = signal<ExecutionState>('not-applicable');
  readonly decisionMode = signal<DecisionMode>('idle');
  readonly mutationState = signal<MutationState>('idle');
  readonly decisionFeedback = signal<string | null>(null);
  readonly executionRequestMode = signal<ExecutionRequestMode>('idle');
  readonly executionFeedback = signal<string | null>(null);
  readonly rejectionError = signal<string | null>(null);
  readonly rejectionForm = new FormGroup({
    reason: new FormControl('', { nonNullable: true }),
  });

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

  canApprove(item: RemediationProposal): boolean {
    const user = this.auth.currentUser();
    return item.status === 'pending_approval' && user?.role === 'admin' &&
      user.id !== item.proposed_by_user_id;
  }

  canReject(item: RemediationProposal): boolean {
    const user = this.auth.currentUser();
    return item.status === 'pending_approval' && !!user &&
      (user.role === 'admin' ||
        (user.role === 'operator' && user.id === item.proposed_by_user_id));
  }

  canRequestExecution(item: RemediationProposal): boolean {
    return item.status === 'approved' && this.auth.currentUser()?.role === 'admin' &&
      this.executionState() === 'missing' && this.mutationState() === 'idle';
  }

  openExecutionRequest(): void {
    const item = this.proposal();
    if (!item || !this.canRequestExecution(item) || this.decisionMode() !== 'idle') return;
    this.executionRequestMode.set('confirm');
    this.executionFeedback.set(null);
  }

  cancelExecutionRequest(): void {
    if (this.mutationState() !== 'idle') return;
    this.executionRequestMode.set('idle');
  }

  confirmExecutionRequest(): void {
    const item = this.proposal();
    if (!item || !this.canRequestExecution(item) || this.executionRequestMode() !== 'confirm' ||
        this.decisionMode() !== 'idle') return;
    const generation = this.generation;
    this.mutationState.set('requesting-execution');
    this.executionFeedback.set(null);
    this.api.requestExecution(item.id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (record) => {
        if (!this.isCurrent(item.id, generation)) return;
        this.execution.set(record);
        this.executionState.set('ready');
        this.executionRequestMode.set('idle');
        this.mutationState.set('idle');
        this.executionFeedback.set('Execution request accepted. Execution is processed asynchronously. Use Refresh to check the latest durable status.');
      },
      error: (error: unknown) => {
        if (!this.isCurrent(item.id, generation)) return;
        this.executionRequestMode.set('idle');
        if (error instanceof HttpErrorResponse) {
          if (error.status === 404) {
            this.mutationState.set('idle');
            this.proposal.set(null);
            this.execution.set(null);
            this.executionState.set('not-applicable');
            this.proposalState.set('not-found');
            return;
          }
          if (error.status === 403) {
            this.mutationState.set('idle');
            this.executionFeedback.set('You do not have permission to request remediation execution.');
            return;
          }
          if (error.status === 409) {
            const detail = knownRemediationDetail(error);
            if (detail === 'Execution has already been requested for this remediation proposal') {
              this.reconcileExecution(item.id, generation, 'duplicate');
              return;
            }
            if (detail === 'Remediation proposal is not approved for execution') {
              this.mutationState.set('idle');
              this.executionState.set('unconfirmed');
              this.executionFeedback.set('Proposal state no longer allows an execution request. Loading the latest state.');
              this.refreshRequest.next();
              return;
            }
          }
        }
        this.reconcileExecution(item.id, generation, 'ambiguous');
      },
    });
  }

  private reconcileExecution(id: string, generation: number, reason: 'duplicate' | 'ambiguous'): void {
    this.executionState.set('reconciling');
    this.executionFeedback.set(reason === 'duplicate'
      ? 'Execution was already requested. Loading the current execution state.'
      : 'The execution request could not be confirmed. Checking durable state.');
    this.api.getExecution(id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (record) => {
        if (!this.isCurrent(id, generation)) return;
        this.execution.set(record);
        this.executionState.set('ready');
        this.mutationState.set('idle');
        if (reason === 'ambiguous') this.executionFeedback.set('An execution record exists for this proposal.');
      },
      error: (error: unknown) => {
        if (!this.isCurrent(id, generation)) return;
        this.mutationState.set('idle');
        if (error instanceof HttpErrorResponse && error.status === 404 && reason === 'ambiguous') {
          this.executionState.set('missing');
          this.executionFeedback.set('No durable execution record was found. You may request execution again.');
        } else {
          this.executionState.set('unconfirmed');
          this.executionFeedback.set(reason === 'duplicate' && error instanceof HttpErrorResponse && error.status === 404
            ? 'Execution was reported as already requested, but the current execution record could not be loaded. Refresh before taking further action.'
            : 'Execution request state could not be confirmed. Refresh before trying again.');
        }
      },
    });
  }

  isOwnPendingAdmin(item: RemediationProposal): boolean {
    const user = this.auth.currentUser();
    return item.status === 'pending_approval' && user?.role === 'admin' &&
      user.id === item.proposed_by_user_id;
  }

  isOtherPendingOperator(item: RemediationProposal): boolean {
    const user = this.auth.currentUser();
    return item.status === 'pending_approval' && user?.role === 'operator' &&
      user.id !== item.proposed_by_user_id;
  }

  openApproval(): void {
    const item = this.proposal();
    if (item && this.canApprove(item) && this.mutationState() === 'idle') {
      this.decisionMode.set('confirm-approval');
      this.decisionFeedback.set(null);
      this.rejectionError.set(null);
    }
  }

  openRejection(): void {
    const item = this.proposal();
    if (item && this.canReject(item) && this.mutationState() === 'idle') {
      this.decisionMode.set('reject');
      this.decisionFeedback.set(null);
      this.rejectionError.set(null);
    }
  }

  cancelDecision(): void {
    if (this.mutationState() !== 'idle') return;
    this.decisionMode.set('idle');
    this.rejectionForm.reset({ reason: '' });
    this.rejectionError.set(null);
  }

  confirmApproval(): void {
    const item = this.proposal();
    if (!item || !this.canApprove(item) || this.decisionMode() !== 'confirm-approval' ||
        this.mutationState() !== 'idle') return;
    const generation = this.generation;
    this.mutationState.set('approving');
    this.decisionFeedback.set(null);
    this.api.approveProposal(item.id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (returned) => {
        if (!this.isCurrent(item.id, generation)) return;
        this.mutationState.set('idle');
        this.decisionMode.set('idle');
        this.proposal.set(returned);
        this.decisionFeedback.set('Remediation proposal approved.');
        this.readExecution(item.id, generation);
      },
      error: (error: unknown) => {
        if (!this.isCurrent(item.id, generation)) return;
        this.mutationState.set('idle');
        this.handleDecisionError(error, 'approving');
      },
    });
  }

  confirmRejection(): void {
    const item = this.proposal();
    if (!item || !this.canReject(item) || this.decisionMode() !== 'reject' ||
        this.mutationState() !== 'idle') return;
    this.rejectionError.set(null);
    const reason = this.rejectionForm.controls.reason.value.trim();
    if (!reason) this.rejectionError.set('Enter a rejection reason.');
    else if (reason.length > 1000) this.rejectionError.set('Rejection reason must be 1000 characters or fewer.');
    if (this.rejectionError()) return;
    const generation = this.generation;
    this.mutationState.set('rejecting');
    this.decisionFeedback.set(null);
    this.api.rejectProposal(item.id, { rejection_reason: reason })
      .pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
        next: (returned) => {
          if (!this.isCurrent(item.id, generation)) return;
          this.mutationState.set('idle');
          this.decisionMode.set('idle');
          this.rejectionForm.reset({ reason: '' });
          this.proposal.set(returned);
          this.execution.set(null);
          this.executionState.set('not-applicable');
          this.decisionFeedback.set('Remediation proposal rejected.');
        },
        error: (error: unknown) => {
          if (!this.isCurrent(item.id, generation)) return;
          this.mutationState.set('idle');
          this.handleDecisionError(error, 'rejecting');
        },
      });
  }

  refresh(): void {
    if (this.mutationState() !== 'idle') return;
    this.decisionFeedback.set(null);
    this.executionFeedback.set(null);
    this.refreshRequest.next();
  }
  backQueryParams() { return parseRemediationListQuery(this.route.snapshot.queryParamMap).canonicalParams; }

  private isCurrent(id: string, generation: number): boolean {
    return generation === this.generation && this.route.snapshot.paramMap.get('proposalId') === id &&
      this.proposal()?.id === id;
  }

  private handleDecisionError(error: unknown, kind: 'approving' | 'rejecting'): void {
    if (error instanceof HttpErrorResponse) {
      if (error.status === 404) {
        this.proposal.set(null);
        this.execution.set(null);
        this.executionState.set('not-applicable');
        this.proposalState.set('not-found');
        this.decisionMode.set('idle');
        return;
      }
      if (error.status === 403) {
        this.decisionFeedback.set('You do not have permission to make this remediation decision.');
        return;
      }
      if (error.status === 409) {
        const detail = knownRemediationDetail(error);
        if (detail === 'Invalid remediation proposal state transition') {
          this.decisionFeedback.set('Proposal state changed before this decision completed. Loading the latest state.');
          this.decisionMode.set('idle');
          this.refreshRequest.next();
          return;
        }
        if (kind === 'approving' && detail === 'A proposer cannot approve their own remediation proposal') {
          this.decisionFeedback.set('You cannot approve a remediation proposal that you created.');
          return;
        }
      }
    }
    this.decisionFeedback.set(kind === 'approving'
      ? 'Remediation proposal could not be approved. Try again.'
      : 'Remediation proposal could not be rejected. Try again.');
  }

  private readExecution(id: string, generation: number): void {
    this.execution.set(null);
    this.executionState.set('loading');
    this.api.getExecution(id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (execution) => {
        if (!this.isCurrent(id, generation)) return;
        this.execution.set(execution);
        this.executionState.set('ready');
      },
      error: (error: unknown) => {
        if (!this.isCurrent(id, generation)) return;
        this.executionState.set(error instanceof HttpErrorResponse && error.status === 404
          ? 'missing' : 'error');
      },
    });
  }

  private load(id: string | null) {
    this.generation++;
    if (id !== this.routeIdentity) {
      this.decisionFeedback.set(null);
      this.executionFeedback.set(null);
    }
    this.routeIdentity = id;
    this.decisionMode.set('idle');
    this.executionRequestMode.set('idle');
    this.mutationState.set('idle');
    this.rejectionError.set(null);
    this.rejectionForm.reset({ reason: '' });
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
