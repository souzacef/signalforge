import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { Router } from '@angular/router';
import { finalize } from 'rxjs/operators';
import { AuthService } from '../../../core/auth/auth.service';

@Component({
  selector: 'app-login',
  imports: [
    MatButtonModule,
    MatFormFieldModule,
    MatInputModule,
    MatProgressSpinnerModule,
    ReactiveFormsModule,
  ],
  templateUrl: './login.component.html',
  styleUrl: './login.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LoginComponent {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  readonly form = new FormGroup({
    email: new FormControl('', {
      nonNullable: true,
      validators: [Validators.required, Validators.email],
    }),
    password: new FormControl('', {
      nonNullable: true,
      validators: [Validators.required],
    }),
  });
  readonly isSubmitting = signal(false);
  readonly errorMessage = signal<string | null>(null);

  submit(): void {
    this.errorMessage.set(null);
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    this.isSubmitting.set(true);
    const { email, password } = this.form.getRawValue();

    this.auth
      .login(email, password)
      .pipe(finalize(() => this.isSubmitting.set(false)))
      .subscribe({
        next: () => void this.router.navigateByUrl('/incidents'),
        error: (error: unknown) => this.errorMessage.set(this.safeError(error)),
      });
  }

  private safeError(error: unknown): string {
    if (error instanceof HttpErrorResponse && error.status === 401) {
      return 'The email or password is incorrect.';
    }
    if (error instanceof HttpErrorResponse && (error.status === 0 || error.status >= 500)) {
      return 'The SignalForge API is unavailable. Check that the backend is running and try again.';
    }
    return 'We could not sign you in. Please try again.';
  }
}
