import { HttpClient, HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import { computed, inject, Injectable, signal } from '@angular/core';
import { firstValueFrom, Observable, of, throwError } from 'rxjs';
import { catchError, finalize, map, switchMap, tap } from 'rxjs/operators';
import { User } from '../models/user';
import { AccessTokenResponse } from './auth.models';
import { AuthTokenStore } from './auth-token.store';

const AUTH_TOKEN_URL = '/api/v1/auth/token';
const AUTH_ME_URL = '/api/v1/auth/me';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly http = inject(HttpClient);
  private readonly tokenStore = inject(AuthTokenStore);
  private readonly userState = signal<User | null>(null);
  private readonly initializingState = signal(true);

  readonly currentUser = this.userState.asReadonly();
  readonly isAuthenticated = computed(() => this.userState() !== null);
  readonly isInitializing = this.initializingState.asReadonly();

  login(email: string, password: string): Observable<User> {
    this.clearSession();

    const body = new URLSearchParams({ username: email, password }).toString();
    const headers = new HttpHeaders({
      'Content-Type': 'application/x-www-form-urlencoded',
    });

    return this.http.post<AccessTokenResponse>(AUTH_TOKEN_URL, body, { headers }).pipe(
      tap(({ access_token }) => this.tokenStore.write(access_token)),
      switchMap(() => this.http.get<User>(AUTH_ME_URL)),
      tap((user) => this.userState.set(user)),
      catchError((error: unknown) => {
        this.clearSession();
        return throwError(() => error);
      }),
    );
  }

  logout(): void {
    this.clearSession();
  }

  async restoreSession(): Promise<void> {
    const token = this.tokenStore.read();
    if (!token) {
      this.initializingState.set(false);
      return;
    }

    await firstValueFrom(
      this.http.get<User>(AUTH_ME_URL).pipe(
        tap((user) => this.userState.set(user)),
        catchError((error: unknown) => {
          if (error instanceof HttpErrorResponse && error.status === 401) {
            this.clearSession();
          } else {
            this.userState.set(null);
          }
          return of(null);
        }),
        map(() => undefined),
        finalize(() => this.initializingState.set(false)),
      ),
    );
  }

  accessToken(): string | null {
    return this.tokenStore.read();
  }

  invalidateSession(): boolean {
    const hadSession = this.tokenStore.read() !== null || this.userState() !== null;
    this.clearSession();
    return hadSession;
  }

  private clearSession(): void {
    this.tokenStore.clear();
    this.userState.set(null);
  }
}
