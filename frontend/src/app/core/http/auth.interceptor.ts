import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';
import { AuthService } from '../auth/auth.service';

const LOGIN_PATH = '/api/v1/auth/token';

export const authInterceptor: HttpInterceptorFn = (request, next) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const isSignalForgeApi = request.url.startsWith('/api/');
  const isLoginRequest = request.url === LOGIN_PATH;
  const token = isSignalForgeApi && !isLoginRequest ? auth.accessToken() : null;
  const authenticatedRequest = token
    ? request.clone({ setHeaders: { Authorization: `Bearer ${token}` } })
    : request;

  return next(authenticatedRequest).pipe(
    catchError((error: unknown) => {
      if (
        error instanceof HttpErrorResponse &&
        error.status === 401 &&
        token &&
        !isLoginRequest &&
        auth.invalidateSession() &&
        !auth.isInitializing()
      ) {
        void router.navigateByUrl('/login');
      }

      return throwError(() => error);
    }),
  );
};
