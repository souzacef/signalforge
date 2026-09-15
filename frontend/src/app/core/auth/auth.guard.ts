import { inject } from '@angular/core';
import { toObservable } from '@angular/core/rxjs-interop';
import { CanActivateFn, Router, UrlTree } from '@angular/router';
import { filter, map, take } from 'rxjs/operators';
import { AuthService } from './auth.service';

function afterInitialization(destination: () => boolean | UrlTree) {
  const auth = inject(AuthService);
  if (!auth.isInitializing()) {
    return destination();
  }

  return toObservable(auth.isInitializing).pipe(
    filter((isInitializing) => !isInitializing),
    take(1),
    map(() => destination()),
  );
}

export const authGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  return afterInitialization(() => (auth.isAuthenticated() ? true : router.parseUrl('/login')));
};

export const guestGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  return afterInitialization(() => (auth.isAuthenticated() ? router.parseUrl('/incidents') : true));
};
