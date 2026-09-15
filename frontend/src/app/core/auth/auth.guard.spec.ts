import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import {
  ActivatedRouteSnapshot,
  Router,
  RouterStateSnapshot,
  UrlTree,
  provideRouter,
} from '@angular/router';
import { AuthService } from './auth.service';
import { authGuard, guestGuard } from './auth.guard';

describe('auth guards', () => {
  const userState = signal(false);
  const initState = signal(false);
  const auth = {
    isAuthenticated: userState.asReadonly(),
    isInitializing: initState.asReadonly(),
  };
  const route = {} as ActivatedRouteSnapshot;
  const state = {} as RouterStateSnapshot;
  let router: Router;

  beforeEach(() => {
    userState.set(false);
    initState.set(false);
    TestBed.configureTestingModule({
      providers: [provideRouter([]), { provide: AuthService, useValue: auth }],
    });
    router = TestBed.inject(Router);
  });

  it('redirects unauthenticated protected navigation to /login', () => {
    const result = TestBed.runInInjectionContext(() => authGuard(route, state));
    expect(result instanceof UrlTree).toBe(true);
    expect(router.serializeUrl(result as UrlTree)).toBe('/login');
  });

  it('allows authenticated protected navigation', () => {
    userState.set(true);
    const result = TestBed.runInInjectionContext(() => authGuard(route, state));
    expect(result).toBe(true);
  });

  it('redirects an authenticated login visit to /incidents', () => {
    userState.set(true);
    const result = TestBed.runInInjectionContext(() => guestGuard(route, state));
    expect(router.serializeUrl(result as UrlTree)).toBe('/incidents');
  });

  it('allows an unauthenticated login visit', () => {
    const result = TestBed.runInInjectionContext(() => guestGuard(route, state));
    expect(result).toBe(true);
  });
});
