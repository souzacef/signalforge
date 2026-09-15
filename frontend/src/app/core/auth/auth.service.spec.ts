import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { firstValueFrom } from 'rxjs';
import { authInterceptor } from '../http/auth.interceptor';
import { User } from '../models/user';
import { AuthService } from './auth.service';
import { AuthTokenStore } from './auth-token.store';

const user: User = {
  id: 'c4b94450-824f-4eca-9b5a-0fbe9308a632',
  email: 'operator@example.com',
  role: 'operator',
  is_active: true,
  created_at: '2026-09-15T00:00:00Z',
  updated_at: '2026-09-15T00:00:00Z',
};

describe('AuthService', () => {
  let auth: AuthService;
  let tokenStore: AuthTokenStore;
  let http: HttpTestingController;

  beforeEach(() => {
    sessionStorage.clear();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([authInterceptor])),
        provideHttpClientTesting(),
        provideRouter([]),
      ],
    });
    auth = TestBed.inject(AuthService);
    tokenStore = TestBed.inject(AuthTokenStore);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http.verify();
    sessionStorage.clear();
  });

  it('posts form-urlencoded credentials, stores the token, and loads the user', async () => {
    const result = firstValueFrom(auth.login(user.email, 'short-pass'));
    const login = http.expectOne('/api/v1/auth/token');
    expect(login.request.method).toBe('POST');
    expect(login.request.headers.get('Content-Type')).toBe('application/x-www-form-urlencoded');
    expect(new URLSearchParams(login.request.body as string).get('username')).toBe(user.email);
    expect(new URLSearchParams(login.request.body as string).get('password')).toBe('short-pass');
    expect(login.request.headers.has('Authorization')).toBe(false);
    login.flush({ access_token: 'test-token', token_type: 'bearer' });

    const me = http.expectOne('/api/v1/auth/me');
    expect(me.request.headers.get('Authorization')).toBe('Bearer test-token');
    me.flush(user);
    expect(await result).toEqual(user);
    expect(auth.currentUser()).toEqual(user);
    expect(auth.isAuthenticated()).toBe(true);
    expect(tokenStore.read()).toBe('test-token');
  });

  it('does not leave a token after failed login', async () => {
    const result = firstValueFrom(auth.login(user.email, 'incorrect'));
    http
      .expectOne('/api/v1/auth/token')
      .flush(
        { detail: 'Incorrect email or password' },
        { status: 401, statusText: 'Unauthorized' },
      );
    await expect(result).rejects.toBeTruthy();
    expect(tokenStore.read()).toBeNull();
    expect(auth.currentUser()).toBeNull();
  });

  it('restores a valid stored token through /me', async () => {
    tokenStore.write('stored-token');
    const restore = auth.restoreSession();
    const me = http.expectOne('/api/v1/auth/me');
    expect(me.request.headers.get('Authorization')).toBe('Bearer stored-token');
    me.flush(user);
    await restore;
    expect(auth.currentUser()).toEqual(user);
    expect(auth.isInitializing()).toBe(false);
  });

  it('clears an invalid stored token during restoration', async () => {
    tokenStore.write('expired-token');
    const restore = auth.restoreSession();
    http
      .expectOne('/api/v1/auth/me')
      .flush(
        { detail: 'Could not validate credentials' },
        { status: 401, statusText: 'Unauthorized' },
      );
    await restore;
    expect(tokenStore.read()).toBeNull();
    expect(auth.currentUser()).toBeNull();
    expect(auth.isInitializing()).toBe(false);
  });

  it('completes initialization immediately without a token', async () => {
    await auth.restoreSession();
    expect(auth.isInitializing()).toBe(false);
    http.expectNone('/api/v1/auth/me');
  });

  it('clears the token and user on logout', async () => {
    tokenStore.write('stored-token');
    const restore = auth.restoreSession();
    http.expectOne('/api/v1/auth/me').flush(user);
    await restore;
    auth.logout();
    expect(tokenStore.read()).toBeNull();
    expect(auth.currentUser()).toBeNull();
    expect(auth.isAuthenticated()).toBe(false);
  });
});
