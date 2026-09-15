import { HttpClient, provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { firstValueFrom } from 'rxjs';
import { vi } from 'vitest';
import { AuthTokenStore } from '../auth/auth-token.store';
import { AuthService } from '../auth/auth.service';
import { authInterceptor } from './auth.interceptor';

describe('authInterceptor', () => {
  let client: HttpClient;
  let http: HttpTestingController;
  let tokens: AuthTokenStore;
  let auth: AuthService;
  let router: Router;

  beforeEach(() => {
    sessionStorage.clear();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([authInterceptor])),
        provideHttpClientTesting(),
        provideRouter([]),
      ],
    });
    client = TestBed.inject(HttpClient);
    http = TestBed.inject(HttpTestingController);
    tokens = TestBed.inject(AuthTokenStore);
    auth = TestBed.inject(AuthService);
    router = TestBed.inject(Router);
  });

  afterEach(() => {
    http.verify();
    vi.restoreAllMocks();
    sessionStorage.clear();
  });

  it('attaches a bearer token only to relative SignalForge API requests', async () => {
    tokens.write('test-token');
    const apiResult = firstValueFrom(client.get('/api/v1/incidents'));
    const api = http.expectOne('/api/v1/incidents');
    expect(api.request.headers.get('Authorization')).toBe('Bearer test-token');
    api.flush({});
    await apiResult;

    const externalResult = firstValueFrom(client.get('https://example.com/data'));
    const external = http.expectOne('https://example.com/data');
    expect(external.request.headers.has('Authorization')).toBe(false);
    external.flush({});
    await externalResult;
  });

  it('does not attach a bearer or redirect for login 401', async () => {
    tokens.write('old-token');
    const navigate = vi.spyOn(router, 'navigateByUrl');
    const result = firstValueFrom(client.post('/api/v1/auth/token', 'body'));
    const request = http.expectOne('/api/v1/auth/token');
    expect(request.request.headers.has('Authorization')).toBe(false);
    request.flush({}, { status: 401, statusText: 'Unauthorized' });
    await expect(result).rejects.toBeTruthy();
    expect(navigate).not.toHaveBeenCalled();
  });

  it('clears an authenticated session and redirects once on API 401', async () => {
    await auth.restoreSession();
    tokens.write('expired-token');
    const navigate = vi.spyOn(router, 'navigateByUrl').mockResolvedValue(true);
    const result = firstValueFrom(client.get('/api/v1/incidents'));
    http.expectOne('/api/v1/incidents').flush({}, { status: 401, statusText: 'Unauthorized' });
    await expect(result).rejects.toBeTruthy();
    expect(tokens.read()).toBeNull();
    expect(auth.currentUser()).toBeNull();
    expect(navigate).toHaveBeenCalledOnce();
    expect(navigate).toHaveBeenCalledWith('/login');
  });
});
