import { routes } from './app.routes';
import { authGuard } from './core/auth/auth.guard';

describe('Authenticated shell routing', () => {
  it('lands at overview and keeps every operator workspace route under authentication', () => {
    const shell = routes.find((route) => route.path === '');
    expect(shell?.canActivate).toContain(authGuard);
    expect(shell?.children?.find((route) => route.path === '')?.redirectTo).toBe('overview');

    for (const path of [
      'overview',
      'incidents',
      'incidents/:incidentId',
      'remediation',
      'remediation/:proposalId',
    ]) {
      expect(shell?.children?.find((route) => route.path === path)?.loadComponent).toBeDefined();
    }

    expect(shell?.children?.find((route) => route.path === 'overview')?.title).toBe(
      'Overview · SignalForge',
    );
    expect(routes.find((route) => route.path === '**')?.redirectTo).toBe('');
  });
});

describe('Incident detail routing', () => {
  it('places list and detail under the authenticated shell without a role-specific guard', () => {
    const shell = routes.find((route) => route.path === '');
    expect(shell?.canActivate).toContain(authGuard);
    expect(shell?.children?.find((route) => route.path === 'incidents')).toBeDefined();
    const detail = shell?.children?.find((route) => route.path === 'incidents/:incidentId');
    expect(detail?.loadComponent).toBeDefined();
    expect(detail?.canActivate).toBeUndefined();
  });
});

describe('Remediation routing', () => {
  it('places queue and detail under the authenticated shell for all authenticated roles', () => {
    const shell = routes.find((route) => route.path === '');
    expect(shell?.canActivate).toContain(authGuard);
    for (const path of ['remediation', 'remediation/:proposalId']) {
      const route = shell?.children?.find((child) => child.path === path);
      expect(route?.loadComponent).toBeDefined();
      expect(route?.canActivate).toBeUndefined();
    }
  });
});
