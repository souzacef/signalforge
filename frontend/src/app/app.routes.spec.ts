import { routes } from './app.routes';
import { authGuard } from './core/auth/auth.guard';

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
