import { Routes } from '@angular/router';
import { authGuard, guestGuard } from './core/auth/auth.guard';

export const routes: Routes = [
  {
    path: 'login',
    canActivate: [guestGuard],
    loadComponent: () =>
      import('./features/auth/login/login.component').then((component) => component.LoginComponent),
    title: 'Sign in · SignalForge',
  },
  {
    path: '',
    canActivate: [authGuard],
    loadComponent: () =>
      import('./core/layout/app-shell/app-shell.component').then(
        (component) => component.AppShellComponent,
      ),
    children: [
      { path: '', pathMatch: 'full', redirectTo: 'incidents' },
      {
        path: 'incidents',
        loadComponent: () =>
          import('./features/incidents/incidents.component').then(
            (component) => component.IncidentsComponent,
          ),
        title: 'Incidents · SignalForge',
      },
      {
        path: 'remediation',
        loadComponent: () =>
          import('./features/remediation/remediation.component').then(
            (component) => component.RemediationComponent,
          ),
        title: 'Remediation · SignalForge',
      },
    ],
  },
  { path: '**', redirectTo: '' },
];
