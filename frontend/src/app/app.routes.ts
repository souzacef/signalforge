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
      { path: '', pathMatch: 'full', redirectTo: 'overview' },
      {
        path: 'overview',
        loadComponent: () =>
          import('./features/overview/overview.component').then(
            (component) => component.OverviewComponent,
          ),
        title: 'Overview · SignalForge',
      },
      {
        path: 'incidents',
        loadComponent: () =>
          import('./features/incidents/incidents.component').then(
            (component) => component.IncidentsComponent,
          ),
        title: 'Incidents · SignalForge',
      },
      {
        path: 'incidents/:incidentId',
        loadComponent: () =>
          import('./features/incidents/incident-detail.component').then(
            (component) => component.IncidentDetailComponent,
          ),
        title: 'Incident detail · SignalForge',
      },
      {
        path: 'remediation',
        loadComponent: () =>
          import('./features/remediation/remediation.component').then(
            (component) => component.RemediationComponent,
          ),
        title: 'Remediation · SignalForge',
      },
      {
        path: 'remediation/:proposalId',
        loadComponent: () =>
          import('./features/remediation/remediation-detail.component').then(
            (component) => component.RemediationDetailComponent,
          ),
        title: 'Remediation proposal detail · SignalForge',
      },
    ],
  },
  { path: '**', redirectTo: '' },
];
