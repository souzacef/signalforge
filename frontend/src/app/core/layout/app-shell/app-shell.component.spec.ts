import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { vi } from 'vitest';
import { AuthService } from '../../auth/auth.service';
import { User } from '../../models/user';
import { AppShellComponent } from './app-shell.component';

const user: User = {
  id: 'c4b94450-824f-4eca-9b5a-0fbe9308a632',
  email: 'operator@example.com',
  role: 'operator',
  is_active: true,
  created_at: '2026-09-15T00:00:00Z',
  updated_at: '2026-09-15T00:00:00Z',
};

describe('AppShellComponent', () => {
  const logout = vi.fn();
  let router: Router;

  beforeEach(async () => {
    logout.mockReset();
    await TestBed.configureTestingModule({
      imports: [AppShellComponent],
      providers: [
        provideRouter([]),
        { provide: AuthService, useValue: { currentUser: signal(user), logout } },
      ],
    }).compileComponents();
    router = TestBed.inject(Router);
  });

  afterEach(() => vi.restoreAllMocks());

  it('renders authenticated email and human-readable role', () => {
    const fixture = TestBed.createComponent(AppShellComponent);
    fixture.detectChanges();
    const text = (fixture.nativeElement as HTMLElement).textContent;
    expect(text).toContain('operator@example.com');
    expect(text).toContain('Operator');
  });

  it('renders overview first in desktop and mobile navigation and links the product home there', () => {
    const fixture = TestBed.createComponent(AppShellComponent);
    fixture.detectChanges();
    const element = fixture.nativeElement as HTMLElement;
    const home = element.querySelector<HTMLAnchorElement>('a.product');
    expect(home?.getAttribute('href')).toBe('/overview');
    expect(home?.getAttribute('aria-label')).toBe('SignalForge home');

    const navigations = element.querySelectorAll<HTMLElement>(
      'nav[aria-label="Primary navigation"]',
    );
    expect(navigations).toHaveLength(2);
    for (const navigation of navigations) {
      const links = [...navigation.querySelectorAll<HTMLAnchorElement>('a')];
      expect(links.map((link) => link.textContent?.replace(/\s+/g, ''))).toEqual([
        '⌂Overview',
        '▤Incidents',
        '◇Remediation',
      ]);
      expect(links.map((link) => link.getAttribute('href'))).toEqual([
        '/overview',
        '/incidents',
        '/remediation',
      ]);
    }
  });

  it('logs out and returns to login', () => {
    const navigate = vi.spyOn(router, 'navigateByUrl').mockResolvedValue(true);
    const fixture = TestBed.createComponent(AppShellComponent);
    fixture.detectChanges();
    fixture.componentInstance.logout();
    expect(logout).toHaveBeenCalledOnce();
    expect(navigate).toHaveBeenCalledWith('/login');
  });
});
