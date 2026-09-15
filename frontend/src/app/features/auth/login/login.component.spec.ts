import { HttpErrorResponse } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { vi } from 'vitest';
import { AuthService } from '../../../core/auth/auth.service';
import { User } from '../../../core/models/user';
import { LoginComponent } from './login.component';

const user: User = {
  id: 'c4b94450-824f-4eca-9b5a-0fbe9308a632',
  email: 'operator@example.com',
  role: 'operator',
  is_active: true,
  created_at: '2026-09-15T00:00:00Z',
  updated_at: '2026-09-15T00:00:00Z',
};

describe('LoginComponent', () => {
  const login = vi.fn();
  let router: Router;

  beforeEach(async () => {
    login.mockReset();
    await TestBed.configureTestingModule({
      imports: [LoginComponent],
      providers: [provideRouter([]), { provide: AuthService, useValue: { login } }],
    }).compileComponents();
    router = TestBed.inject(Router);
  });

  afterEach(() => vi.restoreAllMocks());

  it('validates email and required password without a length restriction', () => {
    const fixture = TestBed.createComponent(LoginComponent);
    const component = fixture.componentInstance;
    component.submit();
    expect(component.form.controls.email.touched).toBe(true);
    expect(component.form.controls.password.touched).toBe(true);
    expect(login).not.toHaveBeenCalled();

    component.form.controls.email.setValue('invalid');
    component.form.controls.password.setValue('short');
    expect(component.form.invalid).toBe(true);
    component.form.controls.email.setValue(user.email);
    expect(component.form.valid).toBe(true);
  });

  it('associates validation messages only with touched invalid controls', () => {
    const fixture = TestBed.createComponent(LoginComponent);
    fixture.detectChanges();
    const element = fixture.nativeElement as HTMLElement;
    const email = element.querySelector<HTMLInputElement>('#email');
    const password = element.querySelector<HTMLInputElement>('#password');

    expect(email?.hasAttribute('aria-describedby')).toBe(false);
    expect(email?.hasAttribute('aria-invalid')).toBe(false);
    expect(password?.hasAttribute('aria-describedby')).toBe(false);
    expect(password?.hasAttribute('aria-invalid')).toBe(false);
    expect(element.querySelector('#email-error')).toBeNull();
    expect(element.querySelector('#password-error')).toBeNull();

    fixture.componentInstance.submit();
    fixture.detectChanges();

    expect(email?.getAttribute('aria-describedby')).toBe('email-error');
    expect(email?.getAttribute('aria-invalid')).toBe('true');
    expect(element.querySelector('#email-error')?.textContent).toContain('Email is required.');
    expect(password?.getAttribute('aria-describedby')).toBe('password-error');
    expect(password?.getAttribute('aria-invalid')).toBe('true');
    expect(element.querySelector('#password-error')?.textContent).toContain(
      'Password is required.',
    );
  });

  it('shows progress while submitting and navigates on success', async () => {
    const response = new Subject<User>();
    login.mockReturnValue(response.asObservable());
    const navigate = vi.spyOn(router, 'navigateByUrl').mockResolvedValue(true);
    const fixture = TestBed.createComponent(LoginComponent);
    const component = fixture.componentInstance;
    component.form.setValue({ email: user.email, password: 'short' });
    component.submit();
    fixture.detectChanges();
    expect(component.isSubmitting()).toBe(true);
    const submit = (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(
      'button[type="submit"]',
    );
    expect(submit?.getAttribute('aria-busy')).toBe('true');
    expect(submit?.textContent).toContain('Signing in');
    expect(submit?.querySelector('mat-spinner')?.getAttribute('aria-hidden')).toBe('true');

    response.next(user);
    response.complete();
    await fixture.whenStable();
    expect(component.isSubmitting()).toBe(false);
    expect(navigate).toHaveBeenCalledWith('/incidents');
  });

  it('shows a generic invalid-credentials message for 401', () => {
    const response = new Subject<User>();
    login.mockReturnValue(response.asObservable());
    const fixture = TestBed.createComponent(LoginComponent);
    const component = fixture.componentInstance;
    component.form.setValue({ email: user.email, password: 'wrong' });
    component.submit();
    response.error(new HttpErrorResponse({ status: 401 }));
    fixture.detectChanges();
    const text = (fixture.nativeElement as HTMLElement).textContent;
    expect(text).toContain('The email or password is incorrect.');
    expect(text).not.toContain('HttpErrorResponse');
    expect((fixture.nativeElement as HTMLElement).querySelector('[role="alert"]')).not.toBeNull();
    expect(component.isSubmitting()).toBe(false);
  });
});
