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
    expect((fixture.nativeElement as HTMLElement).textContent).toContain('Signing in');

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
    expect(component.isSubmitting()).toBe(false);
  });
});
