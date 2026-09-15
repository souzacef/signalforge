import { HttpErrorResponse } from '@angular/common/http';

export function knownRemediationDetail(error: unknown): string | null {
  if (!(error instanceof HttpErrorResponse) || typeof error.error !== 'object' ||
      error.error === null || !('detail' in error.error)) return null;
  const detail: unknown = error.error.detail;
  return typeof detail === 'string' ? detail : null;
}

export const TARGET_CONTROL_PATTERN = /[\u0000-\u001f\u007f-\u009f]/;

export const SERVICE_TARGET_PATTERN = /^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$/;

export function isServiceTarget(target: string): boolean {
  return target.length >= 1 && target.length <= 100 && SERVICE_TARGET_PATTERN.test(target);
}
