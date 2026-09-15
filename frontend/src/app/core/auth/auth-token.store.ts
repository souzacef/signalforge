import { Injectable } from '@angular/core';

const ACCESS_TOKEN_KEY = 'signalforge.access_token';

@Injectable({ providedIn: 'root' })
export class AuthTokenStore {
  read(): string | null {
    return sessionStorage.getItem(ACCESS_TOKEN_KEY);
  }

  write(token: string): void {
    sessionStorage.setItem(ACCESS_TOKEN_KEY, token);
  }

  clear(): void {
    sessionStorage.removeItem(ACCESS_TOKEN_KEY);
  }
}
