export type UserRole = 'viewer' | 'operator' | 'admin';

export interface User {
  readonly id: string;
  readonly email: string;
  readonly role: UserRole;
  readonly is_active: boolean;
  readonly created_at: string;
  readonly updated_at: string;
}

export const ROLE_LABELS: Readonly<Record<UserRole, string>> = {
  viewer: 'Viewer',
  operator: 'Operator',
  admin: 'Admin',
};
