/** Route parameters are untrusted; accept the canonical UUID shape before issuing a request. */
export function isIncidentId(value: string | null): value is string {
  return value !== null &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}
