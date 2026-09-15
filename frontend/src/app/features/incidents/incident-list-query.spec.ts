import { convertToParamMap } from '@angular/router';
import {
  DEFAULT_INCIDENT_LIST_QUERY,
  isoToLocalDateTime,
  localDateTimeToIso,
  parseIncidentListQuery,
} from './incident-list-query';

function parse(params: Record<string, string>) {
  return parseIncidentListQuery(convertToParamMap(params));
}

describe('Incident list query', () => {
  it('uses defaults and accepts each supported page size', () => {
    expect(parse({}).query).toEqual(DEFAULT_INCIDENT_LIST_QUERY);
    for (const limit of ['10', '20', '50', '100']) {
      expect(parse({ limit }).query.limit).toBe(Number(limit));
    }
  });

  it('accepts supported status and severity values', () => {
    expect(parse({ status: 'acknowledged', severity: 'critical' }).query).toMatchObject({
      status: 'acknowledged',
      severity: 'critical',
    });
  });

  it('removes invalid status and severity values', () => {
    const result = parse({ status: 'pending', severity: 'urgent' });
    expect(result.query.status).toBeNull();
    expect(result.query.severity).toBeNull();
    expect(result.canonicalParams).toEqual({});
    expect(result.isCanonical).toBe(false);
  });

  it('falls back for invalid page sizes and offsets', () => {
    expect(parse({ limit: '25' }).query.limit).toBe(20);
    expect(parse({ offset: '-1' }).query.offset).toBe(0);
    expect(parse({ offset: '1.5' }).query.offset).toBe(0);
    expect(parse({ offset: '9007199254740992' }).query.offset).toBe(0);
  });

  it('aligns an offset to the start of its page', () => {
    const result = parse({ limit: '50', offset: '126' });
    expect(result.query.offset).toBe(100);
    expect(result.canonicalParams).toEqual({ limit: 50, offset: 100 });
    expect(result.isCanonical).toBe(false);
  });

  it('trims exact source values and removes blank values', () => {
    expect(parse({ source: '  Prometheus EU  ' }).query.source).toBe('Prometheus EU');
    expect(parse({ source: '   ' }).query.source).toBeNull();
  });

  it('normalizes valid aware datetimes and rejects unaware or malformed values', () => {
    expect(parse({ occurred_from: '2026-06-01T12:30:00-03:00' }).query.occurredFrom).toBe(
      '2026-06-01T15:30:00.000Z',
    );
    expect(parse({ occurred_from: '2026-06-01T12:30:00' }).query.occurredFrom).toBeNull();
    expect(parse({ occurred_to: 'not-a-date' }).query.occurredTo).toBeNull();
    expect(parse({ occurred_to: 'September 15 2026 12:00Z' }).query.occurredTo).toBeNull();
    expect(parse({ occurred_to: '2026-02-30T12:00:00Z' }).query.occurredTo).toBeNull();
  });

  it('removes an invalid occurred range before it reaches the API', () => {
    const result = parse({
      occurred_from: '2026-06-02T00:00:00Z',
      occurred_to: '2026-06-01T00:00:00Z',
    });
    expect(result.query.occurredFrom).toBeNull();
    expect(result.query.occurredTo).toBeNull();
    expect(result.isCanonical).toBe(false);
  });

  it('round-trips an instant through local datetime components without assuming UTC', () => {
    const instant = '2026-09-15T18:42:31.125Z';
    const localValue = isoToLocalDateTime(instant);
    const localDate = new Date(localValue);
    expect(localDate.getFullYear()).toBe(new Date(instant).getFullYear());
    expect(localDate.getMonth()).toBe(new Date(instant).getMonth());
    expect(localDate.getDate()).toBe(new Date(instant).getDate());
    expect(localDate.getHours()).toBe(new Date(instant).getHours());
    expect(localDate.getMinutes()).toBe(new Date(instant).getMinutes());
    expect(localDateTimeToIso(localValue)).toBe(instant);
  });

  it('rejects impossible local dates rather than letting Date normalize them', () => {
    expect(localDateTimeToIso('2026-02-30T12:00')).toBeNull();
  });
});
