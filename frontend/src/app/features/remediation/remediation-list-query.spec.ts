import { convertToParamMap } from '@angular/router';
import {
  DEFAULT_REMEDIATION_LIST_QUERY, isoToLocalDateTime, isRemediationUuid,
  localDateTimeToIso, parseRemediationListQuery,
} from './remediation-list-query';

const id = '57ed68ac-bc67-493e-a621-f63da52d6b12';
const parse = (params: Record<string, string>) => parseRemediationListQuery(convertToParamMap(params));

describe('Remediation list query', () => {
  it('defaults filters and pagination and accepts each supported page size', () => {
    expect(parse({}).query).toEqual(DEFAULT_REMEDIATION_LIST_QUERY);
    for (const limit of ['10', '20', '50', '100']) expect(parse({ limit }).query.limit).toBe(Number(limit));
    expect(parse({ limit: '25' }).query.limit).toBe(20);
  });

  it('accepts supported statuses and action, and drops unsupported values', () => {
    for (const status of ['pending_approval', 'approved', 'rejected'])
      expect(parse({ status }).query.status).toBe(status);
    expect(parse({ action_kind: 'restart_service' }).query.actionKind).toBe('restart_service');
    expect(parse({ status: 'executed', action_kind: 'deploy' }).canonicalParams).toEqual({});
  });

  it('canonicalizes invalid and non-page-aligned offsets', () => {
    expect(parse({ offset: '-1' }).query.offset).toBe(0);
    expect(parse({ offset: '1.2' }).query.offset).toBe(0);
    expect(parse({ offset: '9007199254740992' }).query.offset).toBe(0);
    const result = parse({ limit: '50', offset: '126' });
    expect(result.query.offset).toBe(100);
    expect(result.canonicalParams).toEqual({ limit: 50, offset: 100 });
    expect(result.isCanonical).toBe(false);
  });

  it('trims target and rejects blank or overlong target', () => {
    expect(parse({ target: '  payments-api  ' }).query.target).toBe('payments-api');
    expect(parse({ target: '  ' }).query.target).toBeNull();
    expect(parse({ target: 'x'.repeat(101) }).query.target).toBeNull();
  });

  it('validates both UUID filters and proposal links', () => {
    expect(isRemediationUuid(id)).toBe(true);
    expect(isRemediationUuid('not-uuid')).toBe(false);
    expect(parse({ incident_id: id, proposed_by_user_id: id }).query).toMatchObject({
      incidentId: id, proposedByUserId: id,
    });
    expect(parse({ incident_id: 'bad', proposed_by_user_id: 'bad' }).canonicalParams).toEqual({});
  });

  it('accepts aware datetimes, removes malformed values and invalid ranges', () => {
    expect(parse({ created_from: '2026-06-01T12:30:00-03:00' }).query.createdFrom)
      .toBe('2026-06-01T15:30:00.000Z');
    expect(parse({ created_from: '2026-06-01T12:30:00' }).query.createdFrom).toBeNull();
    expect(parse({ created_to: '2026-02-30T12:00:00Z' }).query.createdTo).toBeNull();
    const result = parse({
      created_from: '2026-06-02T00:00:00Z', created_to: '2026-06-01T00:00:00Z',
    });
    expect(result.query.createdFrom).toBeNull();
    expect(result.query.createdTo).toBeNull();
  });

  it('round trips aware instants through local wall-clock components in any timezone', () => {
    const instant = '2026-09-15T18:42:31.125Z';
    const local = isoToLocalDateTime(instant);
    const date = new Date(local);
    expect(date.getHours()).toBe(new Date(instant).getHours());
    expect(date.getMinutes()).toBe(new Date(instant).getMinutes());
    expect(localDateTimeToIso(local)).toBe(instant);
    expect(localDateTimeToIso('2026-02-30T12:00')).toBeNull();
  });

  it('drops unknown and duplicate query parameters', () => {
    expect(parse({ status: 'approved', surprise: 'x' }).isCanonical).toBe(false);
    expect(parse({ status: 'approved', surprise: 'x' }).canonicalParams).toEqual({ status: 'approved' });
    const duplicate = parseRemediationListQuery(convertToParamMap({ status: ['approved', 'rejected'] }));
    expect(duplicate.isCanonical).toBe(false);
  });
});
