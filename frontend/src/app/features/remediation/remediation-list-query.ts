import { ParamMap, Params } from '@angular/router';
import { RemediationActionKind, RemediationProposalStatus } from './remediation.models';

export const REMEDIATION_PAGE_SIZES = [10, 20, 50, 100] as const;
export type RemediationPageSize = (typeof REMEDIATION_PAGE_SIZES)[number];

export interface RemediationListQuery {
  incidentId: string | null;
  status: RemediationProposalStatus | null;
  actionKind: RemediationActionKind | null;
  target: string | null;
  proposedByUserId: string | null;
  createdFrom: string | null;
  createdTo: string | null;
  limit: RemediationPageSize;
  offset: number;
}

export const DEFAULT_REMEDIATION_LIST_QUERY: RemediationListQuery = {
  incidentId: null, status: null, actionKind: null, target: null,
  proposedByUserId: null, createdFrom: null, createdTo: null, limit: 20, offset: 0,
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AWARE_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?(Z|[+-](\d{2}):(\d{2}))$/i;
const LOCAL_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/;
const STATUSES: readonly RemediationProposalStatus[] = ['pending_approval', 'approved', 'rejected'];
const ACTIONS: readonly RemediationActionKind[] = ['restart_service'];

export function isRemediationUuid(value: string | null): value is string {
  return value !== null && UUID_PATTERN.test(value);
}

export function parseRemediationListQuery(params: ParamMap): {
  query: RemediationListQuery; canonicalParams: Params; isCanonical: boolean;
} {
  const limitValue = Number(params.get('limit'));
  const limit = REMEDIATION_PAGE_SIZES.includes(limitValue as RemediationPageSize)
    ? limitValue as RemediationPageSize : 20;
  const rawOffset = params.get('offset');
  const parsedOffset = rawOffset !== null && /^\d+$/.test(rawOffset) && Number.isSafeInteger(Number(rawOffset))
    ? Number(rawOffset) : 0;
  let createdFrom = normalizeAwareDateTime(params.get('created_from'));
  let createdTo = normalizeAwareDateTime(params.get('created_to'));
  if (createdFrom && createdTo && Date.parse(createdFrom) > Date.parse(createdTo)) {
    createdFrom = null;
    createdTo = null;
  }
  const incidentId = params.get('incident_id');
  const proposedByUserId = params.get('proposed_by_user_id');
  const status = params.get('status');
  const actionKind = params.get('action_kind');
  const target = params.get('target')?.trim() || null;
  const query: RemediationListQuery = {
    incidentId: isRemediationUuid(incidentId) ? incidentId.toLowerCase() : null,
    status: STATUSES.includes(status as RemediationProposalStatus) ? status as RemediationProposalStatus : null,
    actionKind: ACTIONS.includes(actionKind as RemediationActionKind) ? actionKind as RemediationActionKind : null,
    target: target && target.length <= 100 ? target : null,
    proposedByUserId: isRemediationUuid(proposedByUserId) ? proposedByUserId.toLowerCase() : null,
    createdFrom, createdTo, limit, offset: Math.floor(parsedOffset / limit) * limit,
  };
  const canonicalParams = remediationQueryToParams(query);
  const keys = Object.keys(canonicalParams).sort();
  const actual = [...params.keys].sort();
  return {
    query, canonicalParams,
    isCanonical: keys.length === actual.length && keys.every((key, index) =>
      key === actual[index] && params.getAll(key).length === 1 && params.get(key) === String(canonicalParams[key])),
  };
}

export function remediationQueryToParams(query: RemediationListQuery): Params {
  const params: Params = {};
  if (query.incidentId) params['incident_id'] = query.incidentId;
  if (query.status) params['status'] = query.status;
  if (query.actionKind) params['action_kind'] = query.actionKind;
  if (query.target) params['target'] = query.target;
  if (query.proposedByUserId) params['proposed_by_user_id'] = query.proposedByUserId;
  if (query.createdFrom) params['created_from'] = query.createdFrom;
  if (query.createdTo) params['created_to'] = query.createdTo;
  if (query.limit !== 20) params['limit'] = query.limit;
  if (query.offset !== 0) params['offset'] = query.offset;
  return params;
}

export function normalizeAwareDateTime(value: string | null): string | null {
  if (value === null) return null;
  const match = AWARE_PATTERN.exec(value);
  if (!match) return null;
  const [, year, month, day, hour, minute, second, , , zoneHour, zoneMinute] = match;
  const y = Number(year), m = Number(month), d = Number(day);
  const days = new Date(Date.UTC(y, m, 0)).getUTCDate();
  if (y < 1 || m < 1 || m > 12 || d < 1 || d > days || Number(hour) > 23 ||
      Number(minute) > 59 || Number(second ?? 0) > 59 ||
      Number(zoneHour ?? 0) > 23 || Number(zoneMinute ?? 0) > 59) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
}

export function localDateTimeToIso(value: string): string | null {
  const match = LOCAL_PATTERN.exec(value.trim());
  if (!match) return null;
  const [, year, month, day, hour, minute, second, fraction] = match;
  const y = Number(year), m = Number(month), d = Number(day);
  const h = Number(hour), min = Number(minute), s = Number(second ?? 0);
  const ms = Number((fraction ?? '').padEnd(3, '0'));
  const date = new Date(y, m - 1, d, h, min, s, ms);
  if (date.getFullYear() !== y || date.getMonth() !== m - 1 || date.getDate() !== d ||
      date.getHours() !== h || date.getMinutes() !== min || date.getSeconds() !== s ||
      date.getMilliseconds() !== ms) return null;
  return date.toISOString();
}

export function isoToLocalDateTime(value: string | null): string {
  const normalized = normalizeAwareDateTime(value);
  if (!normalized) return '';
  const date = new Date(normalized);
  const pad = (number: number, length = 2) => String(number).padStart(length, '0');
  return `${pad(date.getFullYear(), 4)}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`;
}
