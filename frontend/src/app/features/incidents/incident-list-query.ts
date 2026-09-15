import { ParamMap, Params } from '@angular/router';
import { IncidentSeverity, IncidentStatus } from './incident.models';

export const INCIDENT_PAGE_SIZES = [10, 20, 50, 100] as const;
export type IncidentPageSize = (typeof INCIDENT_PAGE_SIZES)[number];

export interface IncidentListQuery {
  status: IncidentStatus | null;
  severity: IncidentSeverity | null;
  source: string | null;
  occurredFrom: string | null;
  occurredTo: string | null;
  limit: IncidentPageSize;
  offset: number;
}

export interface ParsedIncidentListQuery {
  query: IncidentListQuery;
  canonicalParams: Params;
  isCanonical: boolean;
}

export const DEFAULT_INCIDENT_LIST_QUERY: IncidentListQuery = {
  status: null,
  severity: null,
  source: null,
  occurredFrom: null,
  occurredTo: null,
  limit: 20,
  offset: 0,
};

const STATUSES: readonly IncidentStatus[] = ['open', 'acknowledged', 'resolved'];
const SEVERITIES: readonly IncidentSeverity[] = ['low', 'medium', 'high', 'critical'];
const AWARE_ISO_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?(?:Z|[+-](\d{2}):(\d{2}))$/i;
const LOCAL_DATETIME_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/;

export function parseIncidentListQuery(params: ParamMap): ParsedIncidentListQuery {
  const status = memberOrNull(params.get('status'), STATUSES);
  const severity = memberOrNull(params.get('severity'), SEVERITIES);
  const source = trimmedOrNull(params.get('source'));
  let occurredFrom = normalizeAwareIso(params.get('occurred_from'));
  let occurredTo = normalizeAwareIso(params.get('occurred_to'));
  const limit = pageSizeOrDefault(params.get('limit'));
  const rawOffset = nonnegativeIntegerOrDefault(params.get('offset'));
  const offset = Math.floor(rawOffset / limit) * limit;

  if (
    occurredFrom !== null &&
    occurredTo !== null &&
    Date.parse(occurredFrom) > Date.parse(occurredTo)
  ) {
    occurredFrom = null;
    occurredTo = null;
  }

  const query: IncidentListQuery = {
    status,
    severity,
    source,
    occurredFrom,
    occurredTo,
    limit,
    offset,
  };
  const canonicalParams = incidentQueryToParams(query);

  return {
    query,
    canonicalParams,
    isCanonical: paramMapMatches(params, canonicalParams),
  };
}

export function incidentQueryToParams(query: IncidentListQuery): Params {
  const params: Params = {};
  if (query.status !== null) params['status'] = query.status;
  if (query.severity !== null) params['severity'] = query.severity;
  if (query.source !== null) params['source'] = query.source;
  if (query.occurredFrom !== null) params['occurred_from'] = query.occurredFrom;
  if (query.occurredTo !== null) params['occurred_to'] = query.occurredTo;
  if (query.limit !== DEFAULT_INCIDENT_LIST_QUERY.limit) params['limit'] = query.limit;
  if (query.offset !== DEFAULT_INCIDENT_LIST_QUERY.offset) params['offset'] = query.offset;
  return params;
}

export function localDateTimeToIso(value: string): string | null {
  const match = LOCAL_DATETIME_PATTERN.exec(value.trim());
  if (match === null) return null;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText, fractionText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText ?? 0);
  const millisecond = Number((fractionText ?? '').padEnd(3, '0'));
  const date = new Date(year, month - 1, day, hour, minute, second, millisecond);

  if (
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day ||
    date.getHours() !== hour ||
    date.getMinutes() !== minute ||
    date.getSeconds() !== second ||
    date.getMilliseconds() !== millisecond
  ) {
    return null;
  }

  return date.toISOString();
}

export function isoToLocalDateTime(value: string | null): string {
  const normalized = normalizeAwareIso(value);
  if (normalized === null) return '';
  const date = new Date(normalized);
  return [
    pad(date.getFullYear(), 4),
    '-',
    pad(date.getMonth() + 1),
    '-',
    pad(date.getDate()),
    'T',
    pad(date.getHours()),
    ':',
    pad(date.getMinutes()),
    ':',
    pad(date.getSeconds()),
    '.',
    pad(date.getMilliseconds(), 3),
  ].join('');
}

export function normalizeAwareIso(value: string | null): string | null {
  if (value === null) return null;
  const match = AWARE_ISO_PATTERN.exec(value);
  if (match === null) return null;

  const [
    ,
    yearText,
    monthText,
    dayText,
    hourText,
    minuteText,
    secondText,
    ,
    zoneHourText,
    zoneMinuteText,
  ] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText ?? 0);
  const zoneHour = Number(zoneHourText ?? 0);
  const zoneMinute = Number(zoneMinuteText ?? 0);
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (
    year < 1 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    zoneHour > 23 ||
    zoneMinute > 59
  ) {
    return null;
  }

  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
}

function memberOrNull<T extends string>(value: string | null, values: readonly T[]): T | null {
  return value !== null && values.includes(value as T) ? (value as T) : null;
}

function trimmedOrNull(value: string | null): string | null {
  if (value === null) return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function pageSizeOrDefault(value: string | null): IncidentPageSize {
  const parsed = Number(value);
  return INCIDENT_PAGE_SIZES.includes(parsed as IncidentPageSize)
    ? (parsed as IncidentPageSize)
    : DEFAULT_INCIDENT_LIST_QUERY.limit;
}

function nonnegativeIntegerOrDefault(value: string | null): number {
  if (value === null || !/^\d+$/.test(value)) return DEFAULT_INCIDENT_LIST_QUERY.offset;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : DEFAULT_INCIDENT_LIST_QUERY.offset;
}

function paramMapMatches(params: ParamMap, canonical: Params): boolean {
  const canonicalKeys = Object.keys(canonical).sort();
  const actualKeys = [...params.keys].sort();
  if (canonicalKeys.length !== actualKeys.length) return false;
  return canonicalKeys.every(
    (key, index) =>
      key === actualKeys[index] &&
      params.getAll(key).length === 1 &&
      params.get(key) === String(canonical[key]),
  );
}

function pad(value: number, length = 2): string {
  return String(value).padStart(length, '0');
}
