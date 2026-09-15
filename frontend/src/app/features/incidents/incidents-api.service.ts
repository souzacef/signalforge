import { HttpClient, HttpParams } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { IncidentListQuery } from './incident-list-query';
import { IncidentListResponse } from './incident.models';

@Injectable({ providedIn: 'root' })
export class IncidentsApiService {
  private readonly http = inject(HttpClient);

  listIncidents(query: IncidentListQuery): Observable<IncidentListResponse> {
    let params = new HttpParams().set('limit', query.limit).set('offset', query.offset);
    if (query.status !== null) params = params.set('status', query.status);
    if (query.severity !== null) params = params.set('severity', query.severity);
    if (query.source !== null) params = params.set('source', query.source);
    if (query.occurredFrom !== null) params = params.set('occurred_from', query.occurredFrom);
    if (query.occurredTo !== null) params = params.set('occurred_to', query.occurredTo);
    return this.http.get<IncidentListResponse>('/api/v1/incidents', { params });
  }
}
