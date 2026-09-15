import { ChangeDetectionStrategy, Component } from '@angular/core';
import { PlaceholderPageComponent } from '../../shared/placeholder-page/placeholder-page.component';

@Component({
  selector: 'app-incidents',
  imports: [PlaceholderPageComponent],
  template: `
    <app-placeholder-page
      section="incidents"
      title="Incidents"
      description="The workspace for intake, triage, and incident lifecycle operations."
      nextSlice="Incident intake, triage, and lifecycle operations will appear here in a later Phase 6 slice."
      symbol="▤"
    />
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class IncidentsComponent {}
