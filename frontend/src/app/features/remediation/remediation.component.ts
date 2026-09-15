import { ChangeDetectionStrategy, Component } from '@angular/core';
import { PlaceholderPageComponent } from '../../shared/placeholder-page/placeholder-page.component';

@Component({
  selector: 'app-remediation',
  imports: [PlaceholderPageComponent],
  template: `
    <app-placeholder-page
      section="remediation"
      title="Remediation"
      description="A controlled workspace for human review before operational changes."
      nextSlice="Human-reviewed remediation proposals and controlled execution will appear here in a later Phase 6 slice."
      symbol="◇"
    />
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RemediationComponent {}
