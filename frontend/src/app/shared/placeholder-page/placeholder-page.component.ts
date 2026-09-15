import { ChangeDetectionStrategy, Component, input } from '@angular/core';

@Component({
  selector: 'app-placeholder-page',
  templateUrl: './placeholder-page.component.html',
  styleUrl: './placeholder-page.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PlaceholderPageComponent {
  readonly section = input.required<string>();
  readonly title = input.required<string>();
  readonly description = input.required<string>();
  readonly nextSlice = input.required<string>();
  readonly symbol = input.required<string>();
}
