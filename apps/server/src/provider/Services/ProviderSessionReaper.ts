import { Context } from "effect";
import type { Effect, Scope } from "effect";

export const DEFAULT_PROVIDER_SESSION_REAPER_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
export const DEFAULT_PROVIDER_SESSION_REAPER_FALLBACK_RECONCILE_INTERVAL_MS = 30 * 60 * 1000;
export const DEFAULT_PROVIDER_SESSION_REAPER_STOP_FAILURE_RETRY_INTERVAL_MS = 5 * 1000;

export interface ProviderSessionReaperShape {
  /**
   * Start the background provider session reaper within the provided scope.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class ProviderSessionReaper extends Context.Service<
  ProviderSessionReaper,
  ProviderSessionReaperShape
>()("t3/provider/Services/ProviderSessionReaper") {}
