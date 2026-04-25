import { type ThreadId } from "@t3tools/contracts";
import { Context } from "effect";
import type { Effect, Stream } from "effect";

export interface ProviderSessionDirectoryEventsShape {
  readonly publishChanged: (threadId: ThreadId) => Effect.Effect<void>;
  readonly changes: Stream.Stream<{ readonly threadId: ThreadId }>;
}

export class ProviderSessionDirectoryEvents extends Context.Service<
  ProviderSessionDirectoryEvents,
  ProviderSessionDirectoryEventsShape
>()("t3/provider/Services/ProviderSessionDirectoryEvents") {}
