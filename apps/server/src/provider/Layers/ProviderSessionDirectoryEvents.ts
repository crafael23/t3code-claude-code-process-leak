import { type ThreadId } from "@t3tools/contracts";
import { Effect, Layer, PubSub, Stream } from "effect";

import {
  ProviderSessionDirectoryEvents,
  type ProviderSessionDirectoryEventsShape,
} from "../Services/ProviderSessionDirectoryEvents.ts";

const makeProviderSessionDirectoryEvents = Effect.gen(function* () {
  const pubSub = yield* Effect.acquireRelease(
    PubSub.unbounded<{ readonly threadId: ThreadId }>(),
    PubSub.shutdown,
  );

  return {
    publishChanged: (threadId) => PubSub.publish(pubSub, { threadId }).pipe(Effect.asVoid),
    get changes(): ProviderSessionDirectoryEventsShape["changes"] {
      return Stream.fromPubSub(pubSub);
    },
  } satisfies ProviderSessionDirectoryEventsShape;
});

export const ProviderSessionDirectoryEventsLive = Layer.effect(
  ProviderSessionDirectoryEvents,
  makeProviderSessionDirectoryEvents,
);
