import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import { Effect, Fiber, Stream } from "effect";

import { ProviderSessionDirectoryEvents } from "../Services/ProviderSessionDirectoryEvents.ts";
import { ProviderSessionDirectoryEventsLive } from "./ProviderSessionDirectoryEvents.ts";

it.effect("ProviderSessionDirectoryEventsLive fans out changes to each subscriber", () =>
  Effect.gen(function* () {
    const directoryEvents = yield* ProviderSessionDirectoryEvents;
    const threadId = ThreadId.make("thread-directory-events-fanout");

    const firstFiber = yield* Stream.take(directoryEvents.changes, 1).pipe(
      Stream.runCollect,
      Effect.forkScoped,
    );
    const secondFiber = yield* Stream.take(directoryEvents.changes, 1).pipe(
      Stream.runCollect,
      Effect.forkScoped,
    );
    yield* Effect.yieldNow;

    yield* directoryEvents.publishChanged(threadId);

    const first = yield* Fiber.join(firstFiber);
    const second = yield* Fiber.join(secondFiber);
    assert.deepEqual(Array.from(first), [{ threadId }]);
    assert.deepEqual(Array.from(second), [{ threadId }]);
  }).pipe(Effect.provide(ProviderSessionDirectoryEventsLive)),
);
