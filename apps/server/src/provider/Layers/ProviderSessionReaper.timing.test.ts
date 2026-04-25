import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
  type OrchestrationSession,
} from "@t3tools/contracts";
import { Duration, Effect, Layer, PubSub, Ref, Stream } from "effect";
import { TestClock } from "effect/testing";

import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../../orchestration/Services/OrchestrationEngine.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProviderSessionRuntimeRepositoryLive } from "../../persistence/Layers/ProviderSessionRuntime.ts";
import { ProviderSessionRuntimeRepository } from "../../persistence/Services/ProviderSessionRuntime.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionDirectoryEventsLive } from "./ProviderSessionDirectoryEvents.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";

const defaultModelSelection = {
  provider: "codex",
  model: "gpt-5-codex",
} as const;

const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;

function makeReadModel(
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly latestTurn?: {
      readonly turnId: TurnId;
      readonly state: "running" | "interrupted" | "completed" | "error";
      readonly requestedAt: string;
      readonly startedAt: string | null;
      readonly completedAt: string | null;
      readonly assistantMessageId: MessageId | null;
    } | null;
    readonly session: {
      readonly threadId: ThreadId;
      readonly status: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly providerName: "codex" | "claudeAgent" | "cursor" | "opencode";
      readonly runtimeMode: "approval-required" | "full-access" | "auto-accept-edits";
      readonly activeTurnId: TurnId | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
  }>,
) {
  const now = new Date(0).toISOString();
  const projectId = ProjectId.make("project-provider-session-reaper-timing");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Timing Project",
        workspaceRoot: "/tmp/provider-reaper-timing",
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: threads.map((thread) => ({
      id: thread.id,
      projectId,
      title: `Thread ${thread.id}`,
      modelSelection: defaultModelSelection,
      interactionMode: "default" as const,
      runtimeMode: "full-access" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      latestTurn: thread.latestTurn ?? null,
      messages: [],
      session: thread.session,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
    })),
  };
}

type ReadModelSession = NonNullable<Parameters<typeof makeReadModel>[0][number]["session"]>;

function makeThreadSessionSetEvent(
  threadId: ThreadId,
  session: OrchestrationSession,
): OrchestrationEvent {
  return {
    sequence: 0,
    eventId: EventId.make(`evt-${String(threadId)}-session-set`),
    aggregateKind: "thread",
    aggregateId: threadId,
    type: "thread.session-set",
    occurredAt: session.updatedAt,
    commandId: CommandId.make(`cmd-${String(threadId)}-session-set`),
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: {
      threadId,
      session,
    },
  };
}

function makeHarness(input: {
  readonly initialReadModel: ReturnType<typeof makeReadModel>;
  readonly inactivityThresholdMs: number;
  readonly fallbackReconcileIntervalMs: number;
  readonly stopSessionImplementation?: (request: {
    readonly threadId: ThreadId;
  }) => ReturnType<ProviderServiceShape["stopSession"]>;
}) {
  return Effect.gen(function* () {
    const readModelRef = yield* Ref.make(input.initialReadModel);
    const domainEventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();
    const stopSession = vi.fn<ProviderServiceShape["stopSession"]>((request) =>
      input.stopSessionImplementation ? input.stopSessionImplementation(request) : Effect.void,
    );

    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession,
      listSessions: () => Effect.succeed([]),
      getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
      rollbackConversation: () => unsupported(),
      streamEvents: Stream.empty,
    };

    const orchestrationEngine: OrchestrationEngineShape = {
      getReadModel: () => Ref.get(readModelRef),
      readEvents: () => Stream.empty,
      dispatch: () => unsupported(),
      get streamDomainEvents() {
        return Stream.fromPubSub(domainEventPubSub);
      },
    };

    const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryEventsLayer = ProviderSessionDirectoryEventsLive;
    const directoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(runtimeRepositoryLayer),
      Layer.provide(directoryEventsLayer),
    );
    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: input.inactivityThresholdMs,
      fallbackReconcileIntervalMs: input.fallbackReconcileIntervalMs,
      mode: "deadline",
    }).pipe(
      Layer.provideMerge(directoryLayer),
      Layer.provideMerge(directoryEventsLayer),
      Layer.provideMerge(runtimeRepositoryLayer),
      Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
      Layer.provideMerge(Layer.succeed(OrchestrationEngineService, orchestrationEngine)),
    );

    return {
      layer,
      stopSession,
      setReadModel: (readModel: ReturnType<typeof makeReadModel>) =>
        Ref.set(readModelRef, readModel),
      publishDomainEvent: (event: OrchestrationEvent) =>
        PubSub.publish(domainEventPubSub, event).pipe(Effect.asVoid),
    };
  });
}

it.effect("reaps at the exact inactivity deadline", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-deadline-exact");
      const harness = yield* makeHarness({
        initialReadModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: new Date(0).toISOString(),
            },
          },
        ]),
        inactivityThresholdMs: 1_000,
        fallbackReconcileIntervalMs: 60_000,
      });

      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        const reaper = yield* ProviderSessionReaper;

        yield* repository.upsert({
          threadId,
          providerName: "codex",
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: new Date(0).toISOString(),
          resumeCursor: null,
          runtimePayload: null,
        });

        yield* reaper.start();
        yield* Effect.yieldNow;
        assert.equal(harness.stopSession.mock.calls.length, 0);

        yield* TestClock.adjust(Duration.millis(999));
        yield* Effect.yieldNow;
        assert.equal(harness.stopSession.mock.calls.length, 0);

        yield* TestClock.adjust(Duration.millis(2));
        yield* Effect.yieldNow;
        assert.equal(harness.stopSession.mock.calls.length, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(TestClock.layer())),
  ),
);

it.effect("cancels a pending reap when an active turn starts just before the deadline", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-active-mid-wait");
      const activeTurnId = TurnId.make("turn-active-mid-wait");
      const harness = yield* makeHarness({
        initialReadModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: new Date(0).toISOString(),
            },
          },
        ]),
        inactivityThresholdMs: 1_000,
        fallbackReconcileIntervalMs: 60_000,
      });

      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        const reaper = yield* ProviderSessionReaper;

        yield* repository.upsert({
          threadId,
          providerName: "codex",
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: new Date(0).toISOString(),
          resumeCursor: null,
          runtimePayload: null,
        });

        yield* reaper.start();
        yield* Effect.yieldNow;
        yield* TestClock.adjust(Duration.millis(999));
        yield* Effect.yieldNow;

        const runningSession: ReadModelSession = {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId,
          lastError: null,
          updatedAt: new Date(999).toISOString(),
        };
        yield* harness.setReadModel(
          makeReadModel([
            {
              id: threadId,
              latestTurn: {
                turnId: activeTurnId,
                state: "running",
                requestedAt: new Date(999).toISOString(),
                startedAt: new Date(999).toISOString(),
                completedAt: null,
                assistantMessageId: null,
              },
              session: runningSession,
            },
          ]),
        );
        yield* harness.publishDomainEvent(makeThreadSessionSetEvent(threadId, runningSession));
        yield* Effect.yieldNow;

        yield* TestClock.adjust(Duration.millis(2));
        yield* Effect.yieldNow;
        assert.equal(harness.stopSession.mock.calls.length, 0);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(TestClock.layer())),
  ),
);

it.effect("reconciles overdue bindings after a long suspended sleep", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstThreadId = ThreadId.make("thread-sleep-first");
      const secondThreadId = ThreadId.make("thread-sleep-second");
      const harness = yield* makeHarness({
        initialReadModel: makeReadModel([
          {
            id: firstThreadId,
            session: {
              threadId: firstThreadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: new Date(0).toISOString(),
            },
          },
          {
            id: secondThreadId,
            session: {
              threadId: secondThreadId,
              status: "ready",
              providerName: "claudeAgent",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: new Date(0).toISOString(),
            },
          },
        ]),
        inactivityThresholdMs: 1_000,
        fallbackReconcileIntervalMs: 60_000,
      });

      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        const reaper = yield* ProviderSessionReaper;

        yield* repository.upsert({
          threadId: firstThreadId,
          providerName: "codex",
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: new Date(0).toISOString(),
          resumeCursor: null,
          runtimePayload: null,
        });
        yield* repository.upsert({
          threadId: secondThreadId,
          providerName: "claudeAgent",
          adapterKey: "claudeAgent",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: new Date(500).toISOString(),
          resumeCursor: null,
          runtimePayload: null,
        });

        yield* reaper.start();
        yield* Effect.yieldNow;
        yield* TestClock.adjust(Duration.hours(3));
        yield* Effect.yieldNow;

        const stoppedThreadIds = new Set(
          harness.stopSession.mock.calls.map(([request]) => request.threadId),
        );
        assert.equal(stoppedThreadIds.has(firstThreadId), true);
        assert.equal(stoppedThreadIds.has(secondThreadId), true);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(TestClock.layer())),
  ),
);

it.effect("uses the fallback reconcile to notice bindings that changed without a wake signal", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-fallback-reconcile");
      const harness = yield* makeHarness({
        initialReadModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: new Date(0).toISOString(),
            },
          },
        ]),
        inactivityThresholdMs: 50,
        fallbackReconcileIntervalMs: 100,
      });

      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        const reaper = yield* ProviderSessionReaper;

        yield* reaper.start();
        yield* Effect.yieldNow;

        yield* repository.upsert({
          threadId,
          providerName: "codex",
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: new Date(0).toISOString(),
          resumeCursor: null,
          runtimePayload: null,
        });

        yield* TestClock.adjust(Duration.millis(100));
        yield* Effect.yieldNow;
        assert.equal(harness.stopSession.mock.calls.length, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(TestClock.layer())),
  ),
);

it.effect(
  "applies the recent provider.sendTurn deadline floor before orchestration catches up",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread-send-turn-floor");
        const staleCompletedAt = new Date(0).toISOString();
        const sendTurnAt = new Date(999).toISOString();
        const harness = yield* makeHarness({
          initialReadModel: makeReadModel([
            {
              id: threadId,
              latestTurn: {
                turnId: TurnId.make("turn-send-turn-stale"),
                state: "completed",
                requestedAt: staleCompletedAt,
                startedAt: staleCompletedAt,
                completedAt: staleCompletedAt,
                assistantMessageId: null,
              },
              session: {
                threadId,
                status: "ready",
                providerName: "codex",
                runtimeMode: "full-access",
                activeTurnId: null,
                lastError: null,
                updatedAt: staleCompletedAt,
              },
            },
          ]),
          inactivityThresholdMs: 1_000,
          fallbackReconcileIntervalMs: 60_000,
        });

        yield* Effect.gen(function* () {
          const directory = yield* ProviderSessionDirectory;
          const repository = yield* ProviderSessionRuntimeRepository;
          const reaper = yield* ProviderSessionReaper;

          yield* repository.upsert({
            threadId,
            providerName: "codex",
            adapterKey: "codex",
            runtimeMode: "full-access",
            status: "running",
            lastSeenAt: staleCompletedAt,
            resumeCursor: null,
            runtimePayload: null,
          });

          yield* reaper.start();
          yield* Effect.yieldNow;
          yield* TestClock.adjust(Duration.millis(999));
          yield* Effect.yieldNow;

          yield* directory.upsert({
            threadId,
            provider: "codex",
            status: "running",
            runtimePayload: {
              activeTurnId: TurnId.make("turn-send-turn-new"),
              lastRuntimeEvent: "provider.sendTurn",
              lastRuntimeEventAt: sendTurnAt,
            },
          });
          yield* Effect.yieldNow;

          yield* TestClock.adjust(Duration.millis(2));
          yield* Effect.yieldNow;
          assert.equal(harness.stopSession.mock.calls.length, 0);
        }).pipe(Effect.provide(harness.layer));
      }).pipe(Effect.provide(TestClock.layer())),
    ),
);
