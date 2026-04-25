import type { ProviderKind } from "@t3tools/contracts";
import {
  Cause,
  Clock,
  Duration,
  Effect,
  Layer,
  Metric,
  Option,
  Queue,
  Schedule,
  Stream,
} from "effect";

import {
  increment,
  metricAttributes,
  providerSessionReaperDueCandidatesTotal,
  providerSessionReaperReapLag,
  providerSessionReaperReapedTotal,
  providerSessionReaperReconcileDuration,
  providerSessionReaperScheduleSize,
  providerSessionReaperWakeCoalescedTotal,
  providerSessionReaperWakeupsTotal,
} from "../../observability/Metrics.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionDirectoryEvents } from "../Services/ProviderSessionDirectoryEvents.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";
import type { InvalidAnchorEntry, ReapScheduleEntry } from "./reaperDeadlines.ts";
import { deriveReapEntries } from "./reaperDeadlines.ts";

const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_FALLBACK_RECONCILE_INTERVAL_MS = 30 * 60 * 1000;

export type ProviderSessionReaperMode = "polling" | "deadline" | "deadline-shadow";

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
  readonly fallbackReconcileIntervalMs?: number;
  readonly mode?: ProviderSessionReaperMode;
}

type ReaperSignal =
  | { readonly type: "startup" }
  | { readonly type: "reconcile-all"; readonly reason: string }
  | { readonly type: "runtime-binding-changed"; readonly threadId: string }
  | { readonly type: "orchestration-thread-changed"; readonly threadId: string }
  | { readonly type: "thread-deleted"; readonly threadId: string };

interface ReconcileSnapshot {
  readonly bindingCount: number;
  readonly readModelThreadCount: number;
  readonly entries: ReadonlyArray<ReapScheduleEntry>;
  readonly skippedStopped: number;
  readonly skippedActiveTurn: number;
  readonly invalidAnchors: ReadonlyArray<InvalidAnchorEntry>;
  readonly reconciledAtMs: number;
}

interface CoalescedWake {
  readonly signal: (signal: ReaperSignal) => Effect.Effect<void>;
  readonly await: Effect.Effect<ReaperSignal>;
}

function buildReaperLogContext(input: {
  readonly now: number;
  readonly inactivityThresholdMs: number;
  readonly entry: ReapScheduleEntry;
}) {
  const inactivityAnchorMs = Date.parse(input.entry.anchorAt);
  const idleDurationMs = input.now - inactivityAnchorMs;

  return {
    threadId: input.entry.threadId,
    provider: input.entry.provider,
    bindingStatus: input.entry.bindingStatus,
    readModelThreadPresent: input.entry.readModelThreadPresent,
    sessionStatus: input.entry.sessionStatus,
    sessionUpdatedAt: input.entry.sessionUpdatedAt,
    activeTurnId: input.entry.activeTurnId,
    lastSeenAt: input.entry.lastSeenAt,
    latestTurnId: input.entry.latestTurnId,
    latestTurnState: input.entry.latestTurnState,
    latestTurnRequestedAt: input.entry.latestTurnRequestedAt,
    latestTurnStartedAt: input.entry.latestTurnStartedAt,
    latestTurnCompletedAt: input.entry.latestTurnCompletedAt,
    inactivityAnchorAt: input.entry.anchorAt,
    inactivityAnchorSource: input.entry.anchorSource,
    inactivityAnchorMs,
    deadlineBasisAt: input.entry.deadlineBasisAt,
    deadlineBasisSource: input.entry.deadlineBasisSource,
    deadlineAt: new Date(input.entry.deadlineAtMs).toISOString(),
    deadlineAtMs: input.entry.deadlineAtMs,
    inactivityThresholdMs: input.inactivityThresholdMs,
    idleDurationMs,
    remainingUntilReapMs: Math.max(0, input.entry.deadlineAtMs - input.now),
    reapLagMs: Math.max(0, input.now - input.entry.deadlineAtMs),
  };
}

function buildInvalidAnchorLogContext(input: {
  readonly now: number;
  readonly inactivityThresholdMs: number;
  readonly entry: InvalidAnchorEntry;
}) {
  return {
    threadId: input.entry.threadId,
    provider: input.entry.provider,
    bindingStatus: input.entry.bindingStatus,
    readModelThreadPresent: input.entry.readModelThreadPresent,
    sessionStatus: input.entry.sessionStatus,
    sessionUpdatedAt: input.entry.sessionUpdatedAt,
    activeTurnId: input.entry.activeTurnId,
    lastSeenAt: input.entry.lastSeenAt,
    latestTurnId: input.entry.latestTurnId,
    latestTurnState: input.entry.latestTurnState,
    latestTurnRequestedAt: input.entry.latestTurnRequestedAt,
    latestTurnStartedAt: input.entry.latestTurnStartedAt,
    latestTurnCompletedAt: input.entry.latestTurnCompletedAt,
    inactivityAnchorAt: input.entry.inactivityAnchorAt,
    inactivityAnchorSource: input.entry.inactivityAnchorSource,
    inactivityAnchorMs: null,
    sweepStartedAt: new Date(input.now).toISOString(),
    inactivityThresholdMs: input.inactivityThresholdMs,
    idleDurationMs: null,
    remainingUntilReapMs: null,
  };
}

function wakeReason(signal: ReaperSignal | "timeout"): string {
  if (signal === "timeout") {
    return "timeout";
  }
  if (signal.type === "startup") {
    return "startup";
  }
  if (signal.type === "reconcile-all" && signal.reason === "fallback-tick") {
    return "fallback";
  }
  return `signal:${signal.type}`;
}

function providerBreakdown(
  entries: ReadonlyArray<ReapScheduleEntry>,
): Record<ProviderKind, number> {
  const counts: Record<ProviderKind, number> = {
    codex: 0,
    claudeAgent: 0,
    cursor: 0,
    opencode: 0,
  };
  for (const entry of entries) {
    counts[entry.provider] += 1;
  }
  return counts;
}

const makeCoalescedWake = (mode: ProviderSessionReaperMode) =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(Queue.dropping<ReaperSignal>(1), Queue.shutdown);

    return {
      signal: (signal) =>
        Queue.offer(queue, signal).pipe(
          Effect.flatMap((enqueued) =>
            enqueued ? Effect.void : increment(providerSessionReaperWakeCoalescedTotal, { mode }),
          ),
        ),
      await: Queue.take(queue),
    } satisfies CoalescedWake;
  });

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const providerService = yield* ProviderService;
    const directory = yield* ProviderSessionDirectory;
    const directoryEvents = yield* ProviderSessionDirectoryEvents;
    const orchestrationEngine = yield* OrchestrationEngineService;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_INACTIVITY_THRESHOLD_MS,
    );
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const fallbackReconcileIntervalMs = Math.max(
      1,
      options?.fallbackReconcileIntervalMs ?? DEFAULT_FALLBACK_RECONCILE_INTERVAL_MS,
    );
    const mode = options?.mode ?? "polling";

    const recordWake = (signal: ReaperSignal | "timeout", extra?: Record<string, unknown>) =>
      increment(providerSessionReaperWakeupsTotal, {
        mode,
        reason: wakeReason(signal),
      }).pipe(
        Effect.andThen(
          Effect.logDebug(
            "provider.session.reaper.wake",
            extra === undefined
              ? {
                  mode,
                  reason: wakeReason(signal),
                }
              : {
                  mode,
                  reason: wakeReason(signal),
                  ...extra,
                },
          ),
        ),
      );

    const reconcileAuthoritativeState = () =>
      Effect.gen(function* () {
        const startedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.logDebug("provider.session.reaper.reconcile-started", {
          mode,
          inactivityThresholdMs,
        });

        const readModel = yield* orchestrationEngine.getReadModel();
        const bindings = yield* directory.listBindings();
        const derived = deriveReapEntries({
          bindings,
          readModel,
          inactivityThresholdMs,
        });
        const reconciledAtMs = yield* Clock.currentTimeMillis;
        const reconcileDurationMs = Math.max(0, reconciledAtMs - startedAtMs);

        yield* Metric.update(
          Metric.withAttributes(providerSessionReaperReconcileDuration, metricAttributes({ mode })),
          Duration.millis(reconcileDurationMs),
        );

        for (const invalidAnchor of derived.invalidAnchors) {
          yield* Effect.logWarning(
            "provider.session.reaper.invalid-inactivity-anchor",
            buildInvalidAnchorLogContext({
              now: reconciledAtMs,
              inactivityThresholdMs,
              entry: invalidAnchor,
            }),
          );
        }

        yield* Effect.logDebug("provider.session.reaper.reconcile-completed", {
          mode,
          bindingCount: bindings.length,
          readModelThreadCount: readModel.threads.length,
          scheduleSize: derived.entries.length,
          skippedStoppedCount: derived.skippedStopped,
          skippedActiveTurnCount: derived.skippedActiveTurn,
          invalidAnchorCount: derived.invalidAnchors.length,
          reconcileDurationMs,
        });

        return {
          bindingCount: bindings.length,
          readModelThreadCount: readModel.threads.length,
          entries: derived.entries,
          skippedStopped: derived.skippedStopped,
          skippedActiveTurn: derived.skippedActiveTurn,
          invalidAnchors: derived.invalidAnchors,
          reconciledAtMs,
        } satisfies ReconcileSnapshot;
      });

    const stopDueEntries = (input: {
      readonly entries: ReadonlyArray<ReapScheduleEntry>;
      readonly nowMs: number;
    }) =>
      Effect.gen(function* () {
        yield* increment(
          providerSessionReaperDueCandidatesTotal,
          {
            mode,
          },
          input.entries.length,
        );

        let reapedCount = 0;
        let stopFailedCount = 0;

        for (const entry of input.entries) {
          const logContext = buildReaperLogContext({
            now: input.nowMs,
            inactivityThresholdMs,
            entry,
          });

          yield* Metric.update(
            Metric.withAttributes(
              providerSessionReaperReapLag,
              metricAttributes({
                mode,
                provider: entry.provider,
              }),
            ),
            Duration.millis(Math.max(0, input.nowMs - entry.deadlineAtMs)),
          );

          yield* Effect.logDebug("provider.session.reaper.reap-candidate", {
            ...logContext,
            decision: mode === "deadline-shadow" ? "would_stop_session" : "attempt_stop_session",
          });

          if (mode === "deadline-shadow") {
            yield* Effect.logInfo("provider.session.reaper.shadow-candidate", {
              ...logContext,
              reason: "inactivity_threshold",
            });
            continue;
          }

          const reaped = yield* providerService
            .stopSession({
              threadId: entry.threadId,
            })
            .pipe(
              Effect.tap(() =>
                Effect.logInfo("provider.session.reaped", {
                  ...logContext,
                  reason: "inactivity_threshold",
                }).pipe(
                  Effect.andThen(
                    increment(providerSessionReaperReapedTotal, {
                      mode,
                      provider: entry.provider,
                    }),
                  ),
                ),
              ),
              Effect.as(true),
              Effect.catchCause((cause) => {
                if (Cause.hasInterruptsOnly(cause)) {
                  return Effect.failCause(cause);
                }
                stopFailedCount += 1;
                return Effect.logWarning("provider.session.reaper.stop-failed", {
                  ...logContext,
                  reason: "inactivity_threshold",
                  cause,
                }).pipe(Effect.as(false));
              }),
            );

          if (reaped) {
            reapedCount += 1;
          }
        }

        return {
          reapedCount,
          stopFailedCount,
        } as const;
      });

    const runPollingSweep = Effect.gen(function* () {
      const snapshot = yield* reconcileAuthoritativeState();
      const now = snapshot.reconciledAtMs;
      const dueEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs <= now);
      const futureEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs > now);
      const stopResult = yield* stopDueEntries({
        entries: dueEntries,
        nowMs: now,
      });

      yield* Metric.update(
        Metric.withAttributes(providerSessionReaperScheduleSize, metricAttributes({ mode })),
        futureEntries.length,
      );

      yield* Effect.logDebug("provider.session.reaper.sweep-complete", {
        mode,
        bindingCount: snapshot.bindingCount,
        reapedCount: stopResult.reapedCount,
        skippedStoppedCount: snapshot.skippedStopped,
        skippedActiveTurnCount: snapshot.skippedActiveTurn,
        skippedWithinThresholdCount: futureEntries.length,
        invalidAnchorCount: snapshot.invalidAnchors.length,
        stopFailedCount: stopResult.stopFailedCount,
        inactivityThresholdMs,
        sweepStartedAt: new Date(now).toISOString(),
      });

      if (stopResult.reapedCount > 0) {
        yield* Effect.logInfo("provider.session.reaper.sweep-complete", {
          mode,
          reapedCount: stopResult.reapedCount,
          totalBindings: snapshot.bindingCount,
        });
      }
    });

    const runDeadlineScheduler = Effect.gen(function* () {
      const wake = yield* makeCoalescedWake(mode);

      const signalWake = (signal: ReaperSignal) => wake.signal(signal);

      const forkFeed = (name: string, effect: Effect.Effect<void>) =>
        effect.pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            return Effect.logWarning("provider.session.reaper.signal-stream-failed", {
              mode,
              feed: name,
              cause,
            }).pipe(
              Effect.andThen(signalWake({ type: "reconcile-all", reason: `feed-failed:${name}` })),
            );
          }),
          Effect.forkScoped,
        );

      yield* forkFeed(
        "provider-session-directory-events",
        Stream.runForEach(directoryEvents.changes, (change) =>
          signalWake({
            type: "runtime-binding-changed",
            threadId: change.threadId,
          }),
        ),
      );

      yield* forkFeed(
        "orchestration-domain-events",
        Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => {
          switch (event.type) {
            case "thread.deleted":
              return signalWake({
                type: "thread-deleted",
                threadId: event.payload.threadId,
              });
            case "thread.reverted":
            case "thread.session-set":
            case "thread.turn-diff-completed":
              return signalWake({
                type: "orchestration-thread-changed",
                threadId: event.payload.threadId,
              });
            default:
              return Effect.void;
          }
        }),
      );

      yield* Effect.forkScoped(
        signalWake({ type: "reconcile-all", reason: "fallback-tick" }).pipe(
          Effect.delay(Duration.millis(fallbackReconcileIntervalMs)),
          Effect.forever,
        ),
      );

      yield* signalWake({ type: "startup" });
      let nextDeadlineAtMs: number | undefined = undefined;
      let observedStartupWake = false;

      while (true) {
        if (nextDeadlineAtMs === undefined) {
          const signal = yield* wake.await;
          observedStartupWake ||= signal.type === "startup";
          yield* recordWake(signal);
        } else {
          const waitMs = Math.max(0, nextDeadlineAtMs - (yield* Clock.currentTimeMillis));
          const signal = yield* wake.await.pipe(Effect.timeoutOption(Duration.millis(waitMs)));
          if (Option.isSome(signal)) {
            observedStartupWake ||= signal.value.type === "startup";
            yield* recordWake(signal.value, {
              deadlineAt: new Date(nextDeadlineAtMs).toISOString(),
              waitMs,
            });
          } else {
            yield* recordWake("timeout", {
              deadlineAt: new Date(nextDeadlineAtMs).toISOString(),
              waitMs,
            });
          }
        }

        const snapshotOption = yield* reconcileAuthoritativeState().pipe(
          Effect.map(Option.some),
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            return Effect.logWarning("provider.session.reaper.reconcile-failed", {
              mode,
              cause,
            }).pipe(Effect.as(Option.none<ReconcileSnapshot>()));
          }),
        );

        if (Option.isNone(snapshotOption)) {
          nextDeadlineAtMs = undefined;
          continue;
        }

        const snapshot = snapshotOption.value;
        const now = snapshot.reconciledAtMs;
        const dueEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs <= now);
        const futureEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs > now);

        yield* Metric.update(
          Metric.withAttributes(providerSessionReaperScheduleSize, metricAttributes({ mode })),
          futureEntries.length,
        );

        if (observedStartupWake && dueEntries.length > 0) {
          observedStartupWake = false;
          yield* Effect.logInfo("provider.session.reaper.overdue-on-startup", {
            mode,
            overdueCount: dueEntries.length,
            providers: providerBreakdown(dueEntries),
          });
        }

        if (dueEntries.length > 0) {
          yield* stopDueEntries({
            entries: dueEntries,
            nowMs: now,
          });
          nextDeadlineAtMs = undefined;
          continue;
        }

        observedStartupWake = false;
        const nextEntry = futureEntries[0];
        if (nextEntry === undefined) {
          nextDeadlineAtMs = undefined;
          continue;
        }

        nextDeadlineAtMs = nextEntry.deadlineAtMs;
        yield* Effect.logDebug("provider.session.reaper.next-deadline-selected", {
          ...buildReaperLogContext({
            now,
            inactivityThresholdMs,
            entry: nextEntry,
          }),
          mode,
          waitMs: Math.max(0, nextEntry.deadlineAtMs - now),
        });
      }
    });

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        const worker =
          mode === "polling"
            ? runPollingSweep.pipe(
                Effect.catchCause((cause) => {
                  if (Cause.hasInterruptsOnly(cause)) {
                    return Effect.failCause(cause);
                  }
                  return Effect.logWarning("provider.session.reaper.sweep-failed", {
                    mode,
                    cause,
                  });
                }),
                Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs))),
              )
            : runDeadlineScheduler;

        yield* Effect.forkScoped(worker);

        yield* Effect.logInfo("provider.session.reaper.scheduler-started", {
          mode,
          inactivityThresholdMs,
          sweepIntervalMs,
          fallbackReconcileIntervalMs,
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
