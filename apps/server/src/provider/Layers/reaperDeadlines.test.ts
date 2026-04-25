import { MessageId, ProjectId, ThreadId, TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { deriveReapEntries } from "./reaperDeadlines.ts";

const defaultModelSelection = {
  provider: "codex",
  model: "gpt-5-codex",
} as const;

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
    readonly session?: {
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
  const now = "2026-04-20T12:00:00.000Z";
  const projectId = ProjectId.make("project-provider-reaper-deadlines");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Deadlines Project",
        workspaceRoot: "/tmp/provider-reaper-deadlines",
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
      session: thread.session ?? null,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
    })),
  };
}

describe("deriveReapEntries", () => {
  it("prefers latestTurn.completedAt over newer runtime metadata", () => {
    const threadId = ThreadId.make("thread-anchor-completed");
    const turnId = TurnId.make("turn-anchor-completed");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "completed",
            requestedAt: "2026-04-20T10:00:00.000Z",
            startedAt: "2026-04-20T10:01:00.000Z",
            completedAt: "2026-04-20T10:02:00.000Z",
            assistantMessageId: null,
          },
        },
      ]),
      bindings: [
        {
          threadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:00:00.000Z",
          runtimeMode: "full-access",
        },
      ],
    });

    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]).toMatchObject({
      anchorAt: "2026-04-20T10:02:00.000Z",
      anchorSource: "latest_turn_completed_at",
      deadlineBasisAt: "2026-04-20T10:02:00.000Z",
      deadlineBasisSource: "anchor",
    });
  });

  it("falls back to startedAt, then requestedAt, then lastSeenAt", () => {
    const startedThreadId = ThreadId.make("thread-anchor-started");
    const requestedThreadId = ThreadId.make("thread-anchor-requested");
    const lastSeenThreadId = ThreadId.make("thread-anchor-last-seen");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: startedThreadId,
          latestTurn: {
            turnId: TurnId.make("turn-anchor-started"),
            state: "running",
            requestedAt: "2026-04-20T10:00:00.000Z",
            startedAt: "2026-04-20T10:01:00.000Z",
            completedAt: null,
            assistantMessageId: null,
          },
        },
        {
          id: requestedThreadId,
          latestTurn: {
            turnId: TurnId.make("turn-anchor-requested"),
            state: "running",
            requestedAt: "2026-04-20T10:05:00.000Z",
            startedAt: null,
            completedAt: null,
            assistantMessageId: null,
          },
        },
      ]),
      bindings: [
        {
          threadId: startedThreadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:00:00.000Z",
          runtimeMode: "full-access",
        },
        {
          threadId: requestedThreadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:05:00.000Z",
          runtimeMode: "full-access",
        },
        {
          threadId: lastSeenThreadId,
          provider: "claudeAgent",
          status: "running",
          lastSeenAt: "2026-04-20T11:10:00.000Z",
          runtimeMode: "full-access",
        },
      ],
    });

    expect(result.entries).toMatchObject([
      {
        threadId: startedThreadId,
        anchorAt: "2026-04-20T10:01:00.000Z",
        anchorSource: "latest_turn_started_at",
      },
      {
        threadId: requestedThreadId,
        anchorAt: "2026-04-20T10:05:00.000Z",
        anchorSource: "latest_turn_requested_at",
      },
      {
        threadId: lastSeenThreadId,
        anchorAt: "2026-04-20T11:10:00.000Z",
        anchorSource: "session_last_seen_at",
      },
    ]);
  });

  it("filters stopped bindings and active turns", () => {
    const stoppedThreadId = ThreadId.make("thread-stopped");
    const activeThreadId = ThreadId.make("thread-active");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: activeThreadId,
          session: {
            threadId: activeThreadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: TurnId.make("turn-active"),
            lastError: null,
            updatedAt: "2026-04-20T10:00:00.000Z",
          },
        },
      ]),
      bindings: [
        {
          threadId: stoppedThreadId,
          provider: "codex",
          status: "stopped",
          lastSeenAt: "2026-04-20T09:00:00.000Z",
          runtimeMode: "full-access",
        },
        {
          threadId: activeThreadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T09:05:00.000Z",
          runtimeMode: "full-access",
        },
      ],
    });

    expect(result.entries).toHaveLength(0);
    expect(result.skippedStopped).toBe(1);
    expect(result.skippedActiveTurn).toBe(1);
  });

  it("reports invalid anchors and skips scheduling them", () => {
    const threadId = ThreadId.make("thread-invalid-anchor");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId: TurnId.make("turn-invalid-anchor"),
            state: "completed",
            requestedAt: "2026-04-20T10:00:00.000Z",
            startedAt: "2026-04-20T10:01:00.000Z",
            completedAt: "not-a-date",
            assistantMessageId: null,
          },
        },
      ]),
      bindings: [
        {
          threadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:00:00.000Z",
          runtimeMode: "full-access",
        },
      ],
    });

    expect(result.entries).toHaveLength(0);
    expect(result.invalidAnchors).toMatchObject([
      {
        threadId,
        inactivityAnchorAt: "not-a-date",
        inactivityAnchorSource: "latest_turn_completed_at",
      },
    ]);
  });

  it("uses a recent provider.sendTurn timestamp as a deadline floor", () => {
    const threadId = ThreadId.make("thread-send-turn-floor");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId: TurnId.make("turn-send-turn-floor"),
            state: "completed",
            requestedAt: "2026-04-20T09:00:00.000Z",
            startedAt: "2026-04-20T09:01:00.000Z",
            completedAt: "2026-04-20T09:02:00.000Z",
            assistantMessageId: null,
          },
        },
      ]),
      bindings: [
        {
          threadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:00:00.000Z",
          runtimeMode: "full-access",
          runtimePayload: {
            lastRuntimeEvent: "provider.sendTurn",
            lastRuntimeEventAt: "2026-04-20T11:30:00.000Z",
          },
        },
      ],
    });

    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]).toMatchObject({
      anchorAt: "2026-04-20T09:02:00.000Z",
      anchorSource: "latest_turn_completed_at",
      deadlineBasisAt: "2026-04-20T11:30:00.000Z",
      deadlineBasisSource: "recent_provider_send_turn",
      deadlineAtMs: Date.parse("2026-04-20T11:30:00.000Z") + 1_000,
    });
  });

  it("ignores invalid or unrelated runtime metadata for deadline floors", () => {
    const threadId = ThreadId.make("thread-ignored-floor");
    const result = deriveReapEntries({
      inactivityThresholdMs: 1_000,
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId: TurnId.make("turn-ignored-floor"),
            state: "completed",
            requestedAt: "2026-04-20T09:00:00.000Z",
            startedAt: "2026-04-20T09:01:00.000Z",
            completedAt: "2026-04-20T09:02:00.000Z",
            assistantMessageId: null,
          },
        },
      ]),
      bindings: [
        {
          threadId,
          provider: "codex",
          status: "running",
          lastSeenAt: "2026-04-20T11:00:00.000Z",
          runtimeMode: "full-access",
          runtimePayload: {
            lastRuntimeEvent: "provider.stopAll",
            lastRuntimeEventAt: "not-a-date",
          },
        },
      ],
    });

    expect(result.entries).toHaveLength(1);
    expect(result.entries[0]).toMatchObject({
      deadlineBasisAt: "2026-04-20T09:02:00.000Z",
      deadlineBasisSource: "anchor",
    });
  });
});
