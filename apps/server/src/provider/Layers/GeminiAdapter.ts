import {
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ThreadTokenUsageSnapshot,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  ThreadId,
  TurnId,
  ProviderApprovalDecision,
  EventId,
} from "@t3tools/contracts";
import { Effect, Layer, Stream, Option, Chunk, PubSub, Ref, Fiber } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { GeminiAdapter } from "../Services/GeminiAdapter";
import { ServerSettingsService } from "../../serverSettings";
import { ProviderAdapterProcessError } from "../Errors";
import { parseGenericCliVersion } from "../providerSnapshot";

const makeGeminiAdapter = Effect.gen(function* () {
  const settings = yield* ServerSettingsService;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const getGeminiBinary = Effect.map(settings.getSettings, (s) => s.providers.geminiAgent.binaryPath);

  return {
    provider: "geminiAgent" as const,
    capabilities: {
      sessionModelSwitch: "in-session" as const,
    },

    startSession: (input: any) => Effect.gen(function* () {
      return {
        provider: "geminiAgent" as const,
        threadId: input.threadId,
        cwd: input.cwd ?? "/",
        providerSessionId: `gemini-session-${input.threadId}`,
      };
    }),

    sendTurn: (input: any) => Effect.gen(function* () {
      const binaryPath = yield* getGeminiBinary;
      const pubsub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

      const modelId = input.modelSelection.model;
      const options = input.modelSelection.options as any;

      const args = ["-p", input.input];
      if (modelId) {
        args.push("-m", modelId);
      }
      if (options?.approvalMode) {
        args.push("--approval-mode", options.approvalMode);
      }

      const process = ChildProcess.make(binaryPath, args, {
        cwd: input.cwd ?? "/",
        shell: true,
      });

      const fiber = yield* Effect.fork(Effect.gen(function* () {
        const p = yield* spawner.spawn(process);

        yield* PubSub.publish(pubsub, {
          sequence: 0,
          eventId: EventId.makeUnsafe("ev_gemini_0"),
          type: "turn.started",
          turnId: input.turnId,
          occurredAt: new Date().toISOString(),
          provider: "geminiAgent",
          payload: {
            mode: "completion"
          }
        });

        // Simulating the output since gemini just returns stdout
        const stdoutStream = Stream.fromAsyncIterable(p.stdout);
        yield* Stream.runForEach(stdoutStream, (chunk: any) => {
          const text = new TextDecoder().decode(chunk);
          return PubSub.publish(pubsub, {
            sequence: 0,
            eventId: EventId.makeUnsafe(`ev_gemini_${Date.now()}`),
            type: "item.lifecycle",
            turnId: input.turnId,
            occurredAt: new Date().toISOString(),
            provider: "geminiAgent",
            payload: {
              itemId: RuntimeItemId.makeUnsafe(`item-${Date.now()}`),
              itemType: "assistant_message",
              status: "running",
              data: { text },
            },
          });
        });

        yield* PubSub.publish(pubsub, {
          sequence: 0,
          eventId: EventId.makeUnsafe(`ev_gemini_${Date.now()}`),
          type: "turn.completed",
          turnId: input.turnId,
          occurredAt: new Date().toISOString(),
          provider: "geminiAgent",
          status: "completed" as ProviderRuntimeTurnStatus,
        });

        yield* PubSub.shutdown(pubsub);
      }));

      return Stream.fromPubSub(pubsub);
    }),

    interruptTurn: (threadId: any, turnId?: any) => Effect.succeed(undefined),

    respondToRequest: (threadId: any, requestId: any, decision: any) => Effect.succeed(undefined),

    respondToUserInput: (threadId: any, requestId: any, answers: any) => Effect.succeed(undefined),

    stopSession: (threadId: any) => Effect.succeed(undefined),

    listSessions: () => Effect.gen(function* () {
      const binaryPath = yield* getGeminiBinary;
      const process = ChildProcess.make(binaryPath, ["--list-sessions"], {
        shell: true,
      });
      const p = yield* spawner.spawn(process);
      const stdoutStream = Stream.fromAsyncIterable(p.stdout);
      // Let's just collect it but keep it simple as dummy
      return [];
    }),

    hasSession: (threadId: any) => Effect.succeed(true),

    readThread: (threadId: any, toTurnId?: any) => Effect.fail(new ProviderAdapterProcessError({
      provider: "geminiAgent",
      threadId,
      detail: "readThread not supported",
    })),

    rollbackThread: (threadId: any, toTurnId?: any) => Effect.fail(new ProviderAdapterProcessError({
      provider: "geminiAgent",
      threadId,
      detail: "rollbackThread not supported",
    })),

    stopAll: () => Effect.succeed(undefined),

    streamEvents: Stream.empty,
  };
});

export const GeminiAdapterLive = Layer.effect(GeminiAdapter, makeGeminiAdapter);
