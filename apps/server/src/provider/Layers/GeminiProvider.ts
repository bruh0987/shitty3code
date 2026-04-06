import type {
  GeminiSettings,
  ModelCapabilities,
  ServerProvider,
  ServerProviderModel,
  ServerProviderAuth,
  ServerProviderState,
} from "@t3tools/contracts";
import { Cache, Duration, Effect, Equal, Layer, Option, Result, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { decodeJsonResult } from "@t3tools/shared/schemaJson";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  extractAuthBoolean,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type CommandResult,
} from "../providerSnapshot";
import { makeManagedServerProvider } from "../makeManagedServerProvider";
import { GeminiProvider } from "../Services/GeminiProvider";
import { ServerSettingsService } from "../../serverSettings";
import { ServerSettingsError } from "@t3tools/contracts";

const PROVIDER = "geminiAgent" as const;
const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "gemini-3-flash-preview",
    name: "Gemini 3 Flash",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "gemini-2.5-flash",
    name: "Gemini 2.5 Flash",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "gemini-2.5-pro",
    name: "Gemini 2.5 Pro",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "gemini-3.1-flash-lite-preview",
    name: "Gemini 3.1 Flash Lite",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "gemini-3.1-pro-preview",
    name: "Gemini 3.1 Pro",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "gemini-3-pro-preview",
    name: "Gemini 3 Pro",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
];

export function getGeminiModelCapabilities(model: string | null | undefined): ModelCapabilities {
  const slug = model?.trim();
  return (
    BUILT_IN_MODELS.find((candidate) => candidate.slug === slug)?.capabilities ?? {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    }
  );
}

export function parseGeminiAuthStatusFromOutput(result: CommandResult): {
  readonly status: Exclude<ServerProviderState, "disabled">;
  readonly auth: Pick<ServerProviderAuth, "status">;
  readonly message?: string;
} {
  const lowerOutput = `${result.stdout}\n${result.stderr}`.toLowerCase();

  if (
    lowerOutput.includes("unknown command") ||
    lowerOutput.includes("unrecognized command") ||
    lowerOutput.includes("unexpected argument")
  ) {
    return {
      status: "warning",
      auth: { status: "unknown" },
      message:
        "Gemini Agent authentication status command is unavailable in this version of Gemini.",
    };
  }

  if (
    lowerOutput.includes("not logged in") ||
    lowerOutput.includes("login required") ||
    lowerOutput.includes("authentication required") ||
    lowerOutput.includes("run `gemini login`") ||
    lowerOutput.includes("run gemini login")
  ) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: "Gemini is not authenticated. Run `gemini auth login` and try again.",
    };
  }

  const parsedAuth = (() => {
    const trimmed = result.stdout.trim();
    if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) {
      return { attemptedJsonParse: false as const, auth: undefined as boolean | undefined };
    }
    try {
      return {
        attemptedJsonParse: true as const,
        auth: extractAuthBoolean(JSON.parse(trimmed)),
      };
    } catch {
      return { attemptedJsonParse: false as const, auth: undefined as boolean | undefined };
    }
  })();

  if (parsedAuth.auth === true) {
    return { status: "ready", auth: { status: "authenticated" } };
  }
  if (parsedAuth.auth === false) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: "Gemini is not authenticated. Run `gemini auth login` and try again.",
    };
  }
  if (parsedAuth.attemptedJsonParse) {
    return {
      status: "warning",
      auth: { status: "unknown" },
      message:
        "Could not verify Gemini authentication status from JSON output (missing auth marker).",
    };
  }
  if (result.code === 0) {
    return { status: "ready", auth: { status: "authenticated" } };
  }

  const detail = detailFromResult(result);
  return {
    status: "warning",
    auth: { status: "unknown" },
    message: detail
      ? `Could not verify Gemini authentication status. ${detail}`
      : "Could not verify Gemini authentication status.",
  };
}

// ── Subscription type detection ─────────────────────────────────────
//
// The SDK probe returns typed `AccountInfo.subscriptionType` directly.
// This walker is a best-effort fallback for the `gemini auth status`
// JSON output whose shape is not guaranteed.

/** Keys that directly hold a subscription/plan identifier. */
const SUBSCRIPTION_TYPE_KEYS = [
  "subscriptionType",
  "subscription_type",
  "plan",
  "tier",
  "planType",
  "plan_type",
] as const;

/** Keys whose value may be a nested object containing subscription info. */
const SUBSCRIPTION_CONTAINER_KEYS = ["account", "subscription", "user", "billing"] as const;
const AUTH_METHOD_KEYS = ["authMethod", "auth_method"] as const;
const AUTH_METHOD_CONTAINER_KEYS = ["auth", "account", "session"] as const;

/** Lift an unknown value into `Option<string>` if it is a non-empty string. */
const asNonEmptyString = (v: unknown): Option.Option<string> =>
  typeof v === "string" && v.length > 0 ? Option.some(v) : Option.none();

/** Lift an unknown value into `Option<Record>` if it is a plain object. */
const asRecord = (v: unknown): Option.Option<Record<string, unknown>> =>
  typeof v === "object" && v !== null && !globalThis.Array.isArray(v)
    ? Option.some(v as Record<string, unknown>)
    : Option.none();

/**
 * Walk an unknown parsed JSON value looking for a subscription/plan
 * identifier, returning the first match as an `Option`.
 */
function findSubscriptionType(value: unknown): Option.Option<string> {
  if (globalThis.Array.isArray(value)) {
    return Option.firstSomeOf(value.map(findSubscriptionType));
  }

  return asRecord(value).pipe(
    Option.flatMap((record) => {
      const direct = Option.firstSomeOf(
        SUBSCRIPTION_TYPE_KEYS.map((key) => asNonEmptyString(record[key])),
      );
      if (Option.isSome(direct)) return direct;

      return Option.firstSomeOf(
        SUBSCRIPTION_CONTAINER_KEYS.map((key) =>
          asRecord(record[key]).pipe(Option.flatMap(findSubscriptionType)),
        ),
      );
    }),
  );
}

function findAuthMethod(value: unknown): Option.Option<string> {
  if (globalThis.Array.isArray(value)) {
    return Option.firstSomeOf(value.map(findAuthMethod));
  }

  return asRecord(value).pipe(
    Option.flatMap((record) => {
      const direct = Option.firstSomeOf(
        AUTH_METHOD_KEYS.map((key) => asNonEmptyString(record[key])),
      );
      if (Option.isSome(direct)) return direct;

      return Option.firstSomeOf(
        AUTH_METHOD_CONTAINER_KEYS.map((key) =>
          asRecord(record[key]).pipe(Option.flatMap(findAuthMethod)),
        ),
      );
    }),
  );
}

/**
 * Try to extract a subscription type from the `gemini auth status` JSON
 * output. This is a zero-cost operation on data we already have.
 */
const decodeUnknownJson = decodeJsonResult(Schema.Unknown);

function extractSubscriptionTypeFromOutput(result: CommandResult): string | undefined {
  const parsed = decodeUnknownJson(result.stdout.trim());
  if (Result.isFailure(parsed)) return undefined;
  return Option.getOrUndefined(findSubscriptionType(parsed.success));
}

function extractGeminiAuthMethodFromOutput(result: CommandResult): string | undefined {
  const parsed = decodeUnknownJson(result.stdout.trim());
  if (Result.isFailure(parsed)) return undefined;
  return Option.getOrUndefined(findAuthMethod(parsed.success));
}

// ── Dynamic model capability adjustment ─────────────────────────────

/** Subscription types where the 1M context window is included in the plan. */
const PREMIUM_SUBSCRIPTION_TYPES = new Set([
  "max",
  "maxplan",
  "max5",
  "max20",
  "enterprise",
  "team",
]);

function toTitleCaseWords(value: string): string {
  return value
    .split(/[\s_-]+/g)
    .filter(Boolean)
    .map((part) => part[0]!.toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function geminiSubscriptionLabel(subscriptionType: string | undefined): string | undefined {
  const normalized = subscriptionType?.toLowerCase().replace(/[\s_-]+/g, "");
  if (!normalized) return undefined;

  switch (normalized) {
    case "max":
    case "maxplan":
    case "max5":
    case "max20":
      return "Max";
    case "enterprise":
      return "Enterprise";
    case "team":
      return "Team";
    case "pro":
      return "Pro";
    case "free":
      return "Free";
    default:
      return toTitleCaseWords(subscriptionType!);
  }
}

function normalizeGeminiAuthMethod(authMethod: string | undefined): string | undefined {
  const normalized = authMethod?.toLowerCase().replace(/[\s_-]+/g, "");
  if (!normalized) return undefined;
  if (normalized === "apikey") return "apiKey";
  return undefined;
}

function geminiAuthMetadata(input: {
  readonly subscriptionType: string | undefined;
  readonly authMethod: string | undefined;
}): { readonly type: string; readonly label: string } | undefined {
  if (normalizeGeminiAuthMethod(input.authMethod) === "apiKey") {
    return {
      type: "apiKey",
      label: "Gemini API Key",
    };
  }

  if (input.subscriptionType) {
    const subscriptionLabel = geminiSubscriptionLabel(input.subscriptionType);
    return {
      type: input.subscriptionType,
      label: `Gemini ${subscriptionLabel ?? toTitleCaseWords(input.subscriptionType)} Subscription`,
    };
  }

  return undefined;
}

/**
 * Adjust the built-in model list based on the user's detected subscription.
 *
 * - Premium tiers (Max, Enterprise, Team): 1M context becomes the default.
 * - Other tiers (Pro, free, unknown): 200k context stays the default;
 *   1M remains available as a manual option so users can still enable it.
 */
export function adjustModelsForSubscription(
  baseModels: ReadonlyArray<ServerProviderModel>,
  subscriptionType: string | undefined,
): ReadonlyArray<ServerProviderModel> {
  const normalized = subscriptionType?.toLowerCase().replace(/[\s_-]+/g, "");
  if (!normalized || !PREMIUM_SUBSCRIPTION_TYPES.has(normalized)) {
    return baseModels;
  }

  // Flip 1M to be the default for premium users
  return baseModels.map((model) => {
    const caps = model.capabilities;
    if (!caps || caps.contextWindowOptions.length === 0) return model;

    return {
      ...model,
      capabilities: {
        ...caps,
        contextWindowOptions: caps.contextWindowOptions.map((opt) =>
          opt.value === "1m"
            ? { value: opt.value, label: opt.label, isDefault: true as const }
            : { value: opt.value, label: opt.label },
        ),
      },
    };
  });
}

const runGeminiCommand = Effect.fn("runGeminiCommand")(function* (args: ReadonlyArray<string>) {
  const geminiSettings = yield* Effect.service(ServerSettingsService).pipe(
    Effect.flatMap((service) => service.getSettings),
    Effect.map((settings) => settings.providers.geminiAgent),
  );
  const command = ChildProcess.make(geminiSettings.binaryPath, [...args], {
    shell: process.platform === "win32",
  });
  return yield* spawnAndCollect(geminiSettings.binaryPath, command);
});

export const checkGeminiProviderStatus = Effect.fn("checkGeminiProviderStatus")(function* (
  resolveSubscriptionType?: (binaryPath: string) => Effect.Effect<string | undefined>,
): Effect.fn.Return<
  ServerProvider,
  ServerSettingsError,
  ChildProcessSpawner.ChildProcessSpawner | ServerSettingsService
> {
  const geminiSettings = yield* Effect.service(ServerSettingsService).pipe(
    Effect.flatMap((service) => service.getSettings),
    Effect.map((settings) => settings.providers.geminiAgent),
  );
  const checkedAt = new Date().toISOString();
  const models = providerModelsFromSettings(BUILT_IN_MODELS, PROVIDER, geminiSettings.customModels);

  if (!geminiSettings.enabled) {
    return buildServerProvider({
      provider: PROVIDER,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Gemini is disabled in T3 Code settings.",
      },
    });
  }

  const versionProbe = yield* runGeminiCommand(["--version"]).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      provider: PROVIDER,
      enabled: geminiSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Gemini Agent CLI (`gemini`) is not installed or not on PATH."
          : `Failed to execute Gemini Agent CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      provider: PROVIDER,
      enabled: geminiSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message:
          "Gemini Agent CLI is installed but failed to run. Timed out while running command.",
      },
    });
  }

  const version = versionProbe.success.value;
  const parsedVersion = parseGenericCliVersion(`${version.stdout}\n${version.stderr}`);
  if (version.code !== 0) {
    const detail = detailFromResult(version);
    return buildServerProvider({
      provider: PROVIDER,
      enabled: geminiSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: parsedVersion,
        status: "error",
        auth: { status: "unknown" },
        message: detail
          ? `Gemini Agent CLI is installed but failed to run. ${detail}`
          : "Gemini Agent CLI is installed but failed to run.",
      },
    });
  }

  return buildServerProvider({
    provider: PROVIDER,
    enabled: geminiSettings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version: parsedVersion,
      status: "ready",
      auth: {
        status: "unknown",
      },
    },
  });
});

export const GeminiProviderLive = Layer.effect(
  GeminiProvider,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const checkProvider = checkGeminiProviderStatus().pipe(
      Effect.provideService(ServerSettingsService, serverSettings),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

    return yield* makeManagedServerProvider<GeminiSettings>({
      getSettings: serverSettings.getSettings.pipe(
        Effect.map((settings) => settings.providers.geminiAgent),
        Effect.orDie,
      ),
      streamSettings: serverSettings.streamChanges.pipe(
        Stream.map((settings) => settings.providers.geminiAgent),
      ),
      haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
      checkProvider,
    });
  }),
);
