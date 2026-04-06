/**
 * GeminiAdapter - Gemini Agent implementation of the generic provider adapter contract.
 *
 * This service owns Gemini runtime/session semantics and emits canonical
 * provider runtime events. It does not perform cross-provider routing, shared
 * event fan-out, or checkpoint orchestration.
 *
 * Uses Effect `ServiceMap.Service` for dependency injection and returns the
 * shared provider-adapter error channel with `provider: "geminiAgent"` context.
 *
 * @module GeminiAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * GeminiAdapterShape - Service API for the Gemini Agent provider adapter.
 */
export interface GeminiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "geminiAgent";
}

/**
 * GeminiAdapter - Service tag for Gemini Agent provider adapter operations.
 */
export class GeminiAdapter extends ServiceMap.Service<GeminiAdapter, GeminiAdapterShape>()(
  "t3/provider/Services/GeminiAdapter",
) {}
