/**
 * Structured JSON logger — replaces Cloudflare Workers logger.
 *
 * Same interface and output format as the CF version, but runs on Node.js.
 * Outputs flat JSON lines to stdout/stderr for container log aggregation
 * (kubectl logs, Loki, Datadog, etc.).
 */

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 } as const;
type LogLevel = keyof typeof LEVELS;

const RESERVED_KEYS = new Set(["level", "component", "msg", "ts", "service", "event"]);

const CONSOLE_METHOD: Record<LogLevel, "log" | "warn" | "error"> = {
  debug: "log",
  info: "log",
  warn: "warn",
  error: "error",
};

export interface Logger {
  debug(msg: string, data?: Record<string, unknown>): void;
  info(msg: string, data?: Record<string, unknown>): void;
  warn(msg: string, data?: Record<string, unknown>): void;
  error(msg: string, data?: Record<string, unknown>): void;
  child(context: Record<string, unknown>): Logger;
}

function stripReserved(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj)) {
    if (!RESERVED_KEYS.has(key)) {
      out[key] = obj[key];
    }
  }
  return out;
}

export function createLogger(
  component: string,
  context: Record<string, unknown> = {},
  minLevel: LogLevel = "info"
): Logger {
  const safeContext = stripReserved(context);

  const emit = (level: LogLevel, msg: string, data?: Record<string, unknown>) => {
    if (LEVELS[level] < LEVELS[minLevel]) return;

    const extra: Record<string, unknown> = data ? stripReserved(data) : {};
    if (extra.error instanceof Error) {
      const err = extra.error;
      extra.error_message = err.message;
      extra.error_stack = err.stack;
      extra.error_type = err.constructor.name;
      delete extra.error;
    }

    try {
      console[CONSOLE_METHOD[level]](
        JSON.stringify({
          level,
          service: "control-plane-k8s",
          component,
          msg,
          ...safeContext,
          ...extra,
          ts: Date.now(),
        })
      );
    } catch {
      console.error(
        JSON.stringify({
          level: "error",
          service: "control-plane-k8s",
          component,
          msg: "LOG_SERIALIZE_FAILURE",
          original_msg: msg,
          original_level: level,
          ts: Date.now(),
        })
      );
    }
  };

  return {
    debug: (msg, data) => emit("debug", msg, data),
    info: (msg, data) => emit("info", msg, data),
    warn: (msg, data) => emit("warn", msg, data),
    error: (msg, data) => emit("error", msg, data),
    child: (childCtx) =>
      createLogger(component, { ...safeContext, ...stripReserved(childCtx) }, minLevel),
  };
}

export function parseLogLevel(value?: string): LogLevel {
  if (value && Object.prototype.hasOwnProperty.call(LEVELS, value)) return value as LogLevel;
  return "info";
}
