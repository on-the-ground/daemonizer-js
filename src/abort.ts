/**
 * A standard `DOMException` used to signal that an operation was aborted due to a timeout.
 *
 * This error uses the `"AbortError"` name so it integrates cleanly with `AbortSignal`-based cancellation.
 * It is returned by `withTimeout()` when the specified timeout duration elapses.
 *
 * @example
 * try {
 *   await withTimeout(doSomething, 1000);
 * } catch (err) {
 *   if (err === timeoutError) {
 *     console.warn("Operation timed out");
 *   }
 * }
 */
export const timeoutError = new DOMException("Timeout", "AbortError");

/**
 * Unique symbol used to identify embedded AbortSignal in an object.
 * Prevents key collisions when mixing custom objects with abort handling logic.
 */
export const SIGNAL_KEY = Symbol("abort-signal");

/**
 * A union type representing either a raw `AbortSignal` or an object containing an embedded signal.
 *
 * This abstraction allows flexible signal passing — either directly or wrapped within another structure.
 *
 * @example
 * const direct: SignalSource = new AbortController().signal;
 * const wrapped: SignalSource = { [SIGNAL_KEY]: new AbortController().signal };
 */
export type SignalSource = AbortSignal | { [SIGNAL_KEY]: AbortSignal };

export function isSignalSource(sigSrc: unknown): sigSrc is SignalSource {
  return (
    sigSrc instanceof AbortSignal ||
    (typeof sigSrc === "object" &&
      sigSrc !== null &&
      SIGNAL_KEY in sigSrc &&
      (sigSrc as any)[SIGNAL_KEY] instanceof AbortSignal)
  );
}

/**
 * Merges an external `AbortSignal` with an existing `SignalSource`.
 *
 * If the `sigSrc` is a raw signal, returns a new merged `AbortSignal`.
 * If the `sigSrc` is a wrapped signal object, returns a shallow clone with the embedded signal replaced
 * by the merged one.
 *
 * This is useful for combining timeout signals with externally provided cancellation sources.
 *
 * @param signal - An `AbortSignal` to merge in (e.g. from timeout or internal controller).
 * @param sigSrc - An existing signal or signal-carrying object to merge with.
 * @returns A new `SignalSource` of the same type as `sigSrc` with merged cancellation behavior.
 */
export function mergeAbortSignal<S extends SignalSource>(
  signal: AbortSignal,
  sigSrc: S
): S {
  if (sigSrc instanceof AbortSignal) {
    return mergeAbortSignals([sigSrc, signal]) as S;
  }
  const mergedSignal = mergeAbortSignals([sigSrc[SIGNAL_KEY], signal]);
  const newSigSrc = Object.create(sigSrc);
  newSigSrc[SIGNAL_KEY] = mergedSignal;
  return newSigSrc as S;
}

/**
 * Extracts the `AbortSignal` from a `SignalSource`, whether it's direct or embedded.
 *
 * @param sigSrc - A `SignalSource`, either a raw signal or an object containing one.
 * @returns The corresponding `AbortSignal`.
 */
export function signalFrom(sigSrc: SignalSource): AbortSignal {
  return sigSrc instanceof AbortSignal ? sigSrc : sigSrc[SIGNAL_KEY];
}

/** * Wraps a promise with an AbortSignal to allow it to be aborted.
 * If the signal is already aborted, it rejects immediately.
 * If the signal aborts while the promise is pending, it rejects with an ErrAborted error.
 *
 * @param signal - An AbortSignal to monitor for abortion.
 * @returns A function that takes a promise and returns a new promise that can be aborted.
 */
export const withAbort = <T>(
  promise: Promise<T>,
  signal?: AbortSignal
): Promise<T> => {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(signal.reason);

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });

    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      }
    );
  });
};

/**
 * Runs an async operation with a timeout and optional external AbortSignal.
 *
 * If the operation does not complete within the given timeout,
 * it is aborted with a `timeoutError`. If an external `AbortSignal` is provided
 * and it fires first, the operation is also aborted with that signal's reason.
 *
 * Internally, it merges the external signal (if provided) with an internal timeout-based signal.
 * Then it wraps the callback execution with `withAbort()` to ensure proper cancellation handling.
 *
 * @template T - The type of the resolved value.
 * @param callback - An async function that accepts an `AbortSignal` and returns a `Promise<T>`.
 *                   This function is expected to respect the signal and abort early if triggered.
 * @param timeout - Timeout in milliseconds after which the operation will be aborted.
 * @param extSigSrc - Optional `AbortSignal` to combine with the timeout signal.
 *                         Abortion from either will cancel the operation.
 * @returns A `Promise<T>` that resolves if the callback completes within time,
 *          or rejects with `timeoutError` or the external signal's reason.
 */
export const withTimeout = async <T, S extends SignalSource>(
  callback: (signal: S) => Promise<T>,
  timeout: number,
  extSigSrc?: S
): Promise<T> => {
  const timeoutController = new AbortController();
  const timeoutId = setTimeout(() => {
    timeoutController.abort(timeoutError);
  }, timeout);

  try {
    if (!extSigSrc) {
      return await withAbort(
        callback(timeoutController.signal as S),
        timeoutController.signal
      );
    }
    const newSigSrc = mergeAbortSignal(timeoutController.signal, extSigSrc);
    return await withAbort(callback(newSigSrc), signalFrom(newSigSrc));
  } finally {
    clearTimeout(timeoutId);
  }
};

function mergeAbortSignals(signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();

  const onAbort = () => {
    for (const signal of signals) {
      if (signal.aborted) {
        controller.abort(signal.reason);
        break;
      }
    }
    for (const signal of signals) {
      signal.removeEventListener("abort", onAbort);
    }
  };

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      for (const s of signals) {
        s.removeEventListener("abort", onAbort);
      }
      return controller.signal;
    }
    signal.addEventListener("abort", onAbort);
  }

  return controller.signal;
}
