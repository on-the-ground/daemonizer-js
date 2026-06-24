import { launchEventLoop } from "./event_loop";
import { BoundedQueue } from "./bounded_queue";
import { TaskGroup } from "./task_group";
import { isSignalSource, SignalSource } from "./abort";
import { nanoid } from "nanoid";

/** * Daemon is a utility class that manages an event loop processing events
 * from a bounded queue. It allows pushing events to the queue and handles them
 * asynchronously using a provided handler function.
 * It supports graceful shutdown and ensures that all events are processed before closing.
 *
 * @template E - The type of events to handle.
 * @param signal - An AbortSignal to allow cancellation of the event loop.
 * @param handleEvent - A function that processes each event. It should return a
 * Promise that resolves when the event is handled.
 * @param bufferSize - The size of the bounded queue buffer. Defaults to 10.
 * @param loopIntervalMs - Interval in milliseconds used for two purposes:
 *   (1) to periodically yield control to the JS event loop for fairness,
 *   (2) if `strictInterval` is enabled, to enforce a per-event processing timeout.
 *   Defaults to 8ms (roughly 60fps frame budget).
 * @param strictInterval - If true, each event handler must complete within
 *   `loopIntervalMs`, otherwise it is aborted. If false, handlers may take longer
 *   but yielding still happens at the interval. Defaults to false.
 */
export class Daemon<E, S extends SignalSource> {
  readonly id = nanoid();
  readonly tg: TaskGroup;
  readonly eventStream: BoundedQueue<E>;
  constructor(
    signalSource: S,
    handleEvent: (signalSource: S, event: E) => Promise<void>,
    bufferSize: number = 10,
    loopIntervalMs: number = 8,
    strictInterval: boolean = false
  ) {
    if (!isSignalSource(signalSource)) throw new Error("invalid signalSource");
    const tg = new TaskGroup();
    const eventStream = new BoundedQueue<E>(bufferSize);
    // launchEventLoop is intentionally fire-and-forget.
    // The lifecycle is tracked via the provided TaskGroup.
    launchEventLoop(
      signalSource,
      tg,
      eventStream,
      handleEvent,
      loopIntervalMs,
      strictInterval
    );
    this.eventStream = eventStream;
    this.tg = tg;
  }

  /** * Closes the event handler, stopping it from accepting new events.
   * It waits for the event loop to finish processing all events before resolving.
   */
  close = async () => {
    this.eventStream.close();
    // Wait for the event loop to finish processing.
    await this.tg.wait();
  };

  /** * Pushes an event to the event stream.
   * Returns true if the event was successfully pushed, false if the event handler is closed.
   *
   * @param event - The event to push to the event stream.
   */
  pushEvent = async (event: E): Promise<boolean> => {
    return this.eventStream.push(event);
  };
}
