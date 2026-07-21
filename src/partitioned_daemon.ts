import { Daemon } from "./daemon";
import { isSignalSource, SignalSource } from "./abort";
import { nanoid } from "nanoid";

/** * PartitionedDaemon is a partitioned event processor backed by N Daemon instances,
 * one per partition. Events are routed to a partition by hashing a key extracted from
 * each event. Events with the same key always land on the same partition, preserving
 * per-key ordering. Events on different partitions are processed in parallel.
 *
 * This is analogous to Kafka's partition model: per-partition total order,
 * cross-partition parallelism.
 *
 * @template E - The type of events to handle.
 * @template S - The signal source type (a raw AbortSignal or an object embedding one).
 * @param signalSource - An AbortSignal (or SignalSource) to allow cancellation of every partition's event loop.
 * @param handleEvent - A function that processes each event. Shared by every partition.
 * @param keyExtractor - Maps an event to an integer key that determines its partition.
 * @param partitionCount - Number of partitions (and backing Daemons). Defaults to 4.
 * @param bufferSizePerPartition - Queue capacity per partition; total memory is
 *   partitionCount x bufferSizePerPartition. Defaults to 10.
 * @param loopIntervalMs - Forwarded to each partition's Daemon. Defaults to 8ms.
 * @param strictInterval - Forwarded to each partition's Daemon. Defaults to false.
 */
export class PartitionedDaemon<E, S extends SignalSource> {
  readonly id = nanoid();
  private readonly partitions: Daemon<E, S>[];
  private readonly keyExtractor: (event: E) => number;

  constructor(
    signalSource: S,
    handleEvent: (signalSource: S, event: E) => Promise<void>,
    keyExtractor: (event: E) => number,
    partitionCount: number = 4,
    bufferSizePerPartition: number = 10,
    loopIntervalMs: number = 8,
    strictInterval: boolean = false
  ) {
    if (!isSignalSource(signalSource)) throw new Error("invalid signalSource");
    if (partitionCount <= 0) throw new Error("partitionCount must be greater than 0");

    this.keyExtractor = keyExtractor;
    this.partitions = Array.from(
      { length: partitionCount },
      () =>
        new Daemon<E, S>(
          signalSource,
          handleEvent,
          bufferSizePerPartition,
          loopIntervalMs,
          strictInterval
        )
    );
  }

  private partitionFor = (event: E): Daemon<E, S> => {
    const hash = this.keyExtractor(event);
    const count = this.partitions.length;
    const index = ((hash % count) + count) % count;
    return this.partitions[index];
  };

  /** * Pushes an event to the partition determined by the key extractor,
   * waiting if that partition's queue is full.
   * Returns true if the event was successfully pushed, false if that partition is closed.
   *
   * @param event - The event to push.
   */
  pushEvent = async (event: E): Promise<boolean> => {
    return this.partitionFor(event).pushEvent(event);
  };

  /** * Pushes an event to the partition determined by the key extractor without waiting.
   * Returns true if the event was successfully pushed, false if that partition's queue
   * is full or closed.
   *
   * @param event - The event to push.
   */
  tryPushEvent = (event: E): boolean => {
    return this.partitionFor(event).tryPushEvent(event);
  };

  /** * Closes every partition and waits for all of them to finish processing
   * their queued events.
   */
  close = async (): Promise<void> => {
    await Promise.all(this.partitions.map((p) => p.close()));
  };
}
