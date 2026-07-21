import { Daemon } from "./daemon";
import { SignalSource } from "./abort";
import { nanoid } from "nanoid";

/** * PartitionedDaemon is a partitioned event router backed by N Daemon instances, one per
 * partition. Events are routed to a partition by hashing a key extracted from each event.
 * Events with the same key always land on the same partition, preserving per-key ordering.
 * Events on different partitions are processed in parallel.
 *
 * This is analogous to Kafka's partition model: per-partition total order,
 * cross-partition parallelism.
 *
 * PartitionedDaemon builds each partition itself by calling `partitionFactory` once per
 * index — it owns every Daemon it ends up with, which is also why it's the one that closes
 * them. Since the factory is called fresh for each partition, each call can close over its
 * own local state (its own instance of some class, or nothing at all if partitions are
 * stateless); partitions never see each other's state unless you explicitly share
 * something between the calls yourself.
 *
 * @template E - The type of events to handle.
 * @template S - The signal source type (a raw AbortSignal or an object embedding one).
 * @param partitionFactory - Builds the Daemon for a given partition index. Called once per
 *   index, 0 through `partitionCount - 1`.
 * @param partitionCount - Number of partitions (and calls to `partitionFactory`).
 * @param keyExtractor - Maps an event to an integer key that determines its partition.
 */
export class PartitionedDaemon<E, S extends SignalSource> {
  readonly id = nanoid();
  private readonly partitions: Daemon<E, S>[];

  constructor(
      partitionFactory: (partitionIndex: number) => Daemon<E, S>,
      partitionCount: number,
      private readonly keyExtractor: (event: E) => number,
      ) {
    if (partitionCount <= 0) throw new Error("partitionCount must be greater than 0");
    this.partitions = Array.from({ length: partitionCount }, (_, i) => partitionFactory(i));
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
