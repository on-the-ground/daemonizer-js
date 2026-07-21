import { describe, it, expect } from "@jest/globals";
import { PartitionedDaemon } from "../src/partitioned_daemon";

describe("PartitionedDaemon", () => {
  it("processes events and preserves per-key order across partitions", async () => {
    const handledByKey = new Map<number, number[]>();
    const controller = new AbortController();

    const handleFn = async (_signal: AbortSignal, event: { key: number; seq: number }) => {
      const list = handledByKey.get(event.key) ?? [];
      list.push(event.seq);
      handledByKey.set(event.key, list);
    };

    const daemon = new PartitionedDaemon<{ key: number; seq: number }, AbortSignal>(
      controller.signal,
      handleFn,
      (event) => event.key,
      3,
      5
    );

    for (let key = 0; key < 3; key++) {
      for (let seq = 0; seq < 5; seq++) {
        await daemon.pushEvent({ key, seq });
      }
    }

    await daemon.close();

    for (let key = 0; key < 3; key++) {
      expect(handledByKey.get(key)).toEqual([0, 1, 2, 3, 4]);
    }
  });

  it("routes keys beyond the 32-bit range consistently, without truncation", async () => {
    const handled: number[] = [];
    const controller = new AbortController();
    const bigKey = 2 ** 32 + 5; // exceeds Int32 range; would wrap under bitwise truncation

    const daemon = new PartitionedDaemon<{ key: number; seq: number }, AbortSignal>(
      controller.signal,
      async (_signal, event) => {
        handled.push(event.seq);
      },
      (event) => event.key,
      4,
      5
    );

    for (let seq = 0; seq < 5; seq++) {
      await daemon.pushEvent({ key: bigKey, seq });
    }

    await daemon.close();

    expect(handled).toEqual([0, 1, 2, 3, 4]);
  });

  it("routes negative keys to a valid partition instead of throwing", async () => {
    const handled: number[] = [];
    const controller = new AbortController();

    const daemon = new PartitionedDaemon<number, AbortSignal>(
      controller.signal,
      async (_signal, event) => {
        handled.push(event);
      },
      (event) => event,
      4,
      5
    );

    await daemon.pushEvent(-1);
    await daemon.close();

    expect(handled).toEqual([-1]);
  });

  it("stops accepting new events after close", async () => {
    const controller = new AbortController();
    const daemon = new PartitionedDaemon<number, AbortSignal>(
      controller.signal,
      async () => {},
      (event) => event,
      2,
      1
    );

    await daemon.pushEvent(1);
    await daemon.close();

    const result = await daemon.pushEvent(2);
    expect(result).toBe(false);
  });
});
