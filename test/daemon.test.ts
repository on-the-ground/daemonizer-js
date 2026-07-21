import { describe, it, expect } from "@jest/globals";
import { Daemon } from "../src/daemon";

describe("Daemon", () => {
  it("processes events correctly and shuts down", async () => {
    const handled: number[] = [];
    const controller = new AbortController();

    const handleFn = async (_signal: AbortSignal, event: number) => {
      handled.push(event);
    };

    const daemon = new Daemon<number, AbortSignal>(
      controller.signal,
      handleFn,
      3
    );

    await daemon.pushEvent(1);
    await daemon.pushEvent(2);
    await daemon.pushEvent(3);

    // Give the loop some time to process the events
    await new Promise((r) => setTimeout(r, 10));

    await daemon.close();

    expect(handled).toEqual([1, 2, 3]);
  });

  it("stops accepting new events after close", async () => {
    const abortController = new AbortController();
    const daemon = new Daemon<number, AbortSignal>(
      abortController.signal,
      async () => {},
      1
    );

    await daemon.pushEvent(1);
    await daemon.close();

    const result = await daemon.pushEvent(2);
    expect(result).toBe(false);
  });

  it("tryPushEvent accepts when there is room and rejects when full or closed", async () => {
    const abortController = new AbortController();
    const daemon = new Daemon<number, AbortSignal>(
      abortController.signal,
      async () => new Promise((r) => setTimeout(r, 50)),
      1
    );

    // The loop is already waiting on the queue, so `1` is handed straight to it
    // and the buffer (capacity 1) stays empty.
    expect(daemon.tryPushEvent(1)).toBe(true);
    // Now the buffer holds `2` while `1` is being handled.
    expect(daemon.tryPushEvent(2)).toBe(true);
    // The buffer is full, so `3` is rejected.
    expect(daemon.tryPushEvent(3)).toBe(false);

    await daemon.close();

    expect(daemon.tryPushEvent(4)).toBe(false);
  });
});
