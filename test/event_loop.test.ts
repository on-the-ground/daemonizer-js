import { describe, it, expect } from "@jest/globals";
import { launchEventLoop } from "../src/event_loop";
import { BoundedQueue } from "../src/bounded_queue";
import { TaskGroup } from "../src/task_group";

describe("launchEventLoop", () => {
  it("processes all events", async () => {
    const boundedqueue = new BoundedQueue<number>(10);
    const results: number[] = [];
    const controller = new AbortController();

    [1, 2, 3].forEach((v) => boundedqueue.tryPush(v));
    boundedqueue.close();

    const tg = new TaskGroup();
    launchEventLoop(controller.signal, tg, boundedqueue, async (_, e) => {
      results.push(e);
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(results).toEqual([1, 2, 3]);

    controller.abort();
    await tg.wait();
  });

  it("abort long running handler", async () => {
    const boundedqueue = new BoundedQueue<number>(10);
    const results: number[] = [];
    const controller = new AbortController();

    [1].forEach((v) => boundedqueue.tryPush(v));
    boundedqueue.close();

    const tg = new TaskGroup();
    const start = performance.now();
    const interval = 8;
    launchEventLoop(
      controller.signal,
      tg,
      boundedqueue,
      async (_, e) => {
        await new Promise((resolve) =>
          setTimeout(() => {
            results.push(e);
            resolve(null);
          }, 1000)
        );
      },
      interval,
      true
    );
    await tg.wait();
    const end = performance.now();
    const delta = end - start;
    expect(delta).toBeLessThan(50);
  });
});
