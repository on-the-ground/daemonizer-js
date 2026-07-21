# 🌀 Daemonizer

A minimal async control flow framework for writing browser and Node.js daemons.

Daemonizer helps you manage long-running async event loops, safely respond to cancellation, and yield control to the macro task queue—without starving the event loop.

---

## 🚀 Installation

```bash
yarn add daemonizer
# or
npm install daemonizer
```

---

## ✨ Features

- ✅ **Abort-aware event loop**
- ✅ **Task group with lifecycle tracking**
- ✅ **Yielding mechanism to avoid blocking**
- ✅ **Bounded queue for backpressure**
- ✅ **Partitioned processing for per-key ordering with cross-partition parallelism**
- ✅ **Works in both Node.js and browser**

---

## 🧪 Try in Your Browser

Run the daemon in your browser with no setup:  
👉 [examples/example.html](./examples/example.html)

Open your browser console and watch `tick:` messages stream in real time!

```html
<!-- examples/example.html -->
<!DOCTYPE html>
<html>
  <body>
    <script type="module">
      import { Daemon } from "https://esm.sh/@on-the-ground/daemonizer@latest";

      ///////////// Daemon Example /////////////

      let controller = new AbortController();
      const daemon = new Daemon(controller.signal, async (_signal, event) => {
        await new Promise((r) => setTimeout(r, 1000));
        console.log("tick:", event);
      });

      for (let i = 1; i <= 5; i++) {
        await daemon.pushEvent(i);
      }

      setTimeout(() => controller.abort(), 3000);

      console.log("waiting the daemon down");
      await daemon.close();
      console.log("the daemon got down");

      // Results:
      // waiting the daemon down
      // tick: 1
      // tick: 2
      // the daemon got down
      // tick: 3 <- long running task, use strictInterval to abort it
    </script>
  </body>
</html>
```

---

## 🧠 API Overview

### `Daemon` – The Core Abstraction

```ts
import { Daemon } from "@on-the-ground/daemonizer";

const daemon = new Daemon(signal, async (msg) => {
  // Your background task handler
  console.log("received:", msg);
});

// Push a task into the daemon's queue
await daemon.pushEvent({ type: "log", content: "hello" });

// Or push without waiting for queue space—returns false if full or closed
const accepted = daemon.tryPushEvent({ type: "log", content: "hello" });

// Gracefully shut down when you're done
await daemon.close();
```

#### ✅ Features

- Runs background tasks with structured concurrency
- Backpressure-safe via internal bounded queue
- Auto-shuts down when `AbortSignal` is aborted
- One-liner setup: no boilerplate, no ceremony

---

### `PartitionedDaemon` – Parallel Processing with Per-key Ordering

Backed by N `Daemon` instances, one per partition. Events are routed to a partition
by hashing a key extracted from each event, so events sharing a key always land on
the same partition (preserving order) while different partitions process in parallel—
analogous to Kafka's partition model.

```ts
import { PartitionedDaemon } from "@on-the-ground/daemonizer";

const daemon = new PartitionedDaemon(
  signal,
  async (_signal, event) => {
    console.log("received:", event);
  },
  (event) => event.userId, // key extractor: decides the partition
  4, // partitionCount
  10 // bufferSizePerPartition
);

await daemon.pushEvent({ userId: 42, type: "log", content: "hello" });

// Closes all partitions in parallel and waits for every one to drain.
await daemon.close();
```

#### ✅ Features

- Per-key ordering, cross-partition parallelism
- Same `pushEvent` / `tryPushEvent` / `close` surface as `Daemon`
- Shares one `AbortSignal` and handler across all partitions

---

### 🧰 Low-level Tools (also exported)

### `launchEventLoop(signal, taskGroup, events, handler)`

Runs a long-lived, abortable event loop over an `AsyncIterable`.  
Automatically yields to the macro task queue to prevent starvation.

### `TaskGroup`

Tracks the lifecycle of multiple concurrent async tasks.

- `add(n = 1)`
- `done()`
- `wait(): Promise<void>`

### `MacroTaskYielder`

Yields only if enough time has passed since the last yield.

### `BoundedQueue<T>`

A fixed-capacity async queue. Backpressure-aware and safe for multiple consumers.

---

## 🌐 Compatibility

Daemonizer is fully compatible with:

- ✅ Node.js (v16+)
- ✅ Modern browsers (via bundlers like Vite, Webpack, etc.)

No external runtime dependencies.

---

## 📜 License

MIT © 2025 Joohyung Park  
[github.com/on-the-ground/daemonizer](https://github.com/on-the-ground/daemonizer)

---

> _"The name was subconsciously inspired by countless replays of Judas Priest’s 'Demonizer'."_
