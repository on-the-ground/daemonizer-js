/**
 * TaskGroup is a utility class that allows managing a group of tasks
 * and waiting for all of them to complete. It is useful for coordinating multiple asynchronous
 * operations and ensuring that all tasks are done before proceeding.
 */
export class TaskGroup {
  private count = 0;
  private waiters: (() => void)[] = [];

  add(n: number) {
    this.count += n;
  }

  done() {
    if (this.count <= 0) throw new Error("TaskGroup underflow: done() called more times than add()");
    this.count -= 1;
    if (this.count === 0) {
      this.waiters.forEach((r) => r());
      this.waiters.length = 0;
    }
  }

  async wait(): Promise<void> {
    if (this.count === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}
