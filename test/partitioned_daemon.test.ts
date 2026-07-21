import {describe, expect, it} from "@jest/globals";
import {PartitionedDaemon} from "../src";
import {Daemon} from "../src/daemon";

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
            () => new Daemon(controller.signal, handleFn, 5),
            3,
            (event) => event.key
        );

        for (let key = 0; key < 3; key++) {
            for (let seq = 0; seq < 5; seq++) {
                await daemon.pushEvent({key, seq});
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
            () =>
                new Daemon(
                    controller.signal,
                    async (_signal, event) => {
                        handled.push(event.seq);
                    },
                    5
                ),
            4,
            (event) => event.key
        );

        for (let seq = 0; seq < 5; seq++) {
            await daemon.pushEvent({key: bigKey, seq});
        }

        await daemon.close();

        expect(handled).toEqual([0, 1, 2, 3, 4]);
    });

    it("routes negative keys to a valid partition instead of throwing", async () => {
        const handled: number[] = [];
        const controller = new AbortController();

        const daemon = new PartitionedDaemon<number, AbortSignal>(
            () =>
                new Daemon(
                    controller.signal,
                    async (_signal, event) => {
                        handled.push(event);
                    },
                    5
                ),
            4,
            (event) => event
        );

        await daemon.pushEvent(-1);
        await daemon.close();

        expect(handled).toEqual([-1]);
    });

    it("stops accepting new events after close", async () => {
        const controller = new AbortController();
        const daemon = new PartitionedDaemon<number, AbortSignal>(
            () => new Daemon(controller.signal, async () => {
            }, 1),
            2,
            (event) => event
        );

        await daemon.pushEvent(1);
        await daemon.close();

        const result = await daemon.pushEvent(2);
        expect(result).toBe(false);
    });

    it("lets each partition own independent local state via its own factory call", async () => {
        const controller = new AbortController();
        const partitionCount = 3;
        // each partition gets its own log because the factory is called fresh per index
        const logs: number[][] = Array.from({length: partitionCount}, () => []);

        const daemon = new PartitionedDaemon<{ key: number; seq: number }, AbortSignal>(
            (i) =>
                new Daemon(
                    controller.signal,
                    async (_signal, event) => {
                        logs[i].push(event.seq);
                    },
                    5
                ),
            partitionCount,
            (event) => event.key
        );

        for (let key = 0; key < partitionCount; key++) {
            for (let seq = 0; seq < 4; seq++) {
                await daemon.pushEvent({key, seq});
            }
        }

        await daemon.close();

        // if partitions shared state, every log would contain all 12 events instead of just its own 4
        for (let key = 0; key < partitionCount; key++) {
            expect(logs[key]).toEqual([0, 1, 2, 3]);
        }
    });

    it("rejects a non-positive partition count", () => {
        const controller = new AbortController();
        expect(
            () =>
                new PartitionedDaemon<number, AbortSignal>(
                    () => new Daemon(controller.signal, async () => {
                    }),
                    0,
                    (event) => event
                )
        ).toThrow("partitionCount must be greater than 0");
    });
});
