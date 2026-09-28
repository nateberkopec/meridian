import { describe, expect, it } from "bun:test"

const { guardUpstreamIdle, UpstreamIdleError } = await import("../proxy/streamIdleGuard")
import type { IdleGuardClock } from "../proxy/streamIdleGuard"

type IdleTimerHandle = ReturnType<typeof setTimeout> | number

// A fully controllable clock for the guard: timers never fire on their own, so
// real upstream chunks always win their race against the idle deadline. The
// test fires the idle timer explicitly via advance(), making stall detection
// deterministic instead of racing the wall clock.
function makeFakeClock() {
  let current = 0
  let nextId = 1
  let scheduledTotal = 0
  const pending = new Map<IdleTimerHandle, { fireAt: number; fn: () => void }>()
  const waiters: Array<{ n: number; resolve: () => void }> = []

  const clock: IdleGuardClock = {
    now: () => current,
    setTimeout(fn, ms) {
      const id = nextId++
      pending.set(id, { fireAt: current + ms, fn })
      scheduledTotal++
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (scheduledTotal >= waiters[i]!.n) { waiters[i]!.resolve(); waiters.splice(i, 1) }
      }
      return id
    },
    clearTimeout(handle) { pending.delete(handle) },
  }

  return {
    clock,
    /** Resolves once at least `n` timers have been scheduled in total. */
    waitForScheduled(n: number): Promise<void> {
      if (scheduledTotal >= n) return Promise.resolve()
      return new Promise<void>((resolve) => { waiters.push({ n, resolve }) })
    },
    /** Advance time by `ms`, firing every timer whose deadline has passed. */
    advance(ms: number) {
      current += ms
      for (const [id, t] of [...pending]) {
        if (t.fireAt <= current) { pending.delete(id); t.fn() }
      }
    },
  }
}

// A controllable async iterable: push() emits a value, stall() just waits.
function makeSource<T>() {
  const queue: T[] = []
  let resolveNext: (() => void) | null = null
  let done = false
  const wake = () => { if (resolveNext) { const r = resolveNext; resolveNext = null; r() } }
  return {
    push(v: T) { queue.push(v); wake() },
    finish() { done = true; wake() },
    iterable: {
      async *[Symbol.asyncIterator]() {
        while (true) {
          if (queue.length) { yield queue.shift() as T; continue }
          if (done) return
          await new Promise<void>((r) => { resolveNext = r })
        }
      },
    } as AsyncIterable<T>,
  }
}

describe("guardUpstreamIdle", () => {
  it("passes through messages while the source is active", async () => {
    const src = makeSource<number>()
    const out: number[] = []
    const p = (async () => { for await (const v of guardUpstreamIdle(src.iterable, 500)) out.push(v) })()
    src.push(1); await new Promise((r) => setTimeout(r, 5))
    src.push(2); await new Promise((r) => setTimeout(r, 5))
    src.finish()
    await p
    expect(out).toEqual([1, 2])
  })

  it("throws UpstreamIdleError when the source goes silent even if onStall throws", async () => {
    const src = makeSource<number>()
    const stalls: number[] = []
    const clock = makeFakeClock()
    const p = (async () => { for await (const _ of guardUpstreamIdle(src.iterable, 30, (ms) => { stalls.push(ms); throw new Error("observer failed") }, clock.clock)) { /* drain */ } })()
    src.push(1) // one real chunk, then silence
    // Chunk 1 is delivered (its idle timer is cleared) and a fresh idle timer
    // is armed for the silent gap — the second scheduled timer. Fire it.
    await clock.waitForScheduled(2)
    clock.advance(30)
    let err: unknown
    try { await p } catch (e) { err = e }
    expect(err).toBeInstanceOf(UpstreamIdleError)
    expect(stalls.length).toBe(1)
    expect((err as InstanceType<typeof UpstreamIdleError>).sinceLastMs).toBeGreaterThanOrEqual(30)
  })

  it("trips even before the first chunk (slow TTFB)", async () => {
    const src = makeSource<number>() // never push
    let err: unknown
    try { for await (const _ of guardUpstreamIdle(src.iterable, 20)) { /* none */ } } catch (e) { err = e }
    expect(err).toBeInstanceOf(UpstreamIdleError)
  })

  it("calls return on the source iterator after an idle stall", async () => {
    let returned = false
    const source: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        return {
          next: () => new Promise<IteratorResult<number>>(() => {}),
          return: () => {
            returned = true
            return Promise.resolve({ done: true, value: undefined })
          },
        }
      },
    }

    let err: unknown
    try { for await (const _ of guardUpstreamIdle(source, 20)) { /* none */ } } catch (e) { err = e }
    expect(err).toBeInstanceOf(UpstreamIdleError)
    expect(returned).toBe(true)
  })

  it("does not let SDK stream pings extend the idle deadline", async () => {
    const src = makeSource<{ type: string; event?: { type: string } }>()
    const clock = makeFakeClock()
    const stalls: number[] = []
    const output: unknown[] = []
    let error: unknown
    const pending = (async () => {
      for await (const message of guardUpstreamIdle(src.iterable, 90, ms => stalls.push(ms), clock.clock)) output.push(message)
    })().catch(caught => { error = caught })
    try {
      await clock.waitForScheduled(1)
      for (let i = 0; i < 2; i++) {
        clock.advance(30)
        src.push({ type: "stream_event", event: { type: "ping" } })
        await clock.waitForScheduled(i + 2)
      }
      clock.advance(30)
      await new Promise<void>(resolve => setImmediate(resolve))
      expect(error).toBeInstanceOf(UpstreamIdleError)
      expect(stalls).toEqual([90])
      expect(output).toEqual([])
    } finally {
      src.finish()
      await pending
    }
  })

  it("resets the deadline for model progress between pings", async () => {
    const src = makeSource<{ type: string; event: { type: string } }>()
    const clock = makeFakeClock()
    const content = { type: "stream_event", event: { type: "content_block_delta" } }
    const output: unknown[] = []
    let error: unknown
    const pending = (async () => {
      for await (const message of guardUpstreamIdle(src.iterable, 90, undefined, clock.clock)) output.push(message)
    })().catch(caught => { error = caught })
    try {
      await clock.waitForScheduled(1)
      clock.advance(60)
      src.push(content)
      await clock.waitForScheduled(2)
      clock.advance(60)
      src.push({ type: "stream_event", event: { type: "ping" } })
      await clock.waitForScheduled(3)
      expect(error).toBeUndefined()
      clock.advance(30)
      await new Promise<void>(resolve => setImmediate(resolve))
      expect(error).toBeInstanceOf(UpstreamIdleError)
      expect(output).toEqual([content])
    } finally {
      src.finish()
      await pending
    }
  })

  it("idleMs<=0 disables the guard (pure pass-through)", async () => {
    const src = makeSource<number>()
    const out: number[] = []
    const p = (async () => { for await (const v of guardUpstreamIdle(src.iterable, 0)) out.push(v) })()
    src.push(7); src.finish()
    await p
    expect(out).toEqual([7])
  })
})
