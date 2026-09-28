import { guardUpstreamIdle } from '../src/proxy/streamIdleGuard.ts';

// Advance a virtual clock instead of waiting several minutes.
let now = 0;
let nextId = 0;
const timers = new Map();
const clock = {
  now: () => now,
  setTimeout(fn, ms) {
    const id = ++nextId;
    timers.set(id, { at: now + ms, fn });
    return id;
  },
  clearTimeout(id) { timers.delete(id); },
};
async function* upstream() {
  for (let i = 0; i < 4; i++) {
    await new Promise(resolve => setImmediate(resolve));
    now += 30000;
    for (const [id, timer] of timers) {
      if (timer.at <= now) { timers.delete(id); timer.fn(); }
    }
    await new Promise(resolve => setImmediate(resolve));
    yield { type: 'stream_event', event: { type: 'ping' } };
  }
}
try {
  for await (const event of guardUpstreamIdle(upstream(), 90000, undefined, clock)) {
    console.log(`Forwarded ${event.event.type} at ${now} ms`);
  }
  console.error('FAIL: 120000 ms passed with no model output and no timeout');
  process.exitCode = 1;
} catch (error) {
  if (error.name !== 'UpstreamIdleError') throw error;
  console.log(`PASS: ${error.message}`);
}
