import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..")
const pi = process.argv.find(arg => arg.startsWith("--pi="))?.slice(5) ?? "pi"
const extension = resolve(process.argv.find(arg => arg.startsWith("--extension="))?.slice(12) ?? join(repo, "plugin/pi/index.ts"))
const budgetMs = Number(process.argv.find(arg => arg.startsWith("--budget-ms="))?.slice(12) ?? 2_000)
assert(Number.isFinite(budgetMs) && budgetMs > 0, "--budget-ms must be positive")

async function startup({ name, cache, offline = false }) {
  const cached = cache === "fresh"
  const root = mkdtempSync(join(tmpdir(), "meridian-pi-startup-"))
  const agent = join(root, "agent")
  const sockets = new Set()
  let requests = 0
  let sawRequest
  const discovery = new Promise(resolve => { sawRequest = resolve })
  const server = createServer(() => { requests++; sawRequest() })
  server.on("connection", socket => {
    sockets.add(socket)
    socket.once("close", () => sockets.delete(socket))
  })
  await new Promise((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const baseUrl = `http://127.0.0.1:${server.address().port}`
  mkdirSync(agent)
  writeFileSync(join(agent, "settings.json"), JSON.stringify({ lastChangelogVersion: "9999.0.0" }))
  const model = {
    id: "claude-startup-fixture", name: "Startup fixture", api: "anthropic-messages", provider: "meridian",
    baseUrl: cache === "other" ? `${baseUrl}/other` : baseUrl,
    reasoning: false, input: ["text"], contextWindow: 200_000, maxTokens: 1024,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, compat: { supportsMidConvoToolChanges: false },
  }
  if (cache) writeFileSync(join(agent, "models-store.json"), JSON.stringify({
    meridian: { models: [model], checkedAt: Date.now() - (cache === "expired" ? 8 * 24 * 60 * 60 * 1_000 : 0) },
  }))
  const args = [
    "--mode", "rpc", "--no-session", "--no-extensions", "--no-context-files", "--no-skills",
    "--no-prompt-templates", "--no-themes", "--no-approve", "-e", extension,
    "--model", cached ? "meridian/claude-startup-fixture" : "anthropic/claude-haiku-5-5",
    "--api-key", "synthetic-no-generation",
  ]
  if (offline) args.push("--offline")
  const started = performance.now()
  const child = spawn(pi, args, {
    cwd: root,
    env: {
      PATH: `${dirname(process.execPath)}:${process.env.PATH}`, HOME: root, TMPDIR: tmpdir(),
      PI_CODING_AGENT_DIR: agent, PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0",
      MERIDIAN_BASE_URL: baseUrl,
    },
    stdio: ["pipe", "pipe", "pipe"],
  })
  const closed = new Promise(resolve => child.once("close", resolve))
  let stderr = ""
  child.stderr.setEncoding("utf8")
  child.stderr.on("data", text => { stderr = (stderr + text).slice(-8_000) })
  let deadline
  try {
    const ready = new Promise((resolve, reject) => {
      let buffer = ""
      deadline = setTimeout(() => reject(new Error(`${name}: startup exceeded 10 seconds\n${stderr}`)), 10_000)
      child.once("error", reject)
      child.once("exit", code => reject(new Error(`${name}: Pi exited ${code}\n${stderr}`)))
      child.stdout.setEncoding("utf8")
      child.stdout.on("data", text => {
        buffer += text
        for (let end; (end = buffer.indexOf("\n")) >= 0;) {
          const line = buffer.slice(0, end)
          buffer = buffer.slice(end + 1)
          let record
          try { record = JSON.parse(line) }
          catch (cause) { reject(new Error(`${name}: invalid RPC output`, { cause })); return }
          if (record.id === "startup-proof") resolve(record)
        }
      })
    })
    child.stdin.write(`${JSON.stringify({ id: "startup-proof", type: "get_state" })}\n`)
    const record = await ready
    const readyMs = Math.round(performance.now() - started)
    assert.equal(record.success, true, `${name}: ${record.error ?? "get_state failed"}`)
    if (cached) assert.equal(record.data.model.id, model.id, `${name}: cached Meridian model not selected`)
    if (offline) assert.equal(requests, 0, `${name}: offline startup contacted Meridian`)
    const result = { name, readyMs, budgetMs, requests, responses: 0 }
    if (readyMs >= budgetMs) console.log(JSON.stringify(result))
    assert(readyMs < budgetMs, `${name}: Pi waited ${readyMs} ms for a server that never replies`)
    if (!offline && !cached) {
      let timeout
      try {
        await Promise.race([
          discovery,
          new Promise((_, reject) => {
            timeout = setTimeout(() => reject(new Error(`${name}: permitted background discovery never started`)), 1_000)
          }),
        ])
      } finally {
        clearTimeout(timeout)
      }
    }
    console.log(JSON.stringify({ ...result, requestsAfterReady: requests }))
  } finally {
    clearTimeout(deadline)
    child.kill("SIGKILL")
    await closed
    for (const socket of sockets) socket.destroy()
    await new Promise(resolve => server.close(resolve))
    rmSync(root, { recursive: true, force: true })
  }
}

await startup({ name: "cold cache, unresponsive Meridian" })
await startup({ name: "cached Meridian model, unresponsive Meridian", cache: "fresh" })
await startup({ name: "expired cache, unresponsive Meridian", cache: "expired" })
await startup({ name: "cache for another Meridian", cache: "other" })
await startup({ name: "offline cold cache", offline: true })
