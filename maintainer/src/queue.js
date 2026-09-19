import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { RequestError, clientError } from "./request-error.js";

// Bounds the per-job log kept in memory and sent to the browser; a stuck or
// very verbose build must not grow a job without limit.
const MAX_LOG_LINES = 300;

export class JobQueue extends EventEmitter {
  constructor({ worker }) {
    super();
    this.worker = worker;
    this.jobs = [];
    this.activeByRegion = new Map();
    this.running = false;
    this.idleWaiters = [];
  }

  enqueue(input) {
    const existing = this.activeByRegion.get(input.regionId);
    if (existing) return existing;
    const job = {
      id: randomUUID(),
      ...input,
      status: "queued",
      phase: "queued",
      createdAt: new Date().toISOString(),
      progress: null,
      error: null,
      logs: []
    };
    this.jobs.push(job);
    this.activeByRegion.set(job.regionId, job);
    this.emit("changed", job);
    queueMicrotask(() => this.#pump());
    return job;
  }

  retry(id, { buildMemory } = {}) {
    const original = this.jobs.find((job) => job.id === id);
    if (!original) throw new RequestError(`Job '${id}' not found`, 404);
    if (original.status !== "failed") throw clientError("Only a failed job can be retried");
    return this.enqueue({
      type: original.type,
      regionId: original.regionId,
      name: original.name,
      source: original.source,
      buildMemory,
      retryOf: original.id
    });
  }

  snapshot() {
    return this.jobs.map((job) => ({ ...job }));
  }

  update(job, patch) {
    Object.assign(job, patch);
    this.emit("changed", job);
  }

  // Mirrors a build/publish log line to the console (for `docker logs`) and
  // appends it to the job so the web UI can show it too.
  log(job, line) {
    console.log(line);
    job.logs = [...job.logs, { time: new Date().toISOString(), line }].slice(-MAX_LOG_LINES);
    this.emit("changed", job);
  }

  whenIdle() {
    if (!this.running && !this.jobs.some((job) => ["queued", "running"].includes(job.status))) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.push(resolve));
  }

  async #pump() {
    if (this.running) return;
    this.running = true;
    while (true) {
      const pending = this.jobs.filter((candidate) => candidate.status === "queued");
      if (pending.length === 0) break;
      for (const job of pending) {
        this.update(job, { status: "running", phase: "starting", startedAt: new Date().toISOString() });
        this.log(job, `[queue] ${job.type} ${job.regionId} (job ${job.id}): starting`);
        try {
          await this.worker(job, (patch) => this.update(job, patch), (line) => this.log(job, line));
          this.update(job, { status: "complete", phase: "complete", completedAt: new Date().toISOString() });
          this.log(job, `[queue] ${job.type} ${job.regionId} (job ${job.id}): complete`);
        } catch (error) {
          this.update(job, { status: "failed", phase: "failed", lastPhase: job.phase, error: error.message, completedAt: new Date().toISOString() });
          this.log(job, `[queue] ${job.type} ${job.regionId} (job ${job.id}): failed during ${job.lastPhase} — ${error.message}`);
        } finally {
          this.activeByRegion.delete(job.regionId);
        }
      }
    }
    this.running = false;
    for (const resolve of this.idleWaiters.splice(0)) resolve();
  }
}
