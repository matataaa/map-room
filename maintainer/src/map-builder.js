import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { access, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { ProgressTracker } from "./progress.js";

const exists = (file) => access(file).then(() => true, () => false);

async function readDownloadMetadata(file) {
  try { return JSON.parse(await readFile(file, "utf8")); }
  catch { return null; }
}

function contentRange(value) {
  const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(value ?? "");
  return match ? { start: Number(match[1]), end: Number(match[2]), total: Number(match[3]) } : null;
}

function responseMetadata(url, response, totalBytes) {
  return {
    url,
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
    totalBytes
  };
}

function matchingValidator(metadata, response) {
  const etag = response.headers.get("etag");
  const modified = response.headers.get("last-modified");
  if (metadata.etag) return etag === metadata.etag;
  if (metadata.lastModified) return modified === metadata.lastModified;
  return false;
}

async function cancel(response) {
  try { await response.body?.cancel(); } catch {}
}

export function createMapBuilder({ dataDirectory, fetchImpl = fetch, spawnImpl = spawn }) {
  return async function buildMap({ id, source, output, reuseSource = false, buildMemory, onProgress = () => {}, onLog = () => {} }) {
    const sources = path.join(dataDirectory, "sources");
    await mkdir(sources, { recursive: true });
    const durableSource = path.join(sources, `${id}.osm.pbf`);
    const durableMetadataFile = `${durableSource}.json`;
    const hasDurableSource = await exists(durableSource);
    let canReuseDurableSource = reuseSource && hasDurableSource;
    if (source.url && canReuseDurableSource) {
      const durableMetadata = await readDownloadMetadata(durableMetadataFile);
      if (durableMetadata && durableMetadata.url !== source.url) {
        canReuseDurableSource = false;
      }
    }
    if (source.url && !canReuseDurableSource) {
      const download = `${durableSource}.download`;
      const metadataFile = `${download}.json`;
      let partialBytes = await stat(download).then(({ size }) => size, () => 0);
      let metadata = await readDownloadMetadata(metadataFile);
      const resumable = partialBytes > 0 && metadata?.url === source.url &&
        Boolean(metadata.etag || metadata.lastModified);
      if (partialBytes > 0 && !resumable) {
        await rm(download, { force: true });
        await rm(metadataFile, { force: true });
        partialBytes = 0;
        metadata = null;
      }
      const freshRequest = () => fetchImpl(source.url, { headers: { accept: "application/octet-stream" } });
      let response;
      let append = false;
      let sourceMode = "fresh";
      let range = null;
      if (resumable) {
        response = await fetchImpl(source.url, { headers: {
          accept: "application/octet-stream",
          Range: `bytes=${partialBytes}-`,
          "If-Range": metadata.etag ?? metadata.lastModified
        } });
        range = contentRange(response.headers.get("content-range"));
        if (response.status === 206 && range?.start === partialBytes &&
            (!metadata.totalBytes || range.total === metadata.totalBytes) && matchingValidator(metadata, response)) {
          append = true;
          sourceMode = "resumed";
        } else if ([206, 416].includes(response.status)) {
          await cancel(response);
          await rm(download, { force: true });
          await rm(metadataFile, { force: true });
          partialBytes = 0;
          metadata = null;
          response = await freshRequest();
          range = null;
        } else {
          partialBytes = 0;
          metadata = null;
        }
      } else {
        response = await freshRequest();
      }
      if (!response.ok || !response.body) throw new Error(`Map source returned HTTP ${response.status}`);
      const responseBytes = Number(response.headers.get("content-length")) || null;
      const totalBytes = append ? range.total : responseBytes;
      onLog(`[map-builder] ${id}: ${sourceMode === "resumed" ? "resuming" : "downloading"} source from ${source.url}${totalBytes ? ` (${totalBytes} bytes)` : ""}`);
      const activeMetadata = append
        ? { ...metadata, totalBytes }
        : responseMetadata(source.url, response, totalBytes);
      await writeFile(`${metadataFile}.tmp`, `${JSON.stringify(activeMetadata, null, 2)}\n`, "utf8");
      await rename(`${metadataFile}.tmp`, metadataFile);
      const tracker = new ProgressTracker();
      let completedBytes = append ? partialBytes : 0;
      const stream = Readable.fromWeb(response.body).map((chunk) => {
        completedBytes += chunk.length;
        onProgress({ phase: "downloading", sourceMode, progress: tracker.update({ completedBytes, totalBytes }) });
        return chunk;
      });
      await pipeline(stream, createWriteStream(download, { flags: append ? "a" : "w" }));
      const downloadedBytes = (await stat(download)).size;
      if (totalBytes !== null && downloadedBytes !== totalBytes) {
        throw new Error(`Map source download ended early (${downloadedBytes} of ${totalBytes} bytes)`);
      }
      onLog(`[map-builder] ${id}: source download complete (${downloadedBytes} bytes)`);
      await rename(download, durableSource);
      await rename(metadataFile, durableMetadataFile);
    } else if (source.url) {
      onLog(`[map-builder] ${id}: reusing previously downloaded source`);
      onProgress({ sourceMode: "reused" });
    } else if (!source.file) {
      throw new Error("Map source is not reusable");
    }
    const input = source.file ? path.join(dataDirectory, source.file) : durableSource;
    const temporaryDirectory = path.join(dataDirectory, ".map-room-work", id);
    await mkdir(temporaryDirectory, { recursive: true });
    onProgress({ phase: "building", progress: null });
    const memory = buildMemory ?? process.env.MAP_ROOM_BUILD_MEMORY ?? "2g";
    const planetilerArgs = [
      "-cp", "@/app/jib-classpath-file", "com.onthegomap.planetiler.Main",
      "--download", `--osm-path=${input}`, `--output=${output}`,
      `--download-dir=${sources}`, `--tmpdir=${temporaryDirectory}`, "--force"
    ];
    onLog(`[map-builder] ${id}: starting Planetiler (heap=${memory}) java ${planetilerArgs.join(" ")}`);
    const buildStartedAt = Date.now();
    try {
      await new Promise((resolve, reject) => {
        const child = spawnImpl("/opt/java/openjdk/bin/java", planetilerArgs,
          { stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, JAVA_TOOL_OPTIONS: `-Xmx${memory}` } });
        let errorText = "";
        const forward = (chunk) => {
          errorText = `${errorText}${chunk}`.slice(-4000);
          for (const line of chunk.toString("utf8").split("\n")) {
            if (line.trim() !== "") onLog(`[planetiler:${id}] ${line}`);
          }
        };
        child.stdout.on("data", forward);
        child.stderr.on("data", forward);
        child.once("error", reject);
        child.once("exit", (code) => {
          const elapsedSeconds = ((Date.now() - buildStartedAt) / 1000).toFixed(1);
          if (code === 0) {
            onLog(`[map-builder] ${id}: Planetiler finished in ${elapsedSeconds}s`);
            resolve();
          } else {
            onLog(`[map-builder] ${id}: Planetiler exited with code ${code} after ${elapsedSeconds}s`);
            reject(new Error(`Planetiler failed (${code}): ${errorText.trim()}`));
          }
        });
      });
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  };
}
