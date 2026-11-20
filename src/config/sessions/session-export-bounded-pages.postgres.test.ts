import { spawnSync } from "node:child_process";
import { createHash, randomUUID, type Hash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import {
  installDatabaseProcessRole,
  withEngineStoreAdmission,
} from "../../infra/database-engine/admission-scope.js";
import { getDatabaseEngineSelection } from "../../infra/database-engine/context.js";
import {
  mixedAgentConnection,
  useMixedAgentPair,
} from "../../infra/database-engine/mixed-agent.conformance.test-support.js";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import { isPostgresSqlConnection } from "../../infra/sql-connection.js";
import { withFreshOpenClawAgentDatabaseReadOnly } from "../../state/openclaw-agent-db-readonly-open.js";
import type { DB } from "../../state/openclaw-agent-db.generated.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import { cleanupSessionStateForTest } from "../../test-utils/session-state-cleanup.js";
import { replaceSessionEntrySync } from "./session-accessor.sqlite-entry.js";
import { readTranscriptStatsSync } from "./session-accessor.sqlite-read.js";
import {
  resolveSqliteTranscriptScope,
  toDatabaseOptions,
} from "./session-accessor.sqlite-scope.js";
import { appendTranscriptEventsInTransaction } from "./session-accessor.sqlite-transcript-store.js";
import { SESSION_TRANSCRIPT_WORKER_LIMITS } from "./session-transcript-read-pools.js";
import { streamSessionEntriesInWorker } from "./session-transcript-read-worker-runtime.js";

// Opt-in scale proof: run with the PostgreSQL conformance wrapper and
// OPENCLAW_POSTGRES_EXPORT_PROOF=1. Override event count/bytes to replay the same
// workload on a baseline without retaining its transcript in the host sink.
const enabled = process.env.OPENCLAW_POSTGRES_EXPORT_PROOF === "1";
const eventCount = Number(process.env.OPENCLAW_POSTGRES_EXPORT_PROOF_EVENTS ?? 64);
const eventBytes = Number(process.env.OPENCLAW_POSTGRES_EXPORT_PROOF_BYTES ?? 512 * 1024);
const wrapChars = 800;
const bodyBytes = Math.floor(eventBytes / wrapChars) * wrapChars;
const linesPerEvent = bodyBytes / wrapChars + 2;
const timestampMs = 1_775_020_800_000;
const prefixLineCount = Math.min(linesPerEvent + 1, eventCount * linesPerEvent);

// The persisted hash appends the public line facts to the rendered text. Stream
// the expected facts too, so the proof never keeps a full exported transcript.
function finishExpectedHash(content: Hash, lineCount: number, sessionKind = "unknown"): string {
  content.update("\n");
  for (let line = 0; line < lineCount; line++) {
    content.update((line ? "," : "") + (Math.floor(line / linesPerEvent) + 1));
  }
  content.update("\n");
  for (let line = 0; line < lineCount; line++) {
    content.update((line ? "," : "") + timestampMs);
  }
  content.update("\n[");
  const provenance = JSON.stringify({
    originClass: "untrusted",
    sessionKind,
    observedAt: timestampMs,
  });
  for (let line = 0; line < lineCount; line++) {
    content.update((line ? "," : "") + provenance);
  }
  return content.update(']\n{"state":"absent"}').digest("hex");
}

describe.skipIf(!mixedAgentConnection || !enabled)(
  "PostgreSQL production worker export proof",
  () => {
    const fixture = useMixedAgentPair("postgres", "postgres");
    beforeAll(() => installDatabaseProcessRole("cli-oneshot"));

    it("streams a large session with an incremental sink under the production worker limits", async () => {
      expect(Number.isSafeInteger(eventCount) && eventCount > 0).toBe(true);
      expect(Number.isSafeInteger(eventBytes) && eventBytes >= 1024).toBe(true);
      const database = withEngineStoreAdmission(() =>
        openOpenClawAgentDatabase({ agentId: "main" }),
      );
      const sessionId = randomUUID();
      const scope = {
        agentId: "main",
        sessionId,
        sessionKey: `agent:main:chat:pgexport-${sessionId}`,
        storePath: database.path,
      };
      const expected = createHash("sha256");
      let expectedPrefix: Hash | undefined;
      let expectedLines = 0;
      let maxEventBytes = 0;
      withEngineStoreAdmission(() => {
        replaceSessionEntrySync(scope, { sessionId, updatedAt: timestampMs });
        const resolved = resolveSqliteTranscriptScope(scope);
        for (let first = 0; first < eventCount; first += 8) {
          const events = Array.from({ length: Math.min(8, eventCount - first) }, (_, offset) => {
            const index = first + offset;
            const head = `pgexporthead${index}`;
            const tail = `pgexporttail${index}`;
            const content = head + " " + "x".repeat(bodyBytes) + " " + tail;
            // Markers are independent FTS words; the body has whole 800-character
            // segments. Expected output needs no production wrapping helper.
            for (let part = 0; part < linesPerEvent; part++) {
              const text =
                part === 0 ? head : part === linesPerEvent - 1 ? tail : "x".repeat(wrapChars);
              if (expectedLines++ > 0) {
                expected.update("\n");
              }
              expected.update("User: ").update(text);
              if (expectedLines === prefixLineCount) {
                expectedPrefix = expected.copy();
              }
            }
            const event = {
              type: "message",
              id: `entry-${index}`,
              parentId: index === 0 ? null : `entry-${index - 1}`,
              timestamp: new Date(timestampMs).toISOString(),
              message: { role: "user", content, timestamp: timestampMs },
            };
            maxEventBytes = Math.max(maxEventBytes, Buffer.byteLength(JSON.stringify(event)));
            return event;
          });
          runOpenClawAgentWriteTransaction((writable) => {
            expect(appendTranscriptEventsInTransaction(writable, resolved, events)).toBe(
              events.length,
            );
          }, toDatabaseOptions(resolved));
        }
      });
      const before = withEngineStoreAdmission(() => readTranscriptStatsSync(scope));
      console.log(
        JSON.stringify({
          proof: "postgres-real-worker-workload",
          eventCount,
          eventBytes,
          bodyBytes,
          maxEventBytes,
          rawBytes: before.sizeBytes,
          resourceLimits: SESSION_TRANSCRIPT_WORKER_LIMITS,
        }),
      );
      let digest = createHash("sha256");
      let deliveredLines = 0;
      let pages = 0;
      let resets = 0;
      let maxPageBytes = 0;
      let firstMarker = "";
      let lastMarker = "";
      const [result] = await streamSessionEntriesInWorker(
        [
          {
            absPath: path.join(path.dirname(scope.storePath), `${sessionId}.jsonl`),
            options: scope,
            pages: true,
            messages: false,
            prefixLineCount,
          },
        ],
        async (index, update) => {
          expect(index).toBe(0);
          if ("reset" in update) {
            digest = createHash("sha256");
            deliveredLines = 0;
            pages = 0;
            resets++;
          } else if ("page" in update) {
            expect(update.page.startLine).toBe(deliveredLines);
            expect(update.page.messages).toBeUndefined();
            pages++;
            let pageBytes = 0;
            for (const [offset, line] of update.page.lines.entries()) {
              const eventIndex = Math.floor(deliveredLines / linesPerEvent);
              expect(update.page.lineMap[offset]).toBe(eventIndex + 1);
              expect(update.page.messageTimestampsMs[offset]).toBe(timestampMs);
              expect(update.page.lineProvenance[offset]).toEqual({
                originClass: "untrusted",
                sessionKind: "unknown",
                observedAt: timestampMs,
              });
              if (deliveredLines++ > 0) {
                digest.update("\n");
              } else {
                firstMarker = line.slice(0, 60);
              }
              digest.update(line);
              lastMarker = line.slice(-60);
              pageBytes += Buffer.byteLength(line);
            }
            maxPageBytes = Math.max(maxPageBytes, pageBytes);
          }
        },
      );
      const actualDigest = digest.digest("hex");
      const expectedCliContent = expected.copy();
      const summary = result && "summary" in result ? result.summary : undefined;
      expect(actualDigest).toBe(expected.copy().digest("hex"));
      expect(deliveredLines).toBe(eventCount * linesPerEvent);
      expect(summary).toMatchObject({
        lineCount: deliveredLines,
        hash: finishExpectedHash(expected, expectedLines),
        prefixHash: finishExpectedHash(expectedPrefix!, prefixLineCount),
        lineOriginClasses: ["untrusted"],
        sessionKind: "unknown",
      });
      expect(firstMarker).toContain("pgexporthead0");
      expect(lastMarker).toContain(`pgexporttail${eventCount - 1}`);
      expect(pages).toBeGreaterThan(1);
      expect(resets).toBe(0);
      expect(maxPageBytes).toBeLessThan(2 * 1024 * 1024);
      expect(withEngineStoreAdmission(() => readTranscriptStatsSync(scope))).toEqual(before);
      console.log(
        JSON.stringify({
          proof: "postgres-real-worker-export",
          eventCount,
          eventBytes,
          bodyBytes,
          maxEventBytes,
          rawBytes: before.sizeBytes,
          resourceLimits: SESSION_TRANSCRIPT_WORKER_LIMITS,
          deliveredLines,
          pages,
          maxPageBytes,
          resets,
          digest: actualDigest,
          hash: summary?.hash,
          prefixHash: summary?.prefixHash,
          firstMarker,
          lastMarker,
        }),
      );

      if (process.env.OPENCLAW_POSTGRES_EXPORT_PROOF_CLI === "1") {
        const postgres = getDatabaseEngineSelection()?.postgres;
        if (!postgres) {
          throw new Error("The CLI proof requires the fixture's PostgreSQL engine selection");
        }
        const workspace = path.join(fixture.root, "workspace");
        fs.mkdirSync(workspace, { recursive: true });
        const configPath = path.join(fixture.root, "openclaw.json");
        fs.writeFileSync(
          configPath,
          JSON.stringify({
            database: {
              engine: "sqlite",
              families: { state: { engine: "postgres" }, agent: { engine: "postgres" } },
              postgres: { connection: postgres.connection, schemaPrefix: postgres.schemaPrefix },
            },
            agents: { defaults: { workspace } },
            plugins: { allow: ["memory-core"], slots: { memory: "memory-core" } },
            memory: {
              search: {
                provider: "none",
                fallback: "none",
                sources: ["sessions"],
                rememberAcrossConversations: true,
                store: { vector: { enabled: false } },
              },
            },
          }),
        );
        // The child CLI owns the isolated stores during index/search, after this
        // process has settled its transcript readers and writer borrows.
        await cleanupSessionStateForTest({ stateDir: fixture.root });
        const cli = (label: string, args: string[]) => {
          const childResult = spawnSync("pnpm", ["--silent", "openclaw", ...args], {
            cwd: process.cwd(),
            encoding: "utf8",
            maxBuffer: 4 * 1024 * 1024,
            timeout: 300_000,
            env: {
              ...process.env,
              HOME: fixture.root,
              USERPROFILE: fixture.root,
              NODE_ENV: undefined,
              NODE_OPTIONS: undefined,
              VITEST: undefined,
              OPENCLAW_STATE_DIR: fixture.root,
              OPENCLAW_CONFIG_PATH: configPath,
              OPENCLAW_NO_RESPAWN: "1",
              NO_COLOR: "1",
            },
          });
          const logDir = process.env.OPENCLAW_POSTGRES_EXPORT_PROOF_LOG_DIR;
          if (logDir) {
            fs.mkdirSync(logDir, { recursive: true });
            fs.writeFileSync(
              path.join(logDir, `cli-${label}.log`),
              JSON.stringify(
                {
                  args,
                  status: childResult.status,
                  signal: childResult.signal,
                  error: childResult.error?.message,
                  stdout: childResult.stdout,
                  stderr: childResult.stderr,
                },
                null,
                2,
              ),
            );
          }
          expect(childResult.error).toBeUndefined();
          expect(childResult.status, `${childResult.stderr}\n${childResult.stdout}`).toBe(0);
          return childResult.stdout;
        };
        cli("validate", ["config", "validate"]);
        expect(
          cli("index", ["memory", "index", "--agent", "main", "--force", "--verbose"]),
        ).toContain("Memory index updated (main): 1 file indexed.");
        const memoryPath = `sessions/main/${sessionId}.jsonl`;
        const tailMarker = `pgexporttail${eventCount - 1}`;
        const stored = withEngineStoreAdmission(() =>
          withFreshOpenClawAgentDatabaseReadOnly(
            ({ db }) => {
              const k =
                getNodeSqliteKysely<Pick<DB, "memory_index_sources" | "memory_index_chunks">>(db);
              return {
                postgres: isPostgresSqlConnection(db),
                sources: executeSqliteQuerySync(
                  db,
                  k
                    .selectFrom("memory_index_sources")
                    .select(["path", "source", "size", "hash"])
                    .where("path", "=", memoryPath)
                    .where("source", "=", "sessions"),
                ).rows,
                chunks: executeSqliteQuerySync(
                  db,
                  k
                    .selectFrom("memory_index_chunks")
                    .select(({ fn }) => fn.countAll<number>().as("count"))
                    .where("path", "=", memoryPath)
                    .where("source", "=", "sessions"),
                ).rows[0]!.count,
                tail: executeSqliteQuerySync(
                  db,
                  k
                    .selectFrom("memory_index_chunks")
                    .select(["start_line", "end_line", "text"])
                    .where("path", "=", memoryPath)
                    .where("source", "=", "sessions")
                    .where("text", "like", `%${tailMarker}%`)
                    .limit(1),
                ).rows[0],
              };
            },
            { agentId: "main", path: scope.storePath },
            { snapshot: true },
          ),
        );
        if (!stored.found) {
          throw new Error("The completed CLI index must remain readable through its agent owner");
        }
        expect(stored.value.postgres).toBe(true);
        const cliHash = finishExpectedHash(expectedCliContent, expectedLines, "interactive");
        expect(stored.value.sources).toEqual([
          {
            path: memoryPath,
            source: "sessions",
            size: before.sizeBytes,
            hash: `sqlite:${before.lastMutationAtMs ?? before.maxSeq}:${cliHash}`,
          },
        ]);
        expect(stored.value.chunks).toBeGreaterThan(0);
        expect(stored.value.tail).toMatchObject({ start_line: eventCount, end_line: eventCount });
        expect(stored.value.tail?.text).toContain(tailMarker);
        const status: unknown = JSON.parse(
          cli("status", ["memory", "status", "--agent", "main", "--json"]),
        );
        expect(status).toMatchObject([
          {
            agentId: "main",
            status: {
              files: 1,
              chunks: stored.value.chunks,
              dirty: false,
              provider: "none",
            },
          },
        ]);
        for (const [marker, sourceLine] of [
          ["pgexporthead0", 1],
          [tailMarker, eventCount],
        ] as const) {
          const found: unknown = JSON.parse(
            cli(marker, [
              "memory",
              "search",
              "--agent",
              "main",
              "--query",
              marker,
              "--min-score",
              "0",
              "--max-results",
              "1",
              "--json",
            ]),
          );
          // Search snippets are only the first 700 characters of a chunk; its
          // tail marker is proved in persisted text and in the matching source line.
          expect(found).toMatchObject({
            results: [
              { path: memoryPath, source: "sessions", startLine: sourceLine, endLine: sourceLine },
            ],
          });
        }
        expect(withEngineStoreAdmission(() => readTranscriptStatsSync(scope))).toEqual(before);
        console.log(
          JSON.stringify({
            proof: "postgres-memory-cli",
            eventCount,
            rawBytes: before.sizeBytes,
            postgres: stored.value.postgres,
            sources: stored.value.sources,
            chunks: stored.value.chunks,
            tail: {
              startLine: stored.value.tail?.start_line,
              endLine: stored.value.tail?.end_line,
              suffix: stored.value.tail?.text.slice(-60),
            },
          }),
        );
      }
    }, 600_000);
  },
);
