import { randomUUID } from "node:crypto";
import { channel } from "node:diagnostics_channel";
import path from "node:path";
import {
  cancelWorkerNativeSections,
  createWorkerNativeSectionState,
  createWorkerTaskControl,
} from "@openclaw/worker-runtime/worker";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement } from "../../../test/helpers/promise.js";
import {
  installDatabaseProcessRole,
  withEngineStoreAdmission,
} from "../../infra/database-engine/admission-scope.js";
import {
  mixedAgentConnection,
  useMixedAgentPair,
} from "../../infra/database-engine/mixed-agent.conformance.test-support.js";
import { PostgresSyncConnection } from "../../infra/postgres-sync/connection.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import { replaceSessionEntrySync } from "./session-accessor.sqlite-entry.js";
import { readTranscriptExportSnapshotReadOnlySync } from "./session-accessor.sqlite-export-read.js";
import { readTranscriptStatsSync } from "./session-accessor.sqlite-read.js";
import {
  resolveSqliteTranscriptScope,
  toDatabaseOptions,
} from "./session-accessor.sqlite-scope.js";
import { insertTranscriptRowsWithoutProjectionInTransaction } from "./session-accessor.sqlite-transcript-batch.js";
import { touchTranscriptMutationInTransaction } from "./session-accessor.sqlite-transcript-state.js";
import { replaceSqliteTranscriptEventsInTransaction } from "./session-accessor.sqlite-transcript-store.js";
import { deliverSessionMemoryEntries } from "./session-memory-entry.worker.js";
import { reconcileSessionTranscriptIndexInTransaction } from "./session-transcript-index.js";

const MIB = 1024 * 1024;
const statements = channel("openclaw.database.statement");
let observed: string[] = [];
let payloadPages: { seqs: number[]; bytes: number }[] = [];
let metadataPages: { rows: number; columns: string[] }[] = [];
const observe = (message: unknown) => {
  observed.push((message as { sql?: string }).sql ?? "");
};

afterEach(() => {
  if (process.env.OPENCLAW_POSTGRES_EXPORT_PROOF === "1") {
    const seqs = payloadPages.flatMap((page) => page.seqs);
    console.log(
      JSON.stringify({
        proof: "postgres-payload-pages",
        payloadPages: payloadPages.map((page) => ({
          firstSeq: page.seqs[0],
          lastSeq: page.seqs.at(-1),
          rows: page.seqs.length,
          bytes: page.bytes,
        })),
        totalRows: seqs.length,
        uniqueRows: new Set(seqs).size,
        totalBytes: payloadPages.reduce((sum, page) => sum + page.bytes, 0),
        metadataPages,
      }),
    );
  }
  vi.restoreAllMocks();
  statements.unsubscribe(observe);
  observed = [];
  payloadPages = [];
  metadataPages = [];
});

// The Gateway runs these reads in its transcript worker; the tests admit them on the main thread.
describe.skipIf(!mixedAgentConnection)(
  "PostgreSQL transcript exports above the slice budget",
  () => {
    useMixedAgentPair("postgres", "postgres");
    beforeAll(() => {
      // Deferred slices are read after the batch's first await, as in the transcript worker.
      installDatabaseProcessRole("cli-oneshot");
    });

    function seedLargeSession(rows?: { count?: number; bytes?: number; lastSeq?: number }) {
      const database = withEngineStoreAdmission(() =>
        openOpenClawAgentDatabase({ agentId: "main" }),
      );
      const sessionId = randomUUID();
      const scope = {
        agentId: "main",
        storePath: database.path,
        sessionKey: `agent:main:export-${sessionId}`,
        sessionId,
      };
      const events = [
        { type: "session", version: 3, id: sessionId, timestamp: "2026-04-01T05:46:39.000Z" },
        // By default two 2.5 MiB rows take the transcript past the 4 MiB retained-snapshot limit,
        // and each fills a 2 MiB slice.
        ...Array.from({ length: rows?.count ?? 2 }, (_, index) => ({
          type: "message",
          id: `entry-${index}`,
          parentId: index === 0 ? null : `entry-${index - 1}`,
          timestamp: "2026-04-01T05:46:40.000Z",
          message: {
            role: "user",
            content: "lobster ".repeat((rows?.bytes ?? 2.5 * MIB) / 8),
            timestamp: 1,
          },
        })),
      ];
      withEngineStoreAdmission(() => {
        replaceSessionEntrySync(scope, { sessionId, updatedAt: 1 });
        const resolved = resolveSqliteTranscriptScope(scope);
        const lastSeq = rows?.lastSeq;
        runOpenClawAgentWriteTransaction((writable) => {
          replaceSqliteTranscriptEventsInTransaction(
            writable,
            resolved,
            lastSeq === undefined ? events : events.slice(0, -1),
          );
          if (lastSeq !== undefined) {
            const event = events.at(-1)!;
            insertTranscriptRowsWithoutProjectionInTransaction(writable, sessionId, [
              { event, seq: lastSeq, createdAt: Date.parse(event.timestamp) },
            ]);
            reconcileSessionTranscriptIndexInTransaction(writable.db, sessionId);
            touchTranscriptMutationInTransaction(writable, sessionId);
          }
        }, toDatabaseOptions(resolved));
        expect(readTranscriptStatsSync(scope).sizeBytes).toBeGreaterThan(4 * MIB);
      });
      statements.subscribe(observe);
      // Observe the actual buffered bridge result, without retaining its payload strings.
      // oxlint-disable-next-line typescript/unbound-method -- Forwarded with the original receiver below.
      const query = PostgresSyncConnection.prototype.query;
      vi.spyOn(PostgresSyncConnection.prototype, "query").mockImplementation(function (
        this: PostgresSyncConnection,
        sql,
        parameters,
        readBigInts,
      ) {
        const result = query.call(this, sql, parameters, readBigInts);
        const columns = result.columns.map(({ name }) => name);
        if (sql.includes('from "transcript_events"') && columns.includes("seq")) {
          if (columns.includes("event_json")) {
            payloadPages.push({
              seqs: result.rows.map((row) => Number(row.seq)),
              bytes: result.rows.reduce(
                (sum, row) => sum + Buffer.byteLength(String(row.event_json ?? "")),
                0,
              ),
            });
          } else if (columns.includes("payload_bytes")) {
            metadataPages.push({ rows: result.rows.length, columns });
          }
        }
        return result;
      });
      return { events, scope };
    }

    const begins = () => observed.filter((sql) => /^BEGIN\b/i.test(sql));
    // Head reads project the decoded row as "event_json"; slice reads also select its size.
    const payloadQueries = () => observed.filter((sql) => sql.includes(' as "event_json"'));
    const sliceQueries = () => observed.filter((sql) => sql.includes('as "payload_bytes"'));

    it.each(["ack", "reject", "cancel"] as const)(
      "settles %s at a PostgreSQL page gate without prefetching later slices",
      async (ending) => {
        const { scope } = seedLargeSession();
        const entry = {
          absPath: path.join(path.dirname(scope.storePath), `${scope.sessionId}.jsonl`),
          options: scope,
          pages: true,
          messages: false,
        };
        const entered = Promise.withResolvers<void>();
        const resume = Promise.withResolvers<void>();
        const sections = createWorkerNativeSectionState();
        let requests = 0;
        let consumed = 0;
        let prepared: ReturnType<typeof deliverSessionMemoryEntries> | undefined;
        withEngineStoreAdmission(() => {
          prepared = deliverSessionMemoryEntries(
            [entry],
            {
              consumeInput() {},
              notify() {},
              async request(value) {
                requests++;
                if (requests === 1) {
                  expect(value).toMatchObject({
                    kind: "session-entries",
                    index: 0,
                    page: { startLine: 0 },
                  });
                  entered.resolve();
                  await resume.promise;
                  if (ending === "reject") {
                    throw new Error("delivery unavailable");
                  }
                }
                return {
                  input: undefined,
                  consumed() {
                    consumed++;
                  },
                };
              },
            },
            createWorkerTaskControl(sections, () => true),
          );
        });
        const pending = prepared!;
        const outcome = pending.catch((error: unknown) => error);
        try {
          await awaitGateBeforeSettlement(
            entered.promise,
            pending,
            "Export finished before its first page was acknowledged",
          );
          expect(requests).toBe(1);
          expect(payloadPages.map((page) => page.seqs)).toEqual([[0, 1]]);
          expect(metadataPages.map((page) => page.rows)).toEqual([3]);
          expect(observed.filter((sql) => /^(?:COMMIT|ROLLBACK)$/i.test(sql))).toHaveLength(
            begins().length,
          );
          const reads = { payload: payloadPages.length, metadata: metadataPages.length };
          if (ending === "cancel") {
            cancelWorkerNativeSections(sections);
          }
          resume.resolve();
          if (ending === "ack") {
            await expect(pending).resolves.toEqual({ entries: [] });
            expect(payloadPages.flatMap((page) => page.seqs)).toEqual([0, 1, 2]);
            expect(consumed).toBe(requests);
          } else {
            await expect(pending).rejects.toThrow(
              ending === "reject" ? "delivery unavailable" : "worker task cancelled",
            );
            expect(requests).toBe(1);
            expect({ payload: payloadPages.length, metadata: metadataPages.length }).toEqual(reads);
            expect(consumed).toBe(ending === "reject" ? 0 : 1);
          }
        } finally {
          resume.resolve();
          await outcome;
        }
        expect(begins().length).toBeGreaterThan(1);
        expect(new Set(begins())).toEqual(
          new Set(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"]),
        );
        expect(observed.filter((sql) => sql.includes("pg_advisory_xact_lock"))).toEqual([]);
        expect(payloadQueries()).toEqual([]);
        expect(sliceQueries().length).toBeGreaterThan(0);
      },
    );

    it("single export reads byte-budgeted rows across a large sequence gap", () => {
      const lastSeq = 4_000_000;
      const { events, scope } = seedLargeSession({ lastSeq });
      const snapshot = withEngineStoreAdmission(() =>
        readTranscriptExportSnapshotReadOnlySync(scope),
      );
      expect(snapshot?.events).toEqual(events);
      expect(payloadQueries()).toEqual([]);
      // A legal oversized event advances the page, but never transfers the next event early.
      expect(payloadPages.filter((page) => page.seqs.length > 0).map((page) => page.seqs)).toEqual([
        [0, 1],
        [lastSeq],
      ]);
      expect(metadataPages.length).toBeGreaterThan(0);
      for (const page of metadataPages) {
        expect(page.rows).toBeLessThanOrEqual(256);
        expect(page.columns.toSorted()).toEqual(["payload_bytes", "seq"]);
      }
    });

    // Many small rows never reach the byte budget, so only a row bound keeps PostgreSQL
    // from buffering the whole transcript in one result (#201571).
    it("pages a many-row transcript with row-bounded queries", () => {
      const { events, scope } = seedLargeSession({ count: 600, bytes: 8 * 1024 });
      const snapshot = withEngineStoreAdmission(() =>
        readTranscriptExportSnapshotReadOnlySync(scope),
      );
      expect(snapshot?.events).toEqual(events);
      expect(payloadQueries()).toEqual([]);
      expect(sliceQueries().length).toBeGreaterThanOrEqual(3);
      expect(payloadPages.flatMap((page) => page.seqs)).toEqual(events.map((_, index) => index));
      const maxEventBytes = Math.max(
        ...events.map((event) => Buffer.byteLength(JSON.stringify(event))),
      );
      for (const page of payloadPages) {
        expect(page.bytes).toBeLessThan(2 * MIB + maxEventBytes);
      }
      for (const page of metadataPages) {
        expect(page.rows).toBeLessThanOrEqual(256);
      }
    });
  },
);
