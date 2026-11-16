// Failure modes: the original goal disappears across a wait/restart, a packet changes executor rights,
// duplicate delivery repeats a run, and a dormant adoption profile dispatches work or notifications.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test, type TestContext } from "node:test";
import { taskEvent } from "../examples/recipes/ei-dispatcher/packet.ts";
import { decide } from "../kernel/decide.ts";
import { rows } from "../kernel/evolve.ts";
import { commitLatest, openView } from "../kernel/store.ts";
import { parseRules } from "../protocol/rules.ts";
import type { Job } from "../protocol/job.ts";

const packet = {
  schema_version: 1,
  task_id: "00000000-0000-4000-8000-000000000001",
  project_id: "synthetic-project",
  scope_version: "r1",
  goal: "Keep the agreed project goal through an owner decision.",
  acceptance: ["Return the same goal and evidence after a restart."],
  evidence_refs: ["synthetic:brief-r1"],
};
const dormant = JSON.parse(
  readFileSync(new URL("../examples/recipes/ei-dispatcher/rules.json", import.meta.url), "utf8"),
);

function hub(t: TestContext, active = true) {
  const directory = mkdtempSync(join(tmpdir(), "ei-autoloop-kernel-"));
  const file = join(directory, "hub.db");
  const format = "ei-synthetic-v1";
  const environment = { secret: "synthetic-only", publicUrl: "https://hub.invalid" };
  const connect = () => {
    const db = new DatabaseSync(file);
    db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL");
    openView(db, format);
    return db;
  };
  let db = connect();
  let now = Date.UTC(2026, 10, 16);
  const act = (type: string, ...args: unknown[]) => {
    let value: any;
    commitLatest(db, format, (view) => {
      const result = decide(view, { type, args }, now++, environment);
      value = result.value;
      return result.facts;
    });
    return value;
  };
  const given = active
    ? {
        timezone: "UTC",
        sources: { synthetic: { name: "Synthetic input" } },
        agents: {
          fake: { name: "Never started", worker: "manual", use: "command", command: ["not-executed"], access: "read" },
        },
        channels: {},
        elsewhere: [],
        rules: [
          {
            name: "synthetic",
            when: { source: "synthetic", types: ["ei.task.ready"] },
            to: "fake",
            cwd: directory,
            prompt: "Synthetic only",
            resume: true,
            tell: "never",
            emits: { source: "synthetic", types: ["ei.task.ready"] },
          },
        ],
      }
    : dormant;
  act("rules", parseRules(JSON.stringify(given), now, "UTC"));
  t.after(() => {
    db.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    act,
    rows: (table: Parameters<typeof rows>[1]) => rows(openView(db, format), table),
    restart: () => {
      db.close();
      db = connect();
    },
    claim: (id: string): Job | null => act("claimNext", "manual", id),
  };
}

test("packet stays data; routing, paths and permissions cannot be smuggled through it", () => {
  const event = taskEvent(packet);
  assert.deepEqual(JSON.parse(event.detail), packet);
  assert.equal(event.subject, packet.task_id);
  for (const field of ["cwd", "access", "command", "agent", "approved"]) {
    assert.throws(() => taskEvent({ ...packet, [field]: "override" }), /field/);
  }
  assert.throws(() => taskEvent({ ...packet, goal: "x".repeat(4001) }), /goal/);
  assert.throws(() => taskEvent({ ...packet, acceptance: [] }), /acceptance/);
});

test("original packet and decision survive SQLite reopen; execution completion is not project acceptance", (t) => {
  const h = hub(t);
  const event = taskEvent(packet);
  h.act("ingest", "synthetic", event);
  const first = h.claim("first")!;
  assert.deepEqual(JSON.parse(first.context.events[0].detail), packet);
  h.act("started", first.run, first.token, { session: "synthetic-session" });
  const finish = {
    outcome: "done",
    session: "synthetic-session",
    ask: [
      {
        key: "owner-decision",
        title: "Continue synthetic work?",
        text: "Kernel-only decision",
        options: ["Continue", "Stop"],
        about: JSON.stringify(packet),
      },
    ],
  };
  h.act("finish", first.run, first.token, finish);
  h.act("finish", first.run, first.token, finish);
  assert.equal(h.rows("runs")[0].state, "done");
  assert.equal(h.rows("cards").length, 1);
  assert.equal(h.rows("cards")[0].state, "open");
  assert.equal(h.claim("waiting"), null);
  h.restart();
  const card = h.rows("cards")[0];
  assert.equal(card.state, "open");
  h.act("answer", card.id, { choice: 0, by: "synthetic-owner" });
  assert.equal(h.act("answer", card.id, { choice: 0, by: "synthetic-owner" }).changed, false);
  const continued = h.claim("continued")!;
  assert.equal(continued.session, "synthetic-session");
  assert.deepEqual(JSON.parse(continued.context.answers[0].about), packet);
  assert.equal(continued.context.answers[0].chosen, "Continue");
  h.act("finish", continued.run, continued.token, { outcome: "done", summary: "Synthetic execution ended" });
  assert.equal(h.rows("cards")[0].state, "done");
  assert.equal(h.claim("no-more"), null);
});

test("replayed packet creates one queued run and duplicate result creates no second effect", (t) => {
  const h = hub(t);
  const event = taskEvent(packet);
  const first = h.act("ingest", "synthetic", event);
  const replay = h.act("ingest", "synthetic", event);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.event, first.event);
  assert.equal(h.rows("runs").length, 1);
  const job = h.claim("once")!;
  const result = { outcome: "done", summary: "Only a synthetic result" };
  h.act("finish", job.run, job.token, result);
  h.act("finish", job.run, job.token, result);
  assert.equal(h.rows("runs").length, 1);
  assert.equal(h.claim("twice"), null);
});

test("emitted chain beyond eight is recorded without another dispatch", (t) => {
  const h = hub(t);
  h.act("ingest", "synthetic", { ...taskEvent(packet), hops: 8 });
  const job = h.claim("depth")!;
  h.act("finish", job.run, job.token, {
    outcome: "done",
    events: [{ id: "next", type: "ei.task.ready", subject: packet.task_id, hops: 0 }],
  });
  assert.equal(h.rows("events").find((event) => event.key === "next")!.hops, 9);
  assert.equal(h.claim("over-limit"), null);
  assert.equal(h.rows("deliveries").length, 0);
});

test("published profile has no executor, source, schedule or notification channel", (t) => {
  assert.deepEqual(dormant.sources, {});
  assert.deepEqual(dormant.agents, {});
  assert.deepEqual(dormant.channels, {});
  assert.deepEqual(dormant.rules, []);
  assert.deepEqual(dormant.elsewhere, []);
  const h = hub(t, false);
  assert.equal(h.claim("dormant"), null);
  assert.equal(h.rows("runs").length, 0);
  assert.equal(h.rows("deliveries").length, 0);
});
