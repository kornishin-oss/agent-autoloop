// A data envelope only. Authority, current scope and executor routing must come from the trusted project controller.
import { parseEvent, type EventInput } from "../../../protocol/validate.ts";

export type TaskPacket = {
  schema_version: 1;
  task_id: string;
  project_id: string;
  scope_version: string;
  goal: string;
  acceptance: string[];
  evidence_refs: string[];
};

const fields = ["schema_version", "task_id", "project_id", "scope_version", "goal", "acceptance", "evidence_refs"];
const text = (value: unknown, limit: number): value is string =>
  typeof value === "string" && value.trim().length > 0 && [...value].length <= limit;
const list = (value: unknown, limit: number, width: number, minimum = 0): value is string[] =>
  Array.isArray(value) && value.length >= minimum && value.length <= limit && value.every((x) => text(x, width));

/** Preserves project identity and the agreed goal as event data; does not approve or execute the task. */
export function taskEvent(input: unknown): EventInput {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new Error("packet must be an object");
  const value = input as Record<string, unknown>;
  if (Object.keys(value).some((key) => !fields.includes(key))) throw new Error("unsupported packet field");
  if (value.schema_version !== 1) throw new Error("unsupported schema_version");
  if (typeof value.task_id !== "string" || !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value.task_id))
    throw new Error("task_id must be a canonical UUID");
  for (const key of ["project_id", "scope_version"]) {
    if (!text(value[key], 64) || !/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(value[key] as string))
      throw new Error(key + " must be a bounded identifier");
  }
  if (!text(value.goal, 500)) throw new Error("goal must contain 1..500 codepoints");
  if (!list(value.acceptance, 20, 200, 1)) throw new Error("acceptance must be a non-empty bounded list");
  if (!list(value.evidence_refs, 10, 500)) throw new Error("evidence_refs must be a bounded list");
  const packet: TaskPacket = {
    schema_version: 1,
    task_id: value.task_id,
    project_id: value.project_id as string,
    scope_version: value.scope_version as string,
    goal: value.goal,
    acceptance: [...value.acceptance],
    evidence_refs: [...value.evidence_refs],
  };
  return parseEvent({
    id: packet.task_id + ":" + packet.scope_version,
    type: "ei.task.ready",
    subject: packet.task_id,
    summary: "Project task packet",
    detail: JSON.stringify(packet),
  });
}
