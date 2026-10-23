import { defineClientEntry } from "@fungi.computer/shiitake/client-entry";
import process from "node:process";
import { once } from "node:events";
import { createConnection } from "node:net";
import { createInterface } from "node:readline";
import {
  ExtensionId,
  type ClientActivityV0,
} from "@fungi.computer/shiitake/extensions";
import * as Stream from "effect/Stream";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

/** Pane environment supplied by the local Herdr host. */
export type CorralEnvironment = Readonly<{
  /** Herdr sets this to "1" for managed processes. */
  HERDR_ENV?: string | undefined;
  /** Opaque pane identity inherited by the client. */
  HERDR_PANE_ID?: string | undefined;
  /** Local API socket (or named pipe on Windows). */
  HERDR_SOCKET_PATH?: string | undefined;
}>;
/** Activity supplied by the client; blocked means a human response is pending. */
export type CorralActivity = ClientActivityV0;
const source = "fungi:agent:v1";
let reportSequence = Date.now() * 1000;

type HerdrAgentParams = {
  pane_id: string;
  source: string;
  agent: string;
  seq: number;
  state?: CorralActivity["state"];
  message?: string;
};

const decodeResponse = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ id: Schema.String, result: Schema.Unknown }),
      Schema.Struct({
        id: Schema.String,
        error: Schema.Struct({ code: Schema.String, message: Schema.String }),
      }),
    ]),
  ),
);

function nextSequence() {
  reportSequence = Math.max(reportSequence + 1, Date.now() * 1000);
  return reportSequence;
}

function request(
  endpoint: string,
  pane: string,
  activity: CorralActivity | undefined,
) {
  const state = activity?.state;
  const seq = nextSequence();
  const id = `${source}:${String(seq)}`;
  const params: HerdrAgentParams = {
    pane_id: pane,
    source,
    agent: "fungi",
    seq,
  };
  if (state !== undefined) params.state = state;
  if (state === "blocked")
    params.message =
      activity?.header === undefined
        ? "Waiting for your response"
        : `Waiting for your response: ${activity.header}`;
  const body = {
    id,
    method: state === undefined ? "pane.release_agent" : "pane.report_agent",
    params,
  };
  return Effect.tryPromise({
    try: async (signal) => {
      const socket = createConnection({ path: endpoint, signal });
      const lines = createInterface({ input: socket, crlfDelay: Infinity });
      try {
        const replies = lines[Symbol.asyncIterator]();
        const [, reply] = await Promise.all([
          once(socket, "connect", { signal }).then(() => {
            socket.write(`${JSON.stringify(body)}\n`);
          }),
          replies.next(),
        ]);
        if (reply.done) throw new Error("Herdr closed without a response");
        const response = decodeResponse(reply.value);
        if (response.id !== id) throw new Error("Herdr response ID mismatch");
        if ("error" in response)
          throw new Error(`Herdr report refused: ${response.error.code}`);
      } finally {
        lines.close();
        socket.destroy();
      }
    },
    catch: (cause) => new Error("Herdr report failed", { cause }),
  }).pipe(Effect.timeout("2 seconds"));
}

/** Attach to borrowed activity; never opens a Session watch or starts Herdr. */
export function attachCorral(
  activity: Stream.Stream<CorralActivity, Error>,
  options: Readonly<{
    environment: CorralEnvironment;
    onError: (cause: Error) => void;
  }>,
): Effect.Effect<void, Error, Scope.Scope> {
  const { HERDR_ENV, HERDR_SOCKET_PATH, HERDR_PANE_ID } = options.environment;
  if (HERDR_ENV !== "1" || !HERDR_SOCKET_PATH || !HERDR_PANE_ID)
    return Effect.void;
  const endpoint =
    process.platform === "win32"
      ? `\\\\.\\pipe\\${HERDR_SOCKET_PATH}`
      : HERDR_SOCKET_PATH;
  const send = (activity: CorralActivity | undefined) =>
    request(endpoint, HERDR_PANE_ID, activity).pipe(
      Effect.catch((cause) =>
        Effect.sync(() => {
          options.onError(cause);
        }),
      ),
    );
  return Effect.gen(function* () {
    yield* Effect.acquireRelease(Effect.void, () => send(undefined));
    yield* activity.pipe(
      Stream.changesWith(
        (a, b) => a.state === b.state && a.header === b.header,
      ),
      Stream.runForEach(send),
      Effect.catch((cause) =>
        Effect.sync(() => {
          options.onError(cause);
        }),
      ),
      Effect.forkScoped,
    );
  });
}

/** Ordinary Fungi client extension; disabled outside a Herdr pane. */
const extension = defineClientEntry(
  {
    id: ExtensionId.make("corral"),
    commands: {},
    projection: null,
  },
  {
    attach: ({ activity, onError }) =>
      attachCorral(
        Stream.fromAsyncIterable(
          activity,
          (cause) => new Error("Corral activity delivery failed", { cause }),
        ),
        {
          environment: {
            HERDR_ENV: process.env.HERDR_ENV,
            HERDR_SOCKET_PATH: process.env.HERDR_SOCKET_PATH,
            HERDR_PANE_ID: process.env.HERDR_PANE_ID,
          },
          onError,
        },
      ),
  },
);
export default extension;
