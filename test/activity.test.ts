import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { createInterface } from "node:readline";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import * as Stream from "effect/Stream";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { parseClientEntryV1 } from "@fungi.computer/shiitake/client-entry";
import corral, { attachCorral, type CorralActivity } from "../src/index.js";

const requestSchema = Schema.Struct({
  id: Schema.String,
  method: Schema.Literals(["pane.report_agent", "pane.release_agent"]),
  params: Schema.Struct({
    pane_id: Schema.String,
    source: Schema.String,
    agent: Schema.String,
    seq: Schema.Number,
    state: Schema.optionalKey(
      Schema.Literals(["unknown", "idle", "working", "blocked"]),
    ),
    message: Schema.optionalKey(Schema.String),
  }),
});
type Request = typeof requestSchema.Type;
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(requestSchema),
);

async function herdr(receive: (request: Request, socket: Socket) => void) {
  const root = await mkdtemp(join(tmpdir(), "corral-socket-"));
  const path = join(root, "api.sock");
  const connections = new Set<Socket>();
  const server = createServer((socket) => {
    connections.add(socket);
    socket.once("close", () => connections.delete(socket));
    const lines = createInterface({ input: socket });
    // Cancellation intentionally destroys an unacknowledged client socket.
    const interrupted = (cause: Error) => {
      expect(cause).toMatchObject({ code: "ECONNRESET" });
    };
    socket.on("error", interrupted);
    lines.on("error", interrupted);
    lines.once("line", (line) => {
      receive(decodeRequest(line), socket);
    });
  });
  server.listen(path);
  await once(server, "listening");
  return {
    environment: {
      HERDR_ENV: "1",
      HERDR_SOCKET_PATH: path,
      HERDR_PANE_ID: "test-pane",
    },
    close: async () => {
      for (const socket of connections) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        }),
      );
      await rm(root, { recursive: true, force: true });
    },
  };
}

async function attachmentFor(
  activity: Parameters<typeof attachCorral>[0],
  options: Parameters<typeof attachCorral>[1],
) {
  const scope = await Effect.runPromise(Scope.make());
  await Effect.runPromise(
    attachCorral(activity, options).pipe(Scope.provide(scope)),
  );
  return { close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

it("installs an attach-only client entry through public V1 admission", () => {
  const entry = parseClientEntryV1(corral);
  expect(entry.id).toBe("corral");
  expect(entry.protocol).toBe("shiitake.client/v1");
  expect(entry.contribute).toBeUndefined();
});

it("does not subscribe outside a Herdr socket environment", async () => {
  let subscribed = false;
  const errors: Error[] = [];
  const attachment = await attachmentFor(
    Stream.fromEffect(
      Effect.sync((): CorralActivity => {
        subscribed = true;
        return { state: "idle" };
      }),
    ),
    { environment: {}, onError: (error) => errors.push(error) },
  );
  await attachment.close();
  expect(subscribed).toBe(false);
  expect(errors).toEqual([]);
});

it("reports all states in order with deduplication, monotone sequence and one release", async () => {
  const requests: Request[] = [];
  const errors: Error[] = [];
  const reported = Promise.withResolvers<undefined>();
  const host = await herdr((request, socket) => {
    requests.push(request);
    const response = `${JSON.stringify({ id: request.id, result: {} })}\n`;
    // Response framing must also work when a socket read splits JSON.
    socket.write(response.slice(0, 10));
    socket.end(response.slice(10));
    if (requests.length === 5) reported.resolve(undefined);
  });
  const states: readonly CorralActivity[] = [
    { state: "unknown" },
    { state: "idle" },
    { state: "idle" },
    { state: "working" },
    { state: "blocked" },
    { state: "idle" },
  ];
  const attachment = await attachmentFor(Stream.fromIterable(states), {
    environment: host.environment,
    onError: (error) => errors.push(error),
  });
  try {
    await reported.promise;
    await attachment.close();
    await attachment.close();
    expect(requests.map((request) => request.params.state)).toEqual([
      "unknown",
      "idle",
      "working",
      "blocked",
      "idle",
      undefined,
    ]);
    expect(requests.at(-1)?.method).toBe("pane.release_agent");
    expect(requests[3]?.params.message).toBe("Waiting for your response");
    for (const [index, request] of requests.entries()) {
      expect(request.params.agent).toBe("fungi");
      expect(request.params.source).toBe("fungi:agent:v1");
      expect(request.params.pane_id).toBe("test-pane");
      if (index > 0)
        expect(request.params.seq).toBeGreaterThan(
          requests[index - 1]?.params.seq ?? 0,
        );
    }
    expect(errors).toEqual([]);
  } finally {
    await attachment.close();
    await host.close();
  }
});

it("cancels an unacknowledged socket report and releases exactly once", async () => {
  const started = Promise.withResolvers<undefined>();
  const closed = Promise.withResolvers<undefined>();
  const requests: Request[] = [];
  const errors: Error[] = [];
  const host = await herdr((request, socket) => {
    requests.push(request);
    if (request.method === "pane.report_agent") {
      socket.once("close", () => {
        closed.resolve(undefined);
      });
      started.resolve(undefined);
    } else socket.end(`${JSON.stringify({ id: request.id, result: {} })}\n`);
  });
  const attachment = await attachmentFor(Stream.succeed({ state: "working" }), {
    environment: host.environment,
    onError: (error) => errors.push(error),
  });
  try {
    await started.promise;
    await attachment.close();
    await closed.promise;
    await attachment.close();
    expect(requests.map((request) => request.method)).toEqual([
      "pane.report_agent",
      "pane.release_agent",
    ]);
    expect(errors).toEqual([]);
  } finally {
    await attachment.close();
    await host.close();
  }
});

it("reports a refused response through onError and still releases", async () => {
  const observed = Promise.withResolvers<undefined>();
  const errors: Error[] = [];
  const requests: Request[] = [];
  const host = await herdr((request, socket) => {
    requests.push(request);
    socket.end(
      `${JSON.stringify(
        request.method === "pane.report_agent"
          ? {
              id: request.id,
              error: {
                code: "report_not_accepted",
                message: "Fixture refusal",
              },
            }
          : { id: request.id, result: {} },
      )}\n`,
    );
  });
  const attachment = await attachmentFor(Stream.succeed({ state: "idle" }), {
    environment: host.environment,
    onError: (error) => {
      errors.push(error);
      observed.resolve(undefined);
    },
  });
  try {
    await observed.promise;
    await attachment.close();
    expect(errors).toHaveLength(1);
    expect(errors[0]?.cause).toEqual(
      new Error("Herdr report refused: report_not_accepted"),
    );
    expect(requests.at(-1)?.method).toBe("pane.release_agent");
  } finally {
    await attachment.close();
    await host.close();
  }
});

it("reports a failed borrowed activity stream with its original cause and still releases", async () => {
  const failure = new Error("Activity source failed");
  const errors: Error[] = [];
  const requests: Request[] = [];
  const reported = Promise.withResolvers<undefined>();
  const host = await herdr((request, socket) => {
    requests.push(request);
    socket.end(`${JSON.stringify({ id: request.id, result: {} })}\n`);
  });
  const attachment = await attachmentFor(Stream.fail(failure), {
    environment: host.environment,
    onError: (error) => {
      errors.push(error);
      reported.resolve(undefined);
    },
  });
  try {
    await reported.promise;
    await attachment.close();
    expect(errors).toEqual([failure]);
    expect(errors[0]).toBe(failure);
    expect(requests.map(({ method }) => method)).toEqual([
      "pane.release_agent",
    ]);
  } finally {
    await attachment.close();
    await host.close();
  }
});

it("sends only an attention header with the blocked report", async () => {
  const requests: Request[] = [];
  const reported = Promise.withResolvers<undefined>();
  const host = await herdr((request, socket) => {
    requests.push(request);
    socket.end(`${JSON.stringify({ id: request.id, result: {} })}\n`);
    if (requests.length === 1) reported.resolve(undefined);
  });
  const attachment = await attachmentFor(
    Stream.succeed({ state: "blocked", header: "Scope" }),
    { environment: host.environment, onError: () => undefined },
  );
  try {
    await reported.promise;
    expect(requests[0]?.params.message).toBe(
      "Waiting for your response: Scope",
    );
    const wire = JSON.stringify(requests[0]);
    expect(wire).not.toContain("Which scope should the migration cover?");
    expect(wire).not.toContain("whole repository");
  } finally {
    await attachment.close();
    await host.close();
  }
});
