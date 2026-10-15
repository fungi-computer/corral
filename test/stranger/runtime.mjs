import { createServer } from "node:net";
import { createInterface } from "node:readline";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import process from "node:process";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import corral, { attachCorral } from "@fungi.computer/corral";

if (corral.protocol !== "shiitake.client/v1")
  throw new Error("Corral stranger lost its public client entry protocol");

if (corral.attach === undefined)
  throw new Error("Corral must advertise attachment");

// This installed-artifact proof owns its socket, never the runner's live pane.
process.env.HERDR_ENV = "0";
const noOpErrors = [];
await Effect.runPromise(
  Effect.scoped(
    corral.attach({
      activity: Stream.toAsyncIterable(Stream.succeed("working")),
      signal: new globalThis.AbortController().signal,
      onError: (error) => noOpErrors.push(error),
    }),
  ),
);
if (noOpErrors.length !== 0)
  throw new Error("Corral outside Herdr should be a scoped no-op");

const root = await mkdtemp(join(tmpdir(), "corral-stranger-"));
const socketPath = join(root, "herdr.sock");
const calls = [];
const workingReported = Promise.withResolvers();
const sockets = new Set();
const server = createServer((socket) => {
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
  const lines = createInterface({ input: socket });
  const interrupted = (cause) => {
    if (cause.code !== "ECONNRESET") throw cause;
  };
  socket.on("error", interrupted);
  lines.on("error", interrupted);
  lines.once("line", (line) => {
    const request = JSON.parse(line);
    calls.push(request);
    socket.end(`${JSON.stringify({ id: request.id, result: {} })}\n`);
    if (request.params.state === "working") workingReported.resolve();
  });
});
server.listen(socketPath);
await once(server, "listening");
const scope = await Effect.runPromise(Scope.make());
try {
  await Effect.runPromise(
    attachCorral(Stream.fromIterable(["idle", "working"]), {
      environment: {
        HERDR_ENV: "1",
        HERDR_SOCKET_PATH: socketPath,
        HERDR_PANE_ID: "stranger-pane",
      },
      onError: (error) => {
        throw error;
      },
    }).pipe(Scope.provide(scope)),
  );
  await workingReported.promise;
  await Effect.runPromise(Scope.close(scope, Exit.void));
  if (
    calls.map((request) => request.params.state ?? request.method).join(",") !==
    "idle,working,pane.release_agent"
  )
    throw new Error("Corral stranger did not report and release the pane");
} finally {
  await Effect.runPromise(Scope.close(scope, Exit.void));
  for (const socket of sockets) socket.destroy();
  await new Promise((resolve, reject) =>
    server.close((cause) => (cause ? reject(cause) : resolve())),
  );
  await rm(root, { recursive: true, force: true });
}
