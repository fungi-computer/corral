# Corral (`@fungi.computer/corral`)

Corral reports a Shiitake client's current activity in the active Herdr pane. It
is an optional client extension: the client supplies activity, and Herdr owns
the pane and its lifecycle.

## Install or set up

```sh
npm install @fungi.computer/corral
```

The package ships ESM only and depends on `effect` and
[Shiitake](../shiitake/README.md) at runtime. Its only entrypoint,
`@fungi.computer/corral`, exports the Shiitake client extension as its default
export and `attachCorral` for hosts that supply their own activity stream.

The default extension reads `HERDR_ENV`, `HERDR_SOCKET_PATH`, and
`HERDR_PANE_ID` from its process environment. It attaches only when `HERDR_ENV`
is `1` and both the socket path and pane ID are set. Outside that environment,
attachment has no effect.

## Quick start

This illustrative example attaches a stream supplied by the client. Closing the
Effect scope stops reporting and releases the pane's report:

```ts
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { attachCorral } from "@fungi.computer/corral";

const scope = await Effect.runPromise(Scope.make());
await Effect.runPromise(
  attachCorral(Stream.fromIterable(["idle", "working"]), {
    environment: {
      HERDR_ENV: "1",
      HERDR_SOCKET_PATH: "/path/to/herdr.sock",
      HERDR_PANE_ID: "pane-id",
    },
    onError: (error) => console.error("Herdr report failed", error),
  }).pipe(Scope.provide(scope)),
);
await Effect.runPromise(Scope.close(scope, Exit.void));
```

The snippet is illustrative: a real client supplies its live activity stream and
the Herdr values from its managed environment. The integration tests run report
and release requests against a local socket server in
[`test/activity.test.ts`](test/activity.test.ts).

## Core concepts

The activity stream contains `idle`, `working`, `blocked`, or `unknown`. Corral
reports changes from that stream. It does not open another Session watch or read
a Session database. The client remains responsible for producing activity. The
default entry borrows an AsyncIterable with an independent sliding latest-value
subscription per iterator and a detach signal. `attachCorral` accepts an Effect
stream, including a stream with typed `Error` failures. Source and socket errors
reach `onError`; closing the scope still releases the pane report. Fungi borrows
its existing Session observation and command interaction store. A pending
question or browser hand-off reports `blocked`. Answering it restores the
current activity. Question text, answers and credentials are never included in
reports.

Herdr derives `done` when an unseen background turn returns to `idle`. It owns
whether the user has seen that completion. Corral never guesses completion from
an unavailable observation.

`attachCorral` needs an Effect scope. When that scope closes, Corral cancels its
reporter, closes any in-flight socket, and releases the current pane report. It
sends `pane.report_agent` and `pane.release_agent` over Herdr's API socket with
the `fungi:shiitake:v1` source and `shiitake` agent label. A monotone timestamp
sequence orders reports across attachments and process restarts.

## API reference

See the generated [Corral API reference](docs/api/README.md). The default export
is an attach-only `shiitake.client/v1` entry. `attachCorral` is the lower-level
integration for hosts that supply an activity stream directly.

## Examples and checks

The activity tests cover environment gating, activity changes, cleanup on scope
close, and report failure handling. Run them with:

```sh
pnpm --filter @fungi.computer/corral test
```

## Limits and errors

Corral is trusted local extension code, not a sandbox. It does not start or stop
the Herdr service and does not decide what counts as successful work. Report and
release requests are optional IPC: each is limited to two seconds. Failures are
passed to `onError`. A reporting failure does not veto client activity or
terminal restoration. The caller owns any user-facing recovery.

The wire contract follows Herdr's
[agent integration guide](https://herdr.dev/docs/add-herdr-support/) and its
[Pi extension](https://github.com/herdrdev/herdr/blob/v0.9.3/src/integration/assets/pi/herdr-agent-state.ts).

## Contribute and license

Corral is MIT licensed. See [`LICENSE`](LICENSE).
