import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { attachCorral } from "@fungi.computer/corral";

await Effect.runPromise(
  Effect.scoped(
    attachCorral(
      Stream.fromIterable([{ state: "idle" }, { state: "working" }]),
      {
        environment: {},
        onError: (error) => console.error("Herdr report failed", error),
      },
    ),
  ),
);
console.log("attached outside Herdr"); // no HERDR_ENV: a safe no-op
