import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import corral, {
  attachCorral,
  type CorralActivity,
  type CorralEnvironment,
} from "@fungi.computer/corral";

const environment: CorralEnvironment = {};
const activity: Stream.Stream<CorralActivity> = Stream.succeed({
  state: "idle",
});
const attached: Effect.Effect<void, Error, Scope.Scope> = attachCorral(
  activity,
  { environment, onError: () => undefined },
);
const extensionProtocol: "shiitake.client/v1" = corral.protocol;
if (corral.attach === undefined)
  throw new Error("Corral must advertise attachment");
const extensionAttach: Effect.Effect<void, Error, Scope.Scope> = corral.attach({
  activity: Stream.toAsyncIterable(activity),
  signal: new AbortController().signal,
  onError: () => undefined,
});

void attached;
void extensionAttach;
void extensionProtocol;
