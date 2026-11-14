# mutation_queue

[![pub.dev](https://img.shields.io/pub/v/mutation_queue.svg)](https://pub.dev/packages/mutation_queue)

**Offline-first mutation queue for Dart & Flutter.**

Field crews work in basements, on ships, on remote sites — the network is gone
for hours and comes back for thirty seconds. Every write flows through this
queue: persisted before `enqueue` returns, drained FIFO when connectivity
returns, retried with a per-item attempt limit, and dead-lettered when it
exhausts its budget.

[![CI](https://github.com/hossam1244/mutation_queue/actions/workflows/ci.yml/badge.svg)](https://github.com/hossam1244/mutation_queue/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Install

```yaml
dependencies:
  mutation_queue: ^0.1.0
```

## Why

Making an app work offline is mostly making *writes* work offline. The
scattered ad-hoc versions of this logic (a list, some `isOnline` checks, a
retry loop that maybe survives a restart) miss the hard parts:

| Guarantee | How |
|---|---|
| **Nothing is lost** | `enqueue` persists to your storage *before* returning — a crash can never lose a write |
| **FIFO drain** | `flush` sends in order; a restored queue from yesterday drains before today's |
| **Retry with a limit** | Failures requeue for the next flush; after `maxAttempts` the item is parked |
| **Never silently dropped** | Dead-lettered items stay visible via `deadLetter()` until you `revive()` or `discard()` them |
| **One drain** | Concurrent `flush()` calls share a single in-flight drain — nothing double-sends |
| **Yours at the edges** | Storage and delivery are injected functions — `shared_preferences`, a file, SQLite, whatever |

Extracted from the author's offline-first React Native field app
([crewops-rn](https://github.com/hossam1244/crewops-rn)); this is the same
engine, distilled to pure Dart.

## Usage

```dart
final queue = MutationQueue(
  store: (json) => prefs.setString('queue', jsonEncode(json)),
  load: () => jsonDecode(prefs.getString('queue') ?? '[]') as List,
  send: (mutation) => dio.post('/sync', data: mutation.toJson()),
  maxAttempts: 3,
);

await queue.restore();

// UI: enqueue everywhere, never await the network
checkInButton.onPressed = () => queue.enqueue('checkin', {'site': 'A'});

// Connectivity returns → drain
connectivity.onStatusChanged.listen((status) {
  if (status == ConnectivityResult.online) queue.flush();
});

// Badge: "3 pending · 1 sending · 0 failed"
queue.addListener(() => setState(() => stats = queue.snapshot()));
```

## Testing

10 deterministic tests: persist-before-return, FIFO order, retry-after-outage,
dead-letter after max attempts, `revive`/`discard` semantics, single shared
drain under concurrent flushes, startup restore, listener events, and JSON
round-trip through storage.

```bash
dart test
```

## License

[MIT](LICENSE)
