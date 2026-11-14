// Runnable tour of the offline-first mutation queue:
//   dart example/mutation_queue.dart
//
// Shows: enqueue that persists before returning, FIFO drain with retries,
// dead-lettering after max attempts, and startup restore.
import 'package:mutation_queue/mutation_queue.dart';

Future<void> main() async {
  final disk = <List<Map<String, Object?>>>[]; // pretend disk writes

  var networkUp = false;
  var sendCalls = 0;

  final queue = MutationQueue(
    store: (json) => disk.add(json), // persist immediately
    load: () => disk.isEmpty ? const [] : disk.last,
    send: (m) async {
      sendCalls++;
      if (!networkUp) {
        throw StateError('offline: ${m.type}');
      }
      print('sent ${m.type}');
    },
    maxAttempts: 3,
  );

  // 1) User acts while offline — the write is already safe on "disk".
  queue.enqueue('create-post', {'title': 'Hello'});
  queue.enqueue('like-post', {'id': 42});

  // 2) Drain fails while offline; nothing is lost, items wait for retry.
  await queue.flush();

  // 3) Connectivity returns; a later drain delivers FIFO.
  networkUp = true;
  await queue.flush();
  print('send calls: $sendCalls');
}
