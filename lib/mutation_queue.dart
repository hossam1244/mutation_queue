/// Offline-first mutation queue for Dart and Flutter.
///
/// Enqueue writes that persist before returning, drain them FIFO when
/// connectivity returns, retry failures with a per-item attempt limit, and
/// park anything that exhausts its attempts as dead-letter.
library;

export 'src/mutation_queue.dart';
