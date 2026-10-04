/// The queue core. Pure Dart: no Flutter, no IO — persistence and delivery
/// are injected, so the whole engine is deterministic to test.
library;

/// One unit of deferred work — a check-in, an incident report, any write.
class Mutation {
  Mutation({
    required this.id,
    required this.type,
    required this.payload,
    required this.createdAt,
    required this.attempts,
  });

  final String id;
  final String type;
  final Map<String, Object?> payload;
  final DateTime createdAt;
  final int attempts;

  Mutation copyWith({int? attempts}) => Mutation(
        id: id,
        type: type,
        payload: payload,
        createdAt: createdAt,
        attempts: attempts ?? this.attempts,
      );

  Map<String, Object?> toJson() => {
        'id': id,
        'type': type,
        'payload': payload,
        'created_at': createdAt.toIso8601String(),
        'attempts': attempts,
      };

  factory Mutation.fromJson(Map<String, Object?> json) => Mutation(
        id: json['id']! as String,
        type: json['type']! as String,
        payload: (json['payload'] as Map).cast<String, Object?>(),
        createdAt: DateTime.parse(json['created_at']! as String),
        attempts: json['attempts']! as int,
      );
}

/// Internal queue state — serialized to storage and back.
class QueuedMutation {
  QueuedMutation({required this.mutation, required this.status});

  /// Mutable by design: the queue advances attempts/status in place and
  /// persists after every transition.
  Mutation mutation;
  MutationStatus status;
}

enum MutationStatus { pending, inflight, dead }

/// Delivers a mutation to your backend. Throw to signal failure (the queue
/// will retry); return normally to signal success.
typedef MutationSender = Future<void> Function(Mutation mutation);

/// Persists the queue. Wire this to `shared_preferences`, a file, SQLite —
/// whatever survives an app restart on your target platform.
typedef MutationStorage = void Function(List<Map<String, Object?>> queueJson);

/// Reads the persisted queue back at startup.
typedef MutationLoader = List<Map<String, Object?>> Function();

/// The queue engine.
///
/// ```dart
/// final queue = MutationQueue(
///   store: (json) => prefs.setString('queue', jsonEncode(json)),
///   load: () => jsonDecode(prefs.getString('queue') ?? '[]') as List,
///   send: (m) => api.post('/sync', m.toJson()),
/// );
/// await queue.restore();
/// connectivity.onStatusChanged.listen((s) {
///   if (s == ConnectivityResult.online) queue.flush();
/// });
/// ```
class MutationQueue {
  MutationQueue({
    required MutationStorage store,
    required MutationLoader load,
    required MutationSender send,
    int maxAttempts = 3,
  })  : _store = store,
        _load = load,
        _send = send,
        _maxAttempts = maxAttempts;

  final MutationStorage _store;
  final MutationLoader _load;
  final MutationSender _send;
  final int _maxAttempts;

  List<QueuedMutation> _queue = [];
  Future<void>? _flushing;
  final List<void Function()> _listeners = [];

  /// Restores the persisted queue. Call once at app start, before any enqueue.
  void restore() {
    _queue = _load()
        .map(
          (row) => QueuedMutation(
            mutation: Mutation.fromJson(row),
            status: MutationStatus.pending,
          ),
        )
        .toList();
    _notify();
  }

  /// Adds a mutation. It is persisted **before** this method returns — a
  /// crash right after enqueue can never lose the write.
  Mutation enqueue(
    String type,
    Map<String, Object?> payload, {
    String? id,
    DateTime? createdAt,
  }) {
    final mutation = Mutation(
      id: id ?? _newId(),
      type: type,
      payload: payload,
      createdAt: createdAt ?? DateTime.now(),
      attempts: 0,
    );
    _queue.add(
        QueuedMutation(mutation: mutation, status: MutationStatus.pending));
    _persist();
    _notify();
    return mutation;
  }

  /// Drains the queue FIFO. Concurrent calls share one drain, so nothing
  /// double-sends.
  Future<void> flush() {
    final running = _flushing;
    if (running != null) return running;
    final future = _drain().whenComplete(() => _flushing = null);
    _flushing = future;
    return future;
  }

  Future<void> _drain() async {
    for (final entry in List<QueuedMutation>.from(_queue)) {
      if (entry.status == MutationStatus.dead) continue;
      entry
        ..mutation =
            entry.mutation.copyWith(attempts: entry.mutation.attempts + 1)
        ..status = MutationStatus.inflight;
      _notify();

      try {
        await _send(entry.mutation);
        _queue.removeWhere((e) => e.mutation.id == entry.mutation.id);
        _persist();
        _notify();
      } catch (_) {
        if (entry.mutation.attempts >= _maxAttempts) {
          entry.status = MutationStatus.dead; // parked, visible, never dropped
        } else {
          entry.status = MutationStatus.pending; // retried on the next flush
        }
        _persist();
        _notify();
      }
    }
  }

  /// Mutations that exhausted their attempts — show them, let the user
  /// retry or discard them; they are never silently removed.
  List<Mutation> deadLetter() => _queue
      .where((e) => e.status == MutationStatus.dead)
      .map((e) => e.mutation)
      .toList();

  /// Drops a dead-lettered mutation the caller explicitly gave up on.
  void discard(String id) {
    _queue.removeWhere((e) => e.mutation.id == id);
    _persist();
    _notify();
  }

  /// Gives a dead-lettered mutation a fresh attempt budget and requeues it.
  void revive(String id) {
    for (final entry in _queue) {
      if (entry.mutation.id == id) {
        entry
          ..mutation = entry.mutation.copyWith(attempts: 0)
          ..status = MutationStatus.pending;
      }
    }
    _persist();
    _notify();
  }

  /// Fires on every queue change — wire it to setState/bloc state.
  void addListener(void Function() listener) => _listeners.add(listener);

  void removeListener(void Function() listener) => _listeners.remove(listener);

  /// Counts for UI badges: `pending`, `inflight`, `dead`.
  QueueSnapshot snapshot() {
    int count(MutationStatus status) =>
        _queue.where((e) => e.status == status).length;
    return QueueSnapshot(
      pending: count(MutationStatus.pending),
      inflight: count(MutationStatus.inflight),
      dead: count(MutationStatus.dead),
    );
  }

  bool get isFlushInProgress => _flushing != null;

  void _persist() {
    _store(_queue.map((e) => e.mutation.toJson()).toList());
  }

  void _notify() {
    for (final listener in List.of(_listeners)) {
      listener();
    }
  }

  String _newId() =>
      '${DateTime.now().microsecondsSinceEpoch}-${DateTime.now().hashCode.toRadixString(36)}';
}

class QueueSnapshot {
  const QueueSnapshot(
      {required this.pending, required this.inflight, required this.dead});

  final int pending;
  final int inflight;
  final int dead;

  int get total => pending + inflight + dead;
}
