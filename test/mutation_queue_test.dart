import 'dart:convert';

import 'package:mutation_queue/mutation_queue.dart';
import 'package:test/test.dart';

void main() {
  late List<Map<String, Object?>> persisted;

  MutationQueue build({
    required Future<void> Function(Mutation) send,
    int maxAttempts = 3,
    List<Map<String, Object?>> initial = const [],
  }) {
    persisted = List.of(initial);
    final queue = MutationQueue(
      store: (json) => persisted = List.of(json),
      load: () => persisted,
      send: send,
      maxAttempts: maxAttempts,
    );
    queue.restore();
    return queue;
  }

  test('enqueue persists before anything else happens', () {
    final queue = build(send: (m) async {});
    queue.enqueue('checkin', {'note': 'hi'});

    expect(persisted.length, 1);
    expect(persisted.first['type'], 'checkin');
  });

  test('drains FIFO', () async {
    final order = <String>[];
    final queue =
        build(send: (m) async => order.add(m.payload['n']! as String));
    queue.enqueue('a', {'n': '1'});
    queue.enqueue('a', {'n': '2'});
    queue.enqueue('a', {'n': '3'});

    await queue.flush();

    expect(order, ['1', '2', '3']);
    expect(queue.snapshot().pending, 0);
  });

  test('failed sends requeue for the next flush and eventually succeed',
      () async {
    var failuresLeft = 1;
    final delivered = <String>[];
    final queue = build(
      send: (m) async {
        if (failuresLeft > 0) {
          failuresLeft--;
          throw Exception('network gone');
        }
        delivered.add(m.payload['n']! as String);
      },
    );
    queue.enqueue('a', {'n': 'important'});

    await queue.flush();
    expect(queue.snapshot().pending, 1, reason: 'requeued, not dropped');

    await queue.flush();
    expect(delivered, ['important']);
    expect(queue.snapshot().total, 0);
  });

  test('dead-letters after max attempts — never silently dropped', () async {
    final queue = build(
      send: (m) async => throw Exception('server rejects'),
      maxAttempts: 2,
    );
    final id = queue.enqueue('incident', {'title': 'bad'}).id;

    await queue.flush();
    await queue.flush();

    final dead = queue.deadLetter();
    expect(dead.length, 1);
    expect(dead.first.id, id);
    expect(dead.first.attempts, 2);
  });

  test('revive gives a dead mutation a fresh budget', () async {
    var attempts = 0;
    final queue = build(
      send: (m) async {
        attempts++;
        if (attempts <= 2) throw Exception('first two fail');
      },
      maxAttempts: 2,
    );
    final id = queue.enqueue('a', {}).id;

    await queue.flush();
    await queue.flush();
    expect(queue.deadLetter().length, 1);

    queue.revive(id);
    await queue.flush();

    expect(queue.deadLetter(), isEmpty);
    expect(queue.snapshot().total, 0);
    expect(attempts, 3);
  });

  test('discard removes a dead mutation explicitly', () async {
    final queue = build(send: (m) async => throw Exception(), maxAttempts: 1);
    final id = queue.enqueue('a', {}).id;
    await queue.flush();

    queue.discard(id);

    expect(queue.snapshot().total, 0);
    expect(persisted, isEmpty);
  });

  test('concurrent flushes share one drain — nothing double-sends', () async {
    var sends = 0;
    final queue = build(
      send: (m) async {
        sends++;
        await Future<void>.delayed(const Duration(milliseconds: 30));
      },
    );
    queue.enqueue('a', {});
    queue.enqueue('b', {});

    await Future.wait([queue.flush(), queue.flush(), queue.flush()]);

    expect(sends, 2);
  });

  test('restore brings back the persisted queue', () {
    final queue = build(send: (m) async {}, initial: [
      {
        'id': 'survivor',
        'type': 'checkin',
        'payload': {'n': 1},
        'created_at': '2026-01-01T00:00:00.000',
        'attempts': 1,
      },
    ]);

    expect(queue.snapshot().pending, 1);
  });

  test('listeners fire on enqueue and on drain events', () async {
    var events = 0;
    final queue = build(send: (m) async {});
    queue.addListener(() => events++);

    queue.enqueue('a', {});
    await queue.flush();

    expect(events, greaterThanOrEqualTo(3)); // enqueue, inflight, removed
  });

  test('payload survives a JSON round-trip through storage', () {
    final queue = build(send: (m) async {});
    final mutation = queue.enqueue('incident', {
      'title': 'Missing railing',
      'severity': 3,
      'nested': {'site': 'B'},
    });

    final restored = Mutation.fromJson(
      jsonDecode(jsonEncode(persisted.first)) as Map<String, Object?>,
    );

    expect(restored.id, mutation.id);
    expect(restored.payload['severity'], 3);
    expect((restored.payload['nested']! as Map)['site'], 'B');
  });
}
