package com.acme.collab.snapshot;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Operation;
import com.acme.collab.common.OtEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Drains per-document op logs from Redis Streams, composes them onto the last snapshot,
 * archives ops and the new snapshot, and trims the stream. Idempotent and safe to run on several nodes.
 */
@Service
public class SnapshotService {
  private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);
  static final String DIRTY = "dirty:docs";

  private final ReactiveStringRedisTemplate redis;
  private final ObjectMapper mapper;
  private final ArchiveStore archive;
  private final int opsThreshold;
  private final long maxAgeMillis;
  private final long retainOps;
  private final Map<String, Long> lastSnapshotAt = new ConcurrentHashMap<>();

  public SnapshotService(ReactiveStringRedisTemplate redis, ObjectMapper mapper, ArchiveStore archive,
                         @Value("${snapshot.ops-threshold}") int opsThreshold,
                         @Value("${snapshot.max-age-seconds}") long maxAgeSeconds,
                         @Value("${snapshot.retain-ops}") long retainOps) {
    this.redis = redis;
    this.mapper = mapper;
    this.archive = archive;
    this.opsThreshold = opsThreshold;
    this.maxAgeMillis = maxAgeSeconds * 1000;
    this.retainOps = retainOps;
  }

  @Scheduled(fixedDelayString = "${snapshot.interval-ms}")
  public void tick() {
    redis.opsForSet().members(DIRTY)
        .flatMap(id -> snapshotIfNeeded(id)
            .onErrorResume(e -> {
              log.warn("snapshot failed for {}", id, e);
              return redis.opsForSet().add(DIRTY, id).then();         // retry next tick
            }), 8)
        .subscribe(v -> { }, e -> log.error("snapshot tick failed", e));
  }

  Mono<Void> snapshotIfNeeded(String docId) {
    return redis.opsForValue().get(snapKey(docId))
        .map(this::readSnapshot)
        .defaultIfEmpty(DocumentSnapshot.empty(docId))
        .flatMap(base -> redis.opsForStream()
            .range(opsKey(docId), Range.rightUnbounded(Range.Bound.exclusive(base.revision() + "-0")))
            .map(rec -> readOp(String.valueOf(rec.getValue().get("op"))))
            .collectList()
            .flatMap(ops -> compose(docId, base, ops)));
  }

  private Mono<Void> compose(String docId, DocumentSnapshot base, List<Operation> ops) {
    if (ops.isEmpty()) return redis.opsForSet().remove(DIRTY, docId).then();
    long now = System.currentTimeMillis();
    long last = lastSnapshotAt.getOrDefault(docId, 0L);
    if (ops.size() < opsThreshold && now - last < maxAgeMillis) return Mono.empty();   // keep dirty, try later

    // Unmark first: an op arriving while we work re-adds the mark, so nothing is lost.
    return redis.opsForSet().remove(DIRTY, docId).then(Mono.defer(() -> {
      String content = base.content();
      for (Operation op : ops) content = OtEngine.apply(content, op.components());
      long rev = ops.get(ops.size() - 1).serverSeq();
      var snap = new DocumentSnapshot(docId, rev, content, now);
      return archive.saveOps(docId, ops)                                // archive first, then publish snapshot
          .then(archive.saveSnapshot(snap))
          .then(redis.opsForValue().set(snapKey(docId), writeJson(snap)))
          .then(redis.opsForStream().trim(opsKey(docId), retainOps))
          .doOnSuccess(x -> {
            lastSnapshotAt.put(docId, now);
            log.info("snapshot doc={} rev={} ops={}", docId, rev, ops.size());
          })
          .then();
    }));
  }

  static String opsKey(String id) { return "doc:{" + id + "}:ops"; }
  static String snapKey(String id) { return "snap:{" + id + "}"; }

  private DocumentSnapshot readSnapshot(String json) {
    try { return mapper.readValue(json, DocumentSnapshot.class); } catch (Exception e) { throw new IllegalStateException(e); }
  }
  private Operation readOp(String json) {
    try { return mapper.readValue(json, Operation.class); } catch (Exception e) { throw new IllegalStateException(e); }
  }
  private String writeJson(Object o) {
    try { return mapper.writeValueAsString(o); } catch (Exception e) { throw new IllegalStateException(e); }
  }
}
