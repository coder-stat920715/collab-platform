package com.acme.collab.realtime.application;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.realtime.application.Exceptions.NotOwnerException;
import com.acme.collab.realtime.application.Ports.Broadcaster;
import com.acme.collab.realtime.application.Ports.DirtyTracker;
import com.acme.collab.realtime.application.Ports.LeaseStore;
import com.acme.collab.realtime.application.Ports.OpLog;
import com.acme.collab.realtime.application.Ports.SnapshotRepository;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Owns document actors on this node: lease acquisition, state loading, renewal and idle eviction. */
@Component
public class ActorRegistry {
  private final LeaseStore leases;
  private final SnapshotRepository snapshots;
  private final OpLog opLog;
  private final Broadcaster broadcaster;
  private final DirtyTracker dirty;
  private final String nodeId;
  private final Duration leaseTtl;
  private final int window;
  private final Set<String> owned = ConcurrentHashMap.newKeySet();
  private final Cache<String, Mono<DocumentActor>> actors;

  public ActorRegistry(LeaseStore leases, SnapshotRepository snapshots, OpLog opLog, Broadcaster broadcaster,
                       DirtyTracker dirty,
                       @Value("${collab.node-id}") String nodeId,
                       @Value("${collab.lease-ttl}") Duration leaseTtl,
                       @Value("${collab.actor.history-window}") int window,
                       @Value("${collab.actor.idle-evict}") Duration idleEvict) {
    this.leases = leases;
    this.snapshots = snapshots;
    this.opLog = opLog;
    this.broadcaster = broadcaster;
    this.dirty = dirty;
    this.nodeId = nodeId;
    this.leaseTtl = leaseTtl;
    this.window = window;
    this.actors = Caffeine.newBuilder()
        .expireAfterAccess(idleEvict)
        .removalListener((String docId, Mono<DocumentActor> m, RemovalCause cause) -> {
          if (m != null) m.subscribe(DocumentActor::shutdown, e -> { });
          owned.remove(docId);
          leases.release(docId, nodeId).subscribe(v -> { }, e -> { });
        })
        .build();
  }

  public Mono<DocumentActor> get(String docId) {
    return actors.get(docId, id -> load(id)
        .doOnError(e -> actors.invalidate(id))
        .cache());
  }

  private Mono<DocumentActor> load(String docId) {
    return leases.tryAcquire(docId, nodeId, leaseTtl)
        .switchIfEmpty(Mono.defer(() -> leases.ownerOf(docId)
            .defaultIfEmpty("unknown")
            .flatMap(o -> Mono.<Long>error(new NotOwnerException(o)))))
        .flatMap(epoch -> snapshots.latest(docId)
            .defaultIfEmpty(DocumentSnapshot.empty(docId))
            .flatMap(base -> opLog.readFrom(docId, base.revision()).collectList()
                .map(tail -> new DocumentActor(docId, epoch, base, tail, window, opLog, broadcaster, dirty))))
        .doOnSuccess(a -> owned.add(docId));
  }

  /** Renews every owned lease; if renewal fails the actor is dropped so it stops writing. */
  @Scheduled(fixedRate = 2000)
  void renewLeases() {
    Flux.fromIterable(owned)
        .flatMap(id -> leases.renew(id, nodeId, leaseTtl)
            .filter(ok -> !ok)
            .doOnNext(x -> actors.invalidate(id)))
        .subscribe(x -> { }, e -> { });
    actors.cleanUp();
  }

  public int activeActors() { return owned.size(); }
}
