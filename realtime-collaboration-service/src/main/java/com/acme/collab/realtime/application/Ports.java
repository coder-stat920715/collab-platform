package com.acme.collab.realtime.application;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Frame;
import com.acme.collab.common.Operation;
import com.acme.collab.common.PresenceEvent;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Outbound ports implemented by adapters. */
public final class Ports {
  private Ports() {}

  public interface OpLog {
    Mono<Void> append(String docId, long epoch, Operation op);
    Flux<Operation> readFrom(String docId, long afterSeq);
  }

  public interface Broadcaster {
    Mono<Void> publish(String docId, Operation op);
    Mono<Void> publishPresence(String docId, PresenceEvent ev);
    Flux<Frame> subscribe(String docId);
  }

  public interface LeaseStore {
    Mono<Long> tryAcquire(String docId, String nodeId, Duration ttl);
    Mono<Boolean> renew(String docId, String nodeId, Duration ttl);
    Mono<Void> release(String docId, String nodeId);
    Mono<String> ownerOf(String docId);
  }

  public interface SnapshotRepository {
    Mono<DocumentSnapshot> latest(String docId);
    Mono<Void> save(DocumentSnapshot snapshot);
  }

  public interface PresenceStore {
    Mono<Void> upsert(PresenceEvent ev);
    Mono<Void> remove(String docId, String userId);
    Flux<PresenceEvent> list(String docId);
  }

  public interface DirtyTracker {
    Mono<Void> markDirty(String docId);
  }
}
