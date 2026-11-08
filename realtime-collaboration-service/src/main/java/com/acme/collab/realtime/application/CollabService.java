package com.acme.collab.realtime.application;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Frame;
import com.acme.collab.common.Operation;
import com.acme.collab.common.PresenceEvent;
import com.acme.collab.common.Role;

import com.acme.collab.realtime.application.Exceptions.ForbiddenException;
import com.acme.collab.realtime.application.Ports.Broadcaster;
import com.acme.collab.realtime.application.Ports.OpLog;
import com.acme.collab.realtime.application.Ports.PresenceStore;
import com.acme.collab.realtime.application.Ports.SnapshotRepository;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Map;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class CollabService {
  private final ActorRegistry registry;
  private final PresenceStore presence;
  private final Broadcaster broadcaster;
  private final OpLog opLog;
  private final SnapshotRepository snapshots;
  private final AclClient acl;
  private final Cache<String, Role> roleCache = Caffeine.newBuilder().expireAfterWrite(Duration.ofSeconds(30)).build();

  public CollabService(ActorRegistry registry, PresenceStore presence, Broadcaster broadcaster,
                       OpLog opLog, SnapshotRepository snapshots, AclClient acl) {
    this.registry = registry;
    this.presence = presence;
    this.broadcaster = broadcaster;
    this.opLog = opLog;
    this.snapshots = snapshots;
    this.acl = acl;
  }

  public Mono<Role> authorize(String userId, String displayName, String docId) {
    String key = userId + ":" + docId;
    Role cached = roleCache.getIfPresent(key);
    if (cached != null) return Mono.just(cached);
    return acl.roleOf(userId, displayName, docId)
        .switchIfEmpty(Mono.error(new ForbiddenException("no access to " + docId)))
        .doOnNext(r -> roleCache.put(key, r));
  }

  public Mono<Void> submit(ClientSession s, Operation clientOp) {
    Operation op = clientOp.withIdentity(s.docId(), s.userId());   // identity comes from the session, never the client
    return registry.get(s.docId()).flatMap(actor -> actor.submit(op));
  }

  public Mono<Void> updatePresence(ClientSession s, PresenceEvent ev) {
    PresenceEvent safe = new PresenceEvent(s.docId(), s.userId(), s.displayName(), ev.cursorOffset(),
        ev.selectionStart(), ev.selectionEnd(), ev.color(), PresenceEvent.PresenceType.UPDATE, System.currentTimeMillis());
    return presence.upsert(safe).then(broadcaster.publishPresence(s.docId(), safe));
  }

  public Mono<Void> onJoin(ClientSession s) {
    var join = new PresenceEvent(s.docId(), s.userId(), s.displayName(), 0, 0, 0, null,
        PresenceEvent.PresenceType.JOIN, System.currentTimeMillis());
    return presence.upsert(join).then(broadcaster.publishPresence(s.docId(), join));
  }

  public void onLeave(ClientSession s) {
    var leave = new PresenceEvent(s.docId(), s.userId(), s.displayName(), 0, 0, 0, null,
        PresenceEvent.PresenceType.LEAVE, System.currentTimeMillis());
    presence.remove(s.docId(), s.userId())
        .then(broadcaster.publishPresence(s.docId(), leave))
        .subscribe(v -> { }, e -> { });
  }

  /**
   * Replays everything the client missed. If the log no longer holds the needed range,
   * falls back to the latest snapshot plus the ops after it.
   */
  public Mono<Void> resume(ClientSession s, long lastSeq) {
    return registry.get(s.docId()).<Void>flatMap(actor -> {
      long head = actor.head();
      if (lastSeq > head) {
        return Mono.<Void>error(new Exceptions.InvalidOperationException("lastSeq " + lastSeq + " is ahead of head " + head));
      }
      return opLog.readFrom(s.docId(), lastSeq).collectList().<Void>flatMap(missed -> {
        boolean complete = lastSeq == head
            || (!missed.isEmpty() && missed.get(0).serverSeq() == lastSeq + 1);
        if (complete) {
          missed.forEach(op -> s.send(new Frame("OP", op)));
          s.send(new Frame("READY", Map.of("head", head)));
          return Mono.<Void>empty();
        }
        return snapshots.latest(s.docId())
            .defaultIfEmpty(DocumentSnapshot.empty(s.docId()))
            .<Void>flatMap(snap -> {
              s.send(new Frame("SNAPSHOT", snap));
              return opLog.readFrom(s.docId(), snap.revision())
                  .doOnNext(op -> s.send(new Frame("OP", op)))
                  .then(Mono.fromRunnable(() -> s.send(new Frame("READY", Map.of("head", head)))));
            });
      });
    });
  }

  public Mono<DocumentActor> actor(String docId) { return registry.get(docId); }
}
