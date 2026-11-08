package com.acme.collab.realtime.adapters.in.ws;

import com.acme.collab.common.Frame;
import com.acme.collab.realtime.adapters.out.redis.RedisAdapters.RedisBroadcaster;
import com.acme.collab.realtime.application.ClientSession;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Tracks local sessions per document and multiplexes ONE Redis subscription per document per node:
 * subscribe on first local session, dispose when the last one leaves.
 */
@Component
public class SessionManager {
  private final ConcurrentMap<String, ConcurrentMap<String, ClientSession>> byDoc = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Disposable> subscriptions = new ConcurrentHashMap<>();
  private final RedisBroadcaster broadcaster;
  private final int buffer;

  public SessionManager(RedisBroadcaster broadcaster, @Value("${collab.ws.outbound-buffer}") int buffer) {
    this.broadcaster = broadcaster;
    this.buffer = buffer;
  }

  public ClientSession register(String sessionId, String userId, String displayName, String docId, boolean canEdit) {
    var session = new ClientSession(sessionId, userId, displayName, docId, canEdit, buffer);
    byDoc.computeIfAbsent(docId, d -> new ConcurrentHashMap<>()).put(sessionId, session);
    subscriptions.computeIfAbsent(docId, d ->
        broadcaster.subscribe(d).subscribe(frame -> fanOut(d, frame), e -> subscriptions.remove(d)));
    return session;
  }

  public void unregister(ClientSession s) {
    var sessions = byDoc.get(s.docId());
    if (sessions != null) {
      sessions.remove(s.sessionId());
      if (sessions.isEmpty() && byDoc.remove(s.docId(), sessions)) {
        Optional.ofNullable(subscriptions.remove(s.docId())).ifPresent(Disposable::dispose);
      }
    }
    s.outbound().tryEmitComplete();
  }

  private void fanOut(String docId, Frame frame) {
    Map<String, ClientSession> sessions = byDoc.get(docId);
    if (sessions != null) sessions.values().forEach(s -> s.send(frame));
  }

  public int localSessions() {
    return byDoc.values().stream().mapToInt(Map::size).sum();
  }
}
