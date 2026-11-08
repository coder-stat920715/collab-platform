package com.acme.collab.realtime.application;

import com.acme.collab.common.Frame;
import com.acme.collab.realtime.application.Exceptions.SlowConsumerException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Sinks;

/** One connected socket. The bounded outbound sink protects the node from slow consumers. */
public final class ClientSession {
  private final String sessionId;
  private final String userId;
  private final String displayName;
  private final String docId;
  private final boolean canEdit;
  private final Sinks.Many<Frame> outbound;
  private final AtomicLong lastSeen = new AtomicLong(System.nanoTime());

  public ClientSession(String sessionId, String userId, String displayName, String docId,
                       boolean canEdit, int bufferSize) {
    this.sessionId = sessionId;
    this.userId = userId;
    this.displayName = displayName;
    this.docId = docId;
    this.canEdit = canEdit;
    this.outbound = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(bufferSize));
  }

  public String sessionId() { return sessionId; }
  public String userId() { return userId; }
  public String displayName() { return displayName; }
  public String docId() { return docId; }
  public boolean canEdit() { return canEdit; }
  public Sinks.Many<Frame> outbound() { return outbound; }
  public void touch() { lastSeen.set(System.nanoTime()); }
  public long nanosSinceSeen() { return System.nanoTime() - lastSeen.get(); }

  /** Thread-safe enqueue; a full buffer means the client cannot keep up and is dropped (it will resync). */
  public void send(Frame frame) {
    Sinks.EmitResult r = outbound.tryEmitNext(frame);
    for (int i = 0; i < 100 && r == Sinks.EmitResult.FAIL_NON_SERIALIZED; i++) {
      Thread.onSpinWait();
      r = outbound.tryEmitNext(frame);
    }
    if (r == Sinks.EmitResult.FAIL_OVERFLOW) {
      outbound.tryEmitError(new SlowConsumerException(sessionId));
    }
  }
}
