package com.acme.collab.realtime.application;

import com.acme.collab.common.Component;
import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Operation;
import com.acme.collab.common.OtEngine;
import com.acme.collab.realtime.application.Exceptions.InvalidOperationException;
import com.acme.collab.realtime.application.Exceptions.StaleRevisionException;
import com.acme.collab.realtime.application.Ports.Broadcaster;
import com.acme.collab.realtime.application.Ports.DirtyTracker;
import com.acme.collab.realtime.application.Ports.OpLog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

/**
 * Single-writer sequencer for one document. All commands go through a mailbox processed
 * serially (concatMap), so state needs no locks. Durable append happens before fan-out.
 */
public final class DocumentActor {
  private final String docId;
  private final long epoch;
  private final int window;
  private final OpLog opLog;
  private final Broadcaster broadcaster;
  private final DirtyTracker dirty;
  private final Sinks.Many<Command> mailbox = Sinks.many().unicast().onBackpressureBuffer();
  private final List<Operation> history = new ArrayList<>();
  private final Map<String, Long> lastClientSeq = new HashMap<>();
  private final Disposable loop;
  private volatile String content;
  private volatile long head;

  public DocumentActor(String docId, long epoch, DocumentSnapshot base, List<Operation> tail,
                       int window, OpLog opLog, Broadcaster broadcaster, DirtyTracker dirty) {
    this.docId = docId;
    this.epoch = epoch;
    this.window = window;
    this.opLog = opLog;
    this.broadcaster = broadcaster;
    this.dirty = dirty;
    this.content = base.content();
    this.head = base.revision();
    for (Operation op : tail) {
      if (op.serverSeq() != head + 1) {
        throw new IllegalStateException("gap in op log for " + docId + ": expected " + (head + 1) + " got " + op.serverSeq());
      }
      content = OtEngine.apply(content, op.components());
      head = op.serverSeq();
      remember(op);
    }
    this.loop = mailbox.asFlux().concatMap(this::handle).subscribe();
  }

  public long head() { return head; }
  public String content() { return content; }

  public Mono<Void> submit(Operation op) {
    return Mono.create(sink ->
        mailbox.emitNext(new Command(op, sink), Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1))));
  }

  public void shutdown() {
    mailbox.tryEmitComplete();
    loop.dispose();
  }

  private Mono<Void> handle(Command cmd) {
    Operation in = cmd.op();

    Long seen = lastClientSeq.get(in.clientId());
    if (seen != null && in.clientSeq() <= seen) {            // duplicate resend after reconnect: idempotent
      return Mono.fromRunnable(() -> cmd.sink().success());
    }

    long oldest = head - history.size();
    if (in.baseRevision() > head || in.baseRevision() < oldest) {
      return Mono.fromRunnable(() -> cmd.sink().error(new StaleRevisionException(docId, in.baseRevision(), head)));
    }

    List<Component> comps = in.components();
    try {
      int from = (int) (in.baseRevision() - oldest);
      for (Operation concurrent : history.subList(from, history.size())) {
        comps = OtEngine.transform(concurrent.components(), comps).b();
      }
      if (OtEngine.baseLength(comps) != content.length()) {
        throw new InvalidOperationException("operation must cover the whole document: base length "
            + OtEngine.baseLength(comps) + " != " + content.length());
      }
    } catch (IllegalArgumentException | InvalidOperationException e) {
      return Mono.fromRunnable(() -> cmd.sink().error(
          e instanceof InvalidOperationException ? e : new InvalidOperationException(e.getMessage())));
    }

    Operation sequenced = in.sequenced(head + 1, comps);
    String next = OtEngine.apply(content, comps);

    return opLog.append(docId, epoch, sequenced)                       // durable first
        .then(Mono.fromRunnable(() -> {
          content = next;
          head = sequenced.serverSeq();
          remember(sequenced);
          lastClientSeq.put(in.clientId(), in.clientSeq());
        }))
        .then(dirty.markDirty(docId).onErrorResume(e -> Mono.empty()))
        .then(broadcaster.publish(docId, sequenced))                   // then fan out
        .doOnSuccess(v -> cmd.sink().success())
        .onErrorResume(e -> {
          cmd.sink().error(e);
          return Mono.empty();
        });
  }

  private void remember(Operation op) {
    history.add(op);
    if (history.size() > window) history.remove(0);
  }

  private record Command(Operation op, MonoSink<Void> sink) {}
}
