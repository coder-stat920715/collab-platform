package com.acme.collab.realtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.acme.collab.common.Component.Insert;
import com.acme.collab.common.Component.Retain;
import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Frame;
import com.acme.collab.common.Operation;
import com.acme.collab.common.PresenceEvent;
import com.acme.collab.realtime.application.DocumentActor;
import com.acme.collab.realtime.application.Exceptions.InvalidOperationException;
import com.acme.collab.realtime.application.Ports.Broadcaster;
import com.acme.collab.realtime.application.Ports.DirtyTracker;
import com.acme.collab.realtime.application.Ports.OpLog;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DocumentActorTest {

  static class MemLog implements OpLog, Broadcaster, DirtyTracker {
    final List<Operation> ops = new ArrayList<>();
    final List<Operation> published = new ArrayList<>();
    public Mono<Void> append(String d, long e, Operation op) { return Mono.fromRunnable(() -> ops.add(op)); }
    public Flux<Operation> readFrom(String d, long after) { return Flux.fromIterable(ops).filter(o -> o.serverSeq() > after); }
    public Mono<Void> publish(String d, Operation op) { return Mono.fromRunnable(() -> published.add(op)); }
    public Mono<Void> publishPresence(String d, PresenceEvent ev) { return Mono.empty(); }
    public Flux<Frame> subscribe(String d) { return Flux.never(); }
    public Mono<Void> markDirty(String d) { return Mono.empty(); }
  }

  private DocumentActor actor(MemLog log) {
    return new DocumentActor("d1", 1, DocumentSnapshot.empty("d1"), List.of(), 100, log, log, log);
  }

  private Operation op(String client, long seq, long base, java.util.List<com.acme.collab.common.Component> c) {
    return new Operation("d1", client, seq, client, base, 0, c);
  }

  @Test
  void concurrentOpsAreSequencedAndConverge() {
    var log = new MemLog();
    var actor = actor(log);
    actor.submit(op("A", 1, 0, List.of(new Insert("hello")))).block();       // rev 1: "hello"
    // B and A both edit based on revision 1
    actor.submit(op("A", 2, 1, List.of(new Retain(5), new Insert("!")))).block();
    actor.submit(op("B", 1, 1, List.of(new Insert(">> "), new Retain(5)))).block();
    assertEquals(">> hello!", actor.content());
    assertEquals(3, actor.head());
    assertEquals(3, log.published.size());
  }

  @Test
  void duplicateClientSeqIsIgnored() {
    var log = new MemLog();
    var actor = actor(log);
    var first = op("A", 1, 0, List.of(new Insert("x")));
    actor.submit(first).block();
    actor.submit(first).block();
    assertEquals("x", actor.content());
    assertEquals(1, actor.head());
  }

  @Test
  void opNotCoveringWholeDocumentIsRejected() {
    var log = new MemLog();
    var actor = actor(log);
    actor.submit(op("A", 1, 0, List.of(new Insert("abc")))).block();
    assertThrows(InvalidOperationException.class,
        () -> actor.submit(op("A", 2, 1, List.of(new Retain(1), new Insert("z")))).block());   // retains 1 of 3
  }
}
