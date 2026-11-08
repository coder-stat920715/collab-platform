package com.acme.collab.realtime.adapters.in.ws;

import com.acme.collab.common.Frame;
import com.acme.collab.common.Operation;
import com.acme.collab.common.PresenceEvent;
import com.acme.collab.realtime.application.ClientSession;
import com.acme.collab.realtime.application.CollabService;
import com.acme.collab.realtime.application.Exceptions.BadFrameException;
import com.acme.collab.realtime.application.Exceptions.ForbiddenException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive WebSocket endpoint: /ws/docs/{docId}?userId=..&name=..
 * DEV AUTH: identity comes from query params. Replace with JWT/ticket validation in a WebFilter
 * (see README) before exposing publicly.
 */
@Component
public class CollabWebSocketHandler implements WebSocketHandler {
  private final SessionManager sessions;
  private final CollabService collab;
  private final ObjectMapper mapper;
  private final Duration pingEvery;
  private final Duration deadAfter;

  public CollabWebSocketHandler(SessionManager sessions, CollabService collab, ObjectMapper mapper,
                                @Value("${collab.ws.ping-interval}") Duration pingEvery,
                                @Value("${collab.ws.dead-after}") Duration deadAfter) {
    this.sessions = sessions;
    this.collab = collab;
    this.mapper = mapper;
    this.pingEvery = pingEvery;
    this.deadAfter = deadAfter;
  }

  @Override
  public Mono<Void> handle(WebSocketSession ws) {
    URI uri = ws.getHandshakeInfo().getUri();
    String path = uri.getPath();
    String docId = path.substring(path.lastIndexOf('/') + 1);
    var params = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
    String userId = params.getFirst("userId");
    String name = params.getFirst("name") != null ? params.getFirst("name") : userId;
    if (userId == null || userId.isBlank()) {
      return ws.close(CloseStatus.POLICY_VIOLATION.withReason("userId required"));
    }

    return collab.authorize(userId, name, docId)
        .flatMap(role -> {
          ClientSession s = sessions.register(UUID.randomUUID().toString(), userId, name, docId, role.canEdit());

          Flux<WebSocketMessage> out = s.outbound().asFlux().map(f -> ws.textMessage(toJson(f)));
          Flux<WebSocketMessage> pings = Flux.interval(pingEvery)
              .map(t -> ws.pingMessage(df -> df.wrap(new byte[] {1})));
          Mono<Void> send = ws.send(Flux.merge(out, pings));

          Mono<Void> receive = ws.receive()
              .doOnNext(m -> s.touch())                                   // any frame, incl. pong, proves liveness
              .filter(m -> m.getType() == WebSocketMessage.Type.TEXT)
              .concatMap(m -> route(s, m.getPayloadAsText())
                  .onErrorResume(e -> {
                    s.send(ErrorMapper.toFrame(e));
                    return Mono.empty();
                  }))
              .then();

          Mono<Void> watchdog = Flux.interval(pingEvery)
              .filter(t -> s.nanosSinceSeen() > deadAfter.toNanos())
              .next()
              .then(ws.close(CloseStatus.GOING_AWAY));

          return collab.onJoin(s)
              .onErrorResume(e -> Mono.empty())
              .then(Mono.firstWithSignal(Mono.zip(send, receive).then(), watchdog))
              .doFinally(sig -> {
                sessions.unregister(s);
                collab.onLeave(s);
              });
        })
        .onErrorResume(ForbiddenException.class,
            e -> ws.close(CloseStatus.POLICY_VIOLATION.withReason("forbidden")));
  }

  private Mono<Void> route(ClientSession s, String json) {
    Frame f;
    try {
      f = mapper.readValue(json, Frame.class);
    } catch (Exception e) {
      return Mono.error(new BadFrameException("unparseable frame"));
    }
    if (f.type() == null) return Mono.error(new BadFrameException("missing type"));
    return switch (f.type()) {
      case "OP" -> s.canEdit()
          ? collab.submit(s, mapper.convertValue(f.payload(), Operation.class))
          : Mono.error(new ForbiddenException("read-only access"));
      case "PRESENCE" -> collab.updatePresence(s, mapper.convertValue(f.payload(), PresenceEvent.class));
      case "RESUME" -> {
        Object last = ((Map<?, ?>) f.payload()).get("lastSeq");
        yield collab.resume(s, ((Number) last).longValue());
      }
      default -> Mono.error(new BadFrameException("unknown type " + f.type()));
    };
  }

  private String toJson(Frame f) {
    try {
      return mapper.writeValueAsString(f);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
