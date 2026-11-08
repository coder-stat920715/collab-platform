package com.acme.collab.realtime.adapters.in.rest;

import com.acme.collab.common.PresenceEvent;
import com.acme.collab.realtime.application.CollabService;
import com.acme.collab.realtime.application.Ports.PresenceStore;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Internal read endpoints used by other services and for debugging. */
@RestController
@RequestMapping("/internal/docs/{docId}")
public class DocumentController {
  private final CollabService collab;
  private final PresenceStore presence;

  public DocumentController(CollabService collab, PresenceStore presence) {
    this.collab = collab;
    this.presence = presence;
  }

  @GetMapping("/content")
  public Mono<Map<String, Object>> content(@PathVariable String docId) {
    return collab.actor(docId).map(a -> Map.of("documentId", docId, "revision", a.head(), "content", a.content()));
  }

  @GetMapping("/presence")
  public Flux<PresenceEvent> presence(@PathVariable String docId) {
    return presence.list(docId);
  }
}
