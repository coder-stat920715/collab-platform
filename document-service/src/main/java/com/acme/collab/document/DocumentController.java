package com.acme.collab.document;

import com.acme.collab.common.Role;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Documents and access control. In-memory for the demo; see db/schema.sql for the PostgreSQL model. */
@RestController
public class DocumentController {
  public enum Level { VIEWER, EDITOR, ADMIN }
  public record CreateRequest(String title, String ownerId) {}
  public record AclRequest(String userId, Level role) {}
  public record DocumentDto(String id, String title, String ownerId, Instant createdAt) {}

  private final Map<String, DocumentDto> docs = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Level>> acl = new ConcurrentHashMap<>();

  @PostMapping("/api/v1/documents")
  public DocumentDto create(@RequestBody CreateRequest r) {
    String id = UUID.randomUUID().toString();
    var dto = new DocumentDto(id, r.title(), r.ownerId(), Instant.now());
    docs.put(id, dto);
    acl.computeIfAbsent(id, k -> new ConcurrentHashMap<>()).put(r.ownerId(), Level.ADMIN);
    return dto;
  }

  @GetMapping("/api/v1/documents/{id}")
  public DocumentDto get(@PathVariable String id) {
    var d = docs.get(id);
    if (d == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    return d;
  }

  @PutMapping("/api/v1/documents/{id}/acl")
  public Map<String, Level> grant(@PathVariable String id, @RequestBody AclRequest r) {
    if (!docs.containsKey(id)) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    var m = acl.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
    m.put(r.userId(), r.role());
    return m;
  }

  /** Called by realtime-collaboration-service on WebSocket handshake. 404 means no access. */
  @GetMapping("/internal/acl/{docId}/{userId}")
  public Role role(@PathVariable String docId, @PathVariable String userId) {
    Level level = acl.getOrDefault(docId, Map.of()).get(userId);
    if (level == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    return new Role(userId, userId, level != Level.VIEWER, level == Level.ADMIN);
  }

  public boolean exists(String docId) { return docs.containsKey(docId); }
}
