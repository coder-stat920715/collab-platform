package com.acme.collab.document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;

/** Inline comment threads with @mentions. Mentioned users are notified via notification-service. */
@RestController
public class CommentController {
  private static final Logger log = LoggerFactory.getLogger(CommentController.class);
  private static final Pattern MENTION = Pattern.compile("@([A-Za-z0-9._-]+)");

  public record Comment(String id, String authorId, String body, Instant createdAt) {}
  public record Thread(String id, String documentId, int anchorStart, int anchorEnd, long anchorRev,
                       boolean resolved, List<Comment> comments) {}
  public record NewThread(int anchorStart, int anchorEnd, long anchorRev, String authorId, String body) {}
  public record NewComment(String authorId, String body) {}

  private final Map<String, Thread> threads = new ConcurrentHashMap<>();
  private final DocumentController documents;
  private final WebClient notifications;

  public CommentController(DocumentController documents, WebClient.Builder builder,
                           @Value("${notification.base-url}") String notificationUrl) {
    this.documents = documents;
    this.notifications = builder.baseUrl(notificationUrl).build();
  }

  @PostMapping("/api/v1/documents/{docId}/threads")
  public Thread open(@PathVariable String docId, @RequestBody NewThread r) {
    if (!documents.exists(docId)) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    var t = new Thread(UUID.randomUUID().toString(), docId, r.anchorStart(), r.anchorEnd(), r.anchorRev(),
        false, new CopyOnWriteArrayList<>());
    threads.put(t.id(), t);
    addComment(t, r.authorId(), r.body());
    return t;
  }

  @PostMapping("/api/v1/threads/{threadId}/comments")
  public Thread reply(@PathVariable String threadId, @RequestBody NewComment r) {
    var t = threads.get(threadId);
    if (t == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    addComment(t, r.authorId(), r.body());
    return t;
  }

  @GetMapping("/api/v1/documents/{docId}/threads")
  public List<Thread> list(@PathVariable String docId) {
    return threads.values().stream().filter(t -> t.documentId().equals(docId)).collect(Collectors.toList());
  }

  private void addComment(Thread t, String authorId, String body) {
    t.comments().add(new Comment(UUID.randomUUID().toString(), authorId, body, Instant.now()));
    var mentioned = new ArrayList<String>();
    var m = MENTION.matcher(body);
    while (m.find()) mentioned.add(m.group(1));
    mentioned.stream().distinct().filter(u -> !u.equals(authorId)).forEach(u ->
        notifications.post().uri("/internal/notify")
            .bodyValue(Map.of("userId", u, "type", "MENTION",
                "message", authorId + " mentioned you in a comment", "documentId", t.documentId()))
            .retrieve().toBodilessEntity()
            .subscribe(x -> { }, e -> log.warn("notification failed for {}: {}", u, e.getMessage())));
  }
}
