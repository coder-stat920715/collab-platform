package com.acme.collab.notification;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class NotificationApplication {
  public static void main(String[] args) {
    SpringApplication.run(NotificationApplication.class, args);
  }

  public record Notification(String userId, String type, String message, String documentId, Instant at) {}
  public record NotifyRequest(String userId, String type, String message, String documentId) {}

  /** In-memory inbox. Replace with a Kafka consumer + persistent store and push/email delivery. */
  @RestController
  static class NotificationController {
    private static final Logger log = LoggerFactory.getLogger(NotificationController.class);
    private final Map<String, List<Notification>> inbox = new ConcurrentHashMap<>();

    @PostMapping("/internal/notify")
    public Notification notify(@RequestBody NotifyRequest r) {
      var n = new Notification(r.userId(), r.type(), r.message(), r.documentId(), Instant.now());
      inbox.computeIfAbsent(r.userId(), k -> new CopyOnWriteArrayList<>()).add(n);
      log.info("notify user={} type={} doc={}", r.userId(), r.type(), r.documentId());
      return n;
    }

    @GetMapping("/api/notifications/{userId}")
    public List<Notification> list(@PathVariable String userId) {
      return inbox.getOrDefault(userId, List.of());
    }
  }
}
