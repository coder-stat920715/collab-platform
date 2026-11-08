package com.acme.collab.snapshot;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Operation;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Long-term storage port. This implementation writes JSON files; swap it for S3 (snapshots)
 * and Cassandra (op archive) in production. Writes are idempotent (file name = revision range).
 */
public interface ArchiveStore {
  Mono<Void> saveOps(String docId, List<Operation> ops);
  Mono<Void> saveSnapshot(DocumentSnapshot snapshot);

  @Component
  class FileArchiveStore implements ArchiveStore {
    private final Path root;
    private final ObjectMapper mapper;

    public FileArchiveStore(@Value("${snapshot.archive-dir}") String dir, ObjectMapper mapper) {
      this.root = Path.of(dir);
      this.mapper = mapper;
    }

    @Override
    public Mono<Void> saveOps(String docId, List<Operation> ops) {
      if (ops.isEmpty()) return Mono.empty();
      return Mono.fromRunnable(() -> {
        try {
          Path dir = Files.createDirectories(root.resolve(safe(docId)));
          String name = "ops-" + ops.get(0).serverSeq() + "-" + ops.get(ops.size() - 1).serverSeq() + ".jsonl";
          String body = ops.stream().map(o -> {
            try { return mapper.writeValueAsString(o); } catch (IOException e) { throw new IllegalStateException(e); }
          }).collect(Collectors.joining("\n"));
          Files.writeString(dir.resolve(name), body);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    @Override
    public Mono<Void> saveSnapshot(DocumentSnapshot s) {
      return Mono.fromRunnable(() -> {
        try {
          Path dir = Files.createDirectories(root.resolve(safe(s.documentId())));
          Files.writeString(dir.resolve("snapshot-" + s.revision() + ".json"), mapper.writeValueAsString(s));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private static String safe(String id) { return id.replaceAll("[^A-Za-z0-9._-]", "_"); }
  }
}
