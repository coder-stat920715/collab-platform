package com.acme.collab.realtime.adapters.out.redis;

import com.acme.collab.common.DocumentSnapshot;
import com.acme.collab.common.Frame;
import com.acme.collab.common.Operation;
import com.acme.collab.common.PresenceEvent;
import com.acme.collab.realtime.application.Keys;
import com.acme.collab.realtime.application.Ports.Broadcaster;
import com.acme.collab.realtime.application.Ports.DirtyTracker;
import com.acme.collab.realtime.application.Ports.LeaseStore;
import com.acme.collab.realtime.application.Ports.OpLog;
import com.acme.collab.realtime.application.Ports.PresenceStore;
import com.acme.collab.realtime.application.Ports.SnapshotRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public final class RedisAdapters {
  private RedisAdapters() {}

  static String json(ObjectMapper m, Object o) {
    try {
      return m.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  static <T> T read(ObjectMapper m, String s, Class<T> type) {
    try {
      return m.readValue(s, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  static RedisScript<Long> script(String name) {
    var s = new DefaultRedisScript<Long>();
    s.setLocation(new ClassPathResource("lua/" + name));
    s.setResultType(Long.class);
    return s;
  }

  // ---------------------------------------------------------------- op log (Redis Streams)
  @Component
  public static class RedisOpLog implements OpLog {
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final RedisScript<Long> appendScript = script("append_fenced.lua");

    public RedisOpLog(ReactiveStringRedisTemplate redis, ObjectMapper mapper) {
      this.redis = redis;
      this.mapper = mapper;
    }

    @Override
    public Mono<Void> append(String docId, long epoch, Operation op) {
      return redis.execute(appendScript,
              List.of(Keys.ops(docId), Keys.epoch(docId)),
              List.of(String.valueOf(epoch), String.valueOf(op.serverSeq()), json(mapper, op)))
          .then();
    }

    @Override
    public Flux<Operation> readFrom(String docId, long afterSeq) {
      return redis.opsForStream()
          .range(Keys.ops(docId), Range.rightUnbounded(Range.Bound.exclusive(afterSeq + "-0")))
          .map(rec -> read(mapper, String.valueOf(rec.getValue().get("op")), Operation.class));
    }
  }

  // ---------------------------------------------------------------- pub/sub fan-out
  @Component
  public static class RedisBroadcaster implements Broadcaster {
    private final ReactiveStringRedisTemplate redis;
    private final ReactiveRedisMessageListenerContainer container;
    private final ObjectMapper mapper;

    public RedisBroadcaster(ReactiveStringRedisTemplate redis, ReactiveRedisMessageListenerContainer container,
                            ObjectMapper mapper) {
      this.redis = redis;
      this.container = container;
      this.mapper = mapper;
    }

    @Override
    public Mono<Void> publish(String docId, Operation op) {
      return redis.convertAndSend(Keys.channel(docId), json(mapper, new Frame("OP", op))).then();
    }

    @Override
    public Mono<Void> publishPresence(String docId, PresenceEvent ev) {
      return redis.convertAndSend(Keys.channel(docId), json(mapper, new Frame("PRESENCE", ev))).then();
    }

    @Override
    public Flux<Frame> subscribe(String docId) {
      return container.receive(ChannelTopic.of(Keys.channel(docId)))
          .map(ReactiveSubscription.Message::getMessage)
          .map(msg -> read(mapper, msg, Frame.class))
          .onBackpressureBuffer(10_000)
          .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(5)));
    }
  }

  // ---------------------------------------------------------------- ownership lease with fencing epoch
  @Component
  public static class RedisLeaseStore implements LeaseStore {
    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<Long> renewScript = script("renew.lua");
    private final RedisScript<Long> releaseScript = script("release.lua");

    public RedisLeaseStore(ReactiveStringRedisTemplate redis) { this.redis = redis; }

    @Override
    public Mono<Long> tryAcquire(String docId, String nodeId, Duration ttl) {
      return redis.opsForValue().setIfAbsent(Keys.owner(docId), nodeId, ttl)
          .filter(Boolean::booleanValue)
          .flatMap(ok -> redis.opsForValue().increment(Keys.epoch(docId)));
    }

    @Override
    public Mono<Boolean> renew(String docId, String nodeId, Duration ttl) {
      return redis.execute(renewScript, List.of(Keys.owner(docId)),
              List.of(nodeId, String.valueOf(ttl.toMillis())))
          .next().map(r -> r != null && r > 0).defaultIfEmpty(false);
    }

    @Override
    public Mono<Void> release(String docId, String nodeId) {
      return redis.execute(releaseScript, List.of(Keys.owner(docId)), List.of(nodeId)).then();
    }

    @Override
    public Mono<String> ownerOf(String docId) {
      return redis.opsForValue().get(Keys.owner(docId));
    }
  }

  // ---------------------------------------------------------------- snapshots (read side; written by snapshot service)
  @Component
  public static class RedisSnapshotRepository implements SnapshotRepository {
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;

    public RedisSnapshotRepository(ReactiveStringRedisTemplate redis, ObjectMapper mapper) {
      this.redis = redis;
      this.mapper = mapper;
    }

    @Override
    public Mono<DocumentSnapshot> latest(String docId) {
      return redis.opsForValue().get(Keys.snapshot(docId)).map(s -> read(mapper, s, DocumentSnapshot.class));
    }

    @Override
    public Mono<Void> save(DocumentSnapshot snapshot) {
      return redis.opsForValue().set(Keys.snapshot(snapshot.documentId()), json(mapper, snapshot)).then();
    }
  }

  // ---------------------------------------------------------------- presence (ephemeral, TTL)
  @Component
  public static class RedisPresenceStore implements PresenceStore {
    private static final Duration TTL = Duration.ofSeconds(30);
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;

    public RedisPresenceStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper) {
      this.redis = redis;
      this.mapper = mapper;
    }

    @Override
    public Mono<Void> upsert(PresenceEvent ev) {
      String key = Keys.presence(ev.documentId());
      return redis.opsForHash().put(key, ev.userId(), json(mapper, ev))
          .then(redis.expire(key, TTL)).then();
    }

    @Override
    public Mono<Void> remove(String docId, String userId) {
      return redis.opsForHash().remove(Keys.presence(docId), userId).then();
    }

    @Override
    public Flux<PresenceEvent> list(String docId) {
      return redis.opsForHash().values(Keys.presence(docId)).map(v -> read(mapper, String.valueOf(v), PresenceEvent.class));
    }
  }

  // ---------------------------------------------------------------- dirty set consumed by snapshot service
  @Component
  public static class RedisDirtyTracker implements DirtyTracker {
    private final ReactiveStringRedisTemplate redis;

    public RedisDirtyTracker(ReactiveStringRedisTemplate redis) { this.redis = redis; }

    @Override
    public Mono<Void> markDirty(String docId) {
      return redis.opsForSet().add(Keys.DIRTY, docId).then();
    }
  }
}
