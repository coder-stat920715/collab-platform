# Real-Time Collaboration Platform

Spring Boot 3.3 / Java 21 / WebFlux / Redis implementation of the design in the accompanying
HLD/LLD document: server-sequenced Operational Transformation, one authoritative owner per
document (Redis lease + fencing epoch), Redis Streams op log, Redis Pub/Sub fan-out, background
snapshots, presence, comments with @mentions.

## Modules

| Module | Port | What it does |
|---|---|---|
| `collab-common` | - | OT engine, operation/presence/snapshot models, wire frames |
| `realtime-collaboration-service` | 8081 | Reactive WebSockets, document actors (sequencer), Redis log and fan-out, presence, resync, browser demo |
| `snapshot-persistence-service` | 8084 | Drains op log, builds snapshots, archives ops, trims the stream |
| `document-service` | 8082 | Documents, ACL (Viewer/Editor/Admin), comment threads, @mention detection |
| `notification-service` | 8083 | Receives mention events, in-memory inbox |
| `api-gateway` | 8080 | Spring Cloud Gateway routes |

## Run

```bash
docker compose up -d redis
mvn -DskipTests install
cd realtime-collaboration-service && mvn spring-boot:run     # add the others as needed
```

Open two tabs: `http://localhost:8081/index.html?doc=demo&user=alice` and `...&user=bob`. Type in both.
Only Redis plus the realtime service are required for the editing demo. `scripts/run-local.sh` starts everything.

Tests: `mvn test` (OT convergence, actor sequencing) and `node realtime-collaboration-service/src/test/js/ot.test.js`.

## Wire protocol (JSON text frames)

| Type | Direction | Payload |
|---|---|---|
| `RESUME` | client to server | `{lastSeq}`; server replays missed ops or sends a snapshot, then `READY` |
| `OP` | both | `Operation` (client sends `clientId, clientSeq, baseRevision, components`) |
| `PRESENCE` | both | cursor/selection |
| `SNAPSHOT`, `READY`, `ERROR` | server to client | state and status |

Operations must cover the whole document (retain + delete = document length); the server rejects others.

## Known limits (read before production use)

- **Auth is dev-only.** The WebSocket takes `userId` from the query string. Add JWT/ticket validation in a
  WebFilter, and set `collab.acl.mode=REMOTE` so roles come from document-service.
- No auth-service module; no Kafka or Cassandra; the snapshot service archives to local files.
  `ArchiveStore` is the seam for S3/Cassandra.
- document-service stores data in memory (`db/schema.sql` shows the PostgreSQL model).
- Multi-node routing: a node that does not own a document returns `ERROR NOT_OWNER` with the owner id.
  Route `/ws/docs/{id}` by consistent hash at the gateway (not implemented).
- Comment anchors are stored but not yet transformed through the op log; no revision-history REST/restore.
- Not built or run under Maven in the environment this was generated in; only the OT core (Java) and
  browser OT library (JS) were compiled and fuzz-tested there. Expect small compile fixes on first build.
