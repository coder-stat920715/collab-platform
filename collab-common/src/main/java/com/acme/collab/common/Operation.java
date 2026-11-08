package com.acme.collab.common;

import java.util.List;

/** An edit. Clients send baseRevision + clientSeq; the server stamps serverSeq. */
public record Operation(
    String documentId,
    String clientId,
    long clientSeq,
    String userId,
    long baseRevision,
    long serverSeq,
    List<Component> components) {

  public Operation sequenced(long seq, List<Component> transformed) {
    return new Operation(documentId, clientId, clientSeq, userId, seq - 1, seq, transformed);
  }

  /** Never trust identity fields coming from the wire. */
  public Operation withIdentity(String docId, String user) {
    return new Operation(docId, clientId, clientSeq, user, baseRevision, 0, components);
  }
}
