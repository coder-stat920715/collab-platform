package com.acme.collab.common;

public record DocumentSnapshot(String documentId, long revision, String content, long createdAtMillis) {
  public static DocumentSnapshot empty(String documentId) {
    return new DocumentSnapshot(documentId, 0, "", System.currentTimeMillis());
  }
}
