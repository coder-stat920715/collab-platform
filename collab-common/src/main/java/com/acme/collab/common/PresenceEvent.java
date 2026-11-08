package com.acme.collab.common;

public record PresenceEvent(
    String documentId, String userId, String displayName,
    int cursorOffset, int selectionStart, int selectionEnd,
    String color, PresenceType type, long ts) {
  public enum PresenceType { JOIN, UPDATE, LEAVE }
}
