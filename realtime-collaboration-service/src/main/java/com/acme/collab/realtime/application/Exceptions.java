package com.acme.collab.realtime.application;

public final class Exceptions {
  private Exceptions() {}

  public static class NotOwnerException extends RuntimeException {
    public final String ownerNode;
    public NotOwnerException(String ownerNode) { super("document owned by " + ownerNode); this.ownerNode = ownerNode; }
  }
  public static class StaleRevisionException extends RuntimeException {
    public StaleRevisionException(String docId, long base, long head) {
      super("doc=" + docId + " baseRevision=" + base + " head=" + head);
    }
  }
  public static class InvalidOperationException extends RuntimeException {
    public InvalidOperationException(String m) { super(m); }
  }
  public static class SlowConsumerException extends RuntimeException {
    public SlowConsumerException(String sessionId) { super("slow consumer " + sessionId); }
  }
  public static class ForbiddenException extends RuntimeException {
    public ForbiddenException(String m) { super(m); }
  }
  public static class BadFrameException extends RuntimeException {
    public BadFrameException(String m) { super(m); }
  }
}
