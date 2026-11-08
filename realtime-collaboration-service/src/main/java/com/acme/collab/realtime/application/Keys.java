package com.acme.collab.realtime.application;

/** Redis key helpers. Braces are hash tags so all keys of one document share a cluster slot. */
public final class Keys {
  private Keys() {}
  public static String ops(String docId) { return "doc:{" + docId + "}:ops"; }
  public static String epoch(String docId) { return "doc:{" + docId + "}:epoch"; }
  public static String owner(String docId) { return "doc:{" + docId + "}:owner"; }
  public static String channel(String docId) { return "doc:{" + docId + "}:events"; }
  public static String presence(String docId) { return "presence:{" + docId + "}"; }
  public static String snapshot(String docId) { return "snap:{" + docId + "}"; }
  public static final String DIRTY = "dirty:docs";
}
