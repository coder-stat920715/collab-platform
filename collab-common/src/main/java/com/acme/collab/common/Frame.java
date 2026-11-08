package com.acme.collab.common;

/** Wire envelope. types: OP, PRESENCE, RESUME, SNAPSHOT, READY, ERROR, COMMENT. */
public record Frame(String type, Object payload) {}
