package com.acme.collab.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** One step of a text operation. An operation must cover the whole base document (retain + delete). */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "t")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Component.Retain.class, name = "r"),
    @JsonSubTypes.Type(value = Component.Insert.class, name = "i"),
    @JsonSubTypes.Type(value = Component.Delete.class, name = "d")})
public sealed interface Component {
  record Retain(int count) implements Component {}
  record Insert(String text) implements Component {}
  record Delete(int count) implements Component {}
}
