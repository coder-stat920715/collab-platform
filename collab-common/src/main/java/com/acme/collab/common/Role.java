package com.acme.collab.common;

public record Role(String userId, String displayName, boolean canEdit, boolean canAdmin) {}
