package com.acme.collab.realtime.application;

import com.acme.collab.common.Role;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/** Resolves a user's role on a document. ALLOW_ALL is for local development only. */
@Component
public class AclClient {
  private final String mode;
  private final WebClient client;

  public AclClient(@Value("${collab.acl.mode}") String mode,
                   @Value("${collab.acl.base-url}") String baseUrl,
                   WebClient.Builder builder) {
    this.mode = mode;
    this.client = builder.baseUrl(baseUrl).build();
  }

  public Mono<Role> roleOf(String userId, String displayName, String docId) {
    if ("ALLOW_ALL".equalsIgnoreCase(mode)) {
      return Mono.just(new Role(userId, displayName, true, true));
    }
    return client.get().uri("/internal/acl/{doc}/{user}", docId, userId)
        .retrieve()
        .bodyToMono(Role.class)
        .onErrorResume(e -> Mono.empty());
  }
}
