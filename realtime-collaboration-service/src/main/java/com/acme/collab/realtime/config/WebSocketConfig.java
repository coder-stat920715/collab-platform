package com.acme.collab.realtime.config;

import com.acme.collab.realtime.adapters.in.ws.CollabWebSocketHandler;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

@Configuration
public class WebSocketConfig {

  @Bean
  HandlerMapping webSocketMapping(CollabWebSocketHandler handler) {
    return new SimpleUrlHandlerMapping(Map.of("/ws/docs/*", handler), -1);
  }

  @Bean
  WebSocketHandlerAdapter webSocketHandlerAdapter() {
    return new WebSocketHandlerAdapter();
  }

  @Bean
  ReactiveRedisMessageListenerContainer redisListenerContainer(ReactiveRedisConnectionFactory cf) {
    return new ReactiveRedisMessageListenerContainer(cf);
  }
}
