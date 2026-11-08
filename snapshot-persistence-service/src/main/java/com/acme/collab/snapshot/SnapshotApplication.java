package com.acme.collab.snapshot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SnapshotApplication {
  public static void main(String[] args) {
    SpringApplication.run(SnapshotApplication.class, args);
  }
}
