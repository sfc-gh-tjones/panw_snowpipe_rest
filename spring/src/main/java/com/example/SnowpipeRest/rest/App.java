package com.example.SnowpipeRest.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;

/** Entrypoint for the Snowpipe Rest Application */
@SpringBootApplication
public class App {
  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(App.class);
    app.setDefaultProperties(Collections.singletonMap("server.port", 8080));
    app.run(args);
  }
}
