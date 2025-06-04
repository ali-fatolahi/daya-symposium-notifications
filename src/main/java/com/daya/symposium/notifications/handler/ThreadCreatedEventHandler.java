package com.daya.symposium.notifications.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.daya.symposium.core.ThreadCreatedEvent;

@Component
  @KafkaListener(topics="daya-symposium-threads")
public class ThreadCreatedEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
  
  @KafkaHandler
  public void handle(final ThreadCreatedEvent threadCreatedEvent) {
    LOGGER.info("New event received: {}", threadCreatedEvent);
  }
}
