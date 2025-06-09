package com.daya.symposium.notifications.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.daya.symposium.core.ThreadCreatedEvent;
import com.daya.symposium.notifications.error.NonRetryableException;
import com.daya.symposium.notifications.error.RetryableException;

@Component
@KafkaListener(topics="daya-symposium-threads")
public class ThreadCreatedEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
  private RestTemplate restTemplate;

  public ThreadCreatedEventHandler(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }
  
  @KafkaHandler
  public void handle(final ThreadCreatedEvent threadCreatedEvent) {
    LOGGER.info("New event received: title={} id={}", threadCreatedEvent.getTitle(), threadCreatedEvent.getId());

    try {
      ResponseEntity<String> respo = restTemplate.exchange("http://localhost:8082", HttpMethod.GET, null, String.class);
      if (respo.getStatusCode().value() == HttpStatus.OK.value()) {
        LOGGER.info("Got response from remote: {}", respo.getStatusCode());
      }
    } catch (ResourceAccessException rae) {
      LOGGER.error(rae.getMessage());
      throw new RetryableException(rae);
    } catch (HttpServerErrorException hse) {
      LOGGER.error(hse.getMessage());
      throw new NonRetryableException(hse);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      throw new NonRetryableException(e);
    }
  }
}
