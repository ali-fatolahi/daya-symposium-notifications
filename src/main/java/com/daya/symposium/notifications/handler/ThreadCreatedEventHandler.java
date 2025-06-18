package com.daya.symposium.notifications.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.daya.symposium.core.ThreadCreatedEvent;
import com.daya.symposium.notifications.error.NonRetryableException;
import com.daya.symposium.notifications.error.RetryableException;
import com.daya.symposium.notifications.io.ProcessedEventEntity;
import com.daya.symposium.notifications.io.ProcessedEventRepository;

@Component
@KafkaListener(topics="daya-symposium-threads")
public class ThreadCreatedEventHandler {
  private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
  private RestTemplate restTemplate;
  private ProcessedEventRepository processedEventRepository;

  public ThreadCreatedEventHandler(
      RestTemplate restTemplate,
      ProcessedEventRepository processedEventRepository) {
    this.restTemplate = restTemplate;
    this.processedEventRepository = processedEventRepository;
  }
  
  @Transactional
  @KafkaHandler
  public void handle(
      @Payload final ThreadCreatedEvent threadCreatedEvent,
      @Header("messageId") final String messageId,
      @Header(KafkaHeaders.RECEIVED_KEY) final String messageKey) {
    LOGGER.info("New event received: title={} id={}", threadCreatedEvent.getTitle(), threadCreatedEvent.getId());

    final ProcessedEventEntity processedEventEntity =
        processedEventRepository.findByMessageId(messageId);
    if (processedEventEntity != null) {
      LOGGER.info("Event with messageId={} already processed, skipping", messageId);
      return;
    }

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

    try {
      processedEventRepository.save(new ProcessedEventEntity(messageId, threadCreatedEvent.getId()));
    } catch (DataIntegrityViolationException dive) {
      throw new NonRetryableException(dive);
    }
  }
}