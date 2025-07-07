package com.daya.symposium.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import com.daya.symposium.core.ThreadCreatedEvent;
import com.daya.symposium.notifications.handler.ThreadCreatedEventHandler;
import com.daya.symposium.notifications.io.ProcessedEventEntity;
import com.daya.symposium.notifications.io.ProcessedEventRepository;

import net.bytebuddy.asm.Advice.Argument;

@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ThreadCreatedEventHandlerIntegrationTest {
  @MockitoBean
  private ProcessedEventRepository processedEventRepository;

  @MockitoBean
  private RestTemplate restTemplate;

  @Autowired
  private KafkaTemplate<String, Object> kafkaTemplate;

  @MockitoSpyBean
  private ThreadCreatedEventHandler threadCreatedEventHandler;

  @Test
  public void testThreadCreatedEventHandler_OnThreadCreated_HandlesEvent() throws InterruptedException, ExecutionException {
    ThreadCreatedEvent threadCreatedEvent = new ThreadCreatedEvent(
      "thread-id-123",
      "Test Thread Title",
      "This is a test thread body.",
      5,
      new BigDecimal("10.00")
    );

    String messageId = UUID.randomUUID().toString();
    String messageKey = threadCreatedEvent.getId();

    ProducerRecord<String, Object> record =
      new ProducerRecord<>("daya-symposium-threads", messageKey, threadCreatedEvent);
    record.headers().add("messageId", messageId.getBytes());
    record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

    ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
    when(processedEventRepository.findByMessageId(messageId)).thenReturn(processedEventEntity);
    when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);

    String responseBody = "{\"status\": \"success\"}";
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

    when(restTemplate.exchange(
      any(String.class),
      any(HttpMethod.class),
      isNull(),
      eq(String.class)))
      .thenReturn(responseEntity);
    
    kafkaTemplate.send(record).get();
    
    ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ThreadCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ThreadCreatedEvent.class);

    verify(threadCreatedEventHandler, timeout(5000).times(1))
      .handle(
        eventCaptor.capture(),
        messageIdCaptor.capture(),
        messageKeyCaptor.capture()
      );

    assertEquals(messageId, messageIdCaptor.getValue());
    assertEquals(messageKey, messageKeyCaptor.getValue());
    assertEquals(threadCreatedEvent.getId(), eventCaptor.getValue().getId());
  }

}
