package com.daya.symposium.notifications.io;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity implements Serializable {
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue
  private long id;

  @Column(nullable = false, unique = true)
  private String messageId;

  @Column(nullable = false)
  private String threadId;

  public void setId(long id) {
    this.id = id;
  }
  
  public ProcessedEventEntity() {
  }

  public ProcessedEventEntity(String messageId, String threadId) {
    this.messageId = messageId;
    this.threadId = threadId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public void setThreadId(String threadId) {
    this.threadId = threadId;
  }

  public static long getSerialversionuid() {
    return serialVersionUID;
  }

  public long getId() {
    return id;
  }

  public String getMessageId() {
    return messageId;
  }

  public String getThreadId() {
    return threadId;
  }
}
