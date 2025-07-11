package com.daya.symposium.notifications.io;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {
  ProcessedEventEntity findByMessageId(String messageId);
}