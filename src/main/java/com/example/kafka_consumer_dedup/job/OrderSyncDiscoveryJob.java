package com.example.kafka_consumer_dedup.job;

import com.example.kafka_consumer_dedup.service.OrderSyncPublishService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled trigger for the publish pipeline.
 *
 * Controlled by ordersync.scheduler-enabled (default: false).
 *   false → job silently skips every tick; use /demo endpoints for manual control
 *   true  → job fires automatically every discovery-interval-millis
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSyncDiscoveryJob {

    private final OrderSyncPublishService publishService;

    @Value("${ordersync.scheduler-enabled:false}")
    private boolean schedulerEnabled;

    @Scheduled(fixedDelayString = "${ordersync.discovery-interval-millis:12000}")
    public void discovery() {
        if (!schedulerEnabled) {
            log.debug("[JOB] Scheduler disabled — use POST /demo/run or /demo/run-all for manual control.");
            return;
        }
        if (publishService.isCompleted()) {
            log.info("[JOB] All rounds complete. Job is idle.");
            return;
        }
        String result = publishService.runOneRound();
        log.info("[JOB] {}", result);
    }
}
