package com.example.kafka_consumer_dedup.job;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * OrderSyncDiscoveryJob
 */
@Component
public class OrderSyncDiscoveryJob {
    
    @Scheduled(fixedRateString="${ordersync.discovery-interval-millis:12000}")
    public void discovery() {
        System.out.println("pritam");
    }
    
}