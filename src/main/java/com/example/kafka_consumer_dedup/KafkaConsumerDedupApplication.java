package com.example.kafka_consumer_dedup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaConsumerDedupApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerDedupApplication.class, args);
	}

}
