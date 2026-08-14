package com.example.kafka_consumer_dedup.repository;

import com.example.kafka_consumer_dedup.entity.SeedData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SeedDataRepository extends JpaRepository<SeedData, String> {
}
