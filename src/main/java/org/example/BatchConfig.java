package org.example;

import java.nio.charset.StandardCharsets;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.json.JsonObject;

@Configuration
public class BatchConfig {

    private static final int MAX_JSON_SIZE_IN_BYTES = 16 * 1024;
    private static final int JSON_OBJECT_COUNT = 500;

    @Bean
    public Job job(JobRepository jobRepository, Step jsonStep) {
        return new JobBuilder("job", jobRepository)
            .start(jsonStep)
            .build();
    }

    @Bean
    public Step jsonStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager) {
        return new StepBuilder("jsonStep", jobRepository)
            .<JsonObject, JsonObject>chunk(100, transactionManager)
            .reader(objectReader())
            .writer(jsonWriter())
            .build();
    }

    private RandomObjectReader objectReader() {
        return new RandomObjectReader(JSON_OBJECT_COUNT);
    }

    private CappedJsonWriter jsonWriter() {
        return new CappedJsonWriter(MAX_JSON_SIZE_IN_BYTES, StandardCharsets.UTF_8);
    }
}
