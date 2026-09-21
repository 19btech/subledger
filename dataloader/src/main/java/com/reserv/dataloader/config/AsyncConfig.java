package com.reserv.dataloader.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Bounded executor for background file-upload processing (AsyncUploadDispatcher). Deliberately
 * small and bounded — this runs per-pod today; Stage 3 (docs/K8S_SCALING_STRATEGY.md) scales pod
 * count instead of growing this pool.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    @Value("${fyntrac.upload.async.core-pool-size:2}")
    private int corePoolSize;

    @Value("${fyntrac.upload.async.max-pool-size:4}")
    private int maxPoolSize;

    @Value("${fyntrac.upload.async.queue-capacity:50}")
    private int queueCapacity;

    @Bean("uploadTaskExecutor")
    public Executor uploadTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("upload-async-");
        executor.initialize();
        return executor;
    }
}
