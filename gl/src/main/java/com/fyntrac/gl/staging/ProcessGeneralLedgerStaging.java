package com.fyntrac.gl.staging;

import com.fyntrac.gl.entity.StageGeneralLedgerEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivityList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class ProcessGeneralLedgerStaging {

    private DataService<StageGeneralLedgerEntry> dataService;
    private MemcachedRepository memcachedRepository;
    // Define chunk size
    private int chunkSize = 5;
    private int threadPoolSize=5;
    private TransactionActivityList keyList;

    @Autowired
    ProcessGeneralLedgerStaging(DataService<StageGeneralLedgerEntry> dataService, MemcachedRepository memcachedRepository) {
        this.dataService =  dataService;
        this.memcachedRepository = memcachedRepository;
    }

    public void process(Records.GeneralLedgerMessageRecord messageRecord) {
        keyList = this.memcachedRepository.getFromCache(messageRecord.dataKey(), TransactionActivityList.class);
        String tenantId = messageRecord.tenantId();
        List<String> dataSet = keyList.get();

        // Create a fixed thread pool
        ExecutorService executor = Executors.newFixedThreadPool(this.threadPoolSize);

        // Split the list into chunks
        List<List<String>> chunks = chunkList(dataSet);

        // Submit tasks for each chunk
        for (List<String> chunk : chunks) {
            executor.submit(() -> processTransactionActivityChunk(chunk));
        }

        // Shutdown the executor
        executor.shutdown();
        // Wait for all tasks to finish (optional)
        while (!executor.isTerminated()) {
            // You can add a sleep or a log here if needed
            log.info("Waiting for all threads to get completed");
        }

        log.info("All chunks processed.");
    }

    // Method to process a chunk of data
    private void processTransactionActivityChunk(List<String> chunk) {
        // Simulate processing by printing the chunk
        log.info("Processing chunk: " + chunk);
        // Add your processing logic here

    }


    // Method to split the list into chunks
    private List<List<String>> chunkList(List<String> list) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += this.chunkSize) {
            chunks.add(new ArrayList<>(list.subList(i, Math.min(i + this.chunkSize, list.size()))));
        }
        return chunks;
    }
}
