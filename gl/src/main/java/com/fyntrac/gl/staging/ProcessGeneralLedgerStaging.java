package com.fyntrac.gl.staging;

import com.fyntrac.common.enums.EntryType;
import com.fyntrac.gl.entity.StageGeneralLedgerEntry;
import com.fyntrac.gl.service.DatasourceService;
import com.fyntrac.gl.service.GeneralLedgerCommonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivityList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.SubledgerMapping;

@Service
@Slf4j
public class ProcessGeneralLedgerStaging {

    private DataService<StageGeneralLedgerEntry> dataService;
    private MemcachedRepository memcachedRepository;
    private GeneralLedgerCommonService glCommonService;
    private DatasourceService datasourceService;
    // Define chunk size
    private int chunkSize = 5;
    private int threadPoolSize=5;
    private TransactionActivityList keyList;

    @Autowired
    ProcessGeneralLedgerStaging(DatasourceService datasourceService,
                                DataService<StageGeneralLedgerEntry> dataService
                                , MemcachedRepository memcachedRepository
                                , GeneralLedgerCommonService glCommonService) {
        this.datasourceService = datasourceService;
        this.dataService =  dataService;
        this.memcachedRepository = memcachedRepository;
        this.glCommonService = glCommonService;
    }

    public void process(Records.GeneralLedgerMessageRecord messageRecord) {
        keyList = this.memcachedRepository.getFromCache(messageRecord.dataKey(), TransactionActivityList.class);
        String tenantId = messageRecord.tenantId();
        this.datasourceService.addDatasource(tenantId);
        List<String> dataSet = keyList.get();

        // Create a fixed thread pool
        ExecutorService executor = Executors.newFixedThreadPool(this.threadPoolSize);

        // Split the list into chunks
        List<List<String>> chunks = chunkList(dataSet);

        // Submit tasks for each chunk
        for (List<String> chunk : chunks) {
            executor.submit(() -> processTransactionActivityChunk(tenantId, chunk));
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
    private void processTransactionActivityChunk(String tenantId, List<String> chunk) {
        // Simulate processing by printing the chunk
        log.info("Processing chunk: " + chunk);
        for(String transactionActivityKey: chunk) {
            TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, TransactionActivity.class);

            Map<EntryType, SubledgerMapping> mapping = this.glCommonService.getSubledgerMapping(tenantId, transactionActivity);
            log.info("Processing mapping: " + mapping);
        }
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
