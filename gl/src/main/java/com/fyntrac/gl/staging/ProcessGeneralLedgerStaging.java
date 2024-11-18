package com.fyntrac.gl.staging;

import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.gl.entity.StageGeneralLedgerEntry;
import com.fyntrac.gl.service.BaseGeneralLedgerService;
import com.fyntrac.gl.service.DatasourceService;
import com.fyntrac.gl.service.GeneralLedgerCommonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivityList;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.entity.GeneralLedgerEnteryStage;

/**
 * Service to process general ledger staging from transaction activity data.
 * This class acts as a Pulsar consumer and processes messages to generate
 * General Ledger (GL) entries in stages.
 */
@Service
@Slf4j
public class ProcessGeneralLedgerStaging extends BaseGeneralLedgerService {

    private final DataService<StageGeneralLedgerEntry> dataService;
    private final MemcachedRepository memcachedRepository;
    private final GeneralLedgerCommonService glCommonService;
    private final DatasourceService datasourceService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Value("${fyntrac.thread.pool.size}")
    private int threadPoolSize;

    private TransactionActivityList keyList;

    /**
     * Constructor for dependency injection.
     *
     * @param datasourceService    the service to manage data sources
     * @param dataService          the service to handle data persistence
     * @param memcachedRepository  the repository for cached data
     * @param glCommonService      the common service for general ledger operations
     */
    @Autowired
    public ProcessGeneralLedgerStaging(DatasourceService datasourceService,
                                       DataService<StageGeneralLedgerEntry> dataService,
                                       MemcachedRepository memcachedRepository,
                                       GeneralLedgerCommonService glCommonService) {
        this.datasourceService = datasourceService;
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
        this.glCommonService = glCommonService;
    }

    /**
     * Processes a general ledger message record by retrieving data, splitting it into chunks,
     * and processing each chunk concurrently using a thread pool.
     *
     * @param messageRecord the general ledger message record received from Pulsar
     * @throws RuntimeException if any error occurs during processing
     */
    public void process(Records.GeneralLedgerMessageRecord messageRecord) throws ExecutionException, InterruptedException {
        try {
            keyList = this.memcachedRepository.getFromCache(messageRecord.dataKey(), TransactionActivityList.class);
            if (keyList == null || keyList.get().isEmpty()) {
                log.warn("No data found in cache for dataKey: {}", messageRecord.dataKey());
                return;
            }

            String tenantId = messageRecord.tenantId();
            this.datasourceService.addDatasource(tenantId);

            List<String> dataSet = keyList.get();

            ExecutorService executor = Executors.newFixedThreadPool(this.threadPoolSize);

            List<List<String>> chunks = chunkList(dataSet);

            for (List<String> chunk : chunks) {
                executor.submit(() -> {
                    try {
                        processTransactionActivityChunk(tenantId, chunk);
                    } catch (Exception e) {
                        log.error("Error processing chunk: {}", chunk, e);
                        throw new RuntimeException("Error processing chunk", e);
                    }
                });
            }

            executor.shutdown();
            while (!executor.isTerminated()) {
                log.info("Waiting for all threads to complete.");
                Thread.sleep(1000);
            }

            log.info("All chunks processed successfully.");
        } catch (Exception e) {
            log.error("Error in process method for messageRecord: {}", messageRecord, e);
            throw new RuntimeException("Error in process method", e);
        }finally {
            List<String> dataSet = keyList.get();
            for(String key : dataSet) {
                this.memcachedRepository.delete(key);
            }
            this.memcachedRepository.delete(messageRecord.dataKey());
        }
    }

    /**
     * Processes a chunk of transaction activity keys to generate general ledger entries.
     *
     * @param tenantId the tenant ID
     * @param chunk    a list of transaction activity keys to process
     * @throws RuntimeException if any error occurs during chunk processing
     */
    private void processTransactionActivityChunk(String tenantId, List<String> chunk) {
        try {
            log.info("Processing chunk: {}", chunk);
            Set<GeneralLedgerEnteryStage> gleList = new HashSet<>(0);

            for (String transactionActivityKey : chunk) {
                try {
                    TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, TransactionActivity.class);
                    if (transactionActivity == null) {
                        log.warn("No TransactionActivity found for key: {}", transactionActivityKey);
                        continue;
                    }

                    Map<EntryType, SubledgerMapping> mapping = this.glCommonService.getSubledgerMapping(tenantId, transactionActivity);
                    for (Map.Entry<EntryType, SubledgerMapping> entry : mapping.entrySet()) {
                        try {
                            SubledgerMapping slMapping = entry.getValue();
                            if (slMapping == null) {
                                log.error("SubledgerMapping is null for entryType: {}", entry.getKey());
                                throw new RuntimeException("SubledgerMapping is null for entryType: " + entry.getKey());
                            }
                            AccountTypes accountType = this.glCommonService.getAccountType(tenantId, slMapping.getAccountSubType());
                            String accountSubType = slMapping.getAccountSubType();
                            Map<String, Object> attributes = transactionActivity.getAttributes();
                            ChartOfAccount chartOfAccount = this.glCommonService.getChartOfAccount(tenantId, accountSubType, attributes);

                            double debitAmount = 0.0d;
                            double creditAmount = 0.0d;
                            int sign = (transactionActivity.getAmount() > 0) ? -1 : 1;
                            if (slMapping.getEntryType() == EntryType.DEBIT) {
                                debitAmount = transactionActivity.getAmount();
                            } else if (slMapping.getEntryType() == EntryType.CREDIT) {
                                creditAmount = sign * transactionActivity.getAmount();
                            }

                            GeneralLedgerEnteryStage gleStage = GeneralLedgerEnteryStage.builder()
                                    .attributeId(transactionActivity.getAttributeId())
                                    .instrumentId(transactionActivity.getInstrumentId())
                                    .transactionName(transactionActivity.getTransactionName())
                                    .transactionDate(transactionActivity.getTransactionDate())
                                    .periodId(transactionActivity.getPeriodId())
                                    .glAccountNumber(chartOfAccount.getAccountNumber())
                                    .glAccountName(chartOfAccount.getAccountName())
                                    .glAccountSubType(chartOfAccount.getAccountSubtype())
                                    .isReclass(0)
                                    .debitAmount(debitAmount)
                                    .creditAmount(creditAmount)
                                    .attributes(attributes)
                                    .build();

                            gleList.add(gleStage);
                        } catch (Exception e) {
                            log.error("Error processing subledger mapping: {}", entry.getValue(), e);
                            throw new RuntimeException("Error processing subledger mapping", e);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error processing transaction activity key: {}", transactionActivityKey, e);
                    throw new RuntimeException("Error processing transaction activity", e);
                }
            }

            this.dataService.saveAll(gleList, tenantId, GeneralLedgerEnteryStage.class);
        } catch (Exception e) {
            log.error("Error processing transaction activity chunk: {}", chunk, e);
            throw new RuntimeException("Error processing chunk", e);
        }
    }

    /**
     * Splits a list into smaller chunks of a specified size.
     *
     * @param list the list to split
     * @return a list of chunks
     */
    private List<List<String>> chunkList(List<String> list) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += this.chunkSize) {
            chunks.add(new ArrayList<>(list.subList(i, Math.min(i + this.chunkSize, list.size()))));
        }
        return chunks;
    }
}
