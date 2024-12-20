package com.fyntrac.gl.staging;

import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.entity.GeneralLedgerEnteryStage;
import com.fyntrac.common.entity.GeneralLedgerAccountBalanceStage;
import com.fyntrac.common.entity.GeneralLedgerAccountBalance;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.Batch;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.GeneralLedgerAccountService;
import com.fyntrac.gl.service.BaseGeneralLedgerService;
import com.fyntrac.gl.service.DatasourceService;
import com.fyntrac.gl.service.GeneralLedgerCommonService;
import com.mongodb.MongoBulkWriteException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service to process general ledger staging from transaction activity data.
 * This class acts as a Pulsar consumer and processes messages to generate
 * General Ledger (GL) entries in stages.
 */
@Service
@Slf4j
public class ProcessGeneralLedgerStaging extends BaseGeneralLedgerService {

    private final DataService dataService;
    private final MemcachedRepository memcachedRepository;
    private final GeneralLedgerCommonService glCommonService;
    private final DatasourceService datasourceService;
    private final GeneralLedgerAccountService generalLedgerAccountService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Value("${fyntrac.thread.pool.size}")
    private int threadPoolSize;

    private com.fyntrac.common.entity.TransactionActivityList keyList;

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
                                       DataService<GeneralLedgerEnteryStage> dataService,
                                       MemcachedRepository memcachedRepository,
                                       GeneralLedgerCommonService glCommonService,
                                       GeneralLedgerAccountService generalLedgerAccountService) {
        this.datasourceService = datasourceService;
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
        this.glCommonService = glCommonService;
        this.generalLedgerAccountService = generalLedgerAccountService;
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
            keyList = this.memcachedRepository.getFromCache(messageRecord.dataKey(), com.fyntrac.common.entity.TransactionActivityList.class);
            if (keyList == null || keyList.get().isEmpty()) {
                log.warn("No data found in cache for dataKey: {}", messageRecord.dataKey());
                return;
            }

            String tenantId = messageRecord.tenantId();
            this.datasourceService.addDatasource(tenantId);
            this.generalLedgerAccountService.setTenantId(tenantId);
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
        } finally {
            if (keyList != null) {
                List<String> dataSet = keyList.get();
                for (String key : dataSet) {
                    try {
                        this.memcachedRepository.delete(key);
                    } catch (Exception e) {
                        log.error("Error deleting key from cache: {}", key, e);
                        throw e; // Rethrow to ensure the error is propagated
                    }
                }
                try {
                    this.memcachedRepository.delete(messageRecord.dataKey());
                } catch (Exception e) {
                    log.error("Error deleting messageRecord dataKey from cache: {}", messageRecord.dataKey(), e);
                    throw e; // Rethrow to ensure the error is propagated
                }
            }
        }
    }

    /**
     * Processes a chunk of transaction activity keys to generate general ledger entries.
     *
     * @param tenantId the tenant ID
     * @param chunk    a list of transaction activity keys to process
     * @throws RuntimeException if any error occurs during chunk processing
     */
    @Transactional
    private void processTransactionActivityChunk(String tenantId, List<String> chunk) {
        Set<GeneralLedgerEnteryStage> gleList = new HashSet<>(0);
        Collection<GeneralLedgerAccountBalanceStage> accountBalanceList = new ArrayList<>(0);
        try {
            log.info("Processing chunk: {}", chunk);

            for (String transactionActivityKey : chunk) {
                try {
                    TransactionActivity transactionActivity = this.memcachedRepository.getFromCache(transactionActivityKey, com.fyntrac.common.entity.TransactionActivity.class);
                    if (transactionActivity == null) {
                        log.warn("No TransactionActivity found for key: {}", transactionActivityKey);
                        continue;
                    }

                    Map<EntryType, SubledgerMapping> mapping = this.glCommonService.getSubledgerMapping(tenantId, transactionActivity.getTransactionName(), transactionActivity.getAmount());
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
                                    .glAccountType(accountType.getAccountType().name())
                                    .isReclass(0)
                                    .debitAmount(debitAmount)
                                    .creditAmount(creditAmount)
                                    .attributes(attributes)
                                    .batchId(transactionActivity.getBatchId())
                                    .build();

                            gleList.add(gleStage);

                            if (accountType.getAccountType() == AccountType.BALANCESHEET) {
                                GeneralLedgerAccountBalanceStage accountBalanceStage =
                                        GeneralLedgerAccountBalanceStage.builder()
                                                .accountType(AccountType.BALANCESHEET)
                                                .attributeId(transactionActivity.getAttributeId())
                                                .instrumentId(transactionActivity.getInstrumentId())
                                                .periodId(transactionActivity.getPeriodId())
                                                .amount(transactionActivity.getAmount())
                                                .transactionName(transactionActivity.getTransactionName())
                                                .accountNumber(chartOfAccount.getAccountNumber())
                                                .accountName(chartOfAccount.getAccountName())
                                                .accountSubtype(chartOfAccount.getAccountSubtype())
                                                .batchId(transactionActivity.getBatchId()).build();
                                accountBalanceStage.setCode(accountBalanceStage.hashCode());
                                accountBalanceStage.setSubCode(accountBalanceStage.subCode());

                                accountBalanceList.add(accountBalanceStage);

                            }

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

            this.dataService.saveAll(gleList, tenantId, com.fyntrac.common.entity.GeneralLedgerEnteryStage.class);
            this.dataService.saveAll(accountBalanceList, tenantId, GeneralLedgerAccountBalanceStage.class);
        } catch (MongoBulkWriteException e) {
            log.error("Bulk write exception: " + e.getMessage());
            e.getWriteErrors().forEach(error -> {
                log.error("Error at index " + error.getIndex() + ": " + error.getMessage());
            });
            throw e; // Rethrow to trigger transaction rollback
        } catch (Exception e) {
            log.error("Error processing transaction activity chunk: {}", chunk, e);
            throw new RuntimeException("Error processing chunk", e);
        } finally {
            try {
                bookGLEAccountBalance(accountBalanceList);
            } catch (Exception e) {
                log.error("Error booking general ledger account balance", e);
                throw e; // Rethrow to ensure the error is propagated
            }
        }
    }

    /**
     * Books the general ledger account balance.
     *
     * @param accountBalances the collection of account balances to book
     */
    private void bookGLEAccountBalance(Collection<GeneralLedgerAccountBalanceStage> accountBalances) {
        // Implementation for booking general ledger account balance
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

    /**
     * Retrieves the previous account balance for a given amount, account type, and accounting period.
     *
     * @param amount          the amount to check
     * @param accountType     the type of account
     * @param accountingPeriod the accounting period to check
     * @return the previous account balance stage
     */
    private GeneralLedgerAccountBalanceStage getPreviousAccountBalance(double amount, AccountType accountType, AccountingPeriod accountingPeriod) {
        HashSet<String> hashCode = new HashSet<>(0);
        hashCode.add(String.valueOf(accountingPeriod.getPeriodId()));
        hashCode.add(accountType.name());
        GeneralLedgerAccountBalanceStage gleAccountBalance = this.generalLedgerAccountService.getGeneralLedgerAccountBalance(hashCode.hashCode());
        if (gleAccountBalance == null) {
            hashCode = new HashSet<>(0);
            hashCode.add(String.valueOf(accountingPeriod.getPreviousAccountingPeriodId()));
            hashCode.add(accountType.name());
            gleAccountBalance = this.generalLedgerAccountService.getGeneralLedgerAccountBalance(hashCode.hashCode());
        }
        return gleAccountBalance;
    }

    /**
     * Closes the accounting period based on the provided message record.
     *
     * @param accountingPeriodCloseMessageRecord the message record containing accounting period close details
     */
    public void closeAccountingPeriod(Records.AccountingPeriodCloseMessageRecord accountingPeriodCloseMessageRecord) {
        Collection<com.fyntrac.common.entity.Batch> batches = accountingPeriodCloseMessageRecord.batches();
        this.dataService.setTenantId(accountingPeriodCloseMessageRecord.tenantId());
        this.datasourceService.addDatasource(accountingPeriodCloseMessageRecord.tenantId());
        for (com.fyntrac.common.entity.Batch batch : batches) {
            try {
                this.copyGeneralLedgerStageData(accountingPeriodCloseMessageRecord.tenantId(), batch);
                this.copyGeneralLedgerAccountBalanceData(accountingPeriodCloseMessageRecord.tenantId(), batch );
            } catch (Exception e) {
                log.error("Error closing accounting period for batch: {}", batch.getId(), e);
                throw e; // Rethrow to ensure the error is propagated
            }
        }
    }

    private void processReclass() {

    }
    /**
     * Copies general ledger stage data from the source collection to the target collection.
     *
     * @param tenantId the tenant ID
     * @param batch    the batch containing the data to copy
     */
    private void copyGeneralLedgerStageData(String tenantId, Batch batch) {
        String targetCollection = "GeneralLedgerEntery";
        String sourceCollection = "GeneralLedgerEnteryStage";
        Criteria criteria = Criteria.where("batchId").is(batch.getId());
        try {
            this.dataService.copyData(tenantId, criteria, targetCollection, sourceCollection, GeneralLedgerEnteryStage.class,
                    "attributeId", "transactionName", "transactionDate", "periodId", "glAccountNumber", "instrumentId", "glAccountName", "glAccountType", "glAccountSubType", "debitAmount", "creditAmount", "isReclass");
        } catch (Exception e) {
            log.error("Error copying general ledger stage data for batch: {}", batch.getId(), e);
            throw e; // Rethrow to ensure the error is propagated
        }
    }

    /**
     * Copies general ledger account balance data from the source collection to the target collection.
     *
     * @param tenantId the tenant ID
     * @param batch    the batch containing the data to copy
     */
    private void copyGeneralLedgerAccountBalanceData(String tenantId, Batch batch) {
        String targetCollection = "GeneralLedgerAccountBalance";
        String sourceCollection = "GeneralLedgerAccountBalanceStage";
        Criteria criteria = Criteria.where("batchId").is(batch.getId());
        try {
            this.dataService.copyData(tenantId, criteria, targetCollection, sourceCollection, GeneralLedgerAccountBalance.class,
                    "code", "subCode", "accountNumber", "accountName" , "accountSubtype", "instrumentId", "attributeId", "accountType", "transactionName", "periodId", "amount");
        } catch (Exception e) {
            log.error("Error copying general ledger account balance data for batch: {}", batch.getId(), e);
            throw e; // Rethrow to ensure the error is propagated
        }
    }
}