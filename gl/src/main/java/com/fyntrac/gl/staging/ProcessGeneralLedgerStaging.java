package com.fyntrac.gl.staging;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.GeneralLedgerAccountService;
import com.fyntrac.common.utils.StringUtil;
import com.fyntrac.gl.service.BaseGeneralLedgerService;
import com.fyntrac.gl.service.DatasourceService;
import com.fyntrac.gl.service.GeneralLedgerCommonService;
import com.mongodb.MongoBulkWriteException;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
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
    private final GeneralLedgerCommonService glCommonService;
    private final DatasourceService datasourceService;
    private final GeneralLedgerAccountService generalLedgerAccountService;

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Value("${fyntrac.thread.pool.size}")
    private int threadPoolSize;

    /**
     * Constructor for dependency injection.
     *
     * @param datasourceService    the service to manage data sources
     * @param dataService          the service to handle data persistence
     * @param glCommonService      the common service for general ledger operations
     */
    @Autowired
    public ProcessGeneralLedgerStaging(DatasourceService datasourceService,
                                       DataService<GeneralLedgerEnteryStage> dataService,
                                       GeneralLedgerCommonService glCommonService,
                                       GeneralLedgerAccountService generalLedgerAccountService) {
        this.datasourceService = datasourceService;
        this.dataService = dataService;
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

            String tenantId = messageRecord.tenantId();
            long jobId = messageRecord.jobId();
            this.datasourceService.addDatasource(tenantId);
            this.generalLedgerAccountService.setTenantId(tenantId);

            ExecutorService executor = Executors.newFixedThreadPool(this.threadPoolSize);

            Query countQuery = new Query(Criteria.where("batchId").is(jobId));
            long totalRecords = this.dataService.getMongoTemplate(tenantId).count(countQuery, TransactionActivity.class);
            int totalChunks = (int) Math.ceil((double) totalRecords / chunkSize);

            for (int i = 0; i < totalChunks; i++) {
                final int chunkIndex = i;
                executor.submit(() -> {
                    try {
                        Query chunkQuery = new Query(Criteria.where("batchId").is(jobId));
                        chunkQuery.skip((long) chunkIndex * chunkSize).limit(chunkSize);
                        List<TransactionActivity> chunk = this.dataService.getMongoTemplate(tenantId).find(chunkQuery, TransactionActivity.class);
                        processTransactionActivityChunk(tenantId, chunk);
                    } catch (Exception e) {
                        log.error("Error processing chunk: {}", chunkIndex, e);
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
    private void processTransactionActivityChunk(String tenantId, List<TransactionActivity> chunk) {
        Set<GeneralLedgerEnteryStage> gleList = new HashSet<>(0);
        Collection<GeneralLedgerAccountBalanceStage> accountBalanceList = new ArrayList<>(0);
        try {
            log.info("Processing chunk: {}", chunk);

            for (TransactionActivity transactionActivity : chunk) {
                try {
                    if (transactionActivity == null) {
                        log.warn("No TransactionActivity found for key: {}", transactionActivity.toString());
                        continue;
                    }

                    CacheMap<SubledgerMapping> mapping = this.glCommonService.loadSubledgerMappingCache(tenantId);

                    List<SubledgerMapping> subledgerMappings = new ArrayList<>(0);
                    for(Map.Entry<String , SubledgerMapping> entry : mapping.getMap().entrySet()) {
                        SubledgerMapping m = entry.getValue();
                        if(transactionActivity.getTransactionName().equalsIgnoreCase(m.getTransactionName())) {
                            subledgerMappings.add(m);
                        }
                    }

                        for(SubledgerMapping slMapping : subledgerMappings) {
                            try {
                             if (slMapping == null) {
                                 log.error(String.format("Skipping GL booking for [%s] SubledgerMapping is null for entryType: [%s]", transactionActivity.toString(), slMapping.toString()));
                                 continue;
                             }
                             AccountTypes accountType = this.glCommonService.getAccountType(tenantId, slMapping.getAccountSubType());
                             String accountSubType = slMapping.getAccountSubType();
                             Map<String, Object> attributes = transactionActivity.getAttributes();
                             ChartOfAccount chartOfAccount = this.glCommonService.getChartOfAccount(tenantId, accountSubType, attributes);

                             BigDecimal debitAmount = BigDecimal.valueOf(0L);
                             BigDecimal creditAmount = BigDecimal.valueOf(0L);
                             Sign entrySign = transactionActivity.getAmount().signum() >=0 ? Sign.POSITIVE : Sign.NEGATIVE;

                             if (entrySign == Sign.POSITIVE && slMapping.getEntryType() == EntryType.DEBIT) {
                                 debitAmount = transactionActivity.getAmount().abs();
                             } else if (entrySign == Sign.POSITIVE && slMapping.getEntryType() == EntryType.CREDIT) {
                                 creditAmount = transactionActivity.getAmount().abs();
                             } else if (entrySign == Sign.NEGATIVE && slMapping.getEntryType() == EntryType.DEBIT) {
                                 creditAmount = transactionActivity.getAmount().abs();
                             } else if (entrySign == Sign.NEGATIVE && slMapping.getEntryType() == EntryType.CREDIT) {
                                 debitAmount = transactionActivity.getAmount().abs();
                             }

                             GeneralLedgerEnteryStage gleStage = GeneralLedgerEnteryStage.builder()
                                     .attributeId(transactionActivity.getAttributeId())
                                     .instrumentId(transactionActivity.getInstrumentId())
                                     .transactionName(transactionActivity.getTransactionName())
                                     .postingDate(transactionActivity.getPostingDate())
                                     .periodId(transactionActivity.getPeriodId())
                                     .glAccountNumber(chartOfAccount.getAccountNumber())
                                     .glAccountName(chartOfAccount.getAccountName())
                                     .glAccountSubType(chartOfAccount.getAccountSubtype())
                                     .glAccountType(accountType.getAccountType().name())
                                     .isReclass(0)
                                     .debitAmount(debitAmount.abs())
                                     .creditAmount(creditAmount.abs())
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
                         }catch (Exception e) {
                                log.error("Error processing subledger mapping: {}", slMapping, e);
                                throw new RuntimeException("Error processing subledger mapping", e);
                            }

                        }
                } catch (Exception e) {
                    log.error("Error processing transaction activity key: {}", transactionActivity.toString(), e);
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
                    "attributeId", "transactionName", "transactionDate", "periodId", "glAccountNumber", "instrumentId", "glAccountName", "glAccountType", "glAccountSubType", "debitAmount", "creditAmount", "isReclass", "attributes");
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
            this.dataService.copyData(tenantId, criteria, targetCollection, sourceCollection, Document.class,
                    "code", "subCode", "accountNumber", "accountName" , "accountSubtype", "instrumentId", "attributeId", "accountType", "transactionName", "periodId", "amount");
        } catch (Exception e) {
            log.error("Error copying general ledger account balance data for batch: {}", batch.getId(), e);
            throw e; // Rethrow to ensure the error is propagated
        }
    }
}