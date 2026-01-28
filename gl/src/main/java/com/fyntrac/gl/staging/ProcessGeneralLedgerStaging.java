package com.fyntrac.gl.staging;

import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.EntryType;
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
    private final TransactionActivityQueue transactionActivityQueue;

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
                                       GeneralLedgerAccountService generalLedgerAccountService,
                                        TransactionActivityQueue transactionActivityQueue) {
        this.datasourceService = datasourceService;
        this.dataService = dataService;
        this.glCommonService = glCommonService;
        this.generalLedgerAccountService = generalLedgerAccountService;
        this.transactionActivityQueue = transactionActivityQueue;
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

            int totalChunks = transactionActivityQueue.getTotalChunks(tenantId, jobId, chunkSize);

            for (int i = 0; i < totalChunks; i++) {
                List<TransactionActivity> chunk = transactionActivityQueue.readChunk(tenantId, jobId, chunkSize, i);
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

                    Map<EntryType, SubledgerMapping> mapping = this.glCommonService.getSubledgerMapping(tenantId, StringUtil.removeSpaces(transactionActivity.getTransactionName()), transactionActivity.getAmount());
                    for (Map.Entry<EntryType, SubledgerMapping> entry : mapping.entrySet()) {
                        try {
                            SubledgerMapping slMapping = entry.getValue();
                            if (slMapping == null) {
                                log.error(String.format("Skipping GL booking for [%s] SubledgerMapping is null for entryType: [%s]", transactionActivity.toString(), entry.getKey()));
                                continue;
                            }
                            AccountTypes accountType = this.glCommonService.getAccountType(tenantId, slMapping.getAccountSubType());
                            String accountSubType = slMapping.getAccountSubType();
                            Map<String, Object> attributes = transactionActivity.getAttributes();
                            ChartOfAccount chartOfAccount = this.glCommonService.getChartOfAccount(tenantId, accountSubType, attributes);

                            BigDecimal debitAmount = BigDecimal.valueOf(0L);
                            BigDecimal creditAmount = BigDecimal.valueOf(0L);
                            int sign = (transactionActivity.getAmount().compareTo(BigDecimal.ZERO) > 0) ? -1 : 1;
                            if (slMapping.getEntryType() == EntryType.DEBIT) {
                                debitAmount = transactionActivity.getAmount();
                            } else if (slMapping.getEntryType() == EntryType.CREDIT) {
                                double signedValue = transactionActivity.getAmount().doubleValue();
                                signedValue = signedValue * sign;
                                creditAmount = BigDecimal.valueOf(signedValue);
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