package com.fyntrac.gl.service;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.entity.GeneralLedgerAccountBalance;
import com.fyntrac.common.entity.ReclassValues;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.entity.GeneralLedgerEntery;
import com.fyntrac.common.enums.AccountType;
import com.fyntrac.common.enums.EntryType;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.service.GeneralLedgerAccountBalanceService;
import com.fyntrac.common.utils.Key;
import com.fyntrac.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.fyntrac.common.dto.record.Records.AccountingPeriodCloseMessageRecord;
import com.fyntrac.common.entity.Batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
@Slf4j
public class GeneralLedgerCommonService {

    private final DataService dataService;
    private final MemcachedRepository memcachedRepository;
    private final GeneralLedgerAccountBalanceService generalLedgerAccountBalanceService;
    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    @Value("${fyntrac.thread.pool.size}")
    private int threadPoolSize;

    @Autowired
    GeneralLedgerCommonService(DataService<SubledgerMapping> dataService
            , MemcachedRepository memcachedRepository, GeneralLedgerAccountBalanceService generalLedgerAccountBalanceService) {
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
        this.generalLedgerAccountBalanceService = generalLedgerAccountBalanceService;
    }

    public Map<EntryType, SubledgerMapping> getSubledgerMapping(String tenantId, String transactionName, double amount) {

        Sign sign = (amount > 0 ? com.fyntrac.common.enums.Sign.POSITIVE :  com.fyntrac.common.enums.Sign.NEGATIVE);


        if (!memcachedRepository.ifExists(Key.allSubledgerMappingList(tenantId))) {
            CacheMap<SubledgerMapping> slMapping = new CacheMap<>();
            List<SubledgerMapping> mappings = this.dataService.fetchAllData(tenantId, SubledgerMapping.class);
            // Iterate through the mappings and fill the mappingList
            for (SubledgerMapping mapping : mappings) {
                // Use the hashCode of the mapping as the key
                String key = tenantId
                        + mapping.getTransactionName()
                        + mapping.getEntryType().getValue()
                        + mapping.getSign().getValue();
                slMapping.put(StringUtil.convertToUpperCaseAndRemoveSpaces(key), mapping);
            }
            this.memcachedRepository.putCollectionInCache(Key.allSubledgerMappingList(tenantId), slMapping, 0);
        }

        String debitKey = StringUtil.convertToUpperCaseAndRemoveSpaces(tenantId
                + transactionName
                + EntryType.DEBIT
                + sign.getValue());

            String creditKey = StringUtil.convertToUpperCaseAndRemoveSpaces(tenantId
                + transactionName
                + EntryType.CREDIT
                + sign.getValue());

        CacheMap slMapping;
        slMapping = this.memcachedRepository.getFromCache(Key.allSubledgerMappingList(tenantId), CacheMap.class);

        SubledgerMapping debitSubledgerMapping = null;
        SubledgerMapping creditSubledgerMapping = null;
        Map<EntryType, SubledgerMapping> mappings = new HashMap<>(0);
        if (slMapping != null) {
            debitSubledgerMapping = (SubledgerMapping) slMapping.getValue(debitKey);
            creditSubledgerMapping = (SubledgerMapping) slMapping.getValue(creditKey);
            mappings.put(EntryType.DEBIT, debitSubledgerMapping);
            mappings.put(EntryType.CREDIT, creditSubledgerMapping);

        }
        return mappings;
    }

    /**
     * return accountType of a sl mapping
     * @param tenantId
     * @param accountSubType
     * @return
     */
    public AccountTypes getAccountType(String tenantId, String accountSubType) {

        if (!memcachedRepository.ifExists(Key.allAccountTypeList(tenantId))) {
            CacheMap<AccountTypes> accountTypeMap = new CacheMap<>();
            List<AccountTypes> accountTypes = this.dataService.fetchAllData(tenantId, AccountTypes.class);
            // Iterate through the mappings and fill the mappingList
            for (AccountTypes accountType : accountTypes) {
                // Use the hashCode of the mapping as the key
                String key = accountType.getAccountSubType();
                accountTypeMap.put(key, accountType);
            }
            this.memcachedRepository.putCollectionInCache(Key.allAccountTypeList(tenantId), accountTypeMap, 0);
        }
        CacheMap accountTypes;
        accountTypes = this.memcachedRepository.getFromCache(Key.allAccountTypeList(tenantId), CacheMap.class);

        AccountTypes accountType = null;
        if (accountTypes != null) {
            accountType = (AccountTypes) accountTypes.getValue(accountSubType);
        }
        return accountType;
    }

    public ChartOfAccount getChartOfAccount(String tenantId, String accountSubType, Map<String,Object> attributes) {
        CacheMap<ChartOfAccount> chartOfAccountCacheMap = new CacheMap<>();
        if (!memcachedRepository.ifExists(Key.allChartOfAccountList(tenantId))) {
            List<ChartOfAccount> chartOfAccounts = this.dataService.fetchAllData(tenantId, ChartOfAccount.class);
            // Iterate through the mappings and fill the mappingList
            for (ChartOfAccount chartOfAccount : chartOfAccounts) {
                // Use the hashCode of the mapping as the key
                String key = this.getHashcode(chartOfAccount.getAccountSubtype(), chartOfAccount.getAttributes());
                chartOfAccountCacheMap.put(key, chartOfAccount);
            }
            this.memcachedRepository.putInCache(Key.allChartOfAccountList(tenantId), chartOfAccountCacheMap, 0);
        }

        CacheMap<ChartOfAccount> chartOfAccounts;
        String key = this.getHashcode(accountSubType, attributes);
        chartOfAccountCacheMap = this.memcachedRepository.getFromCache(Key.allChartOfAccountList(tenantId), CacheMap.class);

        ChartOfAccount chartOfAccount = null;
        if (chartOfAccountCacheMap != null) {
            chartOfAccount = chartOfAccountCacheMap.getValue(key);
        }
        return chartOfAccount;
    }
    /**
     * Checks if reclassification is required by comparing two attribute values.
     *
     * @param value1 The previous attribute value.
     * @param value2 The current attribute value.
     * @return True if reclassification is required, otherwise false.
     */
    private boolean checkReclass(Object value1, Object value2) {
        return (value1 == null && value2 != null) ||
                (value1 != null && value2 == null) ||
                (value1 != null && !value1.equals(value2));
    }

    private String getHashcode(String key, Map<String, Object> attributeMap) {
        HashSet<String> set = new HashSet<>();
        set.add(key);
        for(Map.Entry<String, Object> attribute : attributeMap.entrySet()) {
            set.add(attribute.getKey() + attribute.getValue());
        }
        return String.valueOf(set.hashCode());
    }


    public void processReclass(AccountingPeriodCloseMessageRecord accountingPeriodCloseMessage) {
        String tenantId = accountingPeriodCloseMessage.tenantId();

        for(Batch batch : accountingPeriodCloseMessage.batches()) {
            processReclass(tenantId, batch.getId());
        }
    }


    public void processReclass(String tenantId, long batchId) {
        int skip = 0;
        List<ReclassValues> allReclassValues = new ArrayList<>();
        List<ReclassValues> chunk;

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize); // Adjust the number of threads as needed
        List<Future<?>> futures = new ArrayList<>();

        do {
            Query query = new Query(Criteria.where("batchId").is(batchId))
                    .skip(skip)
                    .limit(chunkSize);

            chunk = this.dataService.fetchData(query, ReclassValues.class);
            if (!chunk.isEmpty()) {
                // Create a new list for the current chunk to ensure it's effectively final
                List<ReclassValues> currentChunk = new ArrayList<>(chunk);
                // Submit the current chunk for processing in a separate thread
                futures.add(executorService.submit(() -> processChunk(tenantId, currentChunk)));
                allReclassValues.addAll(currentChunk);
            }
            skip += chunkSize;

        } while (!chunk.isEmpty());

        // Wait for all tasks to complete
        for (Future<?> future : futures) {
            try {
                future.get(); // This will block until the task is complete
            } catch (Exception e) {
                // Handle exceptions as needed
                e.printStackTrace();
            }
        }

        executorService.shutdown(); // Shutdown the executor service

        // Optionally, process all reclass values after fetching
        processAllReclassValues(allReclassValues);
    }

    private void processChunk(String tenantId, List<ReclassValues> chunk) {
        try {
            this.generalLedgerAccountBalanceService.setTenantId(tenantId);
            // Implement your logic to process each chunk here
            Set<GeneralLedgerEntery> gleList = new HashSet<>(0);
            Set<GeneralLedgerAccountBalance> balanceSet = new HashSet<>(0);

            for (ReclassValues reclassValues : chunk) {
                int subCode = generateSubCode(reclassValues);
                Collection<GeneralLedgerAccountBalance> balances = this.generalLedgerAccountBalanceService.getBalance(subCode, reclassValues.getCurrentPeriodId());

                for (GeneralLedgerAccountBalance balance : balances) {
                    Map<EntryType, SubledgerMapping> mapping = this.getSubledgerMapping(tenantId, balance.getTransactionName(), balance.getAmount());

                    for (Map.Entry<EntryType, SubledgerMapping> entry : mapping.entrySet()) {
                        EntryType entryType = entry.getKey();
                        SubledgerMapping subledgerMapping = entry.getValue();
                        AccountTypes accountType = this.getAccountType(tenantId, subledgerMapping.getAccountSubType());
                        if (accountType.getAccountType() != AccountType.BALANCESHEET) {
                            continue;
                        }
                        String accountSubType = subledgerMapping.getAccountSubType();
                        Map<String, Object> attributes = reclassValues.getAttributes();
                        ChartOfAccount chartOfAccount = this.getChartOfAccount(tenantId, accountSubType, attributes);


                        GeneralLedgerEntery gle = GeneralLedgerEntery.builder()
                                .attributeId(balance.getAttributeId())
                                .instrumentId(balance.getInstrumentId())
                                .transactionName(balance.getTransactionName())
                                .transactionDate(reclassValues.getEffectiveDate())
                                .periodId(reclassValues.getCurrentPeriodId())
                                .glAccountNumber(balance.getAccountNumber())
                                .glAccountName(balance.getAccountName())
                                .glAccountSubType(balance.getAccountSubtype())
                                .glAccountType(accountType.getAccountType().name())
                                .isReclass(0)
                                .debitAmount(0.0d)
                                .creditAmount(balance.getAmount() * -1)
                                .attributes(attributes)
                                .build();

                        GeneralLedgerAccountBalance accountBalance =
                                GeneralLedgerAccountBalance.builder()
                                        .accountType(AccountType.BALANCESHEET)
                                        .attributeId(balance.getAttributeId())
                                        .instrumentId(balance.getInstrumentId())
                                        .periodId(reclassValues.getCurrentPeriodId())
                                        .amount(balance.getAmount())
                                        .transactionName(balance.getTransactionName())
                                        .accountNumber(balance.getAccountNumber())
                                        .accountName(balance.getAccountName())
                                        .accountSubtype(balance.getAccountSubtype())
                                        .build();
                        accountBalance.setCode(accountBalance.hashCode());
                        accountBalance.setSubCode(accountBalance.subCode());
                        balanceSet.add(accountBalance);
                        GeneralLedgerEntery glereclass = GeneralLedgerEntery.builder()
                                .attributeId(balance.getAttributeId())
                                .instrumentId(balance.getInstrumentId())
                                .transactionName(balance.getTransactionName())
                                .transactionDate(reclassValues.getEffectiveDate())
                                .periodId(reclassValues.getCurrentPeriodId())
                                .glAccountNumber(chartOfAccount.getAccountNumber())
                                .glAccountName(chartOfAccount.getAccountName())
                                .glAccountSubType(chartOfAccount.getAccountSubtype())
                                .glAccountType(accountType.getAccountType().name())
                                .isReclass(1)
                                .debitAmount(balance.getAmount())
                                .creditAmount(0.0d)
                                .attributes(attributes)
                                .build();

                        gleList.add(gle);
                        gleList.add(glereclass);
                    }
                }

            }
            this.dataService.saveAll(gleList, tenantId, GeneralLedgerEntery.class);
            this.dataService.saveAll(balanceSet, tenantId, GeneralLedgerAccountBalance.class);
        }catch(Exception exp) {
            log.error(exp.getLocalizedMessage());
            throw exp;
        }
    }

    private int generateSubCode(ReclassValues reclassValues) {
        Set<String> hashcode = new HashSet<String>(0);
        hashcode.add(AccountType.BALANCESHEET.name());
        hashcode.add(reclassValues.getInstrumentId());
        hashcode.add(reclassValues.getAttributeId());
        return hashcode.hashCode();
    }
    private void processAllReclassValues(List<ReclassValues> allReclassValues) {
        // Implement your logic to process all reclass values here
    }
}
