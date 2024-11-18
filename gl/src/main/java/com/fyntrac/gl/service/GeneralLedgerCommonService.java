package com.fyntrac.gl.service;

import com.fyntrac.common.enums.EntryType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.entity.SubledgerMapping;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.utils.Key;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.fyntrac.common.cache.collection.CacheMap;
import com.fyntrac.common.enums.Sign;
import com.fyntrac.common.entity.AccountTypes;
import com.fyntrac.common.entity.ChartOfAccount;
import com.fyntrac.common.utils.StringUtil;

@Service
public class GeneralLedgerCommonService {

    DataService dataService;
    MemcachedRepository memcachedRepository;

    @Autowired
    GeneralLedgerCommonService(DataService<SubledgerMapping> dataService, MemcachedRepository memcachedRepository) {
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
    }

    public Map<EntryType, SubledgerMapping> getSubledgerMapping(String tenantId, TransactionActivity transactionActivity) {

        Sign sign = (transactionActivity.getAmount() > 0 ? com.fyntrac.common.enums.Sign.POSITIVE :  com.fyntrac.common.enums.Sign.NEGATIVE);


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
                + transactionActivity.getTransactionName()
                + EntryType.DEBIT
                + sign.getValue());

            String creditKey = StringUtil.convertToUpperCaseAndRemoveSpaces(tenantId
                + transactionActivity.getTransactionName()
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
}
