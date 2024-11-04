package com.fyntrac.gl.service;

import com.fyntrac.common.dto.record.Records;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fyntrac.common.service.DataService;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AttributeService;
import com.fyntrac.common.entity.Attributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.entity.ReclassValues;

@Service
@Slf4j
public class GeneralLedgerReclassService extends BaseGeneralLedgerService{

    DataService dataService;
    MemcachedRepository memcachedRepository;
    AttributeService attributeService;
    Collection<Attributes> reclassAttributes;
    @Autowired
    public GeneralLedgerReclassService(DataService dataService
            , MemcachedRepository memcachedRepository
            , AttributeService attributeService) {
        this.dataService = dataService;
        this.memcachedRepository = memcachedRepository;
        this.attributeService = attributeService;
        this.reclassAttributes = new ArrayList<>(0);
    }

    @Override
    protected void initialize(Map<String, Object> executionContext) {
        String tenantId = (String) executionContext.get("tenantId");
        reclassAttributes = this.attributeService.getReclassableAttributes(tenantId);
    }

    @Override
    protected void perform(Map<String, Object> executionContext) {
        String tenantId = (String) executionContext.get("tenantId");
        String dataKey = (String) executionContext.get("dataKey");

        CacheList<Records.InstrumentAttributeReclassMessageRecord>  reclassMessages = this.memcachedRepository.getFromCache(dataKey, CacheList.class);
        for(Records.InstrumentAttributeReclassMessageRecord messageRecord : reclassMessages.getList()) {
            ReclassValues reclassValues = compareAttributeValues(messageRecord.previousInstrumentAttribute(), messageRecord.currentInstrumentAttribute());
            if(reclassValues != null) {
                this.dataService.save(reclassValues, tenantId);
            }
        }
    }

    private ReclassValues compareAttributeValues(Records.InstrumentAttributeRecord previous, Records.InstrumentAttributeRecord current) {
        Map<String, Object> previousAttributes = previous.attributes();
        Map<String, Object> currentAttributes = current.attributes();
        boolean isReclass = Boolean.FALSE;
        ReclassValues reclassValues = null;

        for (Attributes attribute : this.reclassAttributes) {
            // Check if isReclassable is TRUE (non-zero)
            if (attribute.getIsReclassable() == 1) { // Assuming isReclassable is 1 for TRUE
                String attributeName = attribute.getAttributeName();
                Object value1 = previousAttributes.get(attributeName);
                Object value2 = currentAttributes.get(attributeName);
                isReclass = checkReclass(value1, value2);

                if(isReclass) {
                    reclassValues = this.buildReclassValuesObject(attributeName
                            , value1, value2, previous, current);
                    break;
                }
            }
        }
        return reclassValues; // No differing values found
    }
    private ReclassValues buildReclassValuesObject(String attributeName, Object oldValue, Object newValue, Records.InstrumentAttributeRecord previous, Records.InstrumentAttributeRecord current) {
        return ReclassValues.builder().attributeId(previous.attributeId())
                .attributeName(attributeName)
                .instrumentId(previous.instrumentId())
                .previousPeriodId(previous.periodId())
                .previousVersionId(previous.versionId())
                .currentVersionId(current.versionId())
                .currentPeriodId(current.periodId())
                .oldValue(oldValue)
                .newValue(newValue).build();
    }

    private boolean checkReclass(Object value1, Object value2) {
        // Iterate through the keys of the first map
            // Check if the values are different
            if (value1 == null && value2 != null) {
                return true; // Value in map1 is null, but map2 has a value
            }
            if (value1 != null && value2 == null) {
                return true; // Value in map2 is null, but map1 has a value
            }
            if (value1 != null && !value1.equals(value2)) {
                return true; // Values are different
            }

        return false; // No differing values found
    }

    @Override
    protected void conclude(Map<String, Object> executionContext) throws ExecutionException, InterruptedException {
        String dataKey = (String) executionContext.get("dataKey");
        this.memcachedRepository.delete(dataKey);
    }
}
