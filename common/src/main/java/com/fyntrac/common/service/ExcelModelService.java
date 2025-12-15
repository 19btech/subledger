package com.fyntrac.common.service;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.EventStatus;
import com.fyntrac.common.enums.SourceType;
import com.fyntrac.common.enums.TriggerType;
import com.fyntrac.common.repository.*;
import com.fyntrac.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ExcelModelService {

    private final InstrumentAttributeRepository instrumentRepo;
    private final TransactionActivityRepository activityRepo;
    private final AttributeLevelBalanceRepository balanceRepo;
    private final EventConfigurationRepository eventConfigurationRepo;
    private final InstrumentAttributeService instrumentAttributeService;
    private final ExecutionStateService executionStateService;
    private final EventRepository eventRepository;
    private final DataService<?> dataService;
    private List<TransactionActivity> transactionActivities;
    private List<InstrumentAttribute> instrumentAttributes;

    @Value("${fyntrac.chunk.size}")
    private int pageSize;

    @Autowired
    public ExcelModelService(InstrumentAttributeRepository instrumentRepo,
                             TransactionActivityRepository activityRepo,
                             AttributeLevelBalanceRepository balanceRepo,
                             EventConfigurationRepository eventConfigurationRepo,
                             InstrumentAttributeService instrumentAttributeService,
                             ExecutionStateService executionStateService,
                             EventRepository eventRepository,
                             DataService<?> dataService
    ) {
        this.instrumentRepo = instrumentRepo;
        this.activityRepo = activityRepo;
        this.balanceRepo = balanceRepo;
        this.eventConfigurationRepo = eventConfigurationRepo;
        this.instrumentAttributeService = instrumentAttributeService;
        this.executionStateService = executionStateService;
        this.eventRepository = eventRepository;
        this.dataService = dataService;
        transactionActivities = new ArrayList<>(0);
        instrumentAttributes = new ArrayList<>(0);
    }

    private List<Event> processInstrumentGroup(
            String instrumentId,
            List<InstrumentAttribute> attributes,
            int postingDate) throws ParseException {

        String tenant = TenantContextHolder.getTenant();

        // Load configs under tenant
        List<EventConfiguration> configurationList =
                TenantContextHolder.runWithTenant(tenant,
                        () -> eventConfigurationRepo.findByIsActiveOrderByPriorityAsc(true)
                );

        List<Event> result = new ArrayList<>();

        for (EventConfiguration cfg : configurationList) {

            Map<String, Map<String, Object>> valueMap = new HashMap<>();

            for (InstrumentAttribute attr : attributes) {

                Map<String, Map<String, Object>> configVals =
                        generateEvent(
                                attr,
                                attr.getPostingDate(),
                                postingDate,
                                DateUtil.dateInNumber(attr.getEffectiveDate()),
                                cfg
                        );

                valueMap.putAll(configVals);
            }

            if(!valueMap.isEmpty()) {
                EventDetail detail = EventDetail.builder()
                        .sourceKey("System")
                        .sourceTable("System")
                        .sourceType(SourceType.SYSTEM)
                        .values(valueMap)
                        .build();

                Event event = Event.builder()
                        .eventId(cfg.getEventId())
                        .eventName(cfg.getEventName())
                        .priority(cfg.getPriority())
                        .instrumentId(instrumentId)
                        .postingDate(postingDate)
                        .effectiveDate(postingDate)
                        .lastPlayedPostingDate(postingDate)
                        .status(EventStatus.NOT_STARTED)
                        .eventDetail(detail)
                        .build();

                result.add(event);
            }
        }

        return result;
    }

    public void generateEvent(int postingDate) {
        final String tenant = TenantContextHolder.getTenant();

        int pageNumber = 0;
        final int pageSize = this.pageSize;
        Page<InstrumentAttribute> page;

        // Use virtual thread executor
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            do {
                final int currentPage = pageNumber;

                // Fetch current page under tenant
                page = TenantContextHolder.runWithTenant(tenant,
                        () -> instrumentRepo.findAllByEndDateIsNull(PageRequest.of(currentPage, pageSize))
                );

                List<InstrumentAttribute> attributes = page.getContent();

                if (attributes.isEmpty()) {
                    break;
                }

                // Group by instrumentId
                Map<String, List<InstrumentAttribute>> groups = attributes.stream()
                        .collect(Collectors.groupingBy(InstrumentAttribute::getInstrumentId));

                // Process each group in virtual threads with tenant context
                List<CompletableFuture<List<Event>>> futures = groups.entrySet().stream()
                        .map(entry -> CompletableFuture.supplyAsync(() ->
                                        TenantContextHolder.runWithTenant(tenant, () ->
                                                {
                                                    try {
                                                        return processInstrumentGroup(entry.getKey(), entry.getValue(), postingDate);
                                                    } catch (ParseException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                        ), executor)
                                .exceptionally(ex -> {
                                    log.error("Failed to process instrument group {} for tenant {}",
                                            entry.getKey(), tenant, ex);
                                    return Collections.emptyList();
                                }))
                        .toList();

                // Wait for all completions
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                        futures.toArray(new CompletableFuture[0])
                );

                // Combine results with timeout
                List<Event> pageEvents = allFutures
                        .orTimeout(30, TimeUnit.MINUTES)
                        .thenApply(v -> futures.stream()
                                .map(CompletableFuture::join)
                                .flatMap(List::stream)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()))
                        .join();

                // Save events
                if (!pageEvents.isEmpty()) {
                    TenantContextHolder.runWithTenant(tenant,
                            () -> {
                                eventRepository.saveAll(pageEvents);
                                return null;
                            }
                    );
                    log.info("Saved {} events for tenant {} page {}",
                            pageEvents.size(), tenant, currentPage);
                }

                pageNumber++;

            } while (page.hasNext());

        } catch (Exception ex) {
            log.error("Event generation failed for tenant {}", tenant, ex);
            throw new RuntimeException("Event generation failed for tenant " + tenant, ex);
        }
    }

    public Map<String, Map<String, Object>> generateEvent(InstrumentAttribute currentInstrumentAttribute,
            int attributePostingDate, int postingDate, int effectiveDate, EventConfiguration configuration) throws ParseException {
        Integer eventEffectiveDate = postingDate;
        Integer eventPostingDate = postingDate;

        Map<String, Map<String, Object>> valueMap = new HashMap<>(0);
        String instrumentId = currentInstrumentAttribute.getInstrumentId();
        String attributeId = currentInstrumentAttribute.getAttributeId();

        switch (configuration.getTriggerSetup().getTriggerType()) {
            case TriggerType.ON_MODEL_EXECUTION -> {
                // logic for ON_MODEL_EXECUTION
                valueMap = generateSourceMappingEventDetails(currentInstrumentAttribute, attributePostingDate,
                        postingDate, postingDate,
                        configuration);
            }
            case TriggerType.ON_INSTRUMENT_ADD -> {
                // logic for ON_ATTRIBUTE_ADD
                InstrumentAttribute currentOpenVersion =
                        this.instrumentRepo.findByInstrumentIdAndPostingDate(instrumentId, attributeId,
                                postingDate);
                InstrumentAttribute lastOpenVersion = null;

                if (currentOpenVersion != null) {
                    lastOpenVersion =
                            this.instrumentRepo.findByVersionId(currentOpenVersion.getPreviousVersionId());
                    if(lastOpenVersion == null) {
                        valueMap = generateSourceMappingEventDetails(currentInstrumentAttribute, attributePostingDate
                                , postingDate,
                                effectiveDate,
                                configuration);
                    } else if (currentOpenVersion.getAttributes().size() > lastOpenVersion.getAttributes().size()) {
                            valueMap = generateSourceMappingEventDetails(currentInstrumentAttribute, attributePostingDate, postingDate,
                                    effectiveDate,
                                    configuration);
                                          }
                }
            }
            case TriggerType.ON_ATTRIBUTE_CHANGE -> {
                // logic for ON_ATTRIBUTE_CHANGE
                InstrumentAttribute lastOpenVersion = null;

                lastOpenVersion =
                        this.instrumentRepo.findByVersionId(currentInstrumentAttribute.getPreviousVersionId());
                if (lastOpenVersion != null) {
                    boolean hasChanged = this.hasChanged(lastOpenVersion.getAttributes(),
                            currentInstrumentAttribute.getAttributes());
                    if (hasChanged) {
                        valueMap = generateSourceMappingEventDetails(currentInstrumentAttribute, attributePostingDate, postingDate,
                                effectiveDate,
                                configuration);
                    }
                }
            }
            case TriggerType.ON_TRANSACTION_POST -> {
                // logic for ON_TRANSACTION_POST
                List<Option> triggerSource = configuration.getTriggerSetup().getTriggerSource();

                List<String> dataMappingColumns = triggerSource.stream()
                        .map(Option::getValue)
                        .toList();

                List<TransactionActivity> activities = this.activityRepo.findActiveByTransactions(instrumentId, attributeId,
                        postingDate,
                        dataMappingColumns);

                if (activities != null && !activities.isEmpty()) {
                    valueMap = generateSourceMappingEventDetails(currentInstrumentAttribute, attributePostingDate, postingDate,
                            effectiveDate,
                            configuration);
                }



            }
            case TriggerType.ON_CUSTOM_DATA_TRIGGER -> {
                valueMap = generateSourceMappingEventDetails(instrumentId, attributeId,postingDate, effectiveDate, configuration);
            }
            default -> {
                // optional: handle unexpected value
                throw new IllegalArgumentException("Unknown trigger type: " + configuration.getTriggerSetup());
            }
        }

        return valueMap;
    }

    Map<String, Map<String, Object>> generateSourceMappingEventDetails(String instrumentId, String attributeId,
                                                                       int postingDate,
                                                                       int effectiveDate,
                                                                       EventConfiguration configuration){
        Map<String, Map<String, Object>> valueMap = new HashMap<>(0);
        //Custom Table event generation
        for (SourceMapping sourceMapping : configuration.getSourceMappings()) {


            Map<String, Map<String, Object>> tmpValueMap = getValuesFromCustomTable(
                    instrumentId,
                    attributeId,
                    postingDate,
                    effectiveDate,
                    sourceMapping
            );
            valueMap.putAll(tmpValueMap);
        }
        return valueMap;
    }

    public Map<String, Map<String, Object>> getValuesFromCustomTable(String instrumentId,
            String attributeId,
            Integer postingDate,
            Integer effectiveDate,
            SourceMapping mapping
    ) {

        Map<String, Map<String, Object>> resultMap = new LinkedHashMap<>();

        // ✅ Mandatory columns
        List<String> mandatoryColumns = List.of(
                "instrumentId",
                "attributeId",
                "postingDate",
                "effectiveDate",
                "periodId"
        );

        // ✅ Projection columns (mandatory + dynamic)
        List<String> projectionColumns = new ArrayList<>(mandatoryColumns);

        List<String> columns = Optional.ofNullable(mapping.getSourceColumns())
                .orElse(List.of())
                .stream()
                .map(Option::getValue)   // adjust if needed
                .filter(Objects::nonNull)
                .toList();

        projectionColumns.addAll(columns);

        String collectionName = mapping.getSourceTable();

        // ✅ Build query
        Query query = new Query();

        query.addCriteria(
                Criteria.where("instrumentId").is(instrumentId).and("attributeId").is(attributeId).and("postingDate").is(postingDate)
                        .and("effectiveDate").lte(effectiveDate)
        );

        // ✅ ORDER BY effectiveDate DESC
        query.with(Sort.by(Sort.Direction.DESC, "effectiveDate"));

        // ✅ Projection
        projectionColumns.forEach(query.fields()::include);
        query.fields().include("_id");

        // ✅ Fetch MULTIPLE records
        List<Document> documents = this.dataService
                .getMongoTemplate()
                .find(query, Document.class, collectionName);

        // ✅ Convert to Map<String, Map<String, Object>>
        for (Document doc : documents) {

            String key = doc.getObjectId("_id").toHexString(); // ✅ OUTER MAP KEY

            Map<String, Object> valueMap = new HashMap<>();
            for (String field : doc.keySet()) {
                valueMap.put(field, doc.get(field));
            }

            resultMap.put(key, valueMap);
        }

        return resultMap;
    }


    public Event generateEventFromModelExecution(String instrumentId,
                                                 String attributeId,
                                                 int postingDate,
                                                 EventConfiguration configuration) {
        return null;
    }

    public Event generateEventFromAttributeAddition(String instrumentId,
                                                    String attributeId,
                                                    int postingDate,
                                                    EventConfiguration configuration) {
        return null;
    }

    public Event generateEventFromAttributeChange(String instrumentId,
                                                  String attributeId,
                                                  int postingDate,
                                                  EventConfiguration configuration) {
        return null;
    }


    public Map<String, Map<String, Object>> generateSourceMappingEventDetails(InstrumentAttribute currentInstrumentAttribute,
                                                                              int attributePostingDate,
                                                                              int postingDate,
                                                                              int effectiveDate,
                                                                              EventConfiguration configuration) throws ParseException {
        Map<String,  Object> valueMap = new HashMap<>(0);
        String instrumentId = currentInstrumentAttribute.getInstrumentId();
        String attributeId = currentInstrumentAttribute.getAttributeId();

        for (SourceMapping sourceMapping : configuration.getSourceMappings()) {
            Map<String, Object> tmpValueMap = new HashMap<>(0);
            switch (sourceMapping.getSourceTable().toLowerCase()) {
                case "attribute" -> {
                    // handle attribute
                    tmpValueMap = getValuesFromInstrumentAttribute(attributePostingDate,
                            effectiveDate,
                            currentInstrumentAttribute,
                            sourceMapping);
                    valueMap.putAll(tmpValueMap);
                }
                case "transactions" -> {
                    // handle transactions
                    List<String> dataMappingColumns = sourceMapping.getDataMapping().stream()
                            .map(Option::getValue)
                            .toList();

                    List<TransactionActivity> activities = this.activityRepo.findActiveByTransactions(instrumentId, attributeId,
                            postingDate,
                            dataMappingColumns);

                    return this.getValuesFromTransactionActivity(instrumentId, attributeId, postingDate ,
                            effectiveDate, activities, sourceMapping);

                }
                case "balances" -> {
                    // handle balances
                    tmpValueMap = this.getValuesFromBalance(instrumentId, attributeId, postingDate, sourceMapping);
                    valueMap.putAll(tmpValueMap);;
                }
                case "executionstate" -> {
                    // handle execution state
                    tmpValueMap = getExecutionStateValues();
                    valueMap.putAll(tmpValueMap);;
                }
                default -> {
                    // optional: handle unexpected case
                    throw new IllegalArgumentException("Unknown source table: "
                            + sourceMapping.getSourceTable());
                }
            }
        }
        return this.getExcelModelKeyMap(currentInstrumentAttribute.getInstrumentId(),
                currentInstrumentAttribute.getAttributeId(),
                postingDate, postingDate, valueMap);
    }

    public Map<String, Object> getValuesFromInstrumentAttribute(Integer postingDate,
                                                                             Integer effectiveDate,
                                                                             InstrumentAttribute currentInstrumentAttribute,
                                                                             SourceMapping mapping) throws ParseException {
        Map<String, Object> attributeList = new HashMap<>(0);

        for (Option version : mapping.getVersionType()) {
            InstrumentAttribute instrumentAttribute = getInstrumentAttribute(version.getValue(),
                    currentInstrumentAttribute);
            if (instrumentAttribute != null) {
                Map<String, Object> attributes = filterAttributes(instrumentAttribute.getAttributes(),
                        mapping.getSourceColumns(),
                        version.getValue(),
                        "Attribute");

                attributeList.putAll(attributes);
            }
        }
        return attributeList;
    }

    public InstrumentAttribute getInstrumentAttribute(String version, InstrumentAttribute currentInstrumentAttribute) {
        String instrumentId = currentInstrumentAttribute.getInstrumentId();
        String attributeId = currentInstrumentAttribute.getAttributeId();
        return switch (version.toLowerCase()) {
            case "current" -> {
                List<InstrumentAttribute> instruments =
                        instrumentRepo.findByAttributeIdAndInstrumentIdAndEndDateIsNull(attributeId, instrumentId);
                yield instruments != null && !instruments.isEmpty() ? instruments.getFirst() : null;
            }
            case "prior" -> {
                yield instrumentAttributeService.getInstrumentAttributeByVersionId(currentInstrumentAttribute.getPreviousVersionId());
            }
            case "first" -> {
                List<InstrumentAttribute> instruments =
                        instrumentRepo.findByAttributeIdAndInstrumentIdAndPreviousVersionIdIsZero(attributeId, instrumentId);
                yield instruments != null && !instruments.isEmpty() ? instruments.getFirst() : null;
            }
            default -> throw new IllegalArgumentException("Unknown version type: " + version);
        };
    }

    public Map<String, Object> filterAttributes(
            Map<String, Object> attributes,
            List<Option> attributeNames,
            String version,
            String label) {

        if (attributes == null || attributeNames == null || version == null || label == null) {
            return Map.of();
        }

        // Normalize version and label once
        String upperLabel = label.toUpperCase();
        String upperVersion = version.toUpperCase();

        return attributeNames.stream()
                .filter(opt -> opt.getValue() != null && attributes.containsKey(opt.getValue()))
                .collect(Collectors.toMap(
                        opt -> upperLabel + "_" + opt.getValue() + "_" + upperVersion, // Replace dots with underscores
                        opt -> attributes.get(opt.getValue())
                ));
    }

    public Map<String, Map<String, Object>> getValuesFromTransactionActivity(String instrumentId,
                                                                             String attributeId,
                                                                             Integer postingDate,
                                                                             Integer effectiveDate,
                                                                             List<TransactionActivity> activities,
                                                                             SourceMapping mapping) throws ParseException {
        List<String> dataMappingColumns = mapping.getDataMapping().stream()
                .map(Option::getValue)
                .toList();
        if (mapping.getFieldType().getDisplayName().equalsIgnoreCase("array")) {

            Map<Integer, List<TransactionActivity>> groupedByEffectiveDate = activities.stream()
                    .collect(Collectors.groupingBy(TransactionActivity::getEffectiveDate));

            Map<String, Map<String, Object>> valueMap = new HashMap<>(0);
            for (Integer effectiveDateKey : groupedByEffectiveDate.keySet()) {
                List<TransactionActivity> groupedByEffectiveDateActivities = groupedByEffectiveDate.get(effectiveDateKey);
                Map<String, Object> map = this.getValues(groupedByEffectiveDateActivities);
                if(!map.isEmpty()) {
                    map.put("EffectiveDate", DateUtil.convertIntDateToUtc(effectiveDateKey));
                    Map<String, Map<String, Object>> tmpValueMap = this.getExcelModelKeyMap(instrumentId, attributeId,
                            postingDate, effectiveDateKey, map);
                    valueMap.putAll(tmpValueMap);
                }
            }

            return valueMap;
        } else {
            Map<Integer, List<Records.TransactionActivityAmountRecord>> groupedByEffectiveDate =
                    activities.stream()
                            .collect(Collectors.groupingBy(
                                    TransactionActivity::getEffectiveDate,
                                    Collectors.collectingAndThen(
                                            Collectors.groupingBy(
                                                    TransactionActivity::getTransactionName,
                                                    Collectors.reducing(
                                                            BigDecimal.ZERO,
                                                            TransactionActivity::getAmount,
                                                            BigDecimal::add
                                                    )
                                            ),
                                            map -> map.entrySet().stream()
                                                    .map(e ->
                                                            RecordFactory.createTransactionActivityAmountRecord(
                                                                    e.getKey(),                 // transactionName
                                                                    null,                       // temporary
                                                                    e.getValue()                // totalAmount
                                                            )
                                                    )
                                                    .toList()
                                    )
                            ))
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> entry.getValue().stream()
                                            .map(r -> RecordFactory.createTransactionActivityAmountRecord(
                                                    r.transactionName(),
                                                    entry.getKey(),              // set effectiveDate here
                                                    r.totalAmount()
                                            ))
                                            .toList()
                            ));


            Map<String, Map<String, Object>> valueMap = new HashMap<>(0);
            for (Integer effectiveDateKey : groupedByEffectiveDate.keySet()) {
                List<Records.TransactionActivityAmountRecord> groupedActivities =
                        groupedByEffectiveDate.get(effectiveDateKey);
                Map<String, Object> map = this.getAggregatedValues(groupedActivities);
                map.put("EffectiveDate", DateUtil.convertIntDateToUtc(effectiveDateKey));
                Map<String, Map<String, Object>> tmpValueMap = this.getExcelModelKeyMap(instrumentId, attributeId,
                        postingDate, effectiveDateKey, map);

                valueMap.putAll(tmpValueMap);
            }

            return valueMap;
        }
    }

    public Map<String, Object> getValuesFromBalance(String instrumentId,
                                                                 String attributeId,
                                                                 Integer postingDate,
                                                                 SourceMapping mapping) throws ParseException {
        List<String> dataMappingColumns = mapping.getDataMapping().stream()
                .map(Option::getValue)
                .toList();

        List<AttributeLevelLtd> balances = this.balanceRepo.findLatestBalanceByMetrics(instrumentId, attributeId,
                postingDate,
                dataMappingColumns);

        Map<String, Object> valueMap = this.getBalanceValues(balances);
        valueMap.put("EffectiveDate", DateUtil.convertIntDateToUtc(postingDate));
        return valueMap;
    }

    public Map<String, Object> getValues(List<TransactionActivity> activities) throws ParseException {
        Map<String, Object> valueMap = new HashMap<>(0);
        for (TransactionActivity activity : activities) {
            String key = String.format("%s_%s_%s", "TRANSACTIONS", "AMOUNT", // Replace dots with underscores
                    activity.getTransactionName().toUpperCase());
            valueMap.put(key, activity.getAmount());
            valueMap.put("EffectiveDate", DateUtil.convertIntDateToUtc(activity.getEffectiveDate()));
        }
        return valueMap;
    }

    public Map<String, Object> getAggregatedValues(List<Records.TransactionActivityAmountRecord> activities) {
        Map<String, Object> valueMap = new HashMap<>(0);
        for (Records.TransactionActivityAmountRecord activity : activities) {
            String key = String.format("%s_%s_%s", "TRANSACTIONS", "AMOUNT", // Replace dots with underscores
                    activity.transactionName().toUpperCase());
            valueMap.put(key, activity.totalAmount().doubleValue());
        }
        return valueMap;
    }

    public Map<String, Object> getBalanceValues(List<AttributeLevelLtd> balances) {
        Map<String, Object> valueMap = new HashMap<>(0);
        for (AttributeLevelLtd balance : balances) {
            String beginningBalanceKey = String.format("%s_%s_%s", "BALANCES", "BEGINNINGBALANCE", // Replace dots with underscores
                    balance.getMetricName().toUpperCase());

            String activityKey = String.format("%s_%s_%s", "BALANCES", "ACTIVITY", // Replace dots with underscores
                    balance.getMetricName().toUpperCase());

            String endingBalanceKey = String.format("%s_%s_%s", "BALANCES", "ENDINGBALANCE", // Replace dots with underscores
                    balance.getMetricName().toUpperCase());

            valueMap.put(beginningBalanceKey, balance.getBalance().getBeginningBalance().doubleValue());
            valueMap.put(activityKey, balance.getBalance().getActivity().doubleValue());
            valueMap.put(endingBalanceKey, balance.getBalance().getEndingBalance().doubleValue());
        }
        return valueMap;
    }

    public Map<String, Object> getExecutionStateValues() throws ParseException {
        Map<String, Object> valueMap = new HashMap<>(0);
        ExecutionState executionState = this.executionStateService.getExecutionState();
        String executionDate = String.format("%s_%s", "EXECUTIONSTATE", "EXECUTIONDATE"); // Replace dots with underscores
        String lastExecutionDate = String.format("%s_%s", "EXECUTIONSTATE", "LASTEXECUTIONDATE"); // Replace dots with underscores

        valueMap.put(executionDate, executionState.getExecutionDate());
        valueMap.put(lastExecutionDate, executionState.getLastExecutionDate());
        return valueMap;
    }

    public Map<String, Map<String, Object>> getExcelModelKeyMap(
            String instrumentId,
            String attributeId,
            Integer postingDate,
            Integer effectiveDate,
            Map<String, Object> valueMap
    ) throws ParseException {
        valueMap.put("InstrumentId", instrumentId);
        valueMap.put("AttributeId", attributeId);
        valueMap.put("PostingDate", DateUtil.convertIntDateToUtc(postingDate));
        if(!valueMap.containsKey("EffectiveDate")) {
            valueMap.put("EffectiveDate", DateUtil.convertIntDateToUtc(effectiveDate));
        }


        // Create a simple string key with underscores instead of dots and pipes
        // Also sanitize the attributeId to replace dots with underscores
        String sanitizedAttributeId = attributeId.replace('.', '_');
        String key = String.format("%s_%s_%d_%d", instrumentId, sanitizedAttributeId, postingDate, effectiveDate);

        Map<String, Map<String, Object>> result = new HashMap<>();
        result.put(key, valueMap);
        return result;
    }

    public boolean hasChanged(Map<String, Object> oldMap, Map<String, Object> newMap) {
        if (oldMap == null && newMap == null) return false;
        if (oldMap == null || newMap == null) return true;

        // Quick size difference
        if (oldMap.size() != newMap.size()) return true;

        // Check for value differences
        for (var entry : oldMap.entrySet()) {
            Object newValue = newMap.get(entry.getKey());
            if (!Objects.equals(entry.getValue(), newValue)) {
                return true; // value changed
            }
        }

        // Check for new keys
        for (String key : newMap.keySet()) {
            if (!oldMap.containsKey(key)) {
                return true; // new key added
            }
        }

        return false; // identical
    }
}