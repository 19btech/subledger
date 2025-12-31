package com.fyntrac.common.service;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.CustomTableType;
import com.fyntrac.common.enums.EventStatus;
import com.fyntrac.common.enums.SourceType;
import com.fyntrac.common.enums.TriggerType;
import com.fyntrac.common.repository.*;
import com.fyntrac.common.service.aggregation.MetricLevelAggregationService;
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
    private final CustomTableDefinitionService customTableDefinitionService;
    private final InstrumentReplayStateService instrumentReplayStateService;
    private final MetricLevelAggregationService aggregationService;
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
                             CustomTableDefinitionService customTableDefinitionService,
                             InstrumentReplayStateService instrumentReplayStateService,
                             MetricLevelAggregationService aggregationService,
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
        this.customTableDefinitionService = customTableDefinitionService;
        this.instrumentReplayStateService = instrumentReplayStateService;
        this.aggregationService = aggregationService;
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

        // Correct Type: Outer Key = TableName, Inner Key = RowID, Value = RowData
        Map<String, Map<String, Map<String, Object>>> referenceDataValueMap = new HashMap<>();

        for (EventConfiguration cfg : configurationList) {
            // 1. Skip if trigger type doesn't match
            if (cfg.getTriggerSetup() == null ||
                    cfg.getTriggerSetup().getTriggerType() != TriggerType.ON_CUSTOM_DATA_TRIGGER) {
                continue;
            }

            // 2. Validate Source Mappings
            if (cfg.getSourceMappings() == null || cfg.getSourceMappings().isEmpty()) {
                continue;
            }

            SourceMapping mapping = cfg.getSourceMappings().get(0);
            String sourceTable = mapping.getSourceTable();

            // 3. Fetch Table Definition
            CustomTableDefinition tableDefinition = customTableDefinitionService.getCustomTableDefinition(sourceTable);
            if (tableDefinition == null) {
                continue;
            }

            // 4. Determine Reference Table Name
            String referenceTableName = null;
            if (tableDefinition.getTableType() == CustomTableType.OPERATIONAL) {
                referenceTableName = tableDefinition.getReferenceTable();
            } else if (tableDefinition.getTableType() == CustomTableType.REFERENCE) {
                referenceTableName = tableDefinition.getTableName();
            }

            // 5. Fetch Data if Reference Table is valid
            if (referenceTableName != null && !referenceTableName.isEmpty()) {

                // Check if we already fetched data for this table to avoid redundant DB calls
                // This lambda correctly returns Map<String, Map<String, Object>> matching the inner Map type
                Map<String, Map<String, Object>> tableDataRows =
                        referenceDataValueMap.computeIfAbsent(referenceTableName, k -> {

                            Map<String, Map<String, Object>> rows = new HashMap<>();
                            List<Document> documents = this.dataService.getMongoTemplate().findAll(Document.class, k);

                            for (Document doc : documents) {
                                // Safe check for _id
                                Object idObj = doc.get("_id");
                                if (idObj == null) continue;

                                String rowKey = idObj.toString();

                                // Create a mutable copy of the document data
                                Map<String, Object> values = new HashMap<>(doc);

                                // Add/Overwrite system fields
                                values.put("instrumentId", "system");
                                values.put("attributeId", "system");

                                try {
                                    Date dateVal = DateUtil.convertIntDateToUtc(postingDate);
                                    values.put("postingDate", dateVal);
                                    values.put("effectiveDate", dateVal);
                                } catch (ParseException e) {
                                    throw new RuntimeException("Error parsing date: " + postingDate, e);
                                }

                                rows.put(rowKey, values);
                            }
                            return rows;
                        });

                // 6. Build Event only if data exists
                if (!tableDataRows.isEmpty()) {

                    EventDetail detail = EventDetail.builder()
                            .sourceKey("System")
                            .sourceTable(referenceTableName)
                            .sourceType(SourceType.SYSTEM)
                            .isAscendingOrder(Boolean.FALSE)
                            .values(tableDataRows)
                            .build();

                    Event event = Event.builder()
                            .eventId(referenceTableName)
                            .eventName(referenceTableName)
                            .priority(0)
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
        }

        // Existing logic for other event configurations
        for (EventConfiguration cfg : configurationList) {

            Map<String, Map<String, Object>> valueMap = new HashMap<>();
            boolean isDescendingOrder = cfg.getTriggerSetup().getTriggerType() == TriggerType.ON_REPLAY;
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

            if (!valueMap.isEmpty()) {

                EventDetail detail = EventDetail.builder()
                        .sourceKey("System")
                        .sourceTable("System")
                        .sourceType(SourceType.SYSTEM)
                        .isAscendingOrder(isDescendingOrder)
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

    public void generateEvent(int postingDate) throws Exception{
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
            }case TriggerType.ON_REPLAY -> {

                InstrumentReplayState replayState =
                        this.instrumentReplayStateService.getInstrumentAttributeReplayState(instrumentId,
                                attributeId, postingDate);

                if(replayState == null){
                    return valueMap;
                }

                MetricLevelLtd ltd =  this.aggregationService.findLatestByPostingDate(replayState.getMinEffectiveDate());
                int replayDate = ltd.getPostingDate();
                List<Option> triggerSource = configuration.getTriggerSetup().getTriggerSource();

                List<String> dataMappingColumns = triggerSource.stream()
                        .map(Option::getValue)
                        .toList();

                List<TransactionActivity> activities = this.activityRepo.findActivityByTransactions(instrumentId,
                        attributeId,
                        replayDate,
                        dataMappingColumns);

                if (activities != null && !activities.isEmpty()) {
                    valueMap = getValuesFromReplay(currentInstrumentAttribute,postingDate, replayDate, effectiveDate,
                            activities, configuration);
                }
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
                                                                       EventConfiguration configuration) throws ParseException {
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

    public Map<String, Map<String, Object>> getValuesFromReplay(InstrumentAttribute currentInstrumentAttribute,
            Integer postingDate,
            Integer replayDate,
            Integer effectiveDate,
                                                                List<TransactionActivity> timeLineActivities,
            EventConfiguration configuration
    ) throws ParseException {

        Map<String, Map<String, Object>> valueMap = new LinkedHashMap<>();

        //check if qualify for replay

        String instrumentId = currentInstrumentAttribute.getInstrumentId();
        String attributeId = currentInstrumentAttribute.getAttributeId();



            Map<String, Object> tmpValueMap = new HashMap<>(0);
        for (SourceMapping sourceMapping : configuration.getSourceMappings()) {

            // 1. Validate inputs early
            if (sourceMapping == null) {
                return Collections.emptyMap();
            }

            String collectionName = sourceMapping.getSourceTable();
            if (collectionName == null || collectionName.trim().isEmpty()) {
                return Collections.emptyMap();
            }


            switch (sourceMapping.getSourceTable().toLowerCase()) {
                case "attribute" -> {
                    // handle attribute
                    tmpValueMap = getValuesFromInstrumentAttribute(replayDate,
                            effectiveDate,
                            currentInstrumentAttribute,
                            sourceMapping);

                }
                case "transactions" -> {
                    // handle transactions
                    List<String> dataMappingColumns = sourceMapping.getDataMapping().stream()
                            .map(Option::getValue)
                            .toList();

                    List<TransactionActivity> activities = this.activityRepo.findActivityByTransactions(instrumentId, attributeId,
                            replayDate,
                            dataMappingColumns);

                    valueMap.putAll(this.getReplayValuesFromTransactionActivity(instrumentId, attributeId,
                            postingDate, replayDate,
                            timeLineActivities, activities));
                    //return  valueMap;
                }
                case "balances" -> {
                    // handle balances
                    tmpValueMap = this.getValuesFromBalance(instrumentId, attributeId, replayDate, sourceMapping);

                }
                case "executionstate" -> {
                    // handle execution state
                    tmpValueMap = getExecutionStateValues();

                }
                default -> {
                    // optional: handle unexpected case
                    throw new IllegalArgumentException("Unknown source table: "
                            + sourceMapping.getSourceTable());
                }
            }
        }
        String sanitizedAttributeId = attributeId.replace('.', '_');
        String key = String.format("%s_%s_%d_%d", instrumentId, sanitizedAttributeId, postingDate, replayDate);
        if(valueMap.containsKey(key)) {
            Map<String, Object> vm = valueMap.get(key);

            for(Map.Entry<String, Object> entry : tmpValueMap.entrySet()){
                if(!vm.containsKey(entry.getKey())) {
                    vm.put(entry.getKey(), entry.getValue());
                }
            }

            valueMap.put(key, vm);
            return valueMap;
        }else if(!valueMap.isEmpty()) {
            Map<String, Object> vm = new HashMap<>(0);
            for(Map.Entry<String, Object> entry : tmpValueMap.entrySet()){
                if(!vm.containsKey(entry.getKey())) {
                    vm.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Map<String, Object>> tvm =
                    this.getExcelModelKeyMap(currentInstrumentAttribute.getInstrumentId(),
                    currentInstrumentAttribute.getAttributeId(),
                    postingDate, effectiveDate, vm);

            valueMap.putAll(tvm);
            return valueMap;
        }

        return this.getExcelModelKeyMap(currentInstrumentAttribute.getInstrumentId(),
                currentInstrumentAttribute.getAttributeId(),
                postingDate, effectiveDate, tmpValueMap);
    }

    public Map<String, Map<String, Object>> getValuesFromCustomTable(
            String instrumentId,
            String attributeId,
            Integer postingDate,
            Integer effectiveDate,
            SourceMapping mapping
    ) throws ParseException {

        // 1. Validate inputs early
        if (mapping == null) {
            return Collections.emptyMap();
        }

        String collectionName = mapping.getSourceTable();
        if (collectionName == null || collectionName.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        // 2. Fetch definition to get the correct Reference Column
        CustomTableDefinition customTableDefinition =
                customTableDefinitionService.getCustomTableDefinition(collectionName);

        if (customTableDefinition == null) {
            // Optional: Log warning or throw exception if definition is missing for a valid collection name
            return Collections.emptyMap();
        }

        String referenceColumn = customTableDefinition.getReferenceColumn();

        // 3. Build Projection List (Mandatory + Source Columns)
        Set<String> projectionColumns = new LinkedHashSet<>();
        projectionColumns.add("instrumentId");
        projectionColumns.add("attributeId");
        projectionColumns.add("postingDate");
        projectionColumns.add("effectiveDate");
        projectionColumns.add("periodId");
        projectionColumns.add("_id"); // Explicitly add _id as it is used for the key

        // Safely add dynamic source columns
        if (mapping.getSourceColumns() != null) {
            mapping.getSourceColumns().stream()
                    .filter(Objects::nonNull)
                    .map(Option::getValue)
                    .filter(Objects::nonNull)
                    .forEach(projectionColumns::add);
        }

        // 4. Build Query Criteria
        Criteria criteria = Criteria.where("instrumentId").is(instrumentId)
                .and("attributeId").is(attributeId)
                .and("postingDate").is(postingDate);

        // Apply 'IN' clause for Reference Column if data mapping values exist
        // Check for null to avoid NPE on mapping.getDataMapping()
        if (mapping.getDataMapping() != null && !mapping.getDataMapping().isEmpty()) {
            List<String> mappingValues = mapping.getDataMapping().stream()
                    .filter(Objects::nonNull)
                    .map(Option::getValue)
                    .filter(Objects::nonNull)
                    .toList();

            if (!mappingValues.isEmpty() && referenceColumn != null) {
                criteria.and(referenceColumn).in(mappingValues);
            }
        }

        Query query = new Query(criteria);

        // 5. Apply Sort
        query.with(Sort.by(Sort.Direction.DESC, "effectiveDate"));

        // 6. Apply Projection
        projectionColumns.forEach(field -> query.fields().include(field));

        // 7. Execute Query
        List<Document> documents = this.dataService
                .getMongoTemplate()
                .find(query, Document.class, collectionName);

        // 8. Transform Results
        Map<String, Map<String, Object>> resultMap = new LinkedHashMap<>();

        for (Document doc : documents) {
            // Safe extraction of ObjectId
            Object idObj = doc.get("_id");
            String key = (idObj != null) ? idObj.toString() : UUID.randomUUID().toString();

            Map<String, Object> valueMap = new HashMap<>();

            for (String field : doc.keySet()) {
                Object rawValue = doc.get(field);

                // Apply specific Date conversion logic
                if ("postingDate".equalsIgnoreCase(field) || "effectiveDate".equalsIgnoreCase(field)) {
                    if (rawValue instanceof Integer) {
                        valueMap.put(field, DateUtil.convertIntDateToUtc((Integer) rawValue));
                    } else {
                        // Fallback if data is corrupted or not an Integer
                        valueMap.put(field, rawValue);
                    }
                } else {
                    valueMap.put(field, rawValue);
                }
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
                postingDate, effectiveDate, valueMap);
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
            String label) { // Assuming effectiveDateKey is passed in or available

        if (attributes == null || attributeNames == null || version == null || label == null) {
            return Map.of();
        }

        String upperLabel = label.toUpperCase();
        String upperVersion = version.toUpperCase();
        Map<String, Object> result = new HashMap<>();

        for (Option opt : attributeNames) {
            String key = opt.getValue();

            // Skip if key is null or not in the attributes map
            if (key == null || !attributes.containsKey(key)) {
                continue;
            }

            String newKey = upperLabel + "_" + key + "_" + upperVersion;
            Object value = attributes.get(key);

            // Custom logic for EffectiveDate
            if ("EffectiveDate".equalsIgnoreCase(key)) {
                try {
                    Object intEffectiveDate = attributes.get(key);
                    // Assuming effectiveDateKey is the integer date you want to convert
                    if(intEffectiveDate instanceof Number) {
                        value = DateUtil.convertIntDateToUtc((Integer) intEffectiveDate);
                    }

                } catch (Exception e) {
                    // Log error or handle as needed, falling back to original value if conversion fails
                    System.err.println("Error converting date: " + e.getMessage());
                }
            }

            result.put(newKey, value);
        }

        return result;
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

    public Map<Integer, List<Records.TransactionActivityAmountRecord>> groupActivitiesByEffectiveDate(List<TransactionActivity> activities) {
        if (activities == null || activities.isEmpty()) {
            return Map.of();
        }

        return activities.stream()
                .collect(Collectors.groupingBy(
                        TransactionActivity::getEffectiveDate,
                        Collectors.collectingAndThen(
                                // 1. Group by TransactionName and Sum Amounts
                                Collectors.groupingBy(
                                        TransactionActivity::getTransactionName,
                                        Collectors.reducing(
                                                BigDecimal.ZERO,
                                                TransactionActivity::getAmount,
                                                BigDecimal::add
                                        )
                                ),
                                // 2. Transform the intermediate Map<String, BigDecimal> into List<Record>
                                map -> map.entrySet().stream()
                                        .map(e -> RecordFactory.createTransactionActivityAmountRecord(
                                                e.getKey(),    // transactionName
                                                null,          // effectiveDate (placeholder, will be set by outer collector)
                                                e.getValue()   // totalAmount
                                        ))
                                        .toList()
                        )
                ))
                .entrySet()
                .stream()
                // 3. Re-map to set the correct EffectiveDate from the key
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .map(r -> RecordFactory.createTransactionActivityAmountRecord(
                                        r.transactionName(),
                                        entry.getKey(), // Set actual effectiveDate here
                                        r.totalAmount()
                                ))
                                .toList()
                ));
    }

    public Map<String, Map<String, Object>> getReplayValuesFromTransactionActivity(String instrumentId,
                                                                             String attributeId,
                                                                             Integer postingDate,
                                                                             Integer replayDate,
                                                                                   List<TransactionActivity> timeLineActivities,
                                                                             List<TransactionActivity> activities) throws ParseException {
            Map<Integer, List<Records.TransactionActivityAmountRecord>> groupedByEffectiveDateActivities =
                    groupActivitiesByEffectiveDate(activities);

        Map<Integer, List<Records.TransactionActivityAmountRecord>> groupedByEffectiveDateTimeLines =
                groupActivitiesByEffectiveDate(timeLineActivities);

            Map<String, Map<String, Object>> valueMap = new HashMap<>(0);

        List<Integer> distinctEffectiveDates = timeLineActivities.stream()
                .map(TransactionActivity::getEffectiveDate) // Extract the effectiveDate
                .distinct()                                 // Keep only unique values
                .sorted()                                   // Optional: Sort the dates
                .toList();

            for (Integer effectiveDateKey : distinctEffectiveDates) {
                List<Records.TransactionActivityAmountRecord> groupedActivities = new ArrayList<>(0);
                if(groupedByEffectiveDateActivities.containsKey(effectiveDateKey)) {
                    groupedActivities =
                            groupedByEffectiveDateActivities.get(effectiveDateKey);
                }else{
                    groupedActivities =
                            groupedByEffectiveDateTimeLines.get(effectiveDateKey);
                }

                Map<String, Object> map = this.getAggregatedValues(groupedActivities);
                map.put("EffectiveDate", DateUtil.convertIntDateToUtc(effectiveDateKey));
                Map<String, Map<String, Object>> tmpValueMap = this.getExcelModelKeyMap(instrumentId, attributeId,
                        postingDate, effectiveDateKey, map);

                valueMap.putAll(tmpValueMap);
            }

            return valueMap;

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
        List<AttributeLevelLtd> newBalances = new ArrayList<>(0);

            for(AttributeLevelLtd agg : balances) {
                if(agg.getPostingDate() < postingDate) {
                    BaseLtd balance = BaseLtd.builder().beginningBalance(agg.getBalance().getEndingBalance())
                            .activity(BigDecimal.valueOf(0.0d))
                            .endingBalance(agg.getBalance().getEndingBalance()).build();
                    AttributeLevelLtd newBalance =
                            AttributeLevelLtd.builder().accountingPeriodId(agg.getAccountingPeriodId())
                                    .instrumentId(agg.getInstrumentId())
                                    .attributeId(agg.getAttributeId())
                                    .metricName(agg.getMetricName())
                                    .postingDate(postingDate)
                                    .balance(balance).build();
                    newBalances.add(newBalance);
                }
            }

            if(!newBalances.isEmpty()) {
                balances = newBalances;
            }

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