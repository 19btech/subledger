
package com.reserv.dataloader.service.model;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.entity.Event;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.EventRepository;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.ExcelModelService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.StringUtil;
import com.reserv.dataloader.pulsar.producer.ModelExecutionProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ModelExecutionService {

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    private final InstrumentAttributeService instrumentAttributeService;
    private final MemcachedRepository memcachedRepository;
    private final ModelExecutionProducer modelExecutionProducer;
    private final ExcelModelService excelModelService;
    private final EventRepository eventRepository;

    @Value("${fyntrac.chunk.size}")
    private int pageSize;

    @Autowired
    public ModelExecutionService(InstrumentAttributeService instrumentAttributeService
    , MemcachedRepository memcachedRepository
    , ModelExecutionProducer modelExecutionProducer
    , ExcelModelService excelModelService
    , EventRepository eventRepository) {
        this.instrumentAttributeService = instrumentAttributeService;
        this.memcachedRepository = memcachedRepository;
        this.modelExecutionProducer =modelExecutionProducer;
        this.excelModelService = excelModelService;
        this.eventRepository = eventRepository;
    }

    public void sendModelExecutionMessage(String date) throws Throwable {
        // Page request for chunk size
        int pageNumber = 0;
        boolean hasMoreData = true;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy"); // Define the format

        Date executionDate = DateUtil.parseDate(date, formatter);
        // Loop to fetch and process in chunks
        while (hasMoreData) {

            // Fetch the chunk of data
            List<InstrumentAttribute> chunk = this.instrumentAttributeService.getDistinctInstrumentsByInstrumentId(null, pageNumber, chunkSize);

            Set<String> instrumentIdChunk = new HashSet<>(0);
            for(InstrumentAttribute instrumentAttribute: chunk) {
                instrumentIdChunk.add(instrumentAttribute.getInstrumentId());
            }

            if (!chunk.isEmpty()) {
                // Process this chunk, e.g., send it to your REST API
                //send message to consumen
                boolean isLastPage = Boolean.FALSE;
                this.postModelExecutionMessage(executionDate, new ArrayList<>(instrumentIdChunk), pageNumber, isLastPage);
                pageNumber++;  // Move to the next page
            } else {
                // No more data to fetch
                hasMoreData = false;
            }
        }
    }


    public void sendExcelModelExecutionMessage(String date) throws Throwable {
        final int pageSize = this.pageSize;
        final String tenant = TenantContextHolder.getTenant();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        Date executionDate = DateUtil.parseDate(date, formatter);
        int postingDateNumber = DateUtil.dateInNumber(executionDate);

        // Determine total pages first
        Page<Event> firstPage = TenantContextHolder.runWithTenant(tenant,
                () -> this.eventRepository.findInstrumentIdsByPostingDateAndStatusNotStarted(
                        postingDateNumber, PageRequest.of(0, 1)) // Just to get count
        );

        if (firstPage.getTotalElements() == 0) {
            log.info("No events found for posting date {} and tenant {}", postingDateNumber, tenant);
            return;
        }

        int totalPages = (int) Math.ceil((double) firstPage.getTotalElements() / pageSize);

        // Use virtual thread executor with semaphore for rate limiting
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Semaphore semaphore = new Semaphore(10); // Limit concurrent virtual threads

            List<CompletableFuture<Boolean>> futures = new ArrayList<>();

            for (int pageNumber = 0; pageNumber < totalPages; pageNumber++) {
                final int currentPage = pageNumber;

                CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        semaphore.acquire(); // Rate limiting
                        return TenantContextHolder.runWithTenant(tenant, () -> {
                            try {
                                Page<Event> page = this.eventRepository.findInstrumentIdsByPostingDateAndStatusNotStarted(
                                        postingDateNumber, PageRequest.of(currentPage, pageSize)
                                );

                                List<Event> events = page.getContent();
                                if (!events.isEmpty()) {
                                    Set<String> instrumentIdChunk = events.stream()
                                            .map(Event::getInstrumentId)
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toSet());
                                    this.postModelExecutionMessage(executionDate, new ArrayList<>(instrumentIdChunk),
                                            currentPage, (currentPage  == totalPages - 1));
                                    log.debug("Successfully processed page {}/{}", currentPage + 1, totalPages);
                                    return true;
                                }
                                return false;
                            } catch (Exception e) {
                                log.error("Failed to process page {}: {}", currentPage, e.getMessage());
                                return false;
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Thread interrupted for page {}", currentPage);
                        return false;
                    }
                }, executor);

                futures.add(future);
            }

            // Wait for completion and check results
            List<Boolean> results = futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            long successCount = results.stream().filter(Boolean::booleanValue).count();
            log.info("Completed processing: {}/{} pages successful for tenant {}",
                    successCount, totalPages, tenant);

        } catch (Exception ex) {
            log.error("Event generation failed for tenant {}", tenant, ex);
            throw new RuntimeException("Event generation failed for tenant " + tenant, ex);
        }
    }

    private void postModelExecutionMessage(Date executionDate, List<String> instruments, int page, boolean isLast) {
        CacheList<String> cacheList = new CacheList<>();
        instruments.forEach(cacheList::add);
        int hashCode = Objects.hash(cacheList);
        String tenantId = TenantContextHolder.getTenant();
        String key = "Model" + tenantId + hashCode;
        this.memcachedRepository.putInCache(key, cacheList);
        this.modelExecutionProducer.sendModelExecutionMessage(RecordFactory.createModelExecutionMessage(tenantId,
                DateUtil.dateInNumber(executionDate), key, isLast));
        // Collect into CacheList

    }

    public void executeMode(int executionDate) throws Throwable{
        try {
            excelModelService.generateEvent(executionDate);
           } catch (Throwable e) {
            // log.error(StringUtil.getStackTrace(e));
            throw  new RuntimeException(e.getLocalizedMessage());
        }
    }


}
