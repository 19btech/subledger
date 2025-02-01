
package com.reserv.dataloader.service.model;

import com.fyntrac.common.cache.collection.CacheList;
import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.fyntrac.common.utils.DateUtil;
import com.reserv.dataloader.pulsar.producer.ModelExecutionProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ModelExecutionService {

    @Value("${fyntrac.chunk.size}")
    private int chunkSize;

    private final InstrumentAttributeService instrumentAttributeService;
    private final MemcachedRepository memcachedRepository;
    private final ModelExecutionProducer modelExecutionProducer;
    @Autowired
    public ModelExecutionService(InstrumentAttributeService instrumentAttributeService
    , MemcachedRepository memcachedRepository
    , ModelExecutionProducer modelExecutionProducer) {
        this.instrumentAttributeService = instrumentAttributeService;
        this.memcachedRepository = memcachedRepository;
        this.modelExecutionProducer =modelExecutionProducer;
    }

    public void sendModelExecutionMessage(String date) throws Throwable {
        // Page request for chunk size
        int pageNumber = 0;
        boolean hasMoreData = true;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy"); // Define the format

        Date executionDate = DateUtil.parseDate(date, formatter);
        // Loop to fetch and process in chunks
        while (hasMoreData) {

            // Fetch the chunk of data
            List<InstrumentAttribute> chunk = this.instrumentAttributeService.getInstruments(null, pageNumber, chunkSize);

            if (!chunk.isEmpty()) {
                // Process this chunk, e.g., send it to your REST API
                //send message to consumen
                this.postModelExecutionMessage(executionDate, chunk, pageNumber);
                pageNumber++;  // Move to the next page
            } else {
                // No more data to fetch
                hasMoreData = false;
            }
        }
    }

    private void postModelExecutionMessage(Date executionDate, List<InstrumentAttribute> instruments, int page) {
        CacheList<InstrumentAttribute> cacheList = new CacheList<>();
        instruments.forEach(cacheList::add);
        int hashCode = Objects.hash(cacheList);
        String tenantId = TenantContextHolder.getTenant();
        String key = "Model" + tenantId + hashCode;
        this.memcachedRepository.putInCache(key, cacheList);
        this.modelExecutionProducer.sendModelExecutionMessage(RecordFactory.createCommonMessage(tenantId, executionDate, key));
        // Collect into CacheList

    }
}
