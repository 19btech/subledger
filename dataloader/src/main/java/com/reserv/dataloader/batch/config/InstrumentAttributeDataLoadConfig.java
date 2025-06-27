package com.reserv.dataloader.batch.config;

import com.fyntrac.common.component.InstrumentReplayQueue;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.InstrumentReplaySet;
import com.fyntrac.common.component.TenantDataSourceProvider;
import com.fyntrac.common.entity.InstrumentAttribute;
import com.fyntrac.common.repository.MemcachedRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.service.InstrumentAttributeService;
import com.reserv.dataloader.batch.listener.InstrumentAttributeJobCompletionListener;
import com.reserv.dataloader.batch.mapper.HeaderColumnNameMapper;
import com.reserv.dataloader.batch.processor.InstrumentAttributeItemProcessor;
import com.reserv.dataloader.batch.writer.InstrumentAttributeWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing(modular = true)
@Slf4j
public class InstrumentAttributeDataLoadConfig {
    private final JobRepository jobRepository;
    private final TenantDataSourceProvider dataSourceProvider;
    private MongoTemplate mongoTemplate;
    private MemcachedRepository memcachedRepository;
    private InstrumentAttributeService instrumentAttributeService;
    private AccountingPeriodService accountingPeriodService;
    private ExecutionStateService executionStateService;
    private final InstrumentReplaySet instrumentReplaySet;
    private final InstrumentReplayQueue instrumentReplayQueue;

    public InstrumentAttributeDataLoadConfig(JobRepository jobRepository, MongoTemplate mongoTemplate,
                                             TenantDataSourceProvider dataSourceProvider,
                                             MemcachedRepository memcachedRepository,
                                             InstrumentAttributeService instrumentAttributeService,
                                             AccountingPeriodService accountingPeriodService,
                                             ExecutionStateService executionStateService,
                                             InstrumentReplaySet instrumentReplaySet,
                                             InstrumentReplayQueue instrumentReplayQueue) {
        this.jobRepository = jobRepository;
        this.dataSourceProvider = dataSourceProvider;
        this.mongoTemplate = mongoTemplate;
        this.memcachedRepository = memcachedRepository;
        this.instrumentAttributeService = instrumentAttributeService;
        this.accountingPeriodService = accountingPeriodService;
        this.executionStateService = executionStateService;
        this.instrumentReplaySet = instrumentReplaySet;
        this.instrumentReplayQueue = instrumentReplayQueue;
    }

    @Bean("instrumentAttributeUploadJob")
    public Job instrumentAttributeUploadJob(InstrumentAttributeJobCompletionListener listener, Step instrumentAttributeImportStep) {
        return new JobBuilder("instrumentAttributeUploadJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(instrumentAttributeImportStep)
                .end()
                .build();
    }

    @Bean
    public Step instrumentAttributeImportStep() throws IOException {
        return new StepBuilder("instrumentAttributeImportStep", jobRepository)
                .<Map<String,Object>,InstrumentAttribute>chunk(10, new ResourcelessTransactionManager())
                .reader(genericReader(""))
                .processor(instrumentAttributeMapItemProcessor())
                .writer(instrumentAttributeWriter(dataSourceProvider
                        , this.memcachedRepository
                        , this.instrumentAttributeService
                        , this.accountingPeriodService
                        , this.executionStateService
                        , this.instrumentReplaySet
                , this.instrumentReplayQueue))
                .build();
    }

    @Bean
    public ItemProcessor<Map<String,Object>,InstrumentAttribute> instrumentAttributeMapItemProcessor() {
        return new InstrumentAttributeItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Map<String, Object>> genericReader(@Value("#{jobParameters[filePath]}") String filePath) throws IOException {


        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));
        reader.setLinesToSkip(1); // Skip the header

        // Read actual header line using Apache Commons CSV to get clean column names
        List<String> headerNames = getHeaderNames(filePath);

        // Setup line tokenizer
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setQuoteCharacter('"'); // Handles quoted fields correctly
        tokenizer.setStrict(false); // Allow rows with missing fields
        tokenizer.setNames(headerNames.toArray(new String[0]));

        // Line mapper
        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new HeaderColumnNameMapper());

        reader.setLineMapper(lineMapper);
        return reader;

    }

    private List<String> getHeaderNames(String filePath) throws IOException {
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath));
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))) {

            CSVRecord headerRecord = parser.iterator().next();
            List<String> headers = new ArrayList<>();
            for (String header : headerRecord) {
                headers.add(header.trim());
            }
            return headers;
        }
    }

    @Bean
    public ItemWriter<InstrumentAttribute> instrumentAttributeWriter(TenantDataSourceProvider dataSourceProvider,
                                                                     MemcachedRepository memcachedRepository
                                                            , InstrumentAttributeService instrumentAttributeService
                                                            , AccountingPeriodService accountingPeriodService
    , ExecutionStateService executionStateService
    , InstrumentReplaySet instrumentReplaySet, InstrumentReplayQueue instrumentReplayQueue
                                                                     ) {
        MongoItemWriter<InstrumentAttribute> delegate = new MongoItemWriterBuilder<InstrumentAttribute>()
                .template(mongoTemplate)
                .collection("InstrumentAttribute")
                .build();

        return new InstrumentAttributeWriter(delegate, dataSourceProvider,  memcachedRepository, instrumentAttributeService, accountingPeriodService, executionStateService, instrumentReplaySet, instrumentReplayQueue);
    }
}
