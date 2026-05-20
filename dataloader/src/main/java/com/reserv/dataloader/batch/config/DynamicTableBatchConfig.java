package com.reserv.dataloader.batch.config;

import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.repository.InstrumentAttributeRepository;
import com.fyntrac.common.repository.RefDataValidationLogRepository;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.batch.listener.ValidationLoggingListener;
import com.reserv.dataloader.batch.processor.DynamicDataProcessor;
import com.reserv.dataloader.batch.writer.DynamicMongoWriter;
import com.reserv.dataloader.validation.DynamicTableValidator;
import org.bson.Document;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
public class DynamicTableBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final MongoTemplate mongoTemplate;
    private final CustomTableDefinitionRepository tableDefRepository;
    private final InstrumentAttributeRepository instrumentAttributeRepository;
    private final RefDataValidationLogRepository validationLogRepository;
    private final DynamicTableValidator dynamicTableValidator;
    private final ValidationLoggingListener validationLoggingListener;

    public DynamicTableBatchConfig(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   MongoTemplate mongoTemplate,
                                   CustomTableDefinitionRepository tableDefRepository,
                                   InstrumentAttributeRepository instrumentAttributeRepository,
                                   RefDataValidationLogRepository validationLogRepository,
                                   DynamicTableValidator dynamicTableValidator,
                                   ValidationLoggingListener validationLoggingListener) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.mongoTemplate = mongoTemplate;
        this.tableDefRepository = tableDefRepository;
        this.instrumentAttributeRepository = instrumentAttributeRepository;
        this.validationLogRepository = validationLogRepository;
        this.dynamicTableValidator = dynamicTableValidator;
        this.validationLoggingListener = validationLoggingListener;
    }

    // ------------------------------------------------------------------
    // Job
    // ------------------------------------------------------------------

    @Bean
    public Job dynamicLoadJob() {
        return new JobBuilder("dynamicLoadJob", jobRepository)
                .start(dynamicLoadStep(null, null)) // Parameters injected at runtime via @JobScope
                .build();
    }

    // ------------------------------------------------------------------
    // Step — follows AggregationDataLoadConfig.aggregationImportStep() pattern
    // ------------------------------------------------------------------

    @Bean
    @JobScope
    public Step dynamicLoadStep(@Value("#{jobParameters['tableDefId']}") String tableDefId,
                                @Value("#{jobParameters['filePath']}") String filePath) {

        // 1. Fetch the table definition from DB based on ID passed in Job Parameters
        CustomTableDefinition tableDef = tableDefRepository.findById(tableDefId)
                .orElseThrow(() -> new RuntimeException("Table definition not found for ID: " + tableDefId));

        // 2. Build the @BeforeStep-capable processor (wired with validator + repos)
        DynamicDataProcessor processor = new DynamicDataProcessor(
                tableDef, dynamicTableValidator,
                instrumentAttributeRepository, validationLogRepository);

        return new StepBuilder("dynamicLoadStep", jobRepository)
                .<FieldSet, Document>chunk(100, transactionManager)
                .reader(dynamicReader(tableDef, filePath))
                .processor(processor)
                .writer(new DynamicMongoWriter(mongoTemplate, tableDef.getTableName()))
                .listener(processor)   // @BeforeStep wiring — must be before faultTolerant()
                .faultTolerant()
                .skip(ItemValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener((StepExecutionListener) validationLoggingListener)
                .listener((ItemProcessListener) validationLoggingListener)
                .build();
    }

    // ------------------------------------------------------------------
    // Reader — unchanged from original
    // ------------------------------------------------------------------

    /**
     * Reads the CSV dynamically — column names are taken from the file header row,
     * not hardcoded.
     */
    public FlatFileItemReader<FieldSet> dynamicReader(CustomTableDefinition tableDef, String filePath) {
        String[] csvHeaders = getHeadersFromFile(filePath);
        validateHeaders(csvHeaders, tableDef);

        return new FlatFileItemReaderBuilder<FieldSet>()
                .name("dynamicReader")
                .resource(new FileSystemResource(filePath))
                .linesToSkip(1) // Header is read manually above
                .lineTokenizer(new DelimitedLineTokenizer() {{
                    setNames(csvHeaders);
                    setStrict(false);
                    setQuoteCharacter('"');
                }})
                .fieldSetMapper(new PassThroughFieldSetMapper())
                .build();
    }

    // ------------------------------------------------------------------
    // Helpers — unchanged from original
    // ------------------------------------------------------------------

    private String[] getHeadersFromFile(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            if (line == null) throw new RuntimeException("File is empty: " + filePath);
            return Arrays.stream(line.split(","))
                    .map(String::trim)
                    .map(h -> h.replace("\"", ""))
                    .toArray(String[]::new);
        } catch (IOException e) {
            throw new RuntimeException("Error reading header from file: " + filePath, e);
        }
    }

    private void validateHeaders(String[] fileHeaders, CustomTableDefinition tableDef) {
        Set<String> fileHeaderSet = Arrays.stream(fileHeaders)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        for (CustomTableColumn col : tableDef.getColumns()) {
            if (!col.getNullable() && !fileHeaderSet.contains(col.getColumnName().toLowerCase())) {
                throw new RuntimeException("Missing required column in CSV file: " + col.getColumnName());
            }
        }
    }

    /** Simple mapper that passes the FieldSet through to the processor. */
    public static class PassThroughFieldSetMapper implements FieldSetMapper<FieldSet> {
        @Override
        public FieldSet mapFieldSet(FieldSet fieldSet) {
            return fieldSet;
        }
    }
}