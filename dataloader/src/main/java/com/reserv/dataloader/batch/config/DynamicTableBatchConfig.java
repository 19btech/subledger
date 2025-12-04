package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.processor.DynamicDataProcessor;
import com.reserv.dataloader.batch.writer.DynamicMongoWriter;
import com.fyntrac.common.entity.CustomTableColumn;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import org.bson.Document;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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

    public DynamicTableBatchConfig(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   MongoTemplate mongoTemplate,
                                   CustomTableDefinitionRepository tableDefRepository) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.mongoTemplate = mongoTemplate;
        this.tableDefRepository = tableDefRepository;
    }

    @Bean
    public Job dynamicLoadJob() {
        return new JobBuilder("dynamicLoadJob", jobRepository)
                .start(dynamicLoadStep(null, null)) // Parameters injected at runtime
                .build();
    }

    @Bean
    @JobScope
    public Step dynamicLoadStep(@Value("#{jobParameters['tableDefId']}") String tableDefId,
                                @Value("#{jobParameters['filePath']}") String filePath) {

        // 1. Fetch the definition from DB based on ID passed in Job Parameters
        CustomTableDefinition tableDef = tableDefRepository.findById(tableDefId)
                .orElseThrow(() -> new RuntimeException("Table definition not found for ID: " + tableDefId));

        return new StepBuilder("dynamicLoadStep", jobRepository)
                .<FieldSet, Document>chunk(100, transactionManager)
                // We call dynamicReader manually here. The result is registered with the step.
                .reader(dynamicReader(tableDef, filePath))
                .processor(new DynamicDataProcessor(tableDef))
                .writer(new DynamicMongoWriter(mongoTemplate, tableDef.getTableName()))
                .build();
    }

    // REMOVED @Bean and @StepScope. This is now just a helper method.
    // REMOVED @Value from parameters. It receives values directly from dynamicLoadStep.
    public FlatFileItemReader<FieldSet> dynamicReader(CustomTableDefinition tableDef, String filePath) {
        // 1. Read the actual headers from the file to determine column positions dynamically
        String[] csvHeaders = getHeadersFromFile(filePath);

        // 2. Validate that the file headers contain necessary columns from tableDef
        validateHeaders(csvHeaders, tableDef);

        return new FlatFileItemReaderBuilder<FieldSet>()
                .name("dynamicReader")
                .resource(new FileSystemResource(filePath))
                .linesToSkip(1) // Skip the header line since we read it manually
                .lineTokenizer(new DelimitedLineTokenizer() {{
                    setNames(csvHeaders); // Map indices to names based on the ACTUAL file header
                    setStrict(false);
                    // Use double quote as quote character to handle quoted values correctly during read
                    setQuoteCharacter('"');
                }})
                .fieldSetMapper(new PassThroughFieldSetMapper()) // Return raw FieldSet
                .build();
    }

    /**
     * Reads the first line of the CSV to get the actual header names.
     */
    private String[] getHeadersFromFile(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            if (line == null) {
                throw new RuntimeException("File is empty: " + filePath);
            }
            // Simple split by comma. For complex CSVs (quotes, commas in values),
            // consider using a dedicated CSV library here.
            return Arrays.stream(line.split(","))
                    .map(String::trim)
                    .map(header -> header.replace("\"", "")) // Clean any surrounding quotes
                    .toArray(String[]::new);
        } catch (IOException e) {
            throw new RuntimeException("Error reading header from file: " + filePath, e);
        }
    }

    /**
     * Compares file headers with Table Definition to ensure data integrity.
     * Case-insensitive comparison.
     */
    private void validateHeaders(String[] fileHeaders, CustomTableDefinition tableDef) {
        // Convert all file headers to lower case set for efficient, case-insensitive lookup
        Set<String> fileHeaderSet = Arrays.stream(fileHeaders)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        for (CustomTableColumn col : tableDef.getColumns()) {
            // If a column is NOT nullable (required) in DB, it MUST exist in the CSV file
            // We verify by checking if the lower-case column name exists in our set
            if (!col.getNullable() && !fileHeaderSet.contains(col.getColumnName().toLowerCase())) {
                throw new RuntimeException("Missing required column in CSV file: " + col.getColumnName());
            }
        }
    }

    // Simple mapper that passes the FieldSet through to the processor
    public static class PassThroughFieldSetMapper implements FieldSetMapper<FieldSet> {
        @Override
        public FieldSet mapFieldSet(FieldSet fieldSet) {
            return fieldSet;
        }
    }
}