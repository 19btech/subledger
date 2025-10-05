package com.reserv.dataloader.batch.config;

import com.reserv.dataloader.batch.mapper.HeaderColumnNameMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
public class BatchCommonConfig {

    @Bean
    public JobExecutionDecider jobExecutionDecider() {
        // Decider to check if Job A completed successfully
        return (jobExecution, stepExecution) -> {
            // Check if jobA completed successfully and decide next step
            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                return FlowExecutionStatus.COMPLETED;  // Correct flow status
            }
            return FlowExecutionStatus.FAILED;  // If Job A fails, we stop the flow
        };
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

    protected List<String> getHeaderNames(String filePath) throws IOException {
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
}
