package com.reserv.dataloader.batch.reader;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;

import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Reads an aggregation pipeline's output through a server cursor, one batch at a time.
 *
 * <p>Replaces the {@code mongoTemplate.aggregate(...).iterator()} pattern, where
 * {@code AggregationResults} materializes the ENTIRE result list in heap before the first item is
 * read — for the LTD post-aggregation jobs that list is every (metric, instrument[, attribute])
 * combination in the tenant, which grows with the instrument universe and OOM-killed the pod.
 * {@code allowDiskUse} likewise keeps the server-side {@code $group} from failing once its
 * per-stage memory limit is reached.
 *
 * <p>The MongoTemplate is resolved lazily (at open) because the tenant-scoped template must be
 * looked up on the step's thread, not when the step-scoped bean is created.
 */
public class StreamingAggregationItemReader<T> implements ItemStreamReader<T> {

    private final Supplier<MongoTemplate> mongoTemplate;
    private final Aggregation aggregation;
    private final String collectionName;
    private final Class<T> outputType;

    private Stream<T> stream;
    private Iterator<T> iterator;

    public StreamingAggregationItemReader(Supplier<MongoTemplate> mongoTemplate,
                                          Aggregation aggregation,
                                          String collectionName,
                                          Class<T> outputType,
                                          int cursorBatchSize) {
        this.mongoTemplate = mongoTemplate;
        this.aggregation = aggregation.withOptions(AggregationOptions.builder()
                .allowDiskUse(true)
                .cursorBatchSize(Math.max(1, cursorBatchSize))
                .build());
        this.collectionName = collectionName;
        this.outputType = outputType;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (stream == null) {
            stream = mongoTemplate.get().aggregateStream(aggregation, collectionName, outputType);
            iterator = stream.iterator();
        }
    }

    @Override
    public T read() {
        if (iterator == null) {
            open(new ExecutionContext());
        }
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // Restart from the beginning is the only supported mode: the pipeline output has no
        // stable key to resume from, and the jobs that use this reader are re-run from scratch.
    }

    @Override
    public void close() throws ItemStreamException {
        if (stream != null) {
            stream.close();
            stream = null;
            iterator = null;
        }
    }
}
