package com.reserv.dataloader.batch.reader;

import com.fyntrac.common.component.TransactionActivityQueue;
import com.fyntrac.common.entity.TransactionActivity;
import org.apache.pulsar.shade.javax.annotation.PostConstruct;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

import java.util.Iterator;
import java.util.List;

public class TransactionActivityItemReader implements ItemReader<TransactionActivity>, ItemStream {

    private final Iterator<TransactionActivity> iterator;
    private boolean endReached = false;

    public TransactionActivityItemReader(Iterator<TransactionActivity> iterator) {
        this.iterator = iterator;
    }

    @Override
    public TransactionActivity read() {
        if (endReached || iterator == null || !iterator.hasNext()) {
            endReached = true;
            return null;
        }
        return iterator.next();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.endReached = false;
    }

    @Override
    public void update(ExecutionContext executionContext) {}

    @Override
    public void close() {}
}
