package com.reserv.dataloader.batch.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

public class FlatteningItemWriter<T> implements ItemWriter<List<T>> {

    private final ItemWriter<T> delegate;

    public FlatteningItemWriter(ItemWriter<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(Chunk<? extends List<T>> items) throws Exception {
        List<T> flattened = new ArrayList<>();
        for (List<T> list : items) {
            flattened.addAll(list);
        }
        delegate.write((Chunk<? extends T>) Chunk.of(flattened));
    }
}