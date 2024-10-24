package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.Aggregation;
import org.springframework.batch.item.ItemProcessor;

public class AggregateItemProcessor implements ItemProcessor<Aggregation,Aggregation> {
    @Override
    public Aggregation process(Aggregation item) throws Exception {
        final Aggregation agregation = new Aggregation();
        agregation.setTransactionName(item.getTransactionName());
        agregation.setMetricName(item.getMetricName());

        return agregation;
    }

}
