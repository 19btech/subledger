package com.reserv.dataloader.batch.writer;

import com.fyntrac.common.entity.TransactionActivity;
import com.fyntrac.common.service.TransactionActivityService;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component("reversalWriter")
public class TransactionReversalActivityWriter implements ItemWriter<List<TransactionActivity>> {

        private final TransactionActivityService activityService;

        @Autowired
        public TransactionReversalActivityWriter(TransactionActivityService activityService) {
            this.activityService = activityService;
        }

        @Override
        public void write(Chunk<? extends List<TransactionActivity>> chunk) {
            Set<TransactionActivity> flatList = new HashSet<>();
            for (List<TransactionActivity> list : chunk) {
                flatList.addAll(list);
            }

            System.out.println("WRITER: Writing " + flatList.size() + " activities");
            activityService.save(flatList);
        }
    }

